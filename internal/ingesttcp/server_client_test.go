package ingesttcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/pending"
	"github.com/vmihailenco/msgpack/v5"
)

func TestClientServerStreamsBodyAndMetadata(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	req, target := fixture.register("req-success", nil)
	metadata := pending.Metadata{StatusCode: 206, ContentType: " text/plain ", ContentLength: "11", ETag: `"abc"`}

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Metadata: metadata, Body: strings.NewReader("object-body"), BodyLength: 11,
	})
	if err != nil {
		t.Fatalf("DialAndSend() error = %v", err)
	}

	snap := target.snapshot()
	if snap.body != "object-body" {
		t.Fatalf("body = %q, want object-body", snap.body)
	}
	if snap.metadata.ContentType != "text/plain" || snap.metadata.ContentLength != "11" || snap.metadata.StatusCode != 206 || snap.metadata.ETag != `"abc"` {
		t.Fatalf("metadata = %#v", snap.metadata)
	}
	if snap.startCount != 1 || snap.finishCount != 1 || snap.cancelCount != 0 || snap.finishArg != nil {
		t.Fatalf("target snapshot = %+v, want one successful finish", snap)
	}
}

func TestWrongTokenDoesNotClaimThenCorrectTokenSucceeds(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	req, target := fixture.register("req-token", nil)

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: "wrong-token", Body: strings.NewReader("bad"), BodyLength: 3,
	})
	if err == nil {
		t.Fatal("wrong-token DialAndSend() error = nil, want rejection")
	}
	if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("after wrong token target = %+v, want no claim", snap)
	}

	err = DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("good"), BodyLength: 4,
	})
	if err != nil {
		t.Fatalf("correct-token DialAndSend() error = %v", err)
	}
	if got := target.snapshot().body; got != "good" {
		t.Fatalf("body = %q, want good", got)
	}
}

func TestReplayFailsWhileFirstIngestIsActive(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	writer := newBlockingWriter()
	req, target := fixture.register("req-replay", &fakeIngestTarget{writer: writer})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
			RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("first"), BodyLength: 5,
		})
	}()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("first ingest did not reach target writer")
	}

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("replay"), BodyLength: 6,
	})
	if err == nil {
		t.Fatal("replay DialAndSend() error = nil, want rejection")
	}
	if snap := target.snapshot(); snap.startCount != 1 {
		t.Fatalf("after replay target starts = %d, want 1", snap.startCount)
	}

	close(writer.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first DialAndSend() error = %v", err)
	}
	if got := writer.body(); got != "first" {
		t.Fatalf("first body = %q, want first", got)
	}
}

func TestUnauthorizedClientCertFailsBeforeClaim(t *testing.T) {
	fixture := newTCPFixture(t, "connector.allowed")
	req, target := fixture.register("req-unauthorized", nil)

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.denied"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4,
	})
	if err == nil {
		t.Fatal("DialAndSend() error = nil, want unauthorized rejection")
	}
	if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("target = %+v, want no claim", snap)
	}
}

func TestInvalidHeadersFailBeforeClaim(t *testing.T) {
	tests := map[string]func(pending.Request) []msgpackField{
		"metadata crlf": func(req pending.Request) []msgpackField {
			return rawTCPHeaderFields(req, metadataField(stringField("etag", "ok\r\nbad")))
		},
		"invalid status": func(req pending.Request) []msgpackField {
			return rawTCPHeaderFields(req, metadataField(intField("status_code", 99)))
		},
		"invalid content": func(req pending.Request) []msgpackField {
			return rawTCPHeaderFields(req, metadataField(stringField("content_length", "abc")))
		},
		"invalid body len": func(req pending.Request) []msgpackField {
			return []msgpackField{
				stringField("request_id", req.ID),
				stringField("ingest_token", req.IngestToken),
				intField("body_length", -2),
				metadataField(),
			}
		},
		"oversized metadata": func(req pending.Request) []msgpackField {
			return rawTCPHeaderFields(req, metadataField(stringField("content_type", strings.Repeat("a", 8*1024+1))))
		},
	}
	for name, buildFields := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newTCPFixture(t, "connector.local")
			req, target := fixture.register("req-invalid", nil)
			payload := mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, buildFields(req)...) })
			err := fixture.sendRaw(payload, "")
			if err == nil {
				t.Fatal("sendRaw() error = nil, want rejection")
			}
			if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
				t.Fatalf("target = %+v, want no claim", snap)
			}
		})
	}
}

func TestOversizedHeaderFailsBeforeClaim(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local", func(opts *ServerOptions) { opts.MaxHeaderBytes = 32 })
	req, target := fixture.register("req-oversized", nil)
	payload := mustMsgpack(t, func(enc *msgpack.Encoder) {
		encodeHeaderMap(t, enc, rawTCPHeaderFields(req, metadataField())...)
	})
	if len(payload) <= 32 {
		t.Fatalf("test payload length = %d, want > 32", len(payload))
	}
	if err := fixture.sendRaw(payload, ""); err == nil {
		t.Fatal("sendRaw() error = nil, want oversized rejection")
	}
	if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("target = %+v, want no claim", snap)
	}
}

func TestKnownLengthShortBodyFinishesWithErrorAndNoSuccessAck(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	req, target := fixture.register("req-short", nil)

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("short"), BodyLength: 10,
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("DialAndSend() error = %v, want ErrUnexpectedEOF", err)
	}
	select {
	case <-target.finished:
	case <-time.After(time.Second):
		t.Fatal("target Finish was not called")
	}
	snap := target.snapshot()
	if snap.body != "short" || snap.startCount != 1 || snap.finishCount != 1 || snap.finishArg == nil {
		t.Fatalf("target snapshot = %+v, want short body and finish error", snap)
	}
}

func TestUnknownLengthBodySucceedsWithTLSCloseWrite(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	req, target := fixture.register("req-unknown", nil)

	err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
		RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("unknown-body"), BodyLength: UnknownBodyLength,
	})
	if err != nil {
		t.Fatalf("DialAndSend() error = %v", err)
	}
	snap := target.snapshot()
	if snap.body != "unknown-body" || snap.finishArg != nil {
		t.Fatalf("target snapshot = %+v, want unknown body success", snap)
	}
}

func TestUnknownLengthRequiresCloseWrite(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	done := make(chan error, 1)
	go func() {
		done <- Send(context.Background(), noCloseWriteConn{Conn: clientConn}, ClientRequest{
			RequestID: "req", IngestToken: "tok", Body: strings.NewReader("body"), BodyLength: UnknownBodyLength,
		})
	}()
	if _, err := DecodeHeader(serverConn, DefaultMaxHeaderBytes); err != nil {
		t.Fatalf("DecodeHeader() from client error = %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(serverConn, buf); err != nil {
		t.Fatalf("read body error = %v", err)
	}
	if err := <-done; err == nil || !strings.Contains(err.Error(), "CloseWrite") {
		t.Fatalf("Send() error = %v, want CloseWrite error", err)
	}
}

func TestStreamingBeginsBeforeFullBodyIsAvailable(t *testing.T) {
	fixture := newTCPFixture(t, "connector.local")
	req, target := fixture.register("req-streaming", nil)
	bodyReader, bodyWriter := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
			RequestID: req.ID, IngestToken: req.IngestToken, Body: bodyReader, BodyLength: UnknownBodyLength,
		})
	}()

	if _, err := bodyWriter.Write([]byte("first-")); err != nil {
		t.Fatalf("write first chunk error = %v", err)
	}
	select {
	case <-target.wrote:
	case <-time.After(time.Second):
		t.Fatal("target did not receive first chunk before body completed")
	}
	if got := target.snapshot().body; got != "first-" {
		t.Fatalf("intermediate target body = %q, want first-", got)
	}
	if _, err := bodyWriter.Write([]byte("second")); err != nil {
		t.Fatalf("write second chunk error = %v", err)
	}
	if err := bodyWriter.Close(); err != nil {
		t.Fatalf("bodyWriter.Close() error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("DialAndSend() error = %v", err)
	}
	if got := target.snapshot().body; got != "first-second" {
		t.Fatalf("final target body = %q, want first-second", got)
	}
}

func TestTargetFailuresSurfaceAndCleanup(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		fixture := newTCPFixture(t, "connector.local")
		startErr := errors.New("start failed")
		req, target := fixture.register("req-start-fail", &fakeIngestTarget{startErr: startErr})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
			RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4,
		})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want start failure")
		}
		snap := target.snapshot()
		if snap.startCount != 1 || snap.cancelCount != 1 {
			t.Fatalf("target = %+v, want Start and Cancel", snap)
		}
	})

	t.Run("write", func(t *testing.T) {
		fixture := newTCPFixture(t, "connector.local")
		writeErr := errors.New("write failed")
		req, target := fixture.register("req-write-fail", &fakeIngestTarget{writer: errorWriter{err: writeErr}})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
			RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4,
		})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want write failure")
		}
		snap := target.snapshot()
		if snap.finishCount != 1 || !errors.Is(snap.finishArg, writeErr) {
			t.Fatalf("target = %+v, want Finish(writeErr)", snap)
		}
	})

	t.Run("finish", func(t *testing.T) {
		fixture := newTCPFixture(t, "connector.local")
		finishErr := errors.New("finish failed")
		req, _ := fixture.register("req-finish-fail", &fakeIngestTarget{finishReturn: finishErr})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{
			RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4,
		})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want finish failure")
		}
		if _, err := fixture.registry.StartIngest(req.ID, req.IngestToken, pending.Metadata{}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("StartIngest() after finish failure = %v, want ErrNotFound", err)
		}
	})
}

type tcpFixture struct {
	t           *testing.T
	registry    *pending.Registry
	address     string
	cancel      context.CancelFunc
	caCert      *x509.Certificate
	caKey       *rsa.PrivateKey
	serverDone  chan error
	serverNames []string
}

func newTCPFixture(t *testing.T, allowedIdentity string, options ...func(*ServerOptions)) *tcpFixture {
	t.Helper()
	caKey, caCert := createTestCA(t)
	serverCert := leafTLSCertificate(t, caKey, caCert, "edge.local")
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	serverTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	registry := pending.NewRegistry(pending.Options{Now: func() time.Time { return fixedNow }})
	opts := ServerOptions{Registry: registry, TLSConfig: serverTLS, AllowedConnectorIdentities: []string{allowedIdentity}}
	for _, option := range options {
		option(&opts)
	}
	server, err := NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(ctx, ln) }()
	fixture := &tcpFixture{t: t, registry: registry, address: ln.Addr().String(), cancel: cancel, caCert: caCert, caKey: caKey, serverDone: serverDone}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Server.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Server.Serve() did not stop")
		}
	})
	return fixture
}

var fixedNow = time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

func (f *tcpFixture) register(id string, target *fakeIngestTarget) (pending.Request, *fakeIngestTarget) {
	f.t.Helper()
	if target == nil {
		target = newFakeIngestTarget()
	} else {
		target.init()
	}
	req := pending.Request{ID: id, Deadline: fixedNow.Add(time.Minute), IngestToken: "token-" + id, Method: "GET", Bucket: "demo-bucket", Key: "objects/" + id + ".txt"}
	if err := f.registry.Register(req, target); err != nil {
		f.t.Fatalf("Register() error = %v", err)
	}
	return req, target
}

func (f *tcpFixture) clientTLS(identity string) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(f.caCert)
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		ServerName:   "edge.local",
		Certificates: []tls.Certificate{leafTLSCertificate(f.t, f.caKey, f.caCert, identity)},
	}
}

func rawTCPHeaderFields(req pending.Request, metadata msgpackField) []msgpackField {
	return []msgpackField{
		stringField("request_id", req.ID),
		stringField("ingest_token", req.IngestToken),
		intField("body_length", 0),
		metadata,
	}
}

func (f *tcpFixture) sendRaw(payload []byte, body string) error {
	conn, err := tls.Dial("tcp", f.address, f.clientTLS("connector.local"))
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write(frame(payload)); err != nil {
		return err
	}
	if body != "" {
		if _, err := conn.Write([]byte(body)); err != nil {
			return err
		}
	}
	var ack [1]byte
	_, err = io.ReadFull(conn, ack[:])
	return err
}

type fakeIngestTarget struct {
	mu sync.Mutex

	writer       io.Writer
	startErr     error
	finishReturn error

	body        bytes.Buffer
	metadata    pending.Metadata
	startCount  int
	finishCount int
	cancelCount int
	finishArg   error
	cancelArg   error

	wrote    chan struct{}
	finished chan struct{}
}

type targetSnapshot struct {
	body        string
	metadata    pending.Metadata
	startCount  int
	finishCount int
	cancelCount int
	finishArg   error
	cancelArg   error
}

func newFakeIngestTarget() *fakeIngestTarget {
	t := &fakeIngestTarget{}
	t.init()
	return t
}

func (t *fakeIngestTarget) init() {
	if t.wrote == nil {
		t.wrote = make(chan struct{})
	}
	if t.finished == nil {
		t.finished = make(chan struct{})
	}
}

func (t *fakeIngestTarget) Start(metadata pending.Metadata) (io.Writer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startCount++
	t.metadata = metadata
	if t.startErr != nil {
		return nil, t.startErr
	}
	if t.writer != nil {
		return t.writer, nil
	}
	return targetWriter{target: t}, nil
}

func (t *fakeIngestTarget) Finish(err error) error {
	t.mu.Lock()
	t.finishCount++
	t.finishArg = err
	finishReturn := t.finishReturn
	finished := t.finished
	t.mu.Unlock()
	closeOnce(finished)
	return finishReturn
}

func (t *fakeIngestTarget) Cancel(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancelCount++
	t.cancelArg = err
}

func (t *fakeIngestTarget) snapshot() targetSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return targetSnapshot{body: t.body.String(), metadata: t.metadata, startCount: t.startCount, finishCount: t.finishCount, cancelCount: t.cancelCount, finishArg: t.finishArg, cancelArg: t.cancelArg}
}

type targetWriter struct{ target *fakeIngestTarget }

func (w targetWriter) Write(p []byte) (int, error) {
	w.target.mu.Lock()
	n, err := w.target.body.Write(p)
	wrote := w.target.wrote
	w.target.mu.Unlock()
	closeOnce(wrote)
	return n, err
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	bodyBuf bytes.Buffer
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bodyBuf.Write(p)
}

func (w *blockingWriter) body() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bodyBuf.String()
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

type noCloseWriteConn struct{ net.Conn }

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func createTestCA(t *testing.T) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return key, cert
}

func leafTLSCertificate(t *testing.T, caKey *rsa.PrivateKey, caCert *x509.Certificate, name string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}
}

func TestPrefixRejectsBadLengthWithoutAllocating(t *testing.T) {
	var prefix [prefixBytes]byte
	copy(prefix[:4], protocolMagic[:])
	prefix[4] = protocolVersion
	binary.BigEndian.PutUint32(prefix[5:], uint32(DefaultMaxHeaderBytes+1))
	if _, err := DecodeHeader(bytes.NewReader(prefix[:]), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want oversized error")
	}
}
