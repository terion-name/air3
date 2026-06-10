package ingestsmux

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
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/pending"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/xtaci/smux"
)

func TestClientServerStreamsBodyAndMetadata(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
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

func TestSequentialSenderSendsUseOneTCPConnection(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()

	for _, id := range []string{"req-seq-1", "req-seq-2"} {
		req, target := fixture.register(id, nil)
		if err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader(id), BodyLength: int64(len(id))}); err != nil {
			t.Fatalf("Send(%s) error = %v", id, err)
		}
		if got := target.snapshot().body; got != id {
			t.Fatalf("body for %s = %q", id, got)
		}
	}
	if got := fixture.acceptCount(); got != 1 {
		t.Fatalf("accepted TCP connections = %d, want 1", got)
	}
}

func TestConcurrentSenderSendsUseOneTCPConnection(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		req, target := fixture.register("req-concurrent-"+string(rune('a'+i)), nil)
		body := "body-" + req.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader(body), BodyLength: int64(len(body))}); err != nil {
				errs <- err
				return
			}
			if got := target.snapshot().body; got != body {
				errs <- errors.New("body mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Send() error = %v", err)
		}
	}
	if got := fixture.acceptCount(); got != 1 {
		t.Fatalf("accepted TCP connections = %d, want 1", got)
	}
}

func TestSessionPoolUsesMultipleTCPConnectionsForConcurrentBlockedSends(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender, err := NewSender("tcp", fixture.address, fixture.clientTLS("connector.local"), WithSessionPoolSize(2))
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}
	defer sender.Close()

	const n = 2
	writers := make([]*blockingWriter, n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		writer := newBlockingWriter()
		writers[i] = writer
		defer closeOnce(writer.release)
		req, target := fixture.register("req-pooled-blocked-"+string(rune('a'+i)), &fakeIngestTarget{writer: writer})
		body := "body-" + req.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader(body), BodyLength: int64(len(body))}); err != nil {
				errs <- err
				return
			}
			if got := target.snapshot().body; got != "" {
				errs <- fmt.Errorf("target buffered body = %q, want writer-owned body", got)
			}
		}()
	}

	for _, writer := range writers {
		select {
		case <-writer.entered:
		case <-time.After(time.Second):
			t.Fatal("pooled send did not reach target writer")
		}
	}
	if got := fixture.acceptCount(); got != 2 {
		t.Fatalf("accepted TCP connections = %d, want 2", got)
	}

	for _, writer := range writers {
		closeOnce(writer.release)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("pooled Send() error = %v", err)
		}
	}
	for i, writer := range writers {
		want := "body-req-pooled-blocked-" + string(rune('a'+i))
		if got := writer.body(); got != want {
			t.Fatalf("writer %d body = %q, want %q", i, got, want)
		}
	}
}

func TestSessionPoolCloseClosesAllSessions(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender, err := NewSender("tcp", fixture.address, fixture.clientTLS("connector.local"), WithSessionPoolSize(2))
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}
	defer sender.Close()

	for _, id := range []string{"req-close-pool-1", "req-close-pool-2"} {
		req, target := fixture.register(id, nil)
		if err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader(id), BodyLength: int64(len(id))}); err != nil {
			t.Fatalf("Send(%s) error = %v", id, err)
		}
		if got := target.snapshot().body; got != id {
			t.Fatalf("body for %s = %q", id, got)
		}
	}
	if got := fixture.acceptCount(); got != 2 {
		t.Fatalf("accepted TCP connections = %d, want 2", got)
	}

	sessions := make([]*smux.Session, 0, len(sender.slots))
	for i := range sender.slots {
		sender.slots[i].mu.Lock()
		if sender.slots[i].session == nil {
			sender.slots[i].mu.Unlock()
			t.Fatalf("slot %d session is nil", i)
		}
		sessions = append(sessions, sender.slots[i].session)
		sender.slots[i].mu.Unlock()
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	for i, session := range sessions {
		if !session.IsClosed() {
			t.Fatalf("captured session %d is not closed", i)
		}
		sender.slots[i].mu.Lock()
		if sender.slots[i].session != nil {
			sender.slots[i].mu.Unlock()
			t.Fatalf("slot %d session not cleared", i)
		}
		sender.slots[i].mu.Unlock()
	}
}

func TestWrongTokenDoesNotClaimThenCorrectTokenSucceeds(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	req, target := fixture.register("req-token", nil)
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()

	err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: "wrong-token", Body: strings.NewReader("bad"), BodyLength: 3})
	if err == nil {
		t.Fatal("wrong-token Send() error = nil, want rejection")
	}
	if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("after wrong token target = %+v, want no claim", snap)
	}

	err = sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("good"), BodyLength: 4})
	if err != nil {
		t.Fatalf("correct-token Send() error = %v", err)
	}
	if got := target.snapshot().body; got != "good" {
		t.Fatalf("body = %q, want good", got)
	}
}

func TestReplayFailsWhileFirstIngestIsActive(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	writer := newBlockingWriter()
	req, target := fixture.register("req-replay", &fakeIngestTarget{writer: writer})
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("first"), BodyLength: 5})
	}()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("first ingest did not reach target writer")
	}

	err := sender.Send(context.Background(), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("replay"), BodyLength: 6})
	if err == nil {
		t.Fatal("replay Send() error = nil, want rejection")
	}
	if snap := target.snapshot(); snap.startCount != 1 {
		t.Fatalf("after replay target starts = %d, want 1", snap.startCount)
	}

	close(writer.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if got := writer.body(); got != "first" {
		t.Fatalf("first body = %q, want first", got)
	}
}

func TestUnauthorizedClientCertFailsBeforeClaim(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.allowed")
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

func TestInvalidAndOversizedHeadersFailBeforeClaimAndRequestIsReusable(t *testing.T) {
	tests := map[string]struct {
		maxHeaderBytes int
		buildFields    func(pending.Request) []msgpackField
	}{
		"invalid metadata": {buildFields: func(req pending.Request) []msgpackField {
			return rawSmuxHeaderFields(req, metadataField(stringField("etag", "ok\r\nbad")))
		}},
		"invalid body len": {buildFields: func(req pending.Request) []msgpackField {
			return []msgpackField{stringField("request_id", req.ID), stringField("ingest_token", req.IngestToken), intField("body_length", -2), metadataField()}
		}},
		"oversized raw header": {maxHeaderBytes: 32, buildFields: func(req pending.Request) []msgpackField {
			return rawSmuxHeaderFields(req, metadataField())
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newSmuxFixture(t, "connector.local", func(opts *ServerOptions) { opts.MaxHeaderBytes = tc.maxHeaderBytes })
			req, target := fixture.register("req-invalid", nil)
			payload := mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, tc.buildFields(req)...) })
			if tc.maxHeaderBytes > 0 && len(payload) <= tc.maxHeaderBytes {
				t.Fatalf("test payload length = %d, want > %d", len(payload), tc.maxHeaderBytes)
			}
			if err := fixture.sendRaw(payload, ""); err == nil {
				t.Fatal("sendRaw() error = nil, want rejection")
			}
			if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
				t.Fatalf("target = %+v, want no claim", snap)
			}
			if tc.maxHeaderBytes == 0 {
				if err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("good"), BodyLength: 4}); err != nil {
					t.Fatalf("correct send after invalid header error = %v", err)
				}
				if got := target.snapshot().body; got != "good" {
					t.Fatalf("body after reusable send = %q, want good", got)
				}
			}
		})
	}
}

func TestKnownLengthShortBodyFinishesWithErrorAndNoSuccessAck(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
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

func TestUnknownLengthPublicSenderSucceedsAndSessionRemainsUsable(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()
	req1, target1 := fixture.register("req-unknown", nil)

	err := sender.Send(context.Background(), ClientRequest{RequestID: req1.ID, IngestToken: req1.IngestToken, Body: strings.NewReader("unknown-body"), BodyLength: UnknownBodyLength})
	if err != nil {
		t.Fatalf("unknown-length Send() error = %v", err)
	}
	if snap := target1.snapshot(); snap.body != "unknown-body" || snap.finishArg != nil {
		t.Fatalf("target1 snapshot = %+v, want unknown body success", snap)
	}

	req2, target2 := fixture.register("req-after-unknown", nil)
	if err := sender.Send(context.Background(), ClientRequest{RequestID: req2.ID, IngestToken: req2.IngestToken, Body: strings.NewReader("next"), BodyLength: 4}); err != nil {
		t.Fatalf("Send() after unknown-length error = %v", err)
	}
	if got := target2.snapshot().body; got != "next" {
		t.Fatalf("second body = %q, want next", got)
	}
	if got := fixture.acceptCount(); got != 1 {
		t.Fatalf("accepted TCP connections = %d, want 1", got)
	}
}

func TestTargetFailuresSurfaceAndCleanup(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		fixture := newSmuxFixture(t, "connector.local")
		startErr := errors.New("start failed")
		req, target := fixture.register("req-start-fail", &fakeIngestTarget{startErr: startErr})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want start failure")
		}
		snap := target.snapshot()
		if snap.startCount != 1 || snap.cancelCount != 1 {
			t.Fatalf("target = %+v, want Start and Cancel", snap)
		}
	})

	t.Run("write", func(t *testing.T) {
		fixture := newSmuxFixture(t, "connector.local")
		writeErr := errors.New("write failed")
		req, target := fixture.register("req-write-fail", &fakeIngestTarget{writer: errorWriter{err: writeErr}})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want write failure")
		}
		snap := target.snapshot()
		if snap.finishCount != 1 || !errors.Is(snap.finishArg, writeErr) {
			t.Fatalf("target = %+v, want Finish(writeErr)", snap)
		}
	})

	t.Run("finish", func(t *testing.T) {
		fixture := newSmuxFixture(t, "connector.local")
		finishErr := errors.New("finish failed")
		req, _ := fixture.register("req-finish-fail", &fakeIngestTarget{finishReturn: finishErr})
		err := DialAndSend(context.Background(), "tcp", fixture.address, fixture.clientTLS("connector.local"), ClientRequest{RequestID: req.ID, IngestToken: req.IngestToken, Body: strings.NewReader("body"), BodyLength: 4})
		if err == nil {
			t.Fatal("DialAndSend() error = nil, want finish failure")
		}
		if _, err := fixture.registry.StartIngest(req.ID, req.IngestToken, pending.Metadata{}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("StartIngest() after finish failure = %v, want ErrNotFound", err)
		}
	})
}

func TestStaleSessionRetryReconnectsBeforeWritingRequest(t *testing.T) {
	fixture := newSmuxFixture(t, "connector.local")
	sender := fixture.sender(t, "connector.local")
	defer sender.Close()
	req1, target1 := fixture.register("req-stale-1", nil)
	if err := sender.Send(context.Background(), ClientRequest{RequestID: req1.ID, IngestToken: req1.IngestToken, Body: strings.NewReader("one"), BodyLength: 3}); err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if got := target1.snapshot().body; got != "one" {
		t.Fatalf("first body = %q, want one", got)
	}

	sender.slots[0].mu.Lock()
	if sender.slots[0].session == nil {
		sender.slots[0].mu.Unlock()
		t.Fatal("sender session is nil after first send")
	}
	_ = sender.slots[0].session.Close()
	sender.slots[0].mu.Unlock()

	req2, target2 := fixture.register("req-stale-2", nil)
	if err := sender.Send(context.Background(), ClientRequest{RequestID: req2.ID, IngestToken: req2.IngestToken, Body: strings.NewReader("two"), BodyLength: 3}); err != nil {
		t.Fatalf("second Send() after stale session error = %v", err)
	}
	if got := target2.snapshot().body; got != "two" {
		t.Fatalf("second body = %q, want two", got)
	}
	if got := fixture.acceptCount(); got != 2 {
		t.Fatalf("accepted TCP connections = %d, want 2", got)
	}
}

func TestOptionsCloneCloseAndProtocolDelegation(t *testing.T) {
	badConfig := smux.DefaultConfig()
	badConfig.Version = 99
	if _, err := NewSender("tcp", "127.0.0.1:1", &tls.Config{}, WithSmuxConfig(badConfig)); err == nil {
		t.Fatal("NewSender() with invalid smux config error = nil")
	}
	if _, err := NewSender("tcp", "127.0.0.1:1", &tls.Config{}, WithSessionPoolSize(0)); err == nil {
		t.Fatal("NewSender() with invalid session pool size error = nil")
	}

	fixture := newSmuxFixture(t, "connector.local")
	config := smux.DefaultConfig()
	sender, err := NewSender("tcp", fixture.address, fixture.clientTLS("connector.local"), WithSmuxConfig(config))
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}
	config.Version = 99
	if sender.smuxConfig.Version == 99 {
		t.Fatal("sender smux config was not cloned")
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	var buf bytes.Buffer
	h := Header{RequestID: "req", IngestToken: "token", BodyLength: 0, Metadata: pending.Metadata{StatusCode: 200}}
	if err := EncodeHeader(&buf, h); err != nil {
		t.Fatalf("EncodeHeader() error = %v", err)
	}
	decoded, err := DecodeHeader(&buf, DefaultMaxHeaderBytes)
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}
	if decoded.RequestID != h.RequestID || decoded.IngestToken != h.IngestToken || decoded.Metadata.StatusCode != 200 {
		t.Fatalf("decoded header = %#v", decoded)
	}
}

type smuxFixture struct {
	t          *testing.T
	registry   *pending.Registry
	address    string
	cancel     context.CancelFunc
	caCert     *x509.Certificate
	caKey      *rsa.PrivateKey
	serverDone chan error
	ln         *countingListener
}

func newSmuxFixture(t *testing.T, allowedIdentity string, options ...func(*ServerOptions)) *smuxFixture {
	t.Helper()
	caKey, caCert := createTestCA(t)
	serverCert := leafTLSCertificate(t, caKey, caCert, "edge.local")
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	registry := pending.NewRegistry(pending.Options{Now: func() time.Time { return fixedNow }})
	opts := ServerOptions{Registry: registry, TLSConfig: serverTLS, AllowedConnectorIdentities: []string{allowedIdentity}}
	for _, option := range options {
		option(&opts)
	}
	server, err := NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	baseLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ln := &countingListener{Listener: baseLn}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(ctx, ln) }()
	fixture := &smuxFixture{t: t, registry: registry, address: ln.Addr().String(), cancel: cancel, caCert: caCert, caKey: caKey, serverDone: serverDone, ln: ln}
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

func (f *smuxFixture) register(id string, target *fakeIngestTarget) (pending.Request, *fakeIngestTarget) {
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

func (f *smuxFixture) clientTLS(identity string) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(f.caCert)
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "edge.local", Certificates: []tls.Certificate{leafTLSCertificate(f.t, f.caKey, f.caCert, identity)}}
}

func (f *smuxFixture) sender(t *testing.T, identity string) *Sender {
	t.Helper()
	sender, err := NewSender("tcp", f.address, f.clientTLS(identity))
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}
	return sender
}

func (f *smuxFixture) acceptCount() int {
	return f.ln.count()
}

func (f *smuxFixture) sendRaw(payload []byte, body string) error {
	conn, err := tls.Dial("tcp", f.address, f.clientTLS("connector.local"))
	if err != nil {
		return err
	}
	defer conn.Close()
	session, err := smux.Client(conn, nil)
	if err != nil {
		return err
	}
	defer session.Close()
	stream, err := session.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if _, err := stream.Write(frame(payload)); err != nil {
		return err
	}
	if body != "" {
		if _, err := stream.Write([]byte(body)); err != nil {
			return err
		}
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	var ack [1]byte
	_, err = io.ReadFull(stream, ack[:])
	return err
}

type countingListener struct {
	net.Listener
	mu sync.Mutex
	n  int
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.n++
	l.mu.Unlock()
	return conn, nil
}

func (l *countingListener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

var fixedNow = time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

func rawSmuxHeaderFields(req pending.Request, metadata msgpackField) []msgpackField {
	return []msgpackField{stringField("request_id", req.ID), stringField("ingest_token", req.IngestToken), intField("body_length", 0), metadata}
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
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true, IsCA: true}
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
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
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

type msgpackField struct {
	key   string
	write func(*testing.T, *msgpack.Encoder)
}

func stringField(key, value string) msgpackField {
	return msgpackField{key: key, write: func(t *testing.T, enc *msgpack.Encoder) { t.Helper(); must(t, enc.EncodeString(value)) }}
}

func intField(key string, value int64) msgpackField {
	return msgpackField{key: key, write: func(t *testing.T, enc *msgpack.Encoder) { t.Helper(); must(t, enc.EncodeInt(value)) }}
}

func metadataField(overrides ...msgpackField) msgpackField {
	return msgpackField{key: "metadata", write: func(t *testing.T, enc *msgpack.Encoder) {
		t.Helper()
		fields := []msgpackField{
			intField("status_code", 200),
			stringField("content_type", ""),
			stringField("content_length", ""),
			stringField("content_range", ""),
			stringField("etag", ""),
			stringField("last_modified", ""),
			stringField("accept_ranges", ""),
		}
		for _, override := range overrides {
			for i := range fields {
				if fields[i].key == override.key {
					fields[i] = override
				}
			}
		}
		must(t, enc.EncodeMapLen(len(fields)))
		for _, field := range fields {
			must(t, enc.EncodeString(field.key))
			field.write(t, enc)
		}
	}}
}

func mustMsgpack(t *testing.T, write func(*msgpack.Encoder)) []byte {
	t.Helper()
	var buf bytes.Buffer
	write(msgpack.NewEncoder(&buf))
	return buf.Bytes()
}

func encodeHeaderMap(t *testing.T, enc *msgpack.Encoder, fields ...msgpackField) {
	t.Helper()
	must(t, enc.EncodeMapLen(len(fields)))
	for _, field := range fields {
		must(t, enc.EncodeString(field.key))
		field.write(t, enc)
	}
}

func frame(payload []byte) []byte {
	framed := make([]byte, prefixBytes+len(payload))
	copy(framed[:4], protocolMagic[:])
	framed[4] = protocolVersion
	binary.BigEndian.PutUint32(framed[5:prefixBytes], uint32(len(payload)))
	copy(framed[prefixBytes:], payload)
	return framed
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
