package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/signing"
	"github.com/terion-name/air3/internal/tickets"
)

type fakePublisher struct {
	mu      sync.Mutex
	tickets []tickets.Ticket
	err     error
	on      func(tickets.Ticket)
}

func (p *fakePublisher) PublishTicket(ctx context.Context, t tickets.Ticket) error {
	p.mu.Lock()
	p.tickets = append(p.tickets, t)
	p.mu.Unlock()
	if p.on != nil {
		p.on(t)
	}
	return p.err
}

func (p *fakePublisher) snapshot() []tickets.Ticket {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]tickets.Ticket(nil), p.tickets...)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tickets)
}

func testEdge(pub *fakePublisher, ttl time.Duration) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{})
	cfg := config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Disabled: true},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: ttl, StreamTimeout: time.Minute},
	}
	edge := newEdgeServer(cfg, reg, pub, nil)
	tokens := []string{"req-test", "ingest-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}
	return edge, reg
}

func TestTCPIngestListenerDisabledForHTTPTransport(t *testing.T) {
	tcpIngest, err := newTCPIngestListener(config.EdgeConfig{
		IngestTransport:     config.IngestTransportHTTP,
		IngestTCPListenAddr: "not a valid listen address",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err != nil {
		t.Fatalf("newTCPIngestListener() error = %v, want nil", err)
	}
	if tcpIngest != nil {
		t.Fatalf("newTCPIngestListener() = %#v, want nil for HTTP transport", tcpIngest)
	}
}

func TestTCPIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newTCPIngestListener(config.EdgeConfig{
		IngestTransport:     config.IngestTransportTCP,
		IngestTCPListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newTCPIngestListener() error = %v, want TLS config error", err)
	}
}

func TestTCPIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	tcpIngest, err := newTCPIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportTCP,
		IngestTCPListenAddr:   "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newTCPIngestListener() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- tcpIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = tcpIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("tcpIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("tcpIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingesttcp.DialAndSend(context.Background(), "tcp", tcpIngest.listener.Addr().String(), clientTLS, ingesttcp.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "8"},
				Body:        strings.NewReader("tcp-body"),
				BodyLength:  8,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:           "https://edge.internal/_ingest",
		IngestTransport:     config.IngestTransportTCP,
		IngestTCPListenAddr: tcpIngest.listener.Addr().String(),
		AllowedBuckets:      []string{"demo-bucket"},
		Signing:             config.SigningConfig{Disabled: true},
		Timeouts:            config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, pub, nil)
	tokens := []string{"req-tcp", "ingest-tcp-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		select {
		case err := <-uploadDone:
			t.Fatalf("status = %d, want %d; upload error = %v", got, http.StatusOK, err)
		default:
			t.Fatalf("status = %d, want %d", got, http.StatusOK)
		}
	}
	if body := resp.Body.String(); body != "tcp-body" {
		t.Fatalf("body = %q, want tcp-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("TCP ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TCP ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-tcp" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, tcpIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains TCP listener address", published[0].IngestURL)
	}
}

func TestBadSignatureAndDisallowedPathFailBeforePublish(t *testing.T) {
	pub := &fakePublisher{}
	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, pub, nil)

	badSigReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt?expires=9999999999&sig=bad", nil)
	badSigResp := httptest.NewRecorder()
	edge.ServeHTTP(badSigResp, badSigReq)
	if got := badSigResp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("bad signature status = %d, want %d", got, http.StatusForbidden)
	}

	disallowedReq := httptest.NewRequest(http.MethodGet, "/other-bucket/file.txt", nil)
	disallowedResp := httptest.NewRecorder()
	edge.ServeHTTP(disallowedResp, disallowedReq)
	if got := disallowedResp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("disallowed status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestSignedURLAcceptedAndPublishesTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	reg := pending.NewRegistry(pending.Options{})
	now := time.Now()
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, pub, nil)
	edge.now = func() time.Time { return now }
	edge.newToken = func() (string, error) { return "signed-token", nil }

	signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Expires: now.Add(time.Minute), Secret: "secret"})
	if err != nil {
		t.Fatalf("SignURL() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, signed, nil)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)
	if got := pub.count(); got != 1 {
		t.Fatalf("published %d tickets, want 1", got)
	}
}

func TestRangeRequestValidationAndTicketPropagation(t *testing.T) {
	t.Run("valid unsigned range is included in ticket", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		edge, _ := testEdge(pub, time.Second)
		req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil)
		req.Header.Set("Range", "bytes=0-4")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		tickets := pub.snapshot()
		if len(tickets) != 1 || tickets[0].Range != "bytes=0-4" {
			t.Fatalf("published tickets = %#v, want one ticket with range", tickets)
		}
	})

	t.Run("invalid range is rejected before publish", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, _ := testEdge(pub, time.Second)
		req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil)
		req.Header.Set("Range", "bytes=10-1")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", got, http.StatusBadRequest)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})

	t.Run("signed range query is included in ticket", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		now := time.Now()
		edge := newEdgeServer(config.EdgeConfig{
			IngestURL:      "https://edge.internal/_ingest",
			AllowedBuckets: []string{"demo-bucket"},
			Signing:        config.SigningConfig{Secret: "secret"},
			Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
		}, pending.NewRegistry(pending.Options{}), pub, nil)
		edge.now = func() time.Time { return now }
		edge.newToken = func() (string, error) { return "signed-range-token", nil }

		signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Range: "bytes=0-4", Expires: now.Add(time.Minute), Secret: "secret"})
		if err != nil {
			t.Fatalf("SignURL() error = %v", err)
		}
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, signed, nil))

		tickets := pub.snapshot()
		if len(tickets) != 1 || tickets[0].Range != "bytes=0-4" {
			t.Fatalf("published tickets = %#v, want one ticket with signed range", tickets)
		}
	})

	t.Run("signed range must be part of signature", func(t *testing.T) {
		pub := &fakePublisher{}
		now := time.Now()
		edge := newEdgeServer(config.EdgeConfig{
			IngestURL:      "https://edge.internal/_ingest",
			AllowedBuckets: []string{"demo-bucket"},
			Signing:        config.SigningConfig{Secret: "secret"},
			Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
		}, pending.NewRegistry(pending.Options{}), pub, nil)
		edge.now = func() time.Time { return now }

		signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Expires: now.Add(time.Minute), Secret: "secret"})
		if err != nil {
			t.Fatalf("SignURL() error = %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, signed, nil)
		req.Header.Set("Range", "bytes=0-4")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})
}

func TestPublicErrorAndRequestLogsDoNotLeakSensitiveDetails(t *testing.T) {
	leakyErr := errors.New("dial nats://edge:password@private-nats.internal:4222 subject air3.tickets secret=topsecret")
	var logBuffer bytes.Buffer
	edge, _ := testEdge(&fakePublisher{err: leakyErr}, time.Second)
	edge.logger = slog.New(slog.NewTextHandler(&logBuffer, nil))

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))

	if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
	}
	body := resp.Body.String()
	logs := logBuffer.String()
	for _, secret := range []string{"password", "private-nats.internal", "air3.tickets", "topsecret"} {
		if strings.Contains(body, secret) {
			t.Fatalf("public body leaked %q: %q", secret, body)
		}
		if strings.Contains(logs, secret) {
			t.Fatalf("logs leaked %q: %q", secret, logs)
		}
	}
	if !strings.Contains(body, "backend unavailable") {
		t.Fatalf("public body = %q, want generic backend unavailable message", body)
	}
}

func TestGETStreamsIngestToPublicResponse(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "11", ETag: `"abc"`})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_, _ = io.Copy(stream, strings.NewReader("hello world"))
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	if body := resp.Body.String(); body != "hello world" {
		t.Fatalf("body = %q", body)
	}
	if ct := resp.Result().Header.Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestHEADReturnsMetadataWithoutBody(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "5"})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_, _ = stream.Write([]byte("hello"))
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodHead, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	if body := resp.Body.String(); body != "" {
		t.Fatalf("HEAD body = %q, want empty", body)
	}
	if length := resp.Result().Header.Get("Content-Length"); length != "5" {
		t.Fatalf("content-length = %q", length)
	}
}

func TestMissingObjectMetadataMapsToPublic404(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusNotFound})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/missing.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", got, http.StatusNotFound)
	}
	if body := resp.Body.String(); body != "" {
		t.Fatalf("body = %q, want empty", body)
	}
}

func TestPublishFailureAndConnectorTimeoutMapping(t *testing.T) {
	t.Run("publish failure", func(t *testing.T) {
		edge, reg := testEdge(&fakePublisher{err: errors.New("nats unavailable")}, time.Second)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
		}
		if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("connector timeout", func(t *testing.T) {
		edge, reg := testEdge(&fakePublisher{}, 5*time.Millisecond)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want %d", got, http.StatusGatewayTimeout)
		}
		if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
		}
	})
}

func TestClientDisconnectCancelsPendingState(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil).WithContext(ctx)
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		edge.ServeHTTP(resp, req)
	}()
	for pub.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
	}
}
