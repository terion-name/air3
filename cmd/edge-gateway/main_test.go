package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
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
}

func TestPublishFailureAndConnectorTimeoutMapping(t *testing.T) {
	t.Run("publish failure", func(t *testing.T) {
		edge, _ := testEdge(&fakePublisher{err: errors.New("nats unavailable")}, time.Second)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
		}
	})

	t.Run("connector timeout", func(t *testing.T) {
		edge, _ := testEdge(&fakePublisher{}, 5*time.Millisecond)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want %d", got, http.StatusGatewayTimeout)
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
