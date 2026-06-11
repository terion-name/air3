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
	"github.com/terion-name/air3/internal/ingestquic"
	"github.com/terion-name/air3/internal/ingestsmux"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/publicpath"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/signing"
	"github.com/terion-name/air3/internal/tickets"
)

type fakePublisher struct {
	mu       sync.Mutex
	tickets  []tickets.Ticket
	subjects []string
	err      error
	on       func(tickets.Ticket)
}

func (p *fakePublisher) PublishTicketTo(ctx context.Context, subject string, t tickets.Ticket) error {
	p.mu.Lock()
	p.tickets = append(p.tickets, t)
	p.subjects = append(p.subjects, subject)
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

func (p *fakePublisher) subjectSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.subjects...)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tickets)
}

type fakeFetcher struct {
	mu       sync.Mutex
	requests []s3fetch.Request
	object   *s3fetch.Object
	err      error
}

func (f *fakeFetcher) Fetch(ctx context.Context, req s3fetch.Request) (*s3fetch.Object, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
	return f.object, f.err
}

func (f *fakeFetcher) snapshot() []s3fetch.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]s3fetch.Request(nil), f.requests...)
}

func testEdge(pub *fakePublisher, ttl time.Duration) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{})
	cfg := config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		NATS:           config.NATSConfig{Subject: "air3.tickets", SubjectTemplate: "air3.{server}"},
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

func signedMultiURL(t *testing.T, method, server, bucket, key string, now time.Time) string {
	t.Helper()
	signed, err := signing.SignURLForMode(signing.SignInput{
		Method:  method,
		BaseURL: "https://files.example",
		Server:  server,
		Bucket:  bucket,
		Key:     key,
		Expires: now.Add(time.Minute),
		Secret:  "secret",
	}, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("SignURLForMode() error = %v", err)
	}
	return signed
}

func signedDefaultBucketMultiURL(t *testing.T, method, server, bucket, key string, now time.Time) string {
	t.Helper()
	signed, err := signing.SignURLForModeWithOptions(signing.SignInput{
		Method:  method,
		BaseURL: "https://files.example",
		Server:  server,
		Bucket:  bucket,
		Key:     key,
		Expires: now.Add(time.Minute),
		Secret:  "secret",
	}, publicpath.ModeMulti, signing.SignOptions{DefaultBucketPath: true})
	if err != nil {
		t.Fatalf("SignURLForModeWithOptions() error = %v", err)
	}
	return signed
}

func testMultiEdge(pub *fakePublisher, cfg config.EdgeConfig, fetchers map[string]objectFetcher) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{})
	cfg.IngestURL = "https://edge.internal/_ingest"
	cfg.MultiServer = true
	if cfg.NATS.SubjectTemplate == "" {
		cfg.NATS.SubjectTemplate = "air3.{server}"
	}
	if cfg.Timeouts.PendingRequestTTL == 0 {
		cfg.Timeouts.PendingRequestTTL = time.Second
	}
	edge := newEdgeServer(cfg, reg, pub, nil, fetchers)
	tokens := []string{"req-multi", "ingest-multi-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}
	return edge, reg
}

func TestTCPIngestListenerDisabledForHTTPTransports(t *testing.T) {
	for _, transport := range []config.IngestTransport{config.IngestTransportHTTP, config.IngestTransportHTTP1, config.IngestTransportHTTP2, config.IngestTransportHTTP3} {
		t.Run(string(transport), func(t *testing.T) {
			tcpIngest, err := newTCPIngestListener(config.EdgeConfig{
				IngestTransport:     transport,
				IngestTCPListenAddr: "not a valid listen address",
			}, pending.NewRegistry(pending.Options{}), nil)
			if err != nil {
				t.Fatalf("newTCPIngestListener() error = %v, want nil", err)
			}
			if tcpIngest != nil {
				t.Fatalf("newTCPIngestListener() = %#v, want nil for %s transport", tcpIngest, transport)
			}
		})
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

func TestNonHTTPIngestListenerDisabledForHTTPTransports(t *testing.T) {
	for _, transport := range []config.IngestTransport{config.IngestTransportHTTP, config.IngestTransportHTTP1, config.IngestTransportHTTP2, config.IngestTransportHTTP3} {
		t.Run(string(transport), func(t *testing.T) {
			listener, err := newNonHTTPIngestListener(config.EdgeConfig{
				IngestTransport:     transport,
				IngestTCPListenAddr: "not a valid listen address",
			}, pending.NewRegistry(pending.Options{}), nil)
			if err != nil {
				t.Fatalf("newNonHTTPIngestListener() error = %v, want nil", err)
			}
			if listener != nil {
				t.Fatalf("newNonHTTPIngestListener() = %#v, want nil for %s transport", listener, transport)
			}
		})
	}
}

func TestSMUXIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newSMUXIngestListener(config.EdgeConfig{
		IngestTransport:     config.IngestTransportSMUX,
		IngestTCPListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newSMUXIngestListener() error = %v, want TLS config error", err)
	}
}

func TestSMUXIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	smuxIngest, err := newSMUXIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportSMUX,
		IngestTCPListenAddr:   "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newSMUXIngestListener() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- smuxIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = smuxIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("smuxIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("smuxIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingestsmux.DialAndSend(context.Background(), "tcp", smuxIngest.listener.Addr().String(), clientTLS, ingestsmux.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "9"},
				Body:        strings.NewReader("smux-body"),
				BodyLength:  9,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:           "https://edge.internal/_ingest",
		IngestTransport:     config.IngestTransportSMUX,
		IngestTCPListenAddr: smuxIngest.listener.Addr().String(),
		AllowedBuckets:      []string{"demo-bucket"},
		Signing:             config.SigningConfig{Disabled: true},
		Timeouts:            config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, pub, nil)
	tokens := []string{"req-smux", "ingest-smux-token"}
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
	if body := resp.Body.String(); body != "smux-body" {
		t.Fatalf("body = %q, want smux-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("SMUX ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SMUX ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-smux" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, smuxIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains SMUX listener address", published[0].IngestURL)
	}
}

func TestHTTP3IngestServerSelectedAndNonHTTPDisabled(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	ingestServer := newIngestHTTPServer(config.EdgeConfig{IngestTransport: config.IngestTransportHTTP3, IngestListenAddr: "127.0.0.1:0"}, http.NotFoundHandler(), tlsCfg)
	http3Server, ok := ingestServer.(*http3IngestServer)
	if !ok {
		t.Fatalf("newIngestHTTPServer() = %T, want *http3IngestServer", ingestServer)
	}
	if http3Server.server.TLSConfig != tlsCfg {
		t.Fatalf("HTTP/3 ingest TLS config = %#v, want provided config", http3Server.server.TLSConfig)
	}

	listener, err := newNonHTTPIngestListener(config.EdgeConfig{
		IngestTransport:      config.IngestTransportHTTP3,
		IngestTCPListenAddr:  "not a valid listen address",
		IngestQUICListenAddr: "not a valid listen address",
	}, pending.NewRegistry(pending.Options{}), tlsCfg)
	if err != nil {
		t.Fatalf("newNonHTTPIngestListener() error = %v, want nil", err)
	}
	if listener != nil {
		t.Fatalf("newNonHTTPIngestListener() = %#v, want nil for http3 transport", listener)
	}
}

func TestIngestTLSRequiredForTLSDirectQUICAndHTTP3(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.EdgeConfig
		want bool
	}{
		{name: "plain http", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP}, want: false},
		{name: "explicit mtls", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP, MTLS: config.MTLSPaths{CertFile: "cert.pem"}}, want: true},
		{name: "tcp", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportTCP}, want: true},
		{name: "smux", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportSMUX}, want: true},
		{name: "quic", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportQUIC}, want: true},
		{name: "http3", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP3}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ingestTLSRequired(tt.cfg); got != tt.want {
				t.Fatalf("ingestTLSRequired() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestQUICIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newQUICIngestListener(config.EdgeConfig{
		IngestTransport:      config.IngestTransportQUIC,
		IngestQUICListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newQUICIngestListener() error = %v, want TLS config error", err)
	}
}

func TestQUICIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	listener, err := newNonHTTPIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportQUIC,
		IngestQUICListenAddr:  "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newNonHTTPIngestListener() error = %v", err)
	}
	quicIngest, ok := listener.(*quicIngestListener)
	if !ok {
		t.Fatalf("newNonHTTPIngestListener() = %T, want *quicIngestListener", listener)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- quicIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = quicIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("quicIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("quicIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingestquic.DialAndSend(context.Background(), quicIngest.listener.Addr().String(), clientTLS, ingestquic.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "9"},
				Body:        strings.NewReader("quic-body"),
				BodyLength:  9,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:            "https://edge.internal/_ingest",
		IngestTransport:      config.IngestTransportQUIC,
		IngestQUICListenAddr: quicIngest.listener.Addr().String(),
		AllowedBuckets:       []string{"demo-bucket"},
		Signing:              config.SigningConfig{Disabled: true},
		Timeouts:             config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, pub, nil)
	tokens := []string{"req-quic", "ingest-quic-token"}
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
	if body := resp.Body.String(); body != "quic-body" {
		t.Fatalf("body = %q, want quic-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("QUIC ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for QUIC ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-quic" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, quicIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains QUIC listener address", published[0].IngestURL)
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

func TestSingleServerPublishesDefaultSubjectAndEmptyServer(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testEdge(pub, time.Second)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "" {
		t.Fatalf("ticket server = %q, want empty", published[0].Server)
	}
	subjects := pub.subjectSnapshot()
	if len(subjects) != 1 || subjects[0] != "air3.tickets" {
		t.Fatalf("subjects = %#v, want [air3.tickets]", subjects)
	}
}

func TestMultiServerConnectorPublishesRoutedSubjectAndTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Disabled: true},
	}, nil)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "blue" {
		t.Fatalf("ticket server = %q, want blue", published[0].Server)
	}
	subjects := pub.subjectSnapshot()
	if len(subjects) != 1 || subjects[0] != "air3.blue" {
		t.Fatalf("subjects = %#v, want [air3.blue]", subjects)
	}
}

func TestMultiServerDefaultBucketShortPathPublishesRoutedTicket(t *testing.T) {
	for _, tt := range []struct {
		name    string
		path    string
		wantKey string
	}{
		{name: "single segment key", path: "/blue/file.txt", wantKey: "file.txt"},
		{name: "additional path segments", path: "/blue/archive/file.txt", wantKey: "archive/file.txt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{err: errors.New("stop after publish")}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets:       []string{"demo"},
				ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
				Signing:              config.SigningConfig{Disabled: true},
			}, nil)
			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tt.path, nil))

			published := pub.snapshot()
			if len(published) != 1 {
				t.Fatalf("published tickets = %#v, want one", published)
			}
			if published[0].Server != "blue" || published[0].Bucket != "demo" || published[0].Key != tt.wantKey {
				t.Fatalf("published ticket = %#v, want server blue bucket demo key %q", published[0], tt.wantKey)
			}
			subjects := pub.subjectSnapshot()
			if len(subjects) != 1 || subjects[0] != "air3.blue" {
				t.Fatalf("subjects = %#v, want [air3.blue]", subjects)
			}
		})
	}
}

func TestDirectDefaultBucketShortPathBypassesNATS(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 5, Body: io.NopCloser(strings.NewReader("hello"))}}
	edge, reg := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets:       []string{"connector-bucket"},
		ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"demo"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})
	edge.newToken = func() (string, error) {
		t.Fatal("direct default-bucket path allocated a connector token")
		return "", nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))

	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	requests := fetcher.snapshot()
	if len(requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", requests)
	}
	if requests[0].Bucket != "demo" || requests[0].Key != "file.txt" {
		t.Fatalf("fetch request = %#v, want bucket demo key file.txt", requests[0])
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if _, err := reg.StartIngest("req-multi", "ingest-multi-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("direct alias registered pending request: %v", err)
	}
}

func TestDefaultBucketShortPathAllowlistUsesResolvedBucket(t *testing.T) {
	t.Run("routed", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"other"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			Signing:              config.SigningConfig{Disabled: true},
		}, nil)

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})

	t.Run("direct", func(t *testing.T) {
		pub := &fakePublisher{}
		fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"demo"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			DirectServers: map[string]config.S3Config{
				"blue": {AllowedBuckets: []string{"other"}},
			},
			Signing: config.SigningConfig{Disabled: true},
		}, map[string]objectFetcher{"blue": fetcher})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
		if got := len(fetcher.snapshot()); got != 0 {
			t.Fatalf("fetch calls = %d, want 0", got)
		}
	})
}

func TestSignedDefaultBucketShortURLAcceptedAndTamperingRejected(t *testing.T) {
	now := time.Now()
	signed := signedDefaultBucketMultiURL(t, http.MethodGet, "blue", "demo", "file.txt", now)

	t.Run("accepted", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"demo"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			Signing:              config.SigningConfig{Secret: "secret"},
		}, nil)
		edge.now = func() time.Time { return now }
		edge.newToken = func() (string, error) { return "signed-default-token", nil }

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, signed, nil))

		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		if published[0].Server != "blue" || published[0].Bucket != "demo" || published[0].Key != "file.txt" {
			t.Fatalf("published ticket = %#v, want server blue bucket demo key file.txt", published[0])
		}
	})

	for _, tt := range []struct {
		name string
		raw  string
	}{
		{name: "server", raw: strings.Replace(signed, "/blue/", "/green/", 1)},
		{name: "key", raw: strings.Replace(signed, "/file.txt", "/other.txt", 1)},
	} {
		t.Run("tampered "+tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			blueFetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("blue"))}}
			greenFetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("green"))}}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets:       []string{"demo"},
				ServerDefaultBuckets: map[string]string{"BLUE": "demo", "GREEN": "demo"},
				DirectServers: map[string]config.S3Config{
					"blue":  {AllowedBuckets: []string{"demo"}},
					"green": {AllowedBuckets: []string{"demo"}},
				},
				Signing: config.SigningConfig{Secret: "secret"},
			}, map[string]objectFetcher{"blue": blueFetcher, "green": greenFetcher})
			edge.now = func() time.Time { return now }

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tt.raw, nil))
			if got := resp.Result().StatusCode; got != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if got := len(blueFetcher.snapshot()) + len(greenFetcher.snapshot()); got != 0 {
				t.Fatalf("fetch calls = %d, want 0", got)
			}
		})
	}
}

func TestMultiServerSignedURLRejectsServerPathTampering(t *testing.T) {
	pub := &fakePublisher{}
	now := time.Now()
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
	}, nil)
	edge.now = func() time.Time { return now }

	signed := signedMultiURL(t, http.MethodGet, "blue", "demo-bucket", "file.txt", now)
	tampered := strings.Replace(signed, "/blue/", "/red/", 1)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tampered, nil))

	if got := resp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestInvalidMultiServerPathFailsBeforePublishOrFetch(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"demo-bucket"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})

	for _, path := range []string{"/bad%2Falias/demo-bucket/file.txt", "/blue/demo-bucket", "/blue/file.txt"} {
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
		if got := resp.Result().StatusCode; got != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", path, got, http.StatusBadRequest)
		}
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if got := len(fetcher.snapshot()); got != 0 {
		t.Fatalf("fetch calls = %d, want 0", got)
	}
}

func TestDirectAliasGETHEADAndRangeBypassNATS(t *testing.T) {
	for _, tt := range []struct {
		name     string
		method   string
		rangeHdr string
		body     string
		wantBody string
	}{
		{name: "get range", method: http.MethodGet, rangeHdr: "bytes=0-4", body: "hello", wantBody: "hello"},
		{name: "head", method: http.MethodHead, body: "ignored", wantBody: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusPartialContent, ContentType: "text/plain", ContentLength: int64(len(tt.body)), ContentRange: "bytes 0-4/11", ETag: `"abc"`, AcceptRanges: "bytes", Body: io.NopCloser(strings.NewReader(tt.body))}}
			edge, reg := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets: []string{"connector-bucket"},
				DirectServers: map[string]config.S3Config{
					"blue": {AllowedBuckets: []string{"demo-bucket"}},
				},
				Signing: config.SigningConfig{Disabled: true},
			}, map[string]objectFetcher{"blue": fetcher})
			edge.newToken = func() (string, error) { t.Fatal("direct alias allocated a connector token"); return "", nil }

			req := httptest.NewRequest(tt.method, "/blue/demo-bucket/file.txt", nil)
			if tt.rangeHdr != "" {
				req.Header.Set("Range", tt.rangeHdr)
			}
			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != http.StatusPartialContent {
				t.Fatalf("status = %d, want %d", got, http.StatusPartialContent)
			}
			if body := resp.Body.String(); body != tt.wantBody {
				t.Fatalf("body = %q, want %q", body, tt.wantBody)
			}
			if ct := resp.Result().Header.Get("Content-Type"); ct != "text/plain" {
				t.Fatalf("content-type = %q, want text/plain", ct)
			}
			requests := fetcher.snapshot()
			if len(requests) != 1 {
				t.Fatalf("fetch requests = %#v, want one", requests)
			}
			if requests[0].Method != tt.method || requests[0].Bucket != "demo-bucket" || requests[0].Key != "file.txt" || requests[0].Range != tt.rangeHdr {
				t.Fatalf("fetch request = %#v", requests[0])
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if _, err := reg.StartIngest("req-multi", "ingest-multi-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("direct alias registered pending request: %v", err)
			}
		})
	}
}

func TestDirectAliasUsesAliasSpecificAllowlist(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"other-bucket"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if got := len(fetcher.snapshot()); got != 0 {
		t.Fatalf("fetch calls = %d, want 0", got)
	}
}

func TestDirectAliasErrorMapping(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{
		{name: "not found", err: s3fetch.ErrNotFound, want: http.StatusNotFound},
		{name: "invalid request", err: s3fetch.ErrInvalidRequest, want: http.StatusBadRequest},
		{name: "unavailable", err: errors.New("s3 secret=topsecret endpoint private.internal"), want: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			var logBuffer bytes.Buffer
			fetcher := &fakeFetcher{err: tt.err}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				DirectServers: map[string]config.S3Config{
					"blue": {AllowedBuckets: []string{"demo-bucket"}},
				},
				Signing: config.SigningConfig{Disabled: true},
			}, map[string]objectFetcher{"blue": fetcher})
			edge.logger = slog.New(slog.NewTextHandler(&logBuffer, nil))

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))
			if got := resp.Result().StatusCode; got != tt.want {
				t.Fatalf("status = %d, want %d", got, tt.want)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			for _, secret := range []string{"topsecret", "private.internal"} {
				if strings.Contains(resp.Body.String(), secret) || strings.Contains(logBuffer.String(), secret) {
					t.Fatalf("direct error leaked %q; body=%q logs=%q", secret, resp.Body.String(), logBuffer.String())
				}
			}
		})
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
