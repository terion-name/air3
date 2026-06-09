package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
)

type fakeFetcher struct {
	requests []s3fetch.Request
	object   *s3fetch.Object
	err      error
}

func (f *fakeFetcher) Fetch(ctx context.Context, req s3fetch.Request) (*s3fetch.Object, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	return f.object, nil
}

type sentIngest struct {
	ticket   tickets.Ticket
	metadata ingestMetadata
	body     string
}

type fakeIngestSender struct {
	sends []sentIngest
	err   error
}

func (s *fakeIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	payload := ""
	if body != nil {
		data, _ := io.ReadAll(body)
		payload = string(data)
	}
	s.sends = append(s.sends, sentIngest{ticket: ticket, metadata: metadata, body: payload})
	return s.err
}

func connectorConfig() config.ConnectorConfig {
	return config.ConnectorConfig{
		AllowedBuckets: []string{"demo-bucket"},
		S3: config.S3Config{
			AllowedBuckets:  []string{"demo-bucket"},
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	}
}

func validTicket(url, method string) tickets.Ticket {
	return tickets.Ticket{Version: tickets.Version, RequestID: "req-1", Bucket: "demo-bucket", Key: "objects/file.txt", Method: method, DeadlineUnixMS: time.Now().Add(time.Minute).UnixMilli(), IngestURL: url, IngestToken: "ingest-token"}
}

func TestConnectorSafeLogErrorRedactsDetails(t *testing.T) {
	leakyErr := errors.New("post https://edge-gateway:9443/_ingest/req failed with secret=topsecret")
	got := safeLogError(leakyErr)
	for _, secret := range []string{"edge-gateway", "9443", "topsecret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("safeLogError() leaked %q in %q", secret, got)
		}
	}
}

func TestIngestHTTPClientClonesDefaultTransportWithConnectorSettings(t *testing.T) {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	defaultDisableCompression := defaultTransport.DisableCompression
	defaultMaxIdleConnsPerHost := defaultTransport.MaxIdleConnsPerHost

	client, err := ingestHTTPClient(config.MTLSPaths{}, 7*time.Second, false)
	if err != nil {
		t.Fatalf("ingestHTTPClient() error = %v", err)
	}
	if client.Timeout != 7*time.Second {
		t.Fatalf("client timeout = %v, want 7s", client.Timeout)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport = %T, want *http.Transport", client.Transport)
	}
	if transport == defaultTransport {
		t.Fatal("ingestHTTPClient() returned http.DefaultTransport instead of a clone")
	}
	if defaultTransport.DisableCompression != defaultDisableCompression || defaultTransport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatal("ingestHTTPClient() mutated http.DefaultTransport")
	}
	if !transport.DisableCompression {
		t.Fatal("transport.DisableCompression = false, want true")
	}
	wantMaxIdleConnsPerHost := defaultMaxIdleConnsPerHost
	if wantMaxIdleConnsPerHost < 32 {
		wantMaxIdleConnsPerHost = 32
	}
	if transport.MaxIdleConnsPerHost != wantMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, wantMaxIdleConnsPerHost)
	}
	if transport.TLSHandshakeTimeout != defaultTransport.TLSHandshakeTimeout || transport.IdleConnTimeout != defaultTransport.IdleConnTimeout || transport.ExpectContinueTimeout != defaultTransport.ExpectContinueTimeout || transport.ResponseHeaderTimeout != defaultTransport.ResponseHeaderTimeout {
		t.Fatalf("transport did not preserve default timeout behavior: got TLSHandshake=%v IdleConn=%v ExpectContinue=%v ResponseHeader=%v", transport.TLSHandshakeTimeout, transport.IdleConnTimeout, transport.ExpectContinueTimeout, transport.ResponseHeaderTimeout)
	}
	if (transport.Proxy == nil) != (defaultTransport.Proxy == nil) || (transport.DialContext == nil) != (defaultTransport.DialContext == nil) {
		t.Fatal("transport did not preserve default proxy/dialer behavior")
	}
	if !transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = false, want true when HTTP/2 is enabled")
	}
	if transport.TLSNextProto != nil {
		t.Fatalf("TLSNextProto = %#v, want nil when HTTP/2 is enabled", transport.TLSNextProto)
	}
	if transport.ReadBufferSize != ingestTransportBufferBytes || transport.WriteBufferSize != ingestTransportBufferBytes {
		t.Fatalf("buffer sizes read=%d write=%d, want %d", transport.ReadBufferSize, transport.WriteBufferSize, ingestTransportBufferBytes)
	}

	client, err = ingestHTTPClient(config.MTLSPaths{}, 7*time.Second, true)
	if err != nil {
		t.Fatalf("ingestHTTPClient(disableHTTP2) error = %v", err)
	}
	disabledTransport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("disabled client transport = %T, want *http.Transport", client.Transport)
	}
	if disabledTransport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = true, want false when HTTP/2 is disabled")
	}
	if disabledTransport.TLSNextProto == nil || len(disabledTransport.TLSNextProto) != 0 {
		t.Fatalf("TLSNextProto = %#v, want empty non-nil map when HTTP/2 is disabled", disabledTransport.TLSNextProto)
	}
	if disabledTransport.ReadBufferSize != ingestTransportBufferBytes || disabledTransport.WriteBufferSize != ingestTransportBufferBytes {
		t.Fatalf("disabled buffer sizes read=%d write=%d, want %d", disabledTransport.ReadBufferSize, disabledTransport.WriteBufferSize, ingestTransportBufferBytes)
	}
	if defaultTransport.DisableCompression != defaultDisableCompression || defaultTransport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatal("ingestHTTPClient(disableHTTP2) mutated http.DefaultTransport")
	}
}

func TestConnectorStreamsFetchedObjectToIngest(t *testing.T) {
	var gotToken, gotStatus, gotContentType, gotMetadataLength, gotBody string
	var gotContentLength int64
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get(ingest.TokenHeader)
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		gotContentType = r.Header.Get("Content-Type")
		gotMetadataLength = r.Header.Get(ingest.ObjectContentLengthHeader)
		gotContentLength = r.ContentLength
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 11, ETag: `"abc"`, Body: io.NopCloser(strings.NewReader("hello world"))}}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotToken != "ingest-token" || gotStatus != "200" || gotContentType != "text/plain" || gotMetadataLength != "11" || gotBody != "hello world" || gotContentLength != 11 {
		t.Fatalf("ingest token=%q status=%q content-type=%q metadata-length=%q content-length=%d body=%q", gotToken, gotStatus, gotContentType, gotMetadataLength, gotContentLength, gotBody)
	}
	if len(fetcher.requests) != 1 || fetcher.requests[0].Bucket != "demo-bucket" || fetcher.requests[0].Key != "objects/file.txt" {
		t.Fatalf("fetch requests = %#v", fetcher.requests)
	}
}

func TestConnectorStreamsUnknownLengthObjectWithChunkedIngest(t *testing.T) {
	var gotMetadataLength, gotBody string
	var gotContentLength int64
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMetadataLength = r.Header.Get(ingest.ObjectContentLengthHeader)
		gotContentLength = r.ContentLength
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: -1, Body: io.NopCloser(strings.NewReader("streamed body"))}}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotMetadataLength != "" || gotContentLength != -1 || gotBody != "streamed body" {
		t.Fatalf("metadata-length=%q content-length=%d body=%q, want absent metadata length, chunked POST, and streamed body", gotMetadataLength, gotContentLength, gotBody)
	}
}

func TestConnectorPassesRangeToFetcherAndIngest(t *testing.T) {
	var gotStatus, gotContentRange string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		gotContentRange = r.Header.Get("Content-Range")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusPartialContent, ContentLength: 5, ContentRange: "bytes 0-4/11", AcceptRanges: "bytes", Body: io.NopCloser(strings.NewReader("hello"))}}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	ticket := validTicket(ts.URL, http.MethodGet)
	ticket.Range = "bytes=0-4"
	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(fetcher.requests) != 1 || fetcher.requests[0].Range != "bytes=0-4" {
		t.Fatalf("fetch requests = %#v, want signed range forwarded", fetcher.requests)
	}
	if gotStatus != "206" || gotContentRange != "bytes 0-4/11" {
		t.Fatalf("ingest status=%q content-range=%q", gotStatus, gotContentRange)
	}
}

func TestConnectorHEADPostsMetadataWithoutBody(t *testing.T) {
	var gotBody string
	var gotContentLength int64
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentLength = r.ContentLength
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		if r.Header.Get(ingest.StatusCodeHeader) != "200" || r.Header.Get(ingest.ObjectContentLengthHeader) != "5" {
			t.Fatalf("headers = %#v", r.Header)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 5, Body: io.NopCloser(strings.NewReader("hello"))}}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodHead)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotBody != "" || gotContentLength != 0 {
		t.Fatalf("HEAD body = %q content-length = %d, want empty body and zero POST content length", gotBody, gotContentLength)
	}
}

func TestHTTPIngestSenderStatusOnlyKeepsMetadataLengthWithoutBody(t *testing.T) {
	var gotMetadataLength, gotBody string
	var gotContentLength int64
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMetadataLength = r.Header.Get(ingest.ObjectContentLengthHeader)
		gotContentLength = r.ContentLength
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	sender := httpIngestSender{client: ts.Client()}
	ticket := validTicket(ts.URL, http.MethodHead)
	metadata := ingestMetadata{StatusCode: http.StatusNotModified, ContentLength: 42}
	if err := sender.Send(context.Background(), ticket, metadata, http.NoBody); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if gotMetadataLength != "42" || gotContentLength != 0 || gotBody != "" {
		t.Fatalf("metadata-length=%q content-length=%d body=%q, want metadata length with empty fixed POST", gotMetadataLength, gotContentLength, gotBody)
	}
}

func TestConnectorMissingObjectPosts404Metadata(t *testing.T) {
	var gotStatus, gotBody string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{err: s3fetch.ErrNotFound}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotStatus != "404" || gotBody != "" {
		t.Fatalf("status=%q body=%q, want 404 empty", gotStatus, gotBody)
	}
}

func TestConnectorDropsUnsafeMetadataHeaderValues(t *testing.T) {
	h := http.Header{}
	metadataForObject(&s3fetch.Object{
		StatusCode:    http.StatusOK,
		ContentType:   "text/plain\r\nX-Leak: secret",
		ContentLength: 7,
		ContentRange:  "bytes 0-6/7",
		ETag:          "\"abc\"\r\nSet-Cookie: leak",
		LastModified:  "Mon, 08 Jun 2026 12:00:00 GMT",
		AcceptRanges:  "bytes",
	}).setHeaders(h)

	if h.Get("Content-Type") != "" || h.Get("ETag") != "" {
		t.Fatalf("unsafe headers were propagated: %#v", h)
	}
	if h.Get(ingest.StatusCodeHeader) != "200" || h.Get(ingest.ObjectContentLengthHeader) != "7" || h.Get("Content-Range") != "bytes 0-6/7" {
		t.Fatalf("safe headers missing: %#v", h)
	}
}

func TestConnectorRejectsDisallowedBucketBeforeFetch(t *testing.T) {
	fetcher := &fakeFetcher{object: &s3fetch.Object{Body: io.NopCloser(strings.NewReader("body"))}}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: http.DefaultClient}, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticket.Bucket = "other-bucket"
	if err := worker.handleTicket(context.Background(), ticket); err == nil {
		t.Fatal("handleTicket() error = nil, want disallowed bucket error")
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
	}
}

func TestConnectorMapsBackendErrorTo503Metadata(t *testing.T) {
	var gotStatus string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{err: errors.New("backend down")}
	worker := newConnector(connectorConfig(), fetcher, httpIngestSender{client: ts.Client()}, nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotStatus != "503" {
		t.Fatalf("status = %q, want 503", gotStatus)
	}
}

func TestConnectorUsesSenderForFetchedObject(t *testing.T) {
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusPartialContent, ContentType: "text/plain", ContentLength: 5, ContentRange: "bytes 0-4/11", Body: io.NopCloser(strings.NewReader("hello"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(connectorConfig(), fetcher, sender, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(sender.sends) != 1 {
		t.Fatalf("sender sends = %#v, want one send", sender.sends)
	}
	sent := sender.sends[0]
	if sent.ticket.RequestID != ticket.RequestID || sent.metadata.StatusCode != http.StatusPartialContent || sent.metadata.ContentLength != 5 || sent.metadata.ContentRange != "bytes 0-4/11" || sent.body != "hello" {
		t.Fatalf("sent ingest = %#v", sent)
	}
}

func TestConnectorUsesSenderForFetchErrorStatusOnly(t *testing.T) {
	fetcher := &fakeFetcher{err: s3fetch.ErrInvalidRequest}
	sender := &fakeIngestSender{}
	worker := newConnector(connectorConfig(), fetcher, sender, nil)

	if err := worker.handleTicket(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(sender.sends) != 1 {
		t.Fatalf("sender sends = %#v, want one send", sender.sends)
	}
	if sender.sends[0].metadata.StatusCode != http.StatusBadRequest || sender.sends[0].body != "" {
		t.Fatalf("sent status/body = %d/%q, want 400 empty", sender.sends[0].metadata.StatusCode, sender.sends[0].body)
	}
}

func TestConnectorRejectsInvalidIngestURLBeforeFetchOrTCPSend(t *testing.T) {
	cfg := connectorConfig()
	cfg.IngestTransport = config.IngestTransportTCP
	fetcher := &fakeFetcher{object: &s3fetch.Object{Body: io.NopCloser(strings.NewReader("body"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(cfg, fetcher, sender, nil)
	ticket := validTicket("http://edge.internal/_ingest/req-1", http.MethodGet)

	if err := worker.handleTicket(context.Background(), ticket); err == nil {
		t.Fatal("handleTicket() error = nil, want invalid ingest URL error")
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
	}
	if len(sender.sends) != 0 {
		t.Fatalf("sender sends = %#v, want none", sender.sends)
	}
}

func TestHTTPIngestSenderRejectsNon2xxResponses(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("rejected"))
	}))
	defer ts.Close()

	sender := httpIngestSender{client: ts.Client()}
	err := sender.Send(context.Background(), validTicket(ts.URL, http.MethodGet), ingestMetadata{StatusCode: http.StatusOK, ContentLength: 4}, strings.NewReader("body"))
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("Send() error = %v, want status 502 error", err)
	}
}

func TestTCPIngestSenderBuildsKnownLengthRequestAndSanitizesMetadata(t *testing.T) {
	tlsCfg := &tls.Config{ServerName: "edge.internal"}
	var gotNetwork, gotAddress string
	var gotTLS *tls.Config
	var gotReq ingesttcp.ClientRequest
	sender := tcpIngestSender{
		address:   "edge.internal:9000",
		tlsConfig: tlsCfg,
		dialAndSend: func(ctx context.Context, network, address string, tlsConfig *tls.Config, req ingesttcp.ClientRequest) error {
			gotNetwork = network
			gotAddress = address
			gotTLS = tlsConfig
			gotReq = req
			return nil
		},
	}
	metadata := ingestMetadata{
		StatusCode:    http.StatusOK,
		ContentType:   " text/plain ",
		ContentLength: 12,
		ContentRange:  " bytes 0-11/12 ",
		ETag:          "\"abc\"\r\nSet-Cookie: leak",
		LastModified:  " Mon, 08 Jun 2026 12:00:00 GMT ",
		AcceptRanges:  " bytes ",
	}

	if err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), metadata, strings.NewReader("hello world!")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if gotNetwork != "tcp" || gotAddress != "edge.internal:9000" || gotTLS != tlsCfg {
		t.Fatalf("dial args network=%q address=%q tls=%p, want tcp edge.internal:9000 %p", gotNetwork, gotAddress, gotTLS, tlsCfg)
	}
	if gotReq.RequestID != "req-1" || gotReq.IngestToken != "ingest-token" || gotReq.BodyLength != 12 {
		t.Fatalf("request id/token/bodyLength = %q/%q/%d", gotReq.RequestID, gotReq.IngestToken, gotReq.BodyLength)
	}
	wantMetadata := pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "12", ContentRange: "bytes 0-11/12", LastModified: "Mon, 08 Jun 2026 12:00:00 GMT", AcceptRanges: "bytes"}
	if gotReq.Metadata != wantMetadata {
		t.Fatalf("metadata = %#v, want %#v", gotReq.Metadata, wantMetadata)
	}
	body, _ := io.ReadAll(gotReq.Body)
	if string(body) != "hello world!" {
		t.Fatalf("body = %q, want hello world!", body)
	}
}

func TestTCPIngestSenderBodyLengthRules(t *testing.T) {
	tests := []struct {
		name       string
		metadata   ingestMetadata
		body       io.Reader
		wantLength int64
	}{
		{name: "no body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: http.NoBody, wantLength: 0},
		{name: "nil body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: nil, wantLength: 0},
		{name: "unknown body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: -1}, body: strings.NewReader("stream"), wantLength: ingesttcp.UnknownBodyLength},
		{name: "known body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: 6}, body: strings.NewReader("stream"), wantLength: 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotReq ingesttcp.ClientRequest
			sender := tcpIngestSender{dialAndSend: func(ctx context.Context, network, address string, tlsConfig *tls.Config, req ingesttcp.ClientRequest) error {
				gotReq = req
				return nil
			}}
			if err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), tc.metadata, tc.body); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if gotReq.BodyLength != tc.wantLength {
				t.Fatalf("BodyLength = %d, want %d", gotReq.BodyLength, tc.wantLength)
			}
		})
	}
}

func TestTCPIngestSenderPropagatesDialAndSendError(t *testing.T) {
	wantErr := errors.New("dial failed")
	sender := tcpIngestSender{dialAndSend: func(ctx context.Context, network, address string, tlsConfig *tls.Config, req ingesttcp.ClientRequest) error {
		return wantErr
	}}

	err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), ingestMetadata{StatusCode: http.StatusOK, ContentLength: 4}, strings.NewReader("body"))
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "send ingest tcp") {
		t.Fatalf("Send() error = %v, want wrapped dial error", err)
	}
}

func TestNewIngestSenderSelectsTransport(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransportHTTP
		sender, err := newIngestSender(cfg)
		if err != nil {
			t.Fatalf("newIngestSender() error = %v", err)
		}
		if _, ok := sender.(httpIngestSender); !ok {
			t.Fatalf("sender = %T, want httpIngestSender", sender)
		}
	})

	t.Run("tcp", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransportTCP
		cfg.IngestTCPAddr = "edge.internal:9000"
		sender, err := newIngestSender(cfg)
		if err != nil {
			t.Fatalf("newIngestSender() error = %v", err)
		}
		tcpSender, ok := sender.(tcpIngestSender)
		if !ok {
			t.Fatalf("sender = %T, want tcpIngestSender", sender)
		}
		if tcpSender.address != "edge.internal:9000" || tcpSender.tlsConfig == nil {
			t.Fatalf("tcp sender address=%q tls=%p, want configured address and TLS config", tcpSender.address, tcpSender.tlsConfig)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransport("quic")
		if _, err := newIngestSender(cfg); err == nil {
			t.Fatal("newIngestSender() error = nil, want unsupported transport error")
		}
	})
}
