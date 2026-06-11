package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/ingestquic"
	"github.com/terion-name/air3/internal/ingestsmux"
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

type fakeQuicSender struct {
	requests   []ingestquic.ClientRequest
	err        error
	closeErr   error
	closeCount int
}

func (s *fakeQuicSender) Send(ctx context.Context, req ingestquic.ClientRequest) error {
	s.requests = append(s.requests, req)
	return s.err
}

func (s *fakeQuicSender) Close() error {
	s.closeCount++
	return s.closeErr
}

type fakeSmuxSender struct {
	requests   []ingestsmux.ClientRequest
	err        error
	closeErr   error
	closeCount int
}

func (s *fakeSmuxSender) Send(ctx context.Context, req ingestsmux.ClientRequest) error {
	s.requests = append(s.requests, req)
	return s.err
}

func (s *fakeSmuxSender) Close() error {
	s.closeCount++
	return s.closeErr
}

type fakeHTTPTransport struct {
	requests   int
	closed     int
	idleClosed int
	closeErr   error
}

func (t *fakeHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.requests++
	_, _ = io.Copy(io.Discard, req.Body)
	return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func (t *fakeHTTPTransport) Close() error {
	t.closed++
	return t.closeErr
}

func (t *fakeHTTPTransport) CloseIdleConnections() {
	t.idleClosed++
}

func connectorConfig() config.ConnectorConfig {
	return config.ConnectorConfig{
		AllowedBuckets: []string{"demo-bucket"},
		IngestPoolSize: 32,
		TicketWorkers:  1,
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

func testConfigOptions(values map[string]string) config.Options {
	if values == nil {
		values = map[string]string{}
	}
	return config.Options{
		Lookup: func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		},
		FileExists: func(string) bool { return false },
	}
}

func mustTicketWorkerPool(t *testing.T, workers int, handle func(context.Context, tickets.Ticket) error, onError func(error)) *ticketWorkerPool {
	t.Helper()
	pool, err := newTicketWorkerPool(workers, handle, onError)
	if err != nil {
		t.Fatalf("newTicketWorkerPool() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = pool.CloseAndWait(ctx)
	})
	return pool
}

func TestTicketWorkerPoolSerialWithOneWorker(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	var mu sync.Mutex
	var order []string
	pool := mustTicketWorkerPool(t, 1, func(ctx context.Context, ticket tickets.Ticket) error {
		started <- ticket.RequestID
		<-release
		mu.Lock()
		order = append(order, ticket.RequestID)
		mu.Unlock()
		return nil
	}, nil)
	if cap(pool.jobs) != 1 {
		t.Fatalf("ticket worker pool job capacity = %d, want 1", cap(pool.jobs))
	}

	ticketOne := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticketOne.RequestID = "req-1"
	ticketTwo := validTicket("https://edge.internal/_ingest/req-2", http.MethodGet)
	ticketTwo.RequestID = "req-2"
	if err := pool.Handle(context.Background(), ticketOne); err != nil {
		t.Fatalf("Handle(ticketOne) error = %v", err)
	}
	if got := <-started; got != "req-1" {
		t.Fatalf("first started ticket = %q, want req-1", got)
	}
	if err := pool.Handle(context.Background(), ticketTwo); err != nil {
		t.Fatalf("Handle(ticketTwo) error = %v", err)
	}
	select {
	case got := <-started:
		t.Fatalf("second ticket started before first completed: %q", got)
	case <-time.After(25 * time.Millisecond):
	}

	releaseAll()
	if err := pool.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "req-1" || order[1] != "req-2" {
		t.Fatalf("completion order = %#v, want serial req-1 then req-2", order)
	}
}

func TestTicketWorkerPoolBoundsParallelWorkers(t *testing.T) {
	const workers = 3
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	var active atomic.Int32
	var maxActive atomic.Int32
	var parallelObserved atomic.Bool
	pool := mustTicketWorkerPool(t, workers, func(ctx context.Context, ticket tickets.Ticket) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			max := maxActive.Load()
			if current <= max || maxActive.CompareAndSwap(max, current) {
				break
			}
		}
		if current > 1 {
			parallelObserved.Store(true)
		}
		started <- struct{}{}
		<-release
		return nil
	}, nil)
	if cap(pool.jobs) != workers {
		t.Fatalf("ticket worker pool job capacity = %d, want %d", cap(pool.jobs), workers)
	}

	requestIDs := []string{"req-1", "req-2", "req-3"}
	for i, requestID := range requestIDs {
		ticket := validTicket("https://edge.internal/_ingest/req", http.MethodGet)
		ticket.RequestID = requestID
		if err := pool.Handle(context.Background(), ticket); err != nil {
			t.Fatalf("Handle(%d) error = %v", i, err)
		}
	}
	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for ticket %d to start", i)
		}
	}
	if got := maxActive.Load(); got > workers {
		t.Fatalf("max active workers = %d, want <= %d", got, workers)
	}
	if !parallelObserved.Load() {
		t.Fatal("parallelism was not observed with multiple workers")
	}

	releaseAll()
	if err := pool.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait() error = %v", err)
	}
}

func TestTicketWorkerPoolBackpressureHonorsContextCancellation(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	pool := mustTicketWorkerPool(t, 1, func(ctx context.Context, ticket tickets.Ticket) error {
		started <- struct{}{}
		<-release
		return nil
	}, nil)
	ticketOne := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticketOne.RequestID = "req-1"
	ticketTwo := validTicket("https://edge.internal/_ingest/req-2", http.MethodGet)
	ticketTwo.RequestID = "req-2"
	ticketThree := validTicket("https://edge.internal/_ingest/req-3", http.MethodGet)
	ticketThree.RequestID = "req-3"
	if err := pool.Handle(context.Background(), ticketOne); err != nil {
		t.Fatalf("Handle(ticketOne) error = %v", err)
	}
	<-started
	if err := pool.Handle(context.Background(), ticketTwo); err != nil {
		t.Fatalf("Handle(ticketTwo) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- pool.Handle(ctx, ticketThree) }()
	select {
	case err := <-errCh:
		t.Fatalf("Handle(ticketThree) returned before queue space was available: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle(ticketThree) error = %v, want context canceled", err)
	}

	releaseAll()
	if err := pool.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait() error = %v", err)
	}
}

func TestTicketWorkerPoolReportsAsyncHandlerErrors(t *testing.T) {
	sentinel := errors.New("sentinel failure")
	reported := make(chan error, 1)
	pool := mustTicketWorkerPool(t, 1, func(ctx context.Context, ticket tickets.Ticket) error {
		return sentinel
	}, func(err error) { reported <- err })
	ticket := validTicket("https://edge.internal/_ingest/req-42", http.MethodGet)
	ticket.RequestID = "req-42"

	if err := pool.Handle(context.Background(), ticket); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, sentinel) {
			t.Fatalf("reported error = %v, want sentinel wrapping", err)
		}
		if !strings.Contains(err.Error(), "handle ticket req-42") {
			t.Fatalf("reported error = %v, want request ID context", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async handler error")
	}
	if err := pool.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait() error = %v", err)
	}
}

func TestTicketWorkerPoolCloseAndWaitReturnsContextError(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	pool := mustTicketWorkerPool(t, 1, func(ctx context.Context, ticket tickets.Ticket) error {
		started <- struct{}{}
		<-release
		return nil
	}, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	if err := pool.Handle(context.Background(), ticket); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.CloseAndWait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseAndWait(canceled) error = %v, want context canceled", err)
	}
	releaseAll()
	if err := pool.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait(background) error = %v", err)
	}
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
	defaultMaxIdleConns := defaultTransport.MaxIdleConns
	defaultMaxIdleConnsPerHost := defaultTransport.MaxIdleConnsPerHost
	poolSize := 64

	client, err := ingestHTTPClient(config.MTLSPaths{}, 7*time.Second, false, poolSize)
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
	if defaultTransport.DisableCompression != defaultDisableCompression || defaultTransport.MaxIdleConns != defaultMaxIdleConns || defaultTransport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatal("ingestHTTPClient() mutated http.DefaultTransport")
	}
	if !transport.DisableCompression {
		t.Fatal("transport.DisableCompression = false, want true")
	}
	if transport.MaxConnsPerHost != poolSize {
		t.Fatalf("MaxConnsPerHost = %d, want %d", transport.MaxConnsPerHost, poolSize)
	}
	if transport.MaxIdleConnsPerHost != poolSize {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, poolSize)
	}
	wantMaxIdleConns := defaultMaxIdleConns
	if wantMaxIdleConns < poolSize {
		wantMaxIdleConns = poolSize
	}
	if transport.MaxIdleConns != wantMaxIdleConns {
		t.Fatalf("MaxIdleConns = %d, want %d", transport.MaxIdleConns, wantMaxIdleConns)
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

	client, err = ingestHTTPClient(config.MTLSPaths{}, 7*time.Second, true, poolSize)
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
	if disabledTransport.MaxConnsPerHost != poolSize || disabledTransport.MaxIdleConnsPerHost != poolSize {
		t.Fatalf("disabled pool limits MaxConnsPerHost=%d MaxIdleConnsPerHost=%d, want %d", disabledTransport.MaxConnsPerHost, disabledTransport.MaxIdleConnsPerHost, poolSize)
	}
	if disabledTransport.ReadBufferSize != ingestTransportBufferBytes || disabledTransport.WriteBufferSize != ingestTransportBufferBytes {
		t.Fatalf("disabled buffer sizes read=%d write=%d, want %d", disabledTransport.ReadBufferSize, disabledTransport.WriteBufferSize, ingestTransportBufferBytes)
	}
	if defaultTransport.DisableCompression != defaultDisableCompression || defaultTransport.MaxIdleConns != defaultMaxIdleConns || defaultTransport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatal("ingestHTTPClient(disableHTTP2) mutated http.DefaultTransport")
	}
}

func TestIngestHTTP3ClientUsesHTTP3TransportAndMTLS(t *testing.T) {
	client, err := ingestHTTP3Client(config.MTLSPaths{ServerName: "edge.internal", InsecureSkipVerify: true}, 9*time.Second)
	if err != nil {
		t.Fatalf("ingestHTTP3Client() error = %v", err)
	}
	if client.Timeout != 9*time.Second {
		t.Fatalf("client timeout = %v, want 9s", client.Timeout)
	}
	transport, ok := client.Transport.(*http3.Transport)
	if !ok {
		t.Fatalf("client transport = %T, want *http3.Transport", client.Transport)
	}
	if !transport.DisableCompression {
		t.Fatal("DisableCompression = false, want true")
	}
	if transport.QUICConfig == nil {
		t.Fatal("QUICConfig = nil, want non-nil")
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig = nil, want mTLS config")
	}
	if transport.TLSClientConfig.ServerName != "edge.internal" || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("TLSClientConfig ServerName=%q InsecureSkipVerify=%t, want connector mTLS settings", transport.TLSClientConfig.ServerName, transport.TLSClientConfig.InsecureSkipVerify)
	}
}

func TestPooledHTTPIngestSenderUsesIndependentClientsRoundRobinAndCloses(t *testing.T) {
	var transports []*fakeHTTPTransport
	sender, err := newPooledHTTPIngestSender(3, func() (*http.Client, error) {
		transport := &fakeHTTPTransport{}
		transports = append(transports, transport)
		return &http.Client{Transport: transport}, nil
	})
	if err != nil {
		t.Fatalf("newPooledHTTPIngestSender() error = %v", err)
	}
	if len(sender.senders) != 3 || len(transports) != 3 {
		t.Fatalf("pool size senders=%d transports=%d, want 3", len(sender.senders), len(transports))
	}
	for i := range sender.senders {
		if sender.senders[i].client == nil || sender.senders[i].client.Transport != transports[i] {
			t.Fatalf("sender %d client/transport was not independently created", i)
		}
	}

	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	metadata := ingestMetadata{StatusCode: http.StatusOK, ContentLength: 4}
	for i := 0; i < 7; i++ {
		if err := sender.Send(context.Background(), ticket, metadata, strings.NewReader("body")); err != nil {
			t.Fatalf("Send(%d) error = %v", i, err)
		}
	}
	wantRequests := []int{3, 2, 2}
	for i, want := range wantRequests {
		if transports[i].requests != want {
			t.Fatalf("transport %d requests = %d, want %d", i, transports[i].requests, want)
		}
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for i, transport := range transports {
		if transport.closed != 1 || transport.idleClosed != 1 {
			t.Fatalf("transport %d close=%d idleClose=%d, want one close and one idle close", i, transport.closed, transport.idleClosed)
		}
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

func TestConnectorAcceptsMatchingServerTicket(t *testing.T) {
	cfg := connectorConfig()
	cfg.ServerName = "west-1"
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentLength: 4, Body: io.NopCloser(strings.NewReader("body"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(cfg, fetcher, sender, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticket.Server = "west-1"

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(fetcher.requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", fetcher.requests)
	}
	if len(sender.sends) != 1 {
		t.Fatalf("sender sends = %#v, want one", sender.sends)
	}
}

func TestConnectorRejectsMissingServerTicketBeforeFetchOrSend(t *testing.T) {
	cfg := connectorConfig()
	cfg.ServerName = "west-1"
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("body"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(cfg, fetcher, sender, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)

	err := worker.handleTicket(context.Background(), ticket)
	if err == nil || !strings.Contains(err.Error(), "server is required") {
		t.Fatalf("handleTicket() error = %v, want missing server error", err)
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
	}
	if len(sender.sends) != 0 {
		t.Fatalf("sender sends = %#v, want none", sender.sends)
	}
}

func TestConnectorRejectsMismatchedServerTicketBeforeFetchOrSend(t *testing.T) {
	cfg := connectorConfig()
	cfg.ServerName = "west-1"
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("body"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(cfg, fetcher, sender, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticket.Server = "east-1"

	err := worker.handleTicket(context.Background(), ticket)
	if err == nil || !strings.Contains(err.Error(), "does not match connector") {
		t.Fatalf("handleTicket() error = %v, want mismatched server error", err)
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
	}
	if len(sender.sends) != 0 {
		t.Fatalf("sender sends = %#v, want none", sender.sends)
	}
}

func TestConnectorAcceptsLegacyTicketWhenServerNameUnset(t *testing.T) {
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentLength: 4, Body: io.NopCloser(strings.NewReader("body"))}}
	sender := &fakeIngestSender{}
	worker := newConnector(connectorConfig(), fetcher, sender, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(fetcher.requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", fetcher.requests)
	}
	if len(sender.sends) != 1 {
		t.Fatalf("sender sends = %#v, want one", sender.sends)
	}
}

func TestLoadConnectorServerNameDerivesQueueSubscribeSubject(t *testing.T) {
	cfg, err := config.LoadConnector(testConfigOptions(map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
		"AIR3_SERVER_NAME":          "west-1",
	}))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if cfg.ServerName != "west-1" {
		t.Fatalf("ServerName = %q, want west-1", cfg.ServerName)
	}
	if cfg.NATS.Subject != "air3.west-1" {
		t.Fatalf("NATS subject = %q, want derived QueueSubscribeTickets subject air3.west-1", cfg.NATS.Subject)
	}

	cfg, err = config.LoadConnector(testConfigOptions(map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":      "access",
		"AIR3_S3_SECRET_ACCESS_KEY":  "secret",
		"AIR3_SERVER_NAME":           "west-1",
		"AIR3_NATS_SUBJECT_TEMPLATE": "air3.{server}.tickets",
	}))
	if err != nil {
		t.Fatalf("LoadConnector() with template error = %v", err)
	}
	if cfg.NATS.Subject != "air3.west-1.tickets" {
		t.Fatalf("templated NATS subject = %q, want air3.west-1.tickets", cfg.NATS.Subject)
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

func TestQUICIngestSenderBuildsKnownLengthRequestAndSanitizesMetadata(t *testing.T) {
	fakeSender := &fakeQuicSender{}
	sender := quicIngestSender{sender: fakeSender}
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
	if len(fakeSender.requests) != 1 {
		t.Fatalf("quic requests = %#v, want one request", fakeSender.requests)
	}
	gotReq := fakeSender.requests[0]
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

func TestQUICIngestSenderBodyLengthRules(t *testing.T) {
	tests := []struct {
		name       string
		metadata   ingestMetadata
		body       io.Reader
		wantLength int64
	}{
		{name: "no body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: http.NoBody, wantLength: 0},
		{name: "nil body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: nil, wantLength: 0},
		{name: "unknown body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: -1}, body: strings.NewReader("stream"), wantLength: ingestquic.UnknownBodyLength},
		{name: "known body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: 6}, body: strings.NewReader("stream"), wantLength: 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeSender := &fakeQuicSender{}
			sender := quicIngestSender{sender: fakeSender}
			if err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), tc.metadata, tc.body); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if len(fakeSender.requests) != 1 {
				t.Fatalf("quic requests = %#v, want one request", fakeSender.requests)
			}
			if fakeSender.requests[0].BodyLength != tc.wantLength {
				t.Fatalf("BodyLength = %d, want %d", fakeSender.requests[0].BodyLength, tc.wantLength)
			}
		})
	}
}

func TestQUICIngestSenderPropagatesSendError(t *testing.T) {
	wantErr := errors.New("session failed")
	sender := quicIngestSender{sender: &fakeQuicSender{err: wantErr}}

	err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), ingestMetadata{StatusCode: http.StatusOK, ContentLength: 4}, strings.NewReader("body"))
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "send ingest quic") {
		t.Fatalf("Send() error = %v, want wrapped quic error", err)
	}
}

func TestQUICIngestSenderCloseForwardsToUnderlyingSender(t *testing.T) {
	wantErr := errors.New("close failed")
	fakeSender := &fakeQuicSender{closeErr: wantErr}
	sender := quicIngestSender{sender: fakeSender}

	err := sender.Close()
	if !errors.Is(err, wantErr) || fakeSender.closeCount != 1 {
		t.Fatalf("Close() error=%v closeCount=%d, want forwarded close error and one close", err, fakeSender.closeCount)
	}
}

func TestSMUXIngestSenderBuildsKnownLengthRequestAndSanitizesMetadata(t *testing.T) {
	fakeSender := &fakeSmuxSender{}
	sender := smuxIngestSender{sender: fakeSender}
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
	if len(fakeSender.requests) != 1 {
		t.Fatalf("smux requests = %#v, want one request", fakeSender.requests)
	}
	gotReq := fakeSender.requests[0]
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

func TestSMUXIngestSenderBodyLengthRules(t *testing.T) {
	tests := []struct {
		name       string
		metadata   ingestMetadata
		body       io.Reader
		wantLength int64
	}{
		{name: "no body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: http.NoBody, wantLength: 0},
		{name: "nil body", metadata: ingestMetadata{StatusCode: http.StatusNotFound, ContentLength: 42}, body: nil, wantLength: 0},
		{name: "unknown body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: -1}, body: strings.NewReader("stream"), wantLength: ingestsmux.UnknownBodyLength},
		{name: "known body", metadata: ingestMetadata{StatusCode: http.StatusOK, ContentLength: 6}, body: strings.NewReader("stream"), wantLength: 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeSender := &fakeSmuxSender{}
			sender := smuxIngestSender{sender: fakeSender}
			if err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), tc.metadata, tc.body); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if len(fakeSender.requests) != 1 {
				t.Fatalf("smux requests = %#v, want one request", fakeSender.requests)
			}
			if fakeSender.requests[0].BodyLength != tc.wantLength {
				t.Fatalf("BodyLength = %d, want %d", fakeSender.requests[0].BodyLength, tc.wantLength)
			}
		})
	}
}

func TestSMUXIngestSenderPropagatesSendError(t *testing.T) {
	wantErr := errors.New("session failed")
	sender := smuxIngestSender{sender: &fakeSmuxSender{err: wantErr}}

	err := sender.Send(context.Background(), validTicket("https://edge.internal/_ingest/req-1", http.MethodGet), ingestMetadata{StatusCode: http.StatusOK, ContentLength: 4}, strings.NewReader("body"))
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "send ingest smux") {
		t.Fatalf("Send() error = %v, want wrapped smux error", err)
	}
}

func TestSMUXIngestSenderCloseForwardsToUnderlyingSender(t *testing.T) {
	wantErr := errors.New("close failed")
	fakeSender := &fakeSmuxSender{closeErr: wantErr}
	sender := smuxIngestSender{sender: fakeSender}

	err := sender.Close()
	if !errors.Is(err, wantErr) || fakeSender.closeCount != 1 {
		t.Fatalf("Close() error=%v closeCount=%d, want forwarded close error and one close", err, fakeSender.closeCount)
	}
}

func TestNewIngestSenderSelectsTransport(t *testing.T) {
	t.Run("http protocols", func(t *testing.T) {
		tests := []struct {
			name              string
			transport         config.IngestTransport
			disableHTTP2Flag  bool
			wantHTTP2Disabled bool
		}{
			{name: "legacy http honors disable flag", transport: config.IngestTransportHTTP, disableHTTP2Flag: true, wantHTTP2Disabled: true},
			{name: "legacy http honors enable flag", transport: config.IngestTransportHTTP, disableHTTP2Flag: false, wantHTTP2Disabled: false},
			{name: "http1 ignores enable flag", transport: config.IngestTransportHTTP1, disableHTTP2Flag: false, wantHTTP2Disabled: true},
			{name: "http2 ignores disable flag", transport: config.IngestTransportHTTP2, disableHTTP2Flag: true, wantHTTP2Disabled: false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				cfg := connectorConfig()
				cfg.IngestTransport = tc.transport
				cfg.IngestDisableHTTP2 = tc.disableHTTP2Flag
				cfg.IngestPoolSize = 7
				sender, err := newIngestSender(cfg)
				if err != nil {
					t.Fatalf("newIngestSender() error = %v", err)
				}
				httpSender, ok := sender.(httpIngestSender)
				if !ok {
					t.Fatalf("sender = %T, want httpIngestSender", sender)
				}
				transport, ok := httpSender.client.Transport.(*http.Transport)
				if !ok {
					t.Fatalf("http transport = %T, want *http.Transport", httpSender.client.Transport)
				}
				if transport.MaxConnsPerHost != cfg.IngestPoolSize || transport.MaxIdleConnsPerHost != cfg.IngestPoolSize {
					t.Fatalf("pool limits MaxConnsPerHost=%d MaxIdleConnsPerHost=%d, want %d", transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost, cfg.IngestPoolSize)
				}
				if tc.wantHTTP2Disabled {
					if transport.ForceAttemptHTTP2 || transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 {
						t.Fatalf("HTTP/2 disabled transport ForceAttemptHTTP2=%t TLSNextProto=%#v, want false and empty non-nil map", transport.ForceAttemptHTTP2, transport.TLSNextProto)
					}
				} else if !transport.ForceAttemptHTTP2 || transport.TLSNextProto != nil {
					t.Fatalf("HTTP/2 enabled transport ForceAttemptHTTP2=%t TLSNextProto=%#v, want true and nil", transport.ForceAttemptHTTP2, transport.TLSNextProto)
				}
			})
		}
	})

	t.Run("http3", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransportHTTP3
		cfg.IngestPoolSize = 3
		cfg.Timeouts.StreamTimeout = 11 * time.Second
		cfg.MTLS.ServerName = "edge.internal"
		cfg.MTLS.InsecureSkipVerify = true
		sender, err := newIngestSender(cfg)
		if err != nil {
			t.Fatalf("newIngestSender() error = %v", err)
		}
		t.Cleanup(func() { _ = closeIngestSender(sender) })
		pooledSender, ok := sender.(*pooledHTTPIngestSender)
		if !ok {
			t.Fatalf("sender = %T, want *pooledHTTPIngestSender", sender)
		}
		if len(pooledSender.senders) != cfg.IngestPoolSize {
			t.Fatalf("http3 pool size = %d, want %d", len(pooledSender.senders), cfg.IngestPoolSize)
		}
		seenTransports := map[*http3.Transport]bool{}
		for i, httpSender := range pooledSender.senders {
			transport, ok := httpSender.client.Transport.(*http3.Transport)
			if !ok {
				t.Fatalf("http transport %d = %T, want *http3.Transport", i, httpSender.client.Transport)
			}
			if seenTransports[transport] {
				t.Fatalf("http3 transport %d reuses a previous transport", i)
			}
			seenTransports[transport] = true
			if httpSender.client.Timeout != 11*time.Second || transport.QUICConfig == nil || !transport.DisableCompression {
				t.Fatalf("http3 client %d timeout=%v quicConfig=%p DisableCompression=%t, want configured HTTP/3 client", i, httpSender.client.Timeout, transport.QUICConfig, transport.DisableCompression)
			}
			if transport.TLSClientConfig == nil || transport.TLSClientConfig.ServerName != "edge.internal" || !transport.TLSClientConfig.InsecureSkipVerify {
				t.Fatalf("http3 TLS config %d = %#v, want connector mTLS settings", i, transport.TLSClientConfig)
			}
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

	t.Run("smux", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransportSMUX
		cfg.IngestTCPAddr = "edge.internal:9000"
		cfg.MTLS.ServerName = "edge.internal"
		cfg.MTLS.InsecureSkipVerify = true
		sender, err := newIngestSender(cfg)
		if err != nil {
			t.Fatalf("newIngestSender() error = %v", err)
		}
		smuxSender, ok := sender.(smuxIngestSender)
		if !ok {
			t.Fatalf("sender = %T, want smuxIngestSender", sender)
		}
		if smuxSender.address != "edge.internal:9000" || smuxSender.tlsConfig == nil || smuxSender.sender == nil {
			t.Fatalf("smux sender address=%q tls=%p sender=%T, want configured address, TLS config, and sender", smuxSender.address, smuxSender.tlsConfig, smuxSender.sender)
		}
		if smuxSender.tlsConfig.ServerName != "edge.internal" || !smuxSender.tlsConfig.InsecureSkipVerify {
			t.Fatalf("smux sender TLS config ServerName=%q InsecureSkipVerify=%t, want connector mTLS settings", smuxSender.tlsConfig.ServerName, smuxSender.tlsConfig.InsecureSkipVerify)
		}
	})

	t.Run("quic", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransportQUIC
		cfg.IngestQUICAddr = "edge.internal:9443"
		cfg.MTLS.ServerName = "edge.internal"
		cfg.MTLS.InsecureSkipVerify = true
		sender, err := newIngestSender(cfg)
		if err != nil {
			t.Fatalf("newIngestSender() error = %v", err)
		}
		quicSender, ok := sender.(quicIngestSender)
		if !ok {
			t.Fatalf("sender = %T, want quicIngestSender", sender)
		}
		if quicSender.sender == nil {
			t.Fatal("quic sender = nil, want reusable sender")
		}
	})

	t.Run("unknown", func(t *testing.T) {
		cfg := connectorConfig()
		cfg.IngestTransport = config.IngestTransport("bogus")
		if _, err := newIngestSender(cfg); err == nil {
			t.Fatal("newIngestSender() error = nil, want unsupported transport error")
		}
	})
}
