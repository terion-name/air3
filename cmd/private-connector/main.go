package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/ingestquic"
	"github.com/terion-name/air3/internal/ingestsmux"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/mtls"
	"github.com/terion-name/air3/internal/natsclient"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
)

const ingestTransportBufferBytes = 256 * 1024

type objectFetcher interface {
	Fetch(context.Context, s3fetch.Request) (*s3fetch.Object, error)
}

type ingestSender interface {
	Send(context.Context, tickets.Ticket, ingestMetadata, io.Reader) error
}

type closeableIngestSender interface {
	Close() error
}

func closeIngestSender(sender any) error {
	closer, ok := sender.(closeableIngestSender)
	if !ok || closer == nil {
		return nil
	}
	return closer.Close()
}

type connector struct {
	cfg     config.ConnectorConfig
	fetcher objectFetcher
	sender  ingestSender
	logger  *slog.Logger
	now     func() time.Time
}

var errTicketWorkerPoolClosed = errors.New("ticket worker pool is closed")

type ticketJob struct {
	ctx    context.Context
	ticket tickets.Ticket
}

type ticketWorkerPool struct {
	jobs    chan ticketJob
	closing chan struct{}
	done    chan struct{}

	handle  natsclient.TicketHandler
	onError natsclient.ErrorHandler

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	enqueueWG sync.WaitGroup
	workerWG  sync.WaitGroup
}

func newTicketWorkerPool(workers int, handle natsclient.TicketHandler, onError natsclient.ErrorHandler) (*ticketWorkerPool, error) {
	if workers < 1 {
		return nil, fmt.Errorf("ticket worker pool size must be at least 1: %d", workers)
	}
	if handle == nil {
		return nil, errors.New("ticket worker pool handler is required")
	}

	pool := &ticketWorkerPool{
		jobs:    make(chan ticketJob, workers),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
		handle:  handle,
		onError: onError,
	}
	pool.workerWG.Add(workers)
	for i := 0; i < workers; i++ {
		go pool.work()
	}
	return pool, nil
}

func (p *ticketWorkerPool) Handle(ctx context.Context, ticket tickets.Ticket) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errTicketWorkerPoolClosed
	}
	p.enqueueWG.Add(1)
	p.mu.Unlock()
	defer p.enqueueWG.Done()

	select {
	case p.jobs <- ticketJob{ctx: ctx, ticket: ticket}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closing:
		return errTicketWorkerPoolClosed
	}
}

func (p *ticketWorkerPool) CloseAndWait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.closing)
		p.mu.Unlock()

		go func() {
			p.enqueueWG.Wait()
			close(p.jobs)
			p.workerWG.Wait()
			close(p.done)
		}()
	})

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close ticket worker pool: %w", ctx.Err())
	}
}

func (p *ticketWorkerPool) work() {
	defer p.workerWG.Done()
	for job := range p.jobs {
		if err := p.handle(job.ctx, job.ticket); err != nil && p.onError != nil {
			p.onError(fmt.Errorf("handle ticket %s: %w", job.ticket.RequestID, err))
		}
	}
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("private connector stopped", "error", safeLogError(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.LoadConnectorFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	fetcher, err := s3fetch.New(ctx, cfg.S3)
	if err != nil {
		return err
	}
	sender, err := newIngestSender(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeIngestSender(sender); err != nil {
			logger.Warn("ingest sender close failed", "error", safeLogError(err))
		}
	}()
	worker := newConnector(cfg, fetcher, sender, logger)

	natsCtx, cancelNATS := context.WithTimeout(ctx, 10*time.Second)
	defer cancelNATS()
	nats, err := natsclient.Connect(natsCtx, cfg.NATS)
	if err != nil {
		return err
	}
	defer nats.Close()

	onError := func(err error) {
		logger.Warn("ticket handling failed", "error", safeLogError(err))
	}
	ticketPool, err := newTicketWorkerPool(cfg.TicketWorkers, worker.handleTicket, onError)
	if err != nil {
		return err
	}
	sub, err := nats.QueueSubscribeTickets(ctx, ticketPool.Handle, onError)
	if err != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()
		_ = ticketPool.CloseAndWait(shutdownCtx)
		return err
	}
	<-ctx.Done()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	_ = sub.Drain(drainCtx)
	if err := ticketPool.CloseAndWait(drainCtx); err != nil {
		logger.Warn("ticket worker pool drain failed", "error", safeLogError(err))
	}
	return nats.Drain(drainCtx)
}

func newConnector(cfg config.ConnectorConfig, fetcher objectFetcher, sender ingestSender, logger *slog.Logger) *connector {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if sender == nil {
		sender = httpIngestSender{client: http.DefaultClient}
	}
	return &connector{cfg: cfg, fetcher: fetcher, sender: sender, logger: logger, now: time.Now}
}

func (c *connector) handleTicket(ctx context.Context, ticket tickets.Ticket) error {
	if err := c.validateTicket(ticket); err != nil {
		return err
	}
	deadline := time.UnixMilli(ticket.DeadlineUnixMS)
	ticketCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	obj, err := c.fetcher.Fetch(ticketCtx, s3fetch.Request{Method: ticket.Method, Bucket: ticket.Bucket, Key: ticket.Key, Range: ticket.Range})
	if err != nil {
		status := statusForFetchError(err)
		return c.sender.Send(ticketCtx, ticket, metadataForStatus(status), http.NoBody)
	}
	defer obj.Body.Close()

	metadata := metadataForObject(obj)
	body := obj.Body
	if ticket.Method == http.MethodHead {
		body = http.NoBody
	}
	return c.sender.Send(ticketCtx, ticket, metadata, body)
}

func (c *connector) validateTicket(ticket tickets.Ticket) error {
	if err := ticket.Validate(c.now()); err != nil {
		return err
	}
	if c.cfg.ServerName != "" {
		if ticket.Server == "" {
			return fmt.Errorf("%w: server is required for connector %q", tickets.ErrInvalidTicket, c.cfg.ServerName)
		}
		if ticket.Server != c.cfg.ServerName {
			return fmt.Errorf("%w: server %q does not match connector %q", tickets.ErrInvalidTicket, ticket.Server, c.cfg.ServerName)
		}
	}
	if !bucketAllowed(ticket.Bucket, c.cfg.AllowedBuckets) || !bucketAllowed(ticket.Bucket, c.cfg.S3.AllowedBuckets) {
		return errors.New("ticket bucket is not allowed")
	}
	if ticket.Method != http.MethodGet && ticket.Method != http.MethodHead {
		return errors.New("ticket method is not supported")
	}
	if err := validateIngestURL(ticket.IngestURL); err != nil {
		return err
	}
	return nil
}

type httpIngestSender struct {
	client *http.Client
}

func (s httpIngestSender) Close() error {
	return closeHTTPClient(s.client)
}

func (s httpIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	if body == nil {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ticket.IngestURL, body)
	if err != nil {
		return fmt.Errorf("create ingest request: %w", err)
	}
	req.Header.Set(ingest.TokenHeader, ticket.IngestToken)
	metadata.setHeaders(req.Header)
	if metadata.ContentLength >= 0 && body != http.NoBody {
		req.ContentLength = metadata.ContentLength
	}
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post ingest: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest rejected with status %d", resp.StatusCode)
	}
	return nil
}

type pooledHTTPIngestSender struct {
	senders []httpIngestSender
	next    atomic.Uint64
}

func newPooledHTTPIngestSender(poolSize int, newClient func() (*http.Client, error)) (*pooledHTTPIngestSender, error) {
	if poolSize < 1 {
		return nil, fmt.Errorf("ingest http sender pool size must be at least 1: %d", poolSize)
	}
	sender := &pooledHTTPIngestSender{senders: make([]httpIngestSender, 0, poolSize)}
	for i := 0; i < poolSize; i++ {
		client, err := newClient()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("create ingest http client %d: %w", i, err), sender.Close())
		}
		sender.senders = append(sender.senders, httpIngestSender{client: client})
	}
	return sender, nil
}

func (s *pooledHTTPIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	if s == nil || len(s.senders) == 0 {
		return errors.New("ingest http sender pool is empty")
	}
	idx := s.next.Add(1) - 1
	return s.senders[idx%uint64(len(s.senders))].Send(ctx, ticket, metadata, body)
}

func (s *pooledHTTPIngestSender) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	for _, sender := range s.senders {
		closeErr = errors.Join(closeErr, sender.Close())
	}
	return closeErr
}

type tcpIngestSender struct {
	address     string
	tlsConfig   *tls.Config
	dialAndSend func(context.Context, string, string, *tls.Config, ingesttcp.ClientRequest) error
}

func (s tcpIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	body, bodyLength := ingestRequestBody(body, metadata, ingesttcp.UnknownBodyLength)

	dialAndSend := s.dialAndSend
	if dialAndSend == nil {
		dialAndSend = ingesttcp.DialAndSend
	}
	req := ingesttcp.ClientRequest{
		RequestID:   ticket.RequestID,
		IngestToken: ticket.IngestToken,
		Metadata:    metadata.pendingMetadata(),
		Body:        body,
		BodyLength:  bodyLength,
	}
	if err := dialAndSend(ctx, "tcp", s.address, s.tlsConfig, req); err != nil {
		return fmt.Errorf("send ingest tcp: %w", err)
	}
	return nil
}

type quicClientSender interface {
	Send(context.Context, ingestquic.ClientRequest) error
}

type quicIngestSender struct {
	sender quicClientSender
}

func (s quicIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	body, bodyLength := ingestRequestBody(body, metadata, ingestquic.UnknownBodyLength)
	req := ingestquic.ClientRequest{
		RequestID:   ticket.RequestID,
		IngestToken: ticket.IngestToken,
		Metadata:    metadata.pendingMetadata(),
		Body:        body,
		BodyLength:  bodyLength,
	}
	if err := s.sender.Send(ctx, req); err != nil {
		return fmt.Errorf("send ingest quic: %w", err)
	}
	return nil
}

func (s quicIngestSender) Close() error {
	return closeIngestSender(s.sender)
}

type smuxClientSender interface {
	Send(context.Context, ingestsmux.ClientRequest) error
}

type smuxIngestSender struct {
	address   string
	tlsConfig *tls.Config
	sender    smuxClientSender
}

func (s smuxIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	body, bodyLength := ingestRequestBody(body, metadata, ingestsmux.UnknownBodyLength)
	req := ingestsmux.ClientRequest{
		RequestID:   ticket.RequestID,
		IngestToken: ticket.IngestToken,
		Metadata:    metadata.pendingMetadata(),
		Body:        body,
		BodyLength:  bodyLength,
	}
	if err := s.sender.Send(ctx, req); err != nil {
		return fmt.Errorf("send ingest smux: %w", err)
	}
	return nil
}

func (s smuxIngestSender) Close() error {
	return closeIngestSender(s.sender)
}

func ingestRequestBody(body io.Reader, metadata ingestMetadata, unknownBodyLength int64) (io.Reader, int64) {
	if body == nil {
		return http.NoBody, 0
	}
	if body == http.NoBody {
		return body, 0
	}
	if metadata.ContentLength >= 0 {
		return body, metadata.ContentLength
	}
	return body, unknownBodyLength
}

func newIngestSender(cfg config.ConnectorConfig) (ingestSender, error) {
	switch cfg.IngestTransport {
	case config.IngestTransportHTTP:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, cfg.IngestDisableHTTP2, cfg.IngestPoolSize)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportHTTP1:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, true, cfg.IngestPoolSize)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportHTTP2:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, false, cfg.IngestPoolSize)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportHTTP3:
		sender, err := newPooledHTTPIngestSender(cfg.IngestPoolSize, func() (*http.Client, error) {
			return ingestHTTP3Client(cfg.MTLS, cfg.Timeouts.StreamTimeout)
		})
		if err != nil {
			return nil, err
		}
		return sender, nil
	case config.IngestTransportTCP:
		tlsCfg, err := ingestClientTLSConfig(cfg.MTLS)
		if err != nil {
			return nil, fmt.Errorf("load ingest tcp client tls config: %w", err)
		}
		return tcpIngestSender{address: cfg.IngestTCPAddr, tlsConfig: tlsCfg}, nil
	case config.IngestTransportSMUX:
		tlsCfg, err := ingestClientTLSConfig(cfg.MTLS)
		if err != nil {
			return nil, fmt.Errorf("load ingest smux client tls config: %w", err)
		}
		sender, err := ingestsmux.NewSender("tcp", cfg.IngestTCPAddr, tlsCfg, ingestsmux.WithSessionPoolSize(cfg.IngestPoolSize))
		if err != nil {
			return nil, fmt.Errorf("create ingest smux sender: %w", err)
		}
		return smuxIngestSender{address: cfg.IngestTCPAddr, tlsConfig: tlsCfg, sender: sender}, nil
	case config.IngestTransportQUIC:
		tlsCfg, err := ingestClientTLSConfig(cfg.MTLS)
		if err != nil {
			return nil, fmt.Errorf("load ingest quic client tls config: %w", err)
		}
		sender, err := ingestquic.NewSender(cfg.IngestQUICAddr, tlsCfg, ingestquic.WithConnectionPoolSize(cfg.IngestPoolSize))
		if err != nil {
			return nil, fmt.Errorf("create ingest quic sender: %w", err)
		}
		return quicIngestSender{sender: sender}, nil
	default:
		return nil, fmt.Errorf("unsupported ingest transport %q", cfg.IngestTransport)
	}
}

type ingestMetadata struct {
	StatusCode    int
	ContentType   string
	ContentLength int64
	ContentRange  string
	ETag          string
	LastModified  string
	AcceptRanges  string
}

func metadataForObject(obj *s3fetch.Object) ingestMetadata {
	status := obj.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	return ingestMetadata{StatusCode: status, ContentType: obj.ContentType, ContentLength: obj.ContentLength, ContentRange: obj.ContentRange, ETag: obj.ETag, LastModified: obj.LastModified, AcceptRanges: obj.AcceptRanges}
}

func metadataForStatus(status int) ingestMetadata {
	return ingestMetadata{StatusCode: status}
}

func (m ingestMetadata) setHeaders(h http.Header) {
	h.Set(ingest.StatusCodeHeader, strconv.Itoa(m.StatusCode))
	setSafeHeader(h, "Content-Type", m.ContentType)
	if m.ContentLength >= 0 {
		h.Set(ingest.ObjectContentLengthHeader, strconv.FormatInt(m.ContentLength, 10))
	}
	setSafeHeader(h, "Content-Range", m.ContentRange)
	setSafeHeader(h, "ETag", m.ETag)
	setSafeHeader(h, "Last-Modified", m.LastModified)
	setSafeHeader(h, "Accept-Ranges", m.AcceptRanges)
}

func (m ingestMetadata) pendingMetadata() pending.Metadata {
	metadata := pending.Metadata{StatusCode: m.StatusCode}
	metadata.ContentType = safeMetadataValue(m.ContentType)
	if m.ContentLength >= 0 {
		metadata.ContentLength = strconv.FormatInt(m.ContentLength, 10)
	}
	metadata.ContentRange = safeMetadataValue(m.ContentRange)
	metadata.ETag = safeMetadataValue(m.ETag)
	metadata.LastModified = safeMetadataValue(m.LastModified)
	metadata.AcceptRanges = safeMetadataValue(m.AcceptRanges)
	return metadata
}

func safeLogError(err error) string {
	if err == nil {
		return ""
	}
	return "redacted"
}

func setSafeHeader(h http.Header, name, value string) {
	value = safeMetadataValue(value)
	if value == "" {
		return
	}
	h.Set(name, value)
}

func safeMetadataValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return value
}

func statusForFetchError(err error) int {
	if errors.Is(err, s3fetch.ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, s3fetch.ErrInvalidRequest) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

func bucketAllowed(bucket string, allowlist []string) bool {
	for _, allowed := range allowlist {
		if bucket == allowed {
			return true
		}
	}
	return false
}

func validateIngestURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return errors.New("ticket ingest url must be an absolute https url without credentials")
	}
	return nil
}

func closeHTTPClient(client *http.Client) error {
	if client == nil {
		return nil
	}
	var closeErr error
	if closer, ok := client.Transport.(interface{ Close() error }); ok {
		closeErr = errors.Join(closeErr, closer.Close())
	}
	client.CloseIdleConnections()
	return closeErr
}

func ingestClientTLSConfig(paths config.MTLSPaths) (*tls.Config, error) {
	return mtls.ClientConfig(mtls.ClientOptions{Files: mtls.Files{CAFile: paths.CAFile, CertFile: paths.CertFile, KeyFile: paths.KeyFile, ServerName: paths.ServerName, InsecureSkipVerify: paths.InsecureSkipVerify}})
}

func ingestHTTP3Client(paths config.MTLSPaths, timeout time.Duration) (*http.Client, error) {
	var tlsCfg *tls.Config
	if paths.CAFile != "" || paths.CertFile != "" || paths.KeyFile != "" || paths.ServerName != "" || paths.InsecureSkipVerify {
		var err error
		tlsCfg, err = ingestClientTLSConfig(paths)
		if err != nil {
			return nil, fmt.Errorf("load ingest client tls config: %w", err)
		}
	}
	return &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig:    tlsCfg,
			QUICConfig:         &quic.Config{},
			DisableCompression: true,
		},
		Timeout: timeout,
	}, nil
}

func ingestHTTPClient(paths config.MTLSPaths, timeout time.Duration, disableHTTP2 bool, poolSize int) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.ReadBufferSize = ingestTransportBufferBytes
	transport.WriteBufferSize = ingestTransportBufferBytes
	transport.MaxConnsPerHost = poolSize
	transport.MaxIdleConnsPerHost = poolSize
	if transport.MaxIdleConns < poolSize {
		transport.MaxIdleConns = poolSize
	}
	if disableHTTP2 {
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	} else {
		transport.ForceAttemptHTTP2 = true
		transport.TLSNextProto = nil
	}
	if paths.CAFile != "" || paths.CertFile != "" || paths.KeyFile != "" || paths.ServerName != "" || paths.InsecureSkipVerify {
		tlsCfg, err := mtls.ClientConfig(mtls.ClientOptions{Files: mtls.Files{CAFile: paths.CAFile, CertFile: paths.CertFile, KeyFile: paths.KeyFile, ServerName: paths.ServerName, InsecureSkipVerify: paths.InsecureSkipVerify}})
		if err != nil {
			return nil, fmt.Errorf("load ingest client tls config: %w", err)
		}
		transport.TLSClientConfig = tlsCfg
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}
