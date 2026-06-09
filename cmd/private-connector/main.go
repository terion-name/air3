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
	"syscall"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
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

type connector struct {
	cfg     config.ConnectorConfig
	fetcher objectFetcher
	sender  ingestSender
	logger  *slog.Logger
	now     func() time.Time
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
	worker := newConnector(cfg, fetcher, sender, logger)

	natsCtx, cancelNATS := context.WithTimeout(ctx, 10*time.Second)
	defer cancelNATS()
	nats, err := natsclient.Connect(natsCtx, cfg.NATS)
	if err != nil {
		return err
	}
	defer nats.Close()

	sub, err := nats.QueueSubscribeTickets(ctx, worker.handleTicket, func(err error) {
		logger.Warn("ticket handling failed", "error", safeLogError(err))
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	_ = sub.Drain(drainCtx)
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

type tcpIngestSender struct {
	address     string
	tlsConfig   *tls.Config
	dialAndSend func(context.Context, string, string, *tls.Config, ingesttcp.ClientRequest) error
}

func (s tcpIngestSender) Send(ctx context.Context, ticket tickets.Ticket, metadata ingestMetadata, body io.Reader) error {
	bodyLength := int64(0)
	if body == nil {
		body = http.NoBody
	} else if body != http.NoBody {
		bodyLength = ingesttcp.UnknownBodyLength
		if metadata.ContentLength >= 0 {
			bodyLength = metadata.ContentLength
		}
	}

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

func newIngestSender(cfg config.ConnectorConfig) (ingestSender, error) {
	switch cfg.IngestTransport {
	case config.IngestTransportHTTP:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, cfg.IngestDisableHTTP2)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportHTTP1:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, true)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportHTTP2:
		client, err := ingestHTTPClient(cfg.MTLS, cfg.Timeouts.StreamTimeout, false)
		if err != nil {
			return nil, err
		}
		return httpIngestSender{client: client}, nil
	case config.IngestTransportTCP:
		tlsCfg, err := mtls.ClientConfig(mtls.ClientOptions{Files: mtls.Files{CAFile: cfg.MTLS.CAFile, CertFile: cfg.MTLS.CertFile, KeyFile: cfg.MTLS.KeyFile, ServerName: cfg.MTLS.ServerName, InsecureSkipVerify: cfg.MTLS.InsecureSkipVerify}})
		if err != nil {
			return nil, fmt.Errorf("load ingest tcp client tls config: %w", err)
		}
		return tcpIngestSender{address: cfg.IngestTCPAddr, tlsConfig: tlsCfg}, nil
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

func ingestHTTPClient(paths config.MTLSPaths, timeout time.Duration, disableHTTP2 bool) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.ReadBufferSize = ingestTransportBufferBytes
	transport.WriteBufferSize = ingestTransportBufferBytes
	if transport.MaxIdleConnsPerHost < 32 {
		transport.MaxIdleConnsPerHost = 32
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
