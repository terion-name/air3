package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/ingestsmux"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/mtls"
	"github.com/terion-name/air3/internal/natsclient"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/signing"
	"github.com/terion-name/air3/internal/tickets"
)

type ticketPublisher interface {
	PublishTicket(context.Context, tickets.Ticket) error
}

type edgeServer struct {
	cfg       config.EdgeConfig
	registry  *pending.Registry
	publisher ticketPublisher
	logger    *slog.Logger
	now       func() time.Time
	newToken  func() (string, error)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("edge gateway stopped", "error", safeLogError(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.LoadEdgeFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsCtx, cancelNATS := context.WithTimeout(ctx, 10*time.Second)
	defer cancelNATS()
	publisher, err := natsclient.Connect(natsCtx, cfg.NATS)
	if err != nil {
		return err
	}
	defer publisher.Close()

	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(cfg, reg, publisher, logger)
	ingestHandler, err := ingest.NewHandler(ingest.Options{Registry: reg, AllowedConnectorIdentities: cfg.AllowedConnectorIdentities, StreamCopyBufferBytes: cfg.StreamCopyBufferBytes})
	if err != nil {
		return err
	}

	publicServer := &http.Server{Addr: cfg.PublicListenAddr, Handler: edge}
	ingestServer := &http.Server{Addr: cfg.IngestListenAddr, Handler: ingestHandler}
	var tlsCfg *tls.Config
	if tlsConfigured(cfg.MTLS) || cfg.IngestTransport.UsesTCPIngestAddr() {
		tlsCfg, err = edgeServerTLSConfig(cfg.MTLS)
		if err != nil {
			return fmt.Errorf("load ingest tls config: %w", err)
		}
	}
	if tlsConfigured(cfg.MTLS) {
		ingestServer.TLSConfig = tlsCfg
		publicServer.TLSConfig = publicTLSConfig(tlsCfg)
	}
	ingestListener, err := newNonHTTPIngestListener(cfg, reg, tlsCfg)
	if err != nil {
		return err
	}
	if ingestListener != nil {
		defer ingestListener.Close()
	}

	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	serverCount := 2
	if ingestListener != nil {
		serverCount++
	}
	errCh := make(chan error, serverCount)
	go func() { errCh <- serveHTTP(publicServer, cfg.MTLS) }()
	go func() { errCh <- serveHTTP(ingestServer, cfg.MTLS) }()
	if ingestListener != nil {
		go func() { errCh <- ingestListener.Serve(serveCtx) }()
	}

	select {
	case <-ctx.Done():
		cancelServe()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = publicServer.Shutdown(shutdownCtx)
		_ = ingestServer.Shutdown(shutdownCtx)
		if ingestListener != nil {
			_ = ingestListener.Close()
		}
		return nil
	case err := <-errCh:
		cancelServe()
		_ = publicServer.Close()
		_ = ingestServer.Close()
		if ingestListener != nil {
			_ = ingestListener.Close()
		}
		return err
	}
}

func newEdgeServer(cfg config.EdgeConfig, reg *pending.Registry, publisher ticketPublisher, logger *slog.Logger) *edgeServer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &edgeServer{cfg: cfg, registry: reg, publisher: publisher, logger: logger, now: time.Now, newToken: randomToken}
}

func (s *edgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	object, err := s.validatePublicRequest(r)
	if err != nil {
		writePublicError(w, statusForValidationError(err), statusText(statusForValidationError(err)))
		return
	}

	reqID, err := s.newToken()
	if err != nil {
		writePublicError(w, http.StatusInternalServerError, "request setup failed")
		return
	}
	ingestToken, err := s.newToken()
	if err != nil {
		writePublicError(w, http.StatusInternalServerError, "request setup failed")
		return
	}
	deadline := s.now().Add(s.cfg.Timeouts.PendingRequestTTL)
	ingestURL, err := ingestURLForRequest(s.cfg.IngestURL, reqID)
	if err != nil {
		writePublicError(w, http.StatusInternalServerError, "request setup failed")
		return
	}

	sink := newResponseSink(w, r.Method, r.Context())
	pendingReq := pending.Request{ID: reqID, Deadline: deadline, IngestToken: ingestToken, Method: r.Method, Bucket: object.bucket, Key: object.key, Range: object.rangeHeader}
	if err := s.registry.Register(pendingReq, sink); err != nil {
		writePublicError(w, http.StatusInternalServerError, "request setup failed")
		return
	}
	defer s.registry.Cancel(reqID, pending.ErrCanceled)

	ticket := tickets.Ticket{Version: tickets.Version, RequestID: reqID, Bucket: object.bucket, Key: object.key, Method: r.Method, Range: object.rangeHeader, DeadlineUnixMS: deadline.UnixMilli(), IngestURL: ingestURL, IngestToken: ingestToken, TraceID: reqID}
	publishCtx, cancelPublish := context.WithDeadline(r.Context(), deadline)
	err = s.publisher.PublishTicket(publishCtx, ticket)
	cancelPublish()
	if err != nil {
		s.logger.Warn("ticket publish failed", "request_id", reqID, "error", safeLogError(err))
		writePublicError(w, http.StatusServiceUnavailable, "backend unavailable")
		return
	}

	waitCtx, cancelWait := context.WithDeadline(r.Context(), deadline)
	defer cancelWait()
	if err := sink.Wait(waitCtx); err != nil {
		s.registry.Cancel(reqID, err)
		if errors.Is(err, context.Canceled) {
			s.logger.Info("public request canceled", "request_id", reqID)
			return
		}
		if sink.Started() {
			s.logger.Warn("public response stream failed", "request_id", reqID, "error", safeLogError(err))
			return
		}
		status := statusForWaitError(err)
		writePublicError(w, status, statusText(status))
		return
	}
}

type publicObject struct {
	bucket      string
	key         string
	rangeHeader string
}

func (s *edgeServer) validatePublicRequest(r *http.Request) (publicObject, error) {
	bucket, key, err := objectFromPath(r.URL.EscapedPath())
	if err != nil {
		return publicObject{}, err
	}
	if !bucketAllowed(bucket, s.cfg.AllowedBuckets) {
		return publicObject{}, errForbidden
	}

	queryRange := ""
	if !s.cfg.Signing.Disabled {
		claims, err := signing.ValidateURL(r.Method, r.URL.RequestURI(), signing.ValidationConfig{Secret: s.cfg.Signing.Secret}, s.now())
		if err != nil {
			return publicObject{}, errUnauthorized
		}
		if claims.Bucket != bucket || claims.Key != key || claims.Method != r.Method {
			return publicObject{}, errUnauthorized
		}
		queryRange = claims.Range
	}

	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	if !s.cfg.Signing.Disabled && rangeHeader != "" && queryRange == "" {
		return publicObject{}, errUnauthorized
	}
	if queryRange != "" {
		if rangeHeader != "" && rangeHeader != queryRange {
			return publicObject{}, errBadRequest
		}
		rangeHeader = queryRange
	}
	if rangeHeader != "" && !validRange(bucket, key, r.Method, rangeHeader, s.now()) {
		return publicObject{}, errBadRequest
	}
	return publicObject{bucket: bucket, key: key, rangeHeader: rangeHeader}, nil
}

var (
	errBadRequest   = errors.New("bad public request")
	errForbidden    = errors.New("object not allowed")
	errUnauthorized = errors.New("unauthorized public request")
)

func objectFromPath(escapedPath string) (string, string, error) {
	cleaned := strings.TrimPrefix(path.Clean("/"+escapedPath), "/")
	parts := strings.SplitN(cleaned, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errBadRequest
	}
	bucket, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", errBadRequest
	}
	key, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", errBadRequest
	}
	if err := tickets.ValidateBucket(bucket); err != nil {
		return "", "", errBadRequest
	}
	if err := tickets.ValidateKey(key); err != nil {
		return "", "", errBadRequest
	}
	return bucket, key, nil
}

func validRange(bucket, key, method, rangeHeader string, now time.Time) bool {
	t := tickets.Ticket{Version: tickets.Version, RequestID: "range-check", Bucket: bucket, Key: key, Method: method, Range: rangeHeader, DeadlineUnixMS: now.Add(time.Minute).UnixMilli(), IngestURL: "https://edge.invalid/_ingest/range-check", IngestToken: "range-check-token"}
	return t.Validate(now) == nil
}

func bucketAllowed(bucket string, allowlist []string) bool {
	for _, allowed := range allowlist {
		if bucket == allowed {
			return true
		}
	}
	return false
}

func ingestURLForRequest(base, requestID string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return "", errors.New("ingest url must be an absolute https url without credentials")
	}
	prefix := strings.TrimSuffix(u.Path, "/")
	if prefix == "" || prefix == "/" {
		prefix = strings.TrimSuffix(ingest.PathPrefix, "/")
	}
	if !strings.HasSuffix(prefix, strings.TrimSuffix(ingest.PathPrefix, "/")) {
		prefix = strings.TrimSuffix(ingest.PathPrefix, "/")
	}
	u.Path = prefix + "/" + url.PathEscape(requestID)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
func statusForValidationError(err error) int {
	switch {
	case errors.Is(err, errForbidden):
		return http.StatusForbidden
	case errors.Is(err, errUnauthorized):
		return http.StatusForbidden
	default:
		return http.StatusBadRequest
	}
}

func statusForWaitError(err error) int {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, pending.ErrExpired) {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

func statusText(status int) string {
	text := http.StatusText(status)
	if text == "" {
		return "request failed"
	}
	return strings.ToLower(text)
}

func safeLogError(err error) string {
	if err == nil {
		return ""
	}
	return "redacted"
}

func writePublicError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

func randomToken() (string, error) {
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func edgeServerTLSConfig(paths config.MTLSPaths) (*tls.Config, error) {
	return mtls.ServerConfig(mtls.ServerOptions{
		Files: mtls.Files{
			CAFile:   paths.CAFile,
			CertFile: paths.CertFile,
			KeyFile:  paths.KeyFile,
		},
		RequireClientCert: paths.CAFile != "",
	})
}

type nonHTTPIngestListener interface {
	Serve(context.Context) error
	Close() error
}

type tcpIngestListener struct {
	server   *ingesttcp.Server
	listener net.Listener
}

type smuxIngestListener struct {
	server   *ingestsmux.Server
	listener net.Listener
}

func newNonHTTPIngestListener(cfg config.EdgeConfig, reg *pending.Registry, tlsCfg *tls.Config) (nonHTTPIngestListener, error) {
	switch cfg.IngestTransport {
	case config.IngestTransportTCP:
		return newTCPIngestListener(cfg, reg, tlsCfg)
	case config.IngestTransportSMUX:
		return newSMUXIngestListener(cfg, reg, tlsCfg)
	default:
		return nil, nil
	}
}

func newTCPIngestListener(cfg config.EdgeConfig, reg *pending.Registry, tlsCfg *tls.Config) (*tcpIngestListener, error) {
	if !cfg.IngestTransport.IsTCP() {
		return nil, nil
	}
	server, err := ingesttcp.NewServer(ingesttcp.ServerOptions{
		Registry:                   reg,
		TLSConfig:                  tlsCfg,
		AllowedConnectorIdentities: cfg.AllowedConnectorIdentities,
		CopyBufferBytes:            cfg.StreamCopyBufferBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingest tcp server: %w", err)
	}
	ln, err := net.Listen("tcp", cfg.IngestTCPListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen ingest tcp: %w", err)
	}
	return &tcpIngestListener{server: server, listener: ln}, nil
}

func newSMUXIngestListener(cfg config.EdgeConfig, reg *pending.Registry, tlsCfg *tls.Config) (*smuxIngestListener, error) {
	if cfg.IngestTransport != config.IngestTransportSMUX {
		return nil, nil
	}
	server, err := ingestsmux.NewServer(ingestsmux.ServerOptions{
		Registry:                   reg,
		TLSConfig:                  tlsCfg,
		AllowedConnectorIdentities: cfg.AllowedConnectorIdentities,
		CopyBufferBytes:            cfg.StreamCopyBufferBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingest smux server: %w", err)
	}
	ln, err := net.Listen("tcp", cfg.IngestTCPListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen ingest smux: %w", err)
	}
	return &smuxIngestListener{server: server, listener: ln}, nil
}

func (l *tcpIngestListener) Serve(ctx context.Context) error {
	return ignoreTCPServerClosed(l.server.Serve(ctx, l.listener))
}

func (l *tcpIngestListener) Close() error {
	return l.listener.Close()
}

func (l *smuxIngestListener) Serve(ctx context.Context) error {
	return ignoreTCPServerClosed(l.server.Serve(ctx, l.listener))
}

func (l *smuxIngestListener) Close() error {
	return l.listener.Close()
}

func ignoreTCPServerClosed(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func serveHTTP(server *http.Server, paths config.MTLSPaths) error {
	if server.TLSConfig != nil {
		return ignoreServerClosed(server.ListenAndServeTLS("", ""))
	}
	if paths.CertFile != "" || paths.KeyFile != "" {
		return ignoreServerClosed(server.ListenAndServeTLS(paths.CertFile, paths.KeyFile))
	}
	return ignoreServerClosed(server.ListenAndServe())
}

func ignoreServerClosed(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func tlsConfigured(paths config.MTLSPaths) bool {
	return paths.CertFile != "" || paths.KeyFile != "" || paths.CAFile != ""
}

func publicTLSConfig(source *tls.Config) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: source.Certificates}
}
