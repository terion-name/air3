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
	"strconv"
	"strings"
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
	"github.com/terion-name/air3/internal/publicpath"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/signing"
	"github.com/terion-name/air3/internal/tickets"
)

type ticketPublisher interface {
	PublishTicketTo(context.Context, string, tickets.Ticket) error
}

type objectFetcher interface {
	Fetch(context.Context, s3fetch.Request) (*s3fetch.Object, error)
}

type edgeServer struct {
	cfg            config.EdgeConfig
	registry       *pending.Registry
	publisher      ticketPublisher
	directFetchers map[string]objectFetcher
	logger         *slog.Logger
	now            func() time.Time
	newToken       func() (string, error)
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

	directFetchers, err := newDirectFetchers(ctx, cfg.DirectServers)
	if err != nil {
		return err
	}

	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(cfg, reg, publisher, logger, directFetchers)
	ingestHandler, err := ingest.NewHandler(ingest.Options{Registry: reg, AllowedConnectorIdentities: cfg.AllowedConnectorIdentities, StreamCopyBufferBytes: cfg.StreamCopyBufferBytes})
	if err != nil {
		return err
	}

	publicServer := &http.Server{Addr: cfg.PublicListenAddr, Handler: edge}
	var tlsCfg *tls.Config
	if ingestTLSRequired(cfg) {
		tlsCfg, err = edgeServerTLSConfig(cfg.MTLS)
		if err != nil {
			return fmt.Errorf("load ingest tls config: %w", err)
		}
	}
	if tlsConfigured(cfg.MTLS) {
		publicServer.TLSConfig = publicTLSConfig(tlsCfg)
	}
	ingestServer := newIngestHTTPServer(cfg, ingestHandler, tlsCfg)
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
	go func() { errCh <- ingestServer.Serve() }()
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

func newDirectFetchers(ctx context.Context, directServers map[string]config.S3Config) (map[string]objectFetcher, error) {
	if len(directServers) == 0 {
		return nil, nil
	}
	fetchers := make(map[string]objectFetcher, len(directServers))
	for alias, s3cfg := range directServers {
		fetcher, err := s3fetch.New(ctx, s3cfg)
		if err != nil {
			return nil, fmt.Errorf("create direct fetcher for %q: %w", alias, err)
		}
		fetchers[alias] = fetcher
	}
	return fetchers, nil
}

func newEdgeServer(cfg config.EdgeConfig, reg *pending.Registry, publisher ticketPublisher, logger *slog.Logger, directFetchers ...map[string]objectFetcher) *edgeServer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	var fetchers map[string]objectFetcher
	if len(directFetchers) > 0 {
		fetchers = directFetchers[0]
	}
	return &edgeServer{cfg: cfg, registry: reg, publisher: publisher, directFetchers: fetchers, logger: logger, now: time.Now, newToken: randomToken}
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
	if fetcher, ok := s.directFetcherFor(object.server); ok {
		s.serveDirect(w, r, object, fetcher)
		return
	}
	if !bucketAllowed(object.bucket, s.cfg.AllowedBuckets) {
		writePublicError(w, http.StatusForbidden, statusText(http.StatusForbidden))
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

	ticket := tickets.Ticket{Version: tickets.Version, RequestID: reqID, Bucket: object.bucket, Key: object.key, Method: r.Method, Range: object.rangeHeader, Server: object.server, DeadlineUnixMS: deadline.UnixMilli(), IngestURL: ingestURL, IngestToken: ingestToken, TraceID: reqID}
	subject, err := s.ticketSubject(object.server)
	if err != nil {
		s.registry.Cancel(reqID, err)
		writePublicError(w, http.StatusInternalServerError, "request setup failed")
		return
	}
	publishCtx, cancelPublish := context.WithDeadline(r.Context(), deadline)
	err = s.publisher.PublishTicketTo(publishCtx, subject, ticket)
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
	server      string
	bucket      string
	key         string
	rangeHeader string
}

func edgeMode(cfg config.EdgeConfig) publicpath.Mode {
	if cfg.MultiServer {
		return publicpath.ModeMulti
	}
	return publicpath.ModeSingle
}

func (s *edgeServer) validatePublicRequest(r *http.Request) (publicObject, error) {
	parsed, err := publicpath.ParseEscapedPath(r.URL.EscapedPath(), edgeMode(s.cfg))
	if err != nil {
		return publicObject{}, errBadRequest
	}
	if err := tickets.ValidateBucket(parsed.Bucket); err != nil {
		return publicObject{}, errBadRequest
	}
	if err := tickets.ValidateKey(parsed.Key); err != nil {
		return publicObject{}, errBadRequest
	}

	queryRange := ""
	if !s.cfg.Signing.Disabled {
		claims, err := signing.ValidateURLForMode(r.Method, r.URL.RequestURI(), signing.ValidationConfig{Secret: s.cfg.Signing.Secret}, s.now(), edgeMode(s.cfg))
		if err != nil {
			return publicObject{}, errUnauthorized
		}
		if claims.Bucket != parsed.Bucket || claims.Key != parsed.Key || claims.Method != r.Method {
			return publicObject{}, errUnauthorized
		}
		if s.cfg.MultiServer && claims.Server != parsed.Server {
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
	if rangeHeader != "" && !validRange(parsed.Bucket, parsed.Key, r.Method, rangeHeader, s.now()) {
		return publicObject{}, errBadRequest
	}
	return publicObject{server: parsed.Server, bucket: parsed.Bucket, key: parsed.Key, rangeHeader: rangeHeader}, nil
}

var (
	errBadRequest   = errors.New("bad public request")
	errUnauthorized = errors.New("unauthorized public request")
)

func (s *edgeServer) directFetcherFor(server string) (objectFetcher, bool) {
	if !s.cfg.MultiServer || server == "" {
		return nil, false
	}
	if _, ok := s.cfg.DirectServers[server]; !ok {
		return nil, false
	}
	return s.directFetchers[server], true
}

func (s *edgeServer) ticketSubject(server string) (string, error) {
	if s.cfg.MultiServer {
		return config.DeriveNATSSubject(s.cfg.NATS.SubjectTemplate, server)
	}
	return s.cfg.NATS.Subject, nil
}

func (s *edgeServer) serveDirect(w http.ResponseWriter, r *http.Request, object publicObject, fetcher objectFetcher) {
	directCfg, ok := s.cfg.DirectServers[object.server]
	if !ok || fetcher == nil {
		writePublicError(w, http.StatusServiceUnavailable, "backend unavailable")
		return
	}
	if !bucketAllowed(object.bucket, directCfg.AllowedBuckets) {
		writePublicError(w, http.StatusForbidden, statusText(http.StatusForbidden))
		return
	}

	fetched, err := fetcher.Fetch(r.Context(), s3fetch.Request{Method: r.Method, Bucket: object.bucket, Key: object.key, Range: object.rangeHeader})
	if err != nil {
		s.writeDirectFetchError(w, object.server, err)
		return
	}
	if fetched == nil {
		s.logger.Warn("direct fetch returned nil object", "server", object.server)
		writePublicError(w, http.StatusServiceUnavailable, "backend unavailable")
		return
	}
	if fetched.Body != nil {
		defer fetched.Body.Close()
	}

	metadata := directMetadata(fetched)
	copyPublicMetadata(w.Header(), metadata)
	w.WriteHeader(publicStatusCode(metadata))
	if r.Method == http.MethodHead || fetched.Body == nil {
		return
	}
	if _, err := io.Copy(w, fetched.Body); err != nil {
		s.logger.Warn("direct response stream failed", "server", object.server, "error", safeLogError(err))
	}
}

func (s *edgeServer) writeDirectFetchError(w http.ResponseWriter, server string, err error) {
	switch {
	case errors.Is(err, s3fetch.ErrNotFound):
		writePublicError(w, http.StatusNotFound, statusText(http.StatusNotFound))
	case errors.Is(err, s3fetch.ErrInvalidRequest):
		writePublicError(w, http.StatusBadRequest, statusText(http.StatusBadRequest))
	default:
		s.logger.Warn("direct fetch failed", "server", server, "error", safeLogError(err))
		writePublicError(w, http.StatusServiceUnavailable, "backend unavailable")
	}
}

func directMetadata(object *s3fetch.Object) pending.Metadata {
	metadata := pending.Metadata{
		StatusCode:   object.StatusCode,
		ContentType:  object.ContentType,
		ContentRange: object.ContentRange,
		ETag:         object.ETag,
		LastModified: object.LastModified,
		AcceptRanges: object.AcceptRanges,
	}
	if object.ContentLength >= 0 {
		metadata.ContentLength = strconv.FormatInt(object.ContentLength, 10)
	}
	return metadata
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
	if errors.Is(err, errUnauthorized) {
		return http.StatusForbidden
	}
	return http.StatusBadRequest
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

type ingestHTTPServer interface {
	Serve() error
	Shutdown(context.Context) error
	Close() error
}

type standardIngestHTTPServer struct {
	server *http.Server
	mtls   config.MTLSPaths
}

type http3IngestServer struct {
	server *http3.Server
}

func newIngestHTTPServer(cfg config.EdgeConfig, handler http.Handler, tlsCfg *tls.Config) ingestHTTPServer {
	if cfg.IngestTransport == config.IngestTransportHTTP3 {
		return &http3IngestServer{server: &http3.Server{Addr: cfg.IngestListenAddr, Handler: handler, TLSConfig: tlsCfg}}
	}

	server := &http.Server{Addr: cfg.IngestListenAddr, Handler: handler}
	if tlsConfigured(cfg.MTLS) {
		server.TLSConfig = tlsCfg
	}
	return &standardIngestHTTPServer{server: server, mtls: cfg.MTLS}
}

func (s *standardIngestHTTPServer) Serve() error {
	return serveHTTP(s.server, s.mtls)
}

func (s *standardIngestHTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *standardIngestHTTPServer) Close() error {
	return s.server.Close()
}

func (s *http3IngestServer) Serve() error {
	return ignoreServerClosed(s.server.ListenAndServe())
}

func (s *http3IngestServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *http3IngestServer) Close() error {
	return s.server.Close()
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

type quicIngestListener struct {
	server   *ingestquic.Server
	listener *quic.Listener
}

func newNonHTTPIngestListener(cfg config.EdgeConfig, reg *pending.Registry, tlsCfg *tls.Config) (nonHTTPIngestListener, error) {
	switch cfg.IngestTransport {
	case config.IngestTransportTCP:
		return newTCPIngestListener(cfg, reg, tlsCfg)
	case config.IngestTransportSMUX:
		return newSMUXIngestListener(cfg, reg, tlsCfg)
	case config.IngestTransportQUIC:
		return newQUICIngestListener(cfg, reg, tlsCfg)
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

func newQUICIngestListener(cfg config.EdgeConfig, reg *pending.Registry, tlsCfg *tls.Config) (*quicIngestListener, error) {
	if cfg.IngestTransport != config.IngestTransportQUIC {
		return nil, nil
	}
	tlsCfg = rawQUICTLSConfig(tlsCfg)
	server, err := ingestquic.NewServer(ingestquic.ServerOptions{
		Registry:                   reg,
		TLSConfig:                  tlsCfg,
		AllowedConnectorIdentities: cfg.AllowedConnectorIdentities,
		CopyBufferBytes:            cfg.StreamCopyBufferBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingest quic server: %w", err)
	}
	ln, err := quic.ListenAddr(cfg.IngestQUICListenAddr, tlsCfg, nil)
	if err != nil {
		return nil, fmt.Errorf("listen ingest quic: %w", err)
	}
	return &quicIngestListener{server: server, listener: ln}, nil
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

func (l *quicIngestListener) Serve(ctx context.Context) error {
	return ignoreTCPServerClosed(l.server.Serve(ctx, l.listener))
}

func (l *quicIngestListener) Close() error {
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

func ingestTLSRequired(cfg config.EdgeConfig) bool {
	return tlsConfigured(cfg.MTLS) || cfg.IngestTransport.UsesTCPIngestAddr() || cfg.IngestTransport.UsesQUICIngestAddr() || cfg.IngestTransport == config.IngestTransportHTTP3
}

// rawQUICALPN mirrors internal/ingestquic's private ALPN so the edge-owned
// listener can be created with the protocol quic-go expects during handshake.
const rawQUICALPN = "air3-ingest-quic/1"

func rawQUICTLSConfig(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	clone := config.Clone()
	for _, proto := range clone.NextProtos {
		if proto == rawQUICALPN {
			return clone
		}
	}
	clone.NextProtos = append([]string{rawQUICALPN}, clone.NextProtos...)
	return clone
}

func tlsConfigured(paths config.MTLSPaths) bool {
	return paths.CertFile != "" || paths.KeyFile != "" || paths.CAFile != ""
}

func publicTLSConfig(source *tls.Config) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: source.Certificates}
}
