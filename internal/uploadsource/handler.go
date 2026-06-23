package uploadsource

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/terion-name/air3/internal/ingest"
)

const defaultStreamCopyBufferBytes = 32 * 1024

type HandlerOptions struct {
	Registry                   *Registry
	AllowedConnectorIdentities []string
	StreamCopyBufferBytes      int
}

type Handler struct {
	registry              *Registry
	authorizer            ingest.ConnectorAuthorizer
	streamCopyBufferBytes int
}

func NewHandler(opts HandlerOptions) (*Handler, error) {
	if opts.Registry == nil {
		return nil, errors.New("upload source registry is required")
	}
	streamCopyBufferBytes := opts.StreamCopyBufferBytes
	if streamCopyBufferBytes <= 0 {
		streamCopyBufferBytes = defaultStreamCopyBufferBytes
	}
	return &Handler{
		registry:              opts.Registry,
		authorizer:            ingest.NewConnectorAuthorizer(opts.AllowedConnectorIdentities),
		streamCopyBufferBytes: streamCopyBufferBytes,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestID, ok := requestIDFromPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if err := h.authorizePeer(r); err != nil {
		http.Error(w, "unauthorized connector", http.StatusUnauthorized)
		return
	}

	claim, err := h.registry.Claim(requestID, r.Header.Get(TokenHeader))
	if err != nil {
		http.Error(w, "upload source rejected", statusForError(err))
		return
	}

	if contentLength := claim.ContentLength(); contentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*contentLength, 10))
	}
	if contentType := claim.ContentType(); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	tracked := &trackingResponseWriter{ResponseWriter: w}
	ctxDone := make(chan struct{})
	defer close(ctxDone)
	go func() {
		select {
		case <-r.Context().Done():
			_ = claim.CloseWithError(r.Context().Err())
		case <-ctxDone:
		}
	}()

	var copyErr error
	defer func() { _ = claim.CloseWithError(copyErr) }()
	_, copyErr = io.CopyBuffer(tracked, claim, make([]byte, h.streamCopyBufferBytes))
	if copyErr != nil && tracked.bytes == 0 {
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Type")
		http.Error(w, "upload source stream failed", http.StatusBadGateway)
	}
}

func requestIDFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, PathPrefix) {
		return "", false
	}
	id := strings.TrimPrefix(path, PathPrefix)
	if id == "" || strings.Contains(id, "/") || !safeToken(id) {
		return "", false
	}
	return id, true
}

func (h *Handler) authorizePeer(r *http.Request) error {
	if r.TLS == nil {
		return h.authorizer.AuthorizePeerCertificates(nil)
	}
	return h.authorizer.AuthorizePeerCertificates(r.TLS.PeerCertificates)
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, ErrInvalidToken):
		return http.StatusUnauthorized
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrExpired), errors.Is(err, ErrCanceled), errors.Is(err, ErrReplayed):
		return http.StatusConflict
	case errors.Is(err, ErrInvalidSource):
		return http.StatusBadRequest
	default:
		return http.StatusBadGateway
	}
}

type trackingResponseWriter struct {
	http.ResponseWriter
	bytes int64
}

func (w *trackingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}
