package ingest

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/terion-name/air3/internal/pending"
)

const (
	TokenHeader                  = "X-Air3-Ingest-Token"
	StatusCodeHeader             = "X-Air3-Status-Code"
	ObjectContentLengthHeader    = "X-Air3-Content-Length"
	PathPrefix                   = "/_ingest/"
	defaultStreamCopyBufferBytes = 32 * 1024
)

type Handler struct {
	registry              *pending.Registry
	authorizer            ConnectorAuthorizer
	streamCopyBufferBytes int
}

type Options struct {
	Registry                   *pending.Registry
	AllowedConnectorIdentities []string
	StreamCopyBufferBytes      int
}

func NewHandler(opts Options) (*Handler, error) {
	if opts.Registry == nil {
		return nil, errors.New("pending registry is required")
	}
	streamCopyBufferBytes := opts.StreamCopyBufferBytes
	if streamCopyBufferBytes <= 0 {
		streamCopyBufferBytes = defaultStreamCopyBufferBytes
	}
	return &Handler{
		registry:              opts.Registry,
		authorizer:            NewConnectorAuthorizer(opts.AllowedConnectorIdentities),
		streamCopyBufferBytes: streamCopyBufferBytes,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
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

	metadata, err := metadataFromHeaders(r.Header)
	if err != nil {
		http.Error(w, "invalid ingest metadata", http.StatusBadRequest)
		return
	}
	if metadata.ContentLength == "" && r.ContentLength >= 0 {
		metadata.ContentLength = strconv.FormatInt(r.ContentLength, 10)
	}
	metadata, err = ValidateMetadata(metadata)
	if err != nil {
		http.Error(w, "invalid ingest metadata", http.StatusBadRequest)
		return
	}
	stream, err := h.registry.StartIngest(requestID, r.Header.Get(TokenHeader), metadata)
	if err != nil {
		status := statusForPendingError(err)
		if !isPendingRejection(err) {
			status = http.StatusBadGateway
		}
		http.Error(w, "ingest rejected", status)
		return
	}

	_, copyErr := io.CopyBuffer(stream, r.Body, make([]byte, h.streamCopyBufferBytes))
	if err := stream.CloseWithError(copyErr); err != nil && copyErr == nil {
		copyErr = err
	}
	if copyErr != nil {
		http.Error(w, "ingest stream failed", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requestIDFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, PathPrefix) {
		return "", false
	}
	id := strings.TrimPrefix(path, PathPrefix)
	if id == "" || strings.Contains(id, "/") {
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

func metadataFromHeaders(h http.Header) (pending.Metadata, error) {
	contentLength := h.Get(ObjectContentLengthHeader)
	if strings.ContainsAny(contentLength, "\r\n") {
		return pending.Metadata{}, fmt.Errorf("unsafe content length")
	}
	if strings.TrimSpace(contentLength) == "" {
		contentLength = h.Get("Content-Length")
	}
	metadata := pending.Metadata{
		ContentType:   h.Get("Content-Type"),
		ContentLength: contentLength,
		ContentRange:  h.Get("Content-Range"),
		ETag:          h.Get("ETag"),
		LastModified:  h.Get("Last-Modified"),
		AcceptRanges:  h.Get("Accept-Ranges"),
	}

	statusText := h.Get(StatusCodeHeader)
	if statusText != "" {
		if strings.ContainsAny(statusText, "\r\n") {
			return pending.Metadata{}, fmt.Errorf("unsafe status code")
		}
		statusText = strings.TrimSpace(statusText)
		if statusText == "" {
			return pending.Metadata{}, fmt.Errorf("invalid status code")
		}
		status, err := strconv.Atoi(statusText)
		if err != nil {
			return pending.Metadata{}, fmt.Errorf("invalid status code")
		}
		metadata.StatusCode = status
	}
	return metadata, nil
}

func isPendingRejection(err error) bool {
	return errors.Is(err, pending.ErrInvalidToken) ||
		errors.Is(err, pending.ErrExpired) ||
		errors.Is(err, pending.ErrCanceled) ||
		errors.Is(err, pending.ErrReplayed) ||
		errors.Is(err, pending.ErrNotFound) ||
		errors.Is(err, pending.ErrInvalidRequest)
}

func statusForPendingError(err error) int {
	switch {
	case errors.Is(err, pending.ErrInvalidToken):
		return http.StatusUnauthorized
	case errors.Is(err, pending.ErrExpired), errors.Is(err, pending.ErrCanceled), errors.Is(err, pending.ErrReplayed):
		return http.StatusConflict
	case errors.Is(err, pending.ErrNotFound):
		return http.StatusNotFound
	default:
		return http.StatusBadRequest
	}
}
