package ingest

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/terion-name/air3/internal/pending"
)

const (
	TokenHeader               = "X-Air3-Ingest-Token"
	StatusCodeHeader          = "X-Air3-Status-Code"
	ObjectContentLengthHeader = "X-Air3-Content-Length"
	PathPrefix                = "/_ingest/"
)

type Handler struct {
	registry          *pending.Registry
	allowedIdentities map[string]struct{}
}

type Options struct {
	Registry                   *pending.Registry
	AllowedConnectorIdentities []string
}

func NewHandler(opts Options) (*Handler, error) {
	if opts.Registry == nil {
		return nil, errors.New("pending registry is required")
	}
	allowed := make(map[string]struct{}, len(opts.AllowedConnectorIdentities))
	for _, identity := range opts.AllowedConnectorIdentities {
		identity = strings.TrimSpace(identity)
		if identity != "" {
			allowed[identity] = struct{}{}
		}
	}
	return &Handler{registry: opts.Registry, allowedIdentities: allowed}, nil
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
	stream, err := h.registry.StartIngest(requestID, r.Header.Get(TokenHeader), metadata)
	if err != nil {
		http.Error(w, "ingest rejected", statusForPendingError(err))
		return
	}

	_, copyErr := io.Copy(stream, r.Body)
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
	if len(h.allowedIdentities) == 0 {
		return nil
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return errors.New("missing connector client certificate")
	}
	cert := r.TLS.PeerCertificates[0]
	for _, identity := range certificateIdentities(cert) {
		if _, ok := h.allowedIdentities[identity]; ok {
			return nil
		}
	}
	return errors.New("connector client certificate identity is not allowed")
}

func certificateIdentities(cert *x509.Certificate) []string {
	identities := make([]string, 0, 1+len(cert.DNSNames)+len(cert.EmailAddresses)+len(cert.URIs))
	if cert.Subject.CommonName != "" {
		identities = append(identities, cert.Subject.CommonName)
	}
	identities = append(identities, cert.DNSNames...)
	identities = append(identities, cert.EmailAddresses...)
	for _, uri := range cert.URIs {
		identities = append(identities, uri.String())
	}
	return identities
}

func metadataFromHeaders(h http.Header) (pending.Metadata, error) {
	contentLength := safeHeaderValue(h.Get(ObjectContentLengthHeader))
	if contentLength == "" {
		contentLength = safeHeaderValue(h.Get("Content-Length"))
	}
	metadata := pending.Metadata{
		ContentType:   safeHeaderValue(h.Get("Content-Type")),
		ContentLength: contentLength,
		ContentRange:  safeHeaderValue(h.Get("Content-Range")),
		ETag:          safeHeaderValue(h.Get("ETag")),
		LastModified:  safeHeaderValue(h.Get("Last-Modified")),
		AcceptRanges:  safeHeaderValue(h.Get("Accept-Ranges")),
	}
	if metadata.ContentLength != "" {
		length, err := strconv.ParseInt(metadata.ContentLength, 10, 64)
		if err != nil || length < 0 {
			return pending.Metadata{}, fmt.Errorf("invalid content length")
		}
	}
	if h.Get("Content-Type") != "" && metadata.ContentType == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe content type")
	}
	if h.Get("Content-Range") != "" && metadata.ContentRange == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe content range")
	}
	if h.Get("ETag") != "" && metadata.ETag == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe etag")
	}
	if h.Get("Last-Modified") != "" && metadata.LastModified == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe last modified")
	}
	if h.Get("Accept-Ranges") != "" && metadata.AcceptRanges == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe accept ranges")
	}

	statusText := safeHeaderValue(h.Get(StatusCodeHeader))
	if h.Get(StatusCodeHeader) != "" && statusText == "" {
		return pending.Metadata{}, fmt.Errorf("unsafe status code")
	}
	if statusText != "" {
		status, err := strconv.Atoi(statusText)
		if err != nil || status < 100 || status > 599 {
			return pending.Metadata{}, fmt.Errorf("invalid status code")
		}
		metadata.StatusCode = status
	}
	return metadata, nil
}

func safeHeaderValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return value
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
