package uploadsource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewHandlerRequiresRegistry(t *testing.T) {
	if _, err := NewHandler(HandlerOptions{}); err == nil {
		t.Fatal("NewHandler() error = nil, want error")
	}
}

func TestHandlerMethodAndPathValidation(t *testing.T) {
	h := newTestHandler(t, newTestRegistry(t), nil)

	methodReq := httptest.NewRequest(http.MethodPost, PathPrefix+"req", nil)
	methodResp := httptest.NewRecorder()
	h.ServeHTTP(methodResp, methodReq)
	if methodResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", methodResp.Code)
	}
	if got := methodResp.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", got)
	}

	pathReq := httptest.NewRequest(http.MethodGet, "/wrong/req", nil)
	pathResp := httptest.NewRecorder()
	h.ServeHTTP(pathResp, pathReq)
	if pathResp.Code != http.StatusNotFound {
		t.Fatalf("wrong path status = %d, want 404", pathResp.Code)
	}

	badIDReq := httptest.NewRequest(http.MethodGet, PathPrefix+"bad%2Fid", nil)
	badIDResp := httptest.NewRecorder()
	h.ServeHTTP(badIDResp, badIDReq)
	if badIDResp.Code != http.StatusNotFound {
		t.Fatalf("bad id status = %d, want 404", badIDResp.Code)
	}
}

func TestHandlerConnectorIdentityAuthorization(t *testing.T) {
	tests := []struct {
		name      string
		allowed   []string
		cert      *x509.Certificate
		wantCode  int
		wantClaim bool
	}{
		{name: "empty allowlist permits missing", wantCode: http.StatusOK, wantClaim: true},
		{name: "allowlist rejects missing", allowed: []string{"connector-a"}, wantCode: http.StatusUnauthorized},
		{name: "allowlist rejects wrong", allowed: []string{"connector-a"}, cert: certificate("connector-b"), wantCode: http.StatusUnauthorized},
		{name: "allowlist accepts matching", allowed: []string{"connector-a"}, cert: certificate("connector-a"), wantCode: http.StatusOK, wantClaim: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTestRegistry(t)
			mustRegister(t, reg, sourceWith("req", "tok", body("ok"), testDeadline))
			h := newTestHandler(t, reg, tc.allowed)
			req := uploadRequest("req", "tok")
			if tc.cert != nil {
				req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{tc.cert}}
			}
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, req)
			if resp.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", resp.Code, tc.wantCode)
			}
			_, err := reg.Claim("req", "tok")
			if tc.wantClaim {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("Claim() after successful handler error = %v, want ErrNotFound", err)
				}
			} else if err != nil {
				t.Fatalf("Claim() after rejected handler error = %v, want nil", err)
			}
		})
	}
}

func TestHandlerMissingOrWrongTokenKeepsSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "missing"},
		{name: "wrong", token: "wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTestRegistry(t)
			mustRegister(t, reg, sourceWith("req", "tok", body("ok"), testDeadline))
			h := newTestHandler(t, reg, nil)
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, uploadRequest("req", tc.token))
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.Code)
			}
			claim, err := reg.Claim("req", "tok")
			if err != nil {
				t.Fatalf("Claim() after wrong token error = %v", err)
			}
			_ = claim.Close()
		})
	}
}

func TestHandlerReplay(t *testing.T) {
	reg := newTestRegistry(t)
	mustRegister(t, reg, sourceWith("req", "tok", body("ok"), testDeadline))
	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	defer claim.Close()

	h := newTestHandler(t, reg, nil)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, uploadRequest("req", "tok"))
	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.Code)
	}
}

func TestHandlerExpiry409AndCleanup(t *testing.T) {
	now := testNow
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	b := newBlockingReadCloser()
	mustRegister(t, reg, sourceWith("req", "tok", b, testDeadline))
	now = testDeadline

	h := newTestHandler(t, reg, nil)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, uploadRequest("req", "tok"))
	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.Code)
	}
	b.waitClosed(t)
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after expiry error = %v, want ErrNotFound", err)
	}
}

func TestHandlerSuccessfulGETReturnsBodyAndHeaders(t *testing.T) {
	reg := newTestRegistry(t)
	length := int64(11)
	src := sourceWith("req", "tok", body("hello world"), testDeadline)
	src.ContentLength = &length
	src.ContentType = "text/plain"
	mustRegister(t, reg, src)
	h := newTestHandler(t, reg, nil)

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, uploadRequest("req", "tok"))
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if got := resp.Body.String(); got != "hello world" {
		t.Fatalf("body = %q", got)
	}
	if got := resp.Header().Get("Content-Length"); got != "11" {
		t.Fatalf("Content-Length = %q, want 11", got)
	}
	if got := resp.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", got)
	}
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after success error = %v, want ErrNotFound", err)
	}
}

func TestHandlerStreamsBeforeEOF(t *testing.T) {
	reg := newTestRegistry(t)
	pr, pw := io.Pipe()
	mustRegister(t, reg, sourceWith("req", "tok", pr, testDeadline))
	h := newTestHandler(t, reg, nil)
	w := newObservedWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(w, uploadRequest("req", "tok"))
	}()

	if _, err := pw.Write([]byte("data")); err != nil {
		t.Fatalf("pipe Write() error = %v", err)
	}
	w.waitWrite(t)
	if got := w.bodyString(); got != "data" {
		t.Fatalf("streamed body before EOF = %q, want data", got)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pipe Close() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish after EOF")
	}
}

func TestHandlerWriteErrorTriggersCleanup(t *testing.T) {
	reg := newTestRegistry(t)
	b := newTrackingReadCloser("hello")
	mustRegister(t, reg, sourceWith("req", "tok", b, testDeadline))
	h := newTestHandler(t, reg, nil)
	w := &errorWriter{header: make(http.Header), err: errors.New("write failed")}

	h.ServeHTTP(w, uploadRequest("req", "tok"))
	if w.status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", w.status)
	}
	if !b.closed() {
		t.Fatal("source body was not closed")
	}
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after write failure error = %v, want ErrNotFound", err)
	}
}

func TestHandlerRequestContextCancellationUnblocks(t *testing.T) {
	reg := newTestRegistry(t)
	b := newBlockingReadCloser()
	mustRegister(t, reg, sourceWith("req", "tok", b, testDeadline))
	h := newTestHandler(t, reg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	req := uploadRequest("req", "tok").WithContext(ctx)
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(resp, req)
	}()

	b.waitReading(t)
	cancel()
	b.waitClosed(t)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not unblock after request cancellation")
	}
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after request cancel error = %v, want ErrNotFound", err)
	}
}

func newTestHandler(t *testing.T, reg *Registry, identities []string) *Handler {
	t.Helper()
	h, err := NewHandler(HandlerOptions{Registry: reg, AllowedConnectorIdentities: identities, StreamCopyBufferBytes: 4})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return h
}

func uploadRequest(requestID, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, PathPrefix+requestID, nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	return req
}

func certificate(commonName string) *x509.Certificate {
	return &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
}

type observedWriter struct {
	header  http.Header
	written chan struct{}
	once    sync.Once
	mu      sync.Mutex
	status  int
	body    strings.Builder
}

func newObservedWriter() *observedWriter {
	return &observedWriter{header: make(http.Header), written: make(chan struct{})}
}

func (w *observedWriter) Header() http.Header { return w.header }

func (w *observedWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status = status
}

func (w *observedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.body.Write(p)
	w.mu.Unlock()
	w.once.Do(func() { close(w.written) })
	return n, err
}

func (w *observedWriter) waitWrite(t *testing.T) {
	t.Helper()
	select {
	case <-w.written:
	case <-time.After(time.Second):
		t.Fatal("no response write observed")
	}
}

func (w *observedWriter) bodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

type errorWriter struct {
	header http.Header
	status int
	err    error
}

func (w *errorWriter) Header() http.Header { return w.header }

func (w *errorWriter) WriteHeader(status int) { w.status = status }

func (w *errorWriter) Write([]byte) (int, error) { return 0, w.err }

type trackingReadCloser struct {
	*strings.Reader
	mu     sync.Mutex
	isDone bool
}

func newTrackingReadCloser(s string) *trackingReadCloser {
	return &trackingReadCloser{Reader: strings.NewReader(s)}
}

func (r *trackingReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isDone = true
	return nil
}

func (r *trackingReadCloser) closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isDone
}
