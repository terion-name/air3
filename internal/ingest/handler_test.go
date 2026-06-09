package ingest

import (
	"bytes"
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

	"github.com/terion-name/air3/internal/pending"
)

func newTestHandler(t *testing.T, reg *pending.Registry, identities ...string) *Handler {
	t.Helper()
	h, err := NewHandler(Options{Registry: reg, AllowedConnectorIdentities: identities})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return h
}

func ingestRequest(now time.Time, id string) pending.Request {
	return pending.Request{
		ID:          id,
		Deadline:    now.Add(time.Minute),
		IngestToken: "token-" + id,
		Method:      "GET",
		Bucket:      "demo-bucket",
		Key:         "objects/" + id + ".txt",
	}
}

func registerPending(t *testing.T, reg *pending.Registry, req pending.Request, target *directTarget) *directTarget {
	t.Helper()
	if target == nil {
		target = &directTarget{}
	}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return target
}

func TestHandlerStreamsBodyToPendingRequest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-handler")
	target := registerPending(t, reg, req, nil)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("object-body"))
	r.Header.Set(TokenHeader, req.IngestToken)
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("ETag", `"abc"`)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
	}
	snap := target.snapshot()
	if snap.body != "object-body" {
		t.Fatalf("body = %q", snap.body)
	}
	if snap.metadata.ContentType != "text/plain" || snap.metadata.ETag != `"abc"` || snap.metadata.ContentLength != "11" {
		t.Fatalf("metadata = %#v", snap.metadata)
	}
	if snap.startCount != 1 || snap.finishCount != 1 || snap.cancelCount != 0 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 1/1/0", snap.startCount, snap.finishCount, snap.cancelCount)
	}
	if snap.finishArg != nil {
		t.Fatalf("Finish() arg = %v, want nil", snap.finishArg)
	}
}

func TestHandlerRejectsTokenReplay(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-replay")
	writer := newBlockingWriter()
	target := registerPending(t, reg, req, &directTarget{writer: writer})
	h := newTestHandler(t, reg)

	firstReq := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("first"))
	firstReq.Header.Set(TokenHeader, req.IngestToken)
	firstRecorder := httptest.NewRecorder()
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h.ServeHTTP(firstRecorder, firstReq)
	}()

	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("first ingest did not reach target writer")
	}

	replayReq := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("replay"))
	replayReq.Header.Set(TokenHeader, req.IngestToken)
	replayRecorder := httptest.NewRecorder()
	h.ServeHTTP(replayRecorder, replayReq)
	if got := replayRecorder.Result().StatusCode; got != http.StatusConflict {
		t.Fatalf("replay status = %d, want %d", got, http.StatusConflict)
	}

	close(writer.release)
	<-serveDone
	if got := firstRecorder.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("first status = %d, want %d", got, http.StatusNoContent)
	}
	if got := writer.body(); got != "first" {
		t.Fatalf("first body = %q", got)
	}
	snap := target.snapshot()
	if snap.startCount != 1 || snap.finishCount != 1 || snap.cancelCount != 0 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 1/1/0", snap.startCount, snap.finishCount, snap.cancelCount)
	}
}

func TestHandlerRejectsLateIngest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-late")
	target := registerPending(t, reg, req, nil)
	now = now.Add(2 * time.Minute)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("late"))
	r.Header.Set(TokenHeader, req.IngestToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusConflict {
		t.Fatalf("status = %d, want %d", got, http.StatusConflict)
	}
	snap := target.snapshot()
	if snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 1 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 0/0/1", snap.startCount, snap.finishCount, snap.cancelCount)
	}
	if !errors.Is(snap.cancelArg, pending.ErrExpired) {
		t.Fatalf("Cancel() arg = %v, want ErrExpired", snap.cancelArg)
	}
}

func TestHandlerRejectsWrongToken(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-bad-token")
	target := registerPending(t, reg, req, nil)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	r.Header.Set(TokenHeader, "wrong")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", got, http.StatusUnauthorized)
	}
	snap := target.snapshot()
	if snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("target lifecycle after wrong token starts=%d finishes=%d cancels=%d, want 0/0/0", snap.startCount, snap.finishCount, snap.cancelCount)
	}

	allowed := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	allowed.Header.Set(TokenHeader, req.IngestToken)
	allowedRecorder := httptest.NewRecorder()
	h.ServeHTTP(allowedRecorder, allowed)
	if got := allowedRecorder.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status after wrong token = %d, want %d", got, http.StatusNoContent)
	}
}

func TestHandlerRequiresConfiguredMTLSIdentity(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-mtls")
	target := registerPending(t, reg, req, nil)
	h := newTestHandler(t, reg, "connector-a")

	missing := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	missing.Header.Set(TokenHeader, req.IngestToken)
	missingRecorder := httptest.NewRecorder()
	h.ServeHTTP(missingRecorder, missing)
	if got := missingRecorder.Result().StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("missing cert status = %d, want %d", got, http.StatusUnauthorized)
	}

	denied := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	denied.Header.Set(TokenHeader, req.IngestToken)
	denied.TLS = tlsState{cert: certificate("connector-b")}.connectionState()
	deniedRecorder := httptest.NewRecorder()
	h.ServeHTTP(deniedRecorder, denied)
	if got := deniedRecorder.Result().StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("wrong cert status = %d, want %d", got, http.StatusUnauthorized)
	}

	allowed := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	allowed.Header.Set(TokenHeader, req.IngestToken)
	allowed.TLS = tlsState{cert: certificate("connector-a")}.connectionState()
	allowedRecorder := httptest.NewRecorder()
	h.ServeHTTP(allowedRecorder, allowed)
	if got := allowedRecorder.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("allowed cert status = %d, want %d", got, http.StatusNoContent)
	}
	if got := target.snapshot().body; got != "body" {
		t.Fatalf("allowed body = %q, want body", got)
	}
}

func TestHeaderAllowlistIgnoresUnsafeMetadata(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-headers")
	target := registerPending(t, reg, req, nil)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, bytes.NewReader([]byte("body")))
	r.Header.Set(TokenHeader, req.IngestToken)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set("Content-Range", "bytes 0-3/4")
	r.Header.Set("Last-Modified", "Mon, 08 Jun 2026 12:00:00 GMT")
	r.Header.Set("Accept-Ranges", "bytes")
	r.Header.Set(StatusCodeHeader, "206")
	r.Header.Set("X-Amz-Request-Id", "secret-internal-id")
	r.Header.Set("Set-Cookie", "session=leak")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
	}

	metadata := target.snapshot().metadata
	if metadata.StatusCode != 206 || metadata.ContentType != "application/octet-stream" || metadata.ContentRange != "bytes 0-3/4" || metadata.AcceptRanges != "bytes" {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestHandlerRejectsUnsafeAllowedHeaderValue(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-unsafe-header")
	target := registerPending(t, reg, req, nil)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	r.Header.Set(TokenHeader, req.IngestToken)
	r.Header.Set("ETag", "ok\r\nbad")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", got, http.StatusBadRequest)
	}
	if snap := target.snapshot(); snap.startCount != 0 || snap.finishCount != 0 || snap.cancelCount != 0 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 0/0/0", snap.startCount, snap.finishCount, snap.cancelCount)
	}
}

func TestHandlerStreamsWithoutReadingWholeBodyFirst(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-streaming")
	writer := newBlockingWriter()
	registerPending(t, reg, req, &directTarget{writer: writer})
	h := newTestHandler(t, reg)
	bodyReader, bodyWriter := io.Pipe()
	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, bodyReader)
	r.Header.Set(TokenHeader, req.IngestToken)
	w := httptest.NewRecorder()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h.ServeHTTP(w, r)
	}()

	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write([]byte("first"))
		writeDone <- err
	}()

	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("ingest did not start writing first chunk before request body closed")
	}
	select {
	case <-serveDone:
		t.Fatal("ServeHTTP returned before target accepted the first chunk")
	case <-time.After(25 * time.Millisecond):
	}
	close(writer.release)
	if err := <-writeDone; err != nil {
		t.Fatalf("request body write error = %v", err)
	}
	_, _ = bodyWriter.Write([]byte("second"))
	_ = bodyWriter.Close()
	<-serveDone

	if got := writer.body(); got != "firstsecond" {
		t.Fatalf("target body = %q, want firstsecond", got)
	}
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
	}
}

func TestHandlerUsesConfiguredStreamCopyBufferSize(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-copy-buffer")
	target := registerPending(t, reg, req, nil)
	h, err := NewHandler(Options{Registry: reg, StreamCopyBufferBytes: 3})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	body := &observingReader{data: []byte("abcdef")}
	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, body)
	r.Header.Set(TokenHeader, req.IngestToken)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	if got := target.snapshot().body; got != "abcdef" {
		t.Fatalf("body = %q", got)
	}
	if got := body.maxReadLen; got > 3 {
		t.Fatalf("max request body Read len = %d, want <= 3", got)
	}
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
	}
}

func TestHandlerTargetErrorsReturnBadGatewayAndCleanRegistry(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		target     *directTarget
		wantFinish bool
		wantCancel bool
		wantErr    error
	}{
		{
			name:       "start error",
			target:     &directTarget{startErr: errors.New("start failed")},
			wantCancel: true,
		},
		{
			name:       "write error",
			target:     &directTarget{writer: erroringWriter{err: errors.New("write failed")}},
			wantFinish: true,
			wantErr:    errors.New("write failed"),
		},
		{
			name:       "finish error",
			target:     &directTarget{finishErr: errors.New("finish failed")},
			wantFinish: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
			req := ingestRequest(now, "req-"+strings.ReplaceAll(tt.name, " ", "-"))
			target := registerPending(t, reg, req, tt.target)
			h := newTestHandler(t, reg)

			r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
			r.Header.Set(TokenHeader, req.IngestToken)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if got := w.Result().StatusCode; got != http.StatusBadGateway {
				t.Fatalf("status = %d, want %d", got, http.StatusBadGateway)
			}
			if _, err := reg.StartIngest(req.ID, req.IngestToken, pending.Metadata{}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("StartIngest() after failure error = %v, want ErrNotFound", err)
			}
			snap := target.snapshot()
			if tt.wantFinish && snap.finishCount != 1 {
				t.Fatalf("finish count = %d, want 1", snap.finishCount)
			}
			if !tt.wantFinish && snap.finishCount != 0 {
				t.Fatalf("finish count = %d, want 0", snap.finishCount)
			}
			if tt.wantCancel && snap.cancelCount != 1 {
				t.Fatalf("cancel count = %d, want 1", snap.cancelCount)
			}
			if !tt.wantCancel && snap.cancelCount != 0 {
				t.Fatalf("cancel count = %d, want 0", snap.cancelCount)
			}
			if tt.wantErr != nil && snap.finishArg == nil {
				t.Fatal("Finish() arg = nil, want copy error")
			}
		})
	}
}

func TestHandlerRejectsInvalidRouteAndMethod(t *testing.T) {
	h := newTestHandler(t, pending.NewRegistry(pending.Options{}))
	methodReq := httptest.NewRequest(http.MethodGet, PathPrefix+"req", nil)
	methodRecorder := httptest.NewRecorder()
	h.ServeHTTP(methodRecorder, methodReq)
	if got := methodRecorder.Result().StatusCode; got != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d, want %d", got, http.StatusMethodNotAllowed)
	}

	pathReq := httptest.NewRequest(http.MethodPost, "/ingest/req", nil)
	pathRecorder := httptest.NewRecorder()
	h.ServeHTTP(pathRecorder, pathReq)
	if got := pathRecorder.Result().StatusCode; got != http.StatusNotFound {
		t.Fatalf("path status = %d, want %d", got, http.StatusNotFound)
	}
}

func TestNewHandlerRequiresRegistry(t *testing.T) {
	if _, err := NewHandler(Options{}); err == nil {
		t.Fatal("NewHandler() error = nil, want error")
	}
}

type observingReader struct {
	data       []byte
	maxReadLen int
}

func (r *observingReader) Read(p []byte) (int, error) {
	if len(p) > r.maxReadLen {
		r.maxReadLen = len(p)
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type directTarget struct {
	mu sync.Mutex

	writer    io.Writer
	startErr  error
	finishErr error

	startCount  int
	finishCount int
	cancelCount int
	metadata    pending.Metadata
	finishArg   error
	cancelArg   error
	body        bytes.Buffer
}

func (t *directTarget) Start(metadata pending.Metadata) (io.Writer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startCount++
	t.metadata = metadata
	if t.startErr != nil {
		return nil, t.startErr
	}
	if t.writer != nil {
		return t.writer, nil
	}
	return directTargetWriter{target: t}, nil
}

func (t *directTarget) Finish(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.finishCount++
	t.finishArg = err
	return t.finishErr
}

func (t *directTarget) Cancel(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cancelCount++
	t.cancelArg = err
}

type directTargetSnapshot struct {
	body        string
	metadata    pending.Metadata
	startCount  int
	finishCount int
	cancelCount int
	finishArg   error
	cancelArg   error
}

func (t *directTarget) snapshot() directTargetSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	return directTargetSnapshot{
		body:        t.body.String(),
		metadata:    t.metadata,
		startCount:  t.startCount,
		finishCount: t.finishCount,
		cancelCount: t.cancelCount,
		finishArg:   t.finishArg,
		cancelArg:   t.cancelArg,
	}
}

type directTargetWriter struct {
	target *directTarget
}

func (w directTargetWriter) Write(p []byte) (int, error) {
	w.target.mu.Lock()
	defer w.target.mu.Unlock()
	return w.target.body.Write(p)
}

type erroringWriter struct {
	err error
}

func (w erroringWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu  sync.Mutex
	buf bytes.Buffer
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *blockingWriter) body() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type tlsState struct {
	cert *x509.Certificate
}

func (s tlsState) connectionState() *tls.ConnectionState {
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{s.cert}}
}

func certificate(commonName string) *x509.Certificate {
	return &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
}

func TestStatusForPendingError(t *testing.T) {
	if got := statusForPendingError(pending.ErrInvalidToken); got != http.StatusUnauthorized {
		t.Fatalf("invalid token status = %d", got)
	}
	if got := statusForPendingError(pending.ErrNotFound); got != http.StatusNotFound {
		t.Fatalf("not found status = %d", got)
	}
	if got := statusForPendingError(errors.New("other")); got != http.StatusBadRequest {
		t.Fatalf("other status = %d", got)
	}
}
