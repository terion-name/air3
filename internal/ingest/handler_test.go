package ingest

import (
	"bytes"
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

func TestHandlerStreamsBodyToPendingRequest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-handler")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("object-body"))
	r.Header.Set(TokenHeader, req.IngestToken)
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("ETag", `"abc"`)
	w := httptest.NewRecorder()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(w, r)
	}()

	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if resp.Metadata.ContentType != "text/plain" || resp.Metadata.ETag != `"abc"` || resp.Metadata.ContentLength != "11" {
		t.Fatalf("metadata = %#v", resp.Metadata)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("Body.Close() error = %v", err)
	}
	wg.Wait()

	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
	}
	if string(body) != "object-body" {
		t.Fatalf("body = %q", body)
	}
}

func TestHandlerRejectsTokenReplay(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-replay")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	h := newTestHandler(t, reg)

	firstReq := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("first"))
	firstReq.Header.Set(TokenHeader, req.IngestToken)
	firstRecorder := httptest.NewRecorder()
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h.ServeHTTP(firstRecorder, firstReq)
	}()

	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	defer resp.Body.Close()

	replayReq := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("replay"))
	replayReq.Header.Set(TokenHeader, req.IngestToken)
	replayRecorder := httptest.NewRecorder()
	h.ServeHTTP(replayRecorder, replayReq)
	if got := replayRecorder.Result().StatusCode; got != http.StatusConflict {
		t.Fatalf("replay status = %d, want %d", got, http.StatusConflict)
	}

	if body, err := io.ReadAll(resp.Body); err != nil || string(body) != "first" {
		t.Fatalf("first body=%q err=%v", body, err)
	}
	<-serveDone
}

func TestHandlerRejectsLateIngest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-late")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	now = now.Add(2 * time.Minute)
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("late"))
	r.Header.Set(TokenHeader, req.IngestToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusConflict {
		t.Fatalf("status = %d, want %d", got, http.StatusConflict)
	}
}

func TestHandlerRejectsWrongToken(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-bad-token")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	h := newTestHandler(t, reg)

	r := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	r.Header.Set(TokenHeader, "wrong")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Result().StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", got, http.StatusUnauthorized)
	}
}

func TestHandlerRequiresConfiguredMTLSIdentity(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-mtls")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	h := newTestHandler(t, reg, "connector-a")

	missing := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	missing.Header.Set(TokenHeader, req.IngestToken)
	missingRecorder := httptest.NewRecorder()
	h.ServeHTTP(missingRecorder, missing)
	if got := missingRecorder.Result().StatusCode; got != http.StatusUnauthorized {
		t.Fatalf("missing cert status = %d, want %d", got, http.StatusUnauthorized)
	}

	allowed := httptest.NewRequest(http.MethodPost, PathPrefix+req.ID, strings.NewReader("body"))
	allowed.Header.Set(TokenHeader, req.IngestToken)
	allowed.TLS = tlsState{cert: certificate("connector-a")}.connectionState()
	allowedRecorder := httptest.NewRecorder()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h.ServeHTTP(allowedRecorder, allowed)
	}()
	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	<-serveDone
	if got := allowedRecorder.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("allowed cert status = %d, want %d", got, http.StatusNoContent)
	}
}

func TestHeaderAllowlistIgnoresUnsafeMetadata(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-headers")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(w, r)
	}()
	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	<-done

	if resp.Metadata.StatusCode != 206 || resp.Metadata.ContentRange != "bytes 0-3/4" || resp.Metadata.AcceptRanges != "bytes" {
		t.Fatalf("metadata = %#v", resp.Metadata)
	}
	headers := resp.Metadata.Header()
	if headers.Get("X-Amz-Request-Id") != "" || headers.Get("Set-Cookie") != "" {
		t.Fatalf("unsafe headers propagated: %#v", headers)
	}
}

func TestHandlerRejectsUnsafeAllowedHeaderValue(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("ETag", "ok\r\nbad")
	if _, err := metadataFromHeaders(hdr); err == nil {
		t.Fatal("metadataFromHeaders() error = nil, want error")
	}
}

func TestHandlerStreamsWithoutReadingWholeBodyFirst(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return now }})
	req := ingestRequest(now, "req-streaming")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
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
	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	defer resp.Body.Close()

	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write([]byte("first"))
		writeDone <- err
	}()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	if string(buf) != "first" {
		t.Fatalf("first chunk = %q", buf)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("request body write error = %v", err)
	}
	_, _ = bodyWriter.Write([]byte("second"))
	_ = bodyWriter.Close()
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(rest) != "second" {
		t.Fatalf("rest = %q", rest)
	}
	<-serveDone
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", got, http.StatusNoContent)
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
