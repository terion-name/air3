package uploadsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestURLForRequest(t *testing.T) {
	tests := []struct {
		name      string
		base      string
		requestID string
		want      string
		wantErr   bool
	}{
		{name: "root", base: "https://private.example.test", requestID: "req-1", want: "https://private.example.test/_upload-source/req-1"},
		{name: "trailing slash", base: "https://private.example.test/", requestID: "req_1.:", want: "https://private.example.test/_upload-source/req_1.:"},
		{name: "existing prefix", base: "https://private.example.test/_upload-source/", requestID: "req", want: "https://private.example.test/_upload-source/req"},
		{name: "nested existing prefix", base: "https://private.example.test/private/_upload-source", requestID: "req", want: "https://private.example.test/private/_upload-source/req"},
		{name: "ingest suffix swapped", base: "https://private.example.test/_ingest", requestID: "req", want: "https://private.example.test/_upload-source/req"},
		{name: "nested ingest suffix swapped", base: "https://private.example.test/private/_ingest/", requestID: "req", want: "https://private.example.test/private/_upload-source/req"},
		{name: "compose default ingest base", base: "https://edge-gateway:9443/ingest", requestID: "req", want: "https://edge-gateway:9443/_upload-source/req"},
		{name: "unknown path replaced", base: "https://private.example.test/private/", requestID: "req", want: "https://private.example.test/_upload-source/req"},
		{name: "reject http", base: "http://private.example.test", requestID: "req", wantErr: true},
		{name: "reject relative", base: "/private", requestID: "req", wantErr: true},
		{name: "reject missing host", base: "https:///private", requestID: "req", wantErr: true},
		{name: "reject userinfo", base: "https://user:pass@private.example.test", requestID: "req", wantErr: true},
		{name: "reject unsafe request id", base: "https://private.example.test", requestID: "bad/id", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := URLForRequest(tc.base, tc.requestID)
			if tc.wantErr {
				if err == nil {
					t.Fatal("URLForRequest() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("URLForRequest() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("URLForRequest() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRegistryRegisterValidation(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	tooLong := strings.Repeat("a", 257)
	negative := int64(-1)
	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		src  Source
		want error
	}{
		{name: "nil body", src: validSource("req", "tok", future), want: ErrInvalidSource},
		{name: "empty request id", src: sourceWith("", "tok", body("x"), future), want: ErrInvalidSource},
		{name: "unsafe request id", src: sourceWith("bad/id", "tok", body("x"), future), want: ErrInvalidSource},
		{name: "long request id", src: sourceWith(tooLong, "tok", body("x"), future), want: ErrInvalidSource},
		{name: "empty token", src: sourceWith("req", "", body("x"), future), want: ErrInvalidSource},
		{name: "unsafe token", src: sourceWith("req", "bad token", body("x"), future), want: ErrInvalidSource},
		{name: "zero deadline", src: sourceWith("req", "tok", body("x"), time.Time{}), want: ErrInvalidSource},
		{name: "expired deadline", src: sourceWith("req", "tok", body("x"), now), want: ErrExpired},
		{name: "negative content length", src: func() Source { s := sourceWith("req", "tok", body("x"), future); s.ContentLength = &negative; return s }(), want: ErrInvalidSource},
		{name: "unsafe content type", src: func() Source {
			s := sourceWith("req", "tok", body("x"), future)
			s.ContentType = "text/plain\r\nX: y"
			return s
		}(), want: ErrInvalidSource},
		{name: "content type too long", src: func() Source {
			s := sourceWith("req", "tok", body("x"), future)
			s.ContentType = strings.Repeat("a", 256)
			return s
		}(), want: ErrInvalidSource},
		{name: "context already done", src: func() Source { s := sourceWith("req", "tok", body("x"), future); s.Context = doneCtx; return s }(), want: ErrCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry(Options{Now: func() time.Time { return now }})
			err := reg.Register(tc.src)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Register() error = %v, want %v", err, tc.want)
			}
			if tc.want == ErrCanceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("Register() error = %v, want context.Canceled cause", err)
			}
		})
	}
}

func TestRegistryRegisterRejectsDuplicateRequestID(t *testing.T) {
	reg := newTestRegistry(t)
	mustRegister(t, reg, sourceWith("req", "tok", body("first"), testDeadline))
	err := reg.Register(sourceWith("req", "tok2", body("second"), testDeadline))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Register(duplicate) error = %v, want ErrAlreadyExists", err)
	}
}

func TestRegistrySuccessfulClaimReadsAndCleansUp(t *testing.T) {
	reg := newTestRegistry(t)
	length := int64(11)
	src := sourceWith("req", "tok", body("hello world"), testDeadline)
	src.ContentLength = &length
	src.ContentType = "text/plain"
	mustRegister(t, reg, src)

	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := claim.ContentLength(); got == nil || *got != 11 {
		t.Fatalf("ContentLength() = %v, want 11", got)
	}
	if got := claim.ContentType(); got != "text/plain" {
		t.Fatalf("ContentType() = %q, want text/plain", got)
	}
	data, err := io.ReadAll(claim)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("body = %q", data)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after close error = %v, want ErrNotFound", err)
	}
}

func TestRegistryWrongTokenThenCorrectToken(t *testing.T) {
	reg := newTestRegistry(t)
	mustRegister(t, reg, sourceWith("req", "tok", body("ok"), testDeadline))
	if _, err := reg.Claim("req", "wrong"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Claim(wrong) error = %v, want ErrInvalidToken", err)
	}
	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim(correct) error = %v", err)
	}
	_ = claim.Close()
}

func TestRegistryReplay(t *testing.T) {
	reg := newTestRegistry(t)
	mustRegister(t, reg, sourceWith("req", "tok", body("ok"), testDeadline))
	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrReplayed) {
		t.Fatalf("Claim(replay) error = %v, want ErrReplayed", err)
	}
	_ = claim.Close()
}

func TestRegistryCancelBeforeClaimClosesAndRemoves(t *testing.T) {
	reg := newTestRegistry(t)
	b := newBlockingReadCloser()
	mustRegister(t, reg, sourceWith("req", "tok", b, testDeadline))
	if !reg.Cancel("req", errors.New("stop")) {
		t.Fatal("Cancel() = false, want true")
	}
	b.waitClosed(t)
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after cancel error = %v, want ErrNotFound", err)
	}
}

func TestRegistryCancelAfterClaimUnblocksActiveReader(t *testing.T) {
	reg := newTestRegistry(t)
	b := newBlockingReadCloser()
	mustRegister(t, reg, sourceWith("req", "tok", b, testDeadline))
	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := claim.Read(make([]byte, 1))
		readDone <- err
	}()
	b.waitReading(t)
	if !reg.Cancel("req", errors.New("cancel read")) {
		t.Fatal("Cancel() = false, want true")
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("Read() error = nil, want error")
		}
	case <-time.After(time.Second):
		t.Fatal("Read() did not unblock")
	}
	if reg.Cancel("req", errors.New("again")) {
		t.Fatal("second Cancel() = true, want false")
	}
}

func TestRegistryExpireRemovesAndCloses(t *testing.T) {
	reg := newTestRegistry(t)
	expiredBody := newBlockingReadCloser()
	liveBody := newBlockingReadCloser()
	mustRegister(t, reg, sourceWith("expired", "tok", expiredBody, testDeadline))
	mustRegister(t, reg, sourceWith("live", "tok", liveBody, testDeadline.Add(time.Hour)))
	if got := reg.Expire(testDeadline); got != 1 {
		t.Fatalf("Expire() = %d, want 1", got)
	}
	expiredBody.waitClosed(t)
	if _, err := reg.Claim("expired", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim(expired) error = %v, want ErrNotFound", err)
	}
	claim, err := reg.Claim("live", "tok")
	if err != nil {
		t.Fatalf("Claim(live) error = %v", err)
	}
	_ = claim.Close()
}

func TestRegistrySourceContextCancellationBeforeClaim(t *testing.T) {
	reg := newTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	b := newBlockingReadCloser()
	src := sourceWith("req", "tok", b, testDeadline)
	src.Context = ctx
	mustRegister(t, reg, src)
	cancel()
	b.waitClosed(t)
	if _, err := reg.Claim("req", "tok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Claim() after context cancel error = %v, want ErrNotFound", err)
	}
}

func TestRegistrySourceContextCancellationDuringActiveClaim(t *testing.T) {
	reg := newTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	b := newBlockingReadCloser()
	src := sourceWith("req", "tok", b, testDeadline)
	src.Context = ctx
	mustRegister(t, reg, src)
	claim, err := reg.Claim("req", "tok")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, err := claim.Read(make([]byte, 1))
		readDone <- err
	}()
	b.waitReading(t)
	cancel()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("Read() error = nil, want error")
		}
	case <-time.After(time.Second):
		t.Fatal("Read() did not unblock")
	}
	if !errors.Is(b.closeErr(), context.Canceled) && !errors.Is(claim.entry.terminalErr, context.Canceled) {
		t.Fatalf("terminalErr = %v, want context.Canceled", claim.entry.terminalErr)
	}
	_ = claim.Close()
}

func TestRegistryConcurrentIndependentSessions(t *testing.T) {
	reg := newTestRegistry(t)
	const count = 32
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("req-%02d", i)
			mustRegister(t, reg, sourceWith(id, "tok", body(id), testDeadline))
			claim, err := reg.Claim(id, "tok")
			if err != nil {
				t.Errorf("Claim(%s) error = %v", id, err)
				return
			}
			data, err := io.ReadAll(claim)
			if err != nil {
				t.Errorf("ReadAll(%s) error = %v", id, err)
			}
			if string(data) != id {
				t.Errorf("ReadAll(%s) = %q", id, data)
			}
			_ = claim.Close()
		}()
	}
	wg.Wait()
}

var testNow = time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
var testDeadline = testNow.Add(time.Minute)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	return NewRegistry(Options{Now: func() time.Time { return testNow }})
}

func validSource(requestID, token string, deadline time.Time) Source {
	return Source{RequestID: requestID, Token: token, Deadline: deadline}
}

func sourceWith(requestID, token string, body io.ReadCloser, deadline time.Time) Source {
	return Source{RequestID: requestID, Token: token, Body: body, Deadline: deadline}
}

func body(s string) io.ReadCloser {
	return io.NopCloser(bytes.NewBufferString(s))
}

func mustRegister(t *testing.T, reg *Registry, src Source) {
	t.Helper()
	if err := reg.Register(src); err != nil {
		t.Fatalf("Register(%s) error = %v", src.RequestID, err)
	}
}

type blockingReadCloser struct {
	reading chan struct{}
	closed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	err     error
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{reading: make(chan struct{}), closed: make(chan struct{})}
}

func (b *blockingReadCloser) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingReadCloser) Close() error {
	b.mu.Lock()
	b.err = context.Canceled
	b.mu.Unlock()
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func (b *blockingReadCloser) waitReading(t *testing.T) {
	t.Helper()
	select {
	case <-b.reading:
	case <-time.After(time.Second):
		t.Fatal("body was not read")
	}
}

func (b *blockingReadCloser) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-b.closed:
	case <-time.After(time.Second):
		t.Fatal("body was not closed")
	}
}

func (b *blockingReadCloser) closeErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}
