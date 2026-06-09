package pending

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"
)

func baseRequest(now time.Time, id string) Request {
	return Request{
		ID:          id,
		Deadline:    now.Add(time.Minute),
		IngestToken: "token-" + id,
		Method:      "GET",
		Bucket:      "demo-bucket",
		Key:         "objects/" + id + ".txt",
	}
}

type fakeTarget struct {
	mu sync.Mutex

	writer       io.Writer
	startErr     error
	finishReturn error

	startCount  int
	finishCount int
	cancelCount int
	metadata    Metadata
	finishArg   error
	cancelArg   error
	body        bytes.Buffer
}

func (t *fakeTarget) Start(metadata Metadata) (io.Writer, error) {
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
	return fakeTargetWriter{target: t}, nil
}

func (t *fakeTarget) Finish(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.finishCount++
	t.finishArg = err
	if t.finishReturn != nil {
		return t.finishReturn
	}
	if t.cancelArg != nil {
		return t.cancelArg
	}
	return nil
}

func (t *fakeTarget) Cancel(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cancelCount++
	t.cancelArg = err
}

func (t *fakeTarget) snapshot() (string, Metadata, int, int, int, error, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.body.String(), t.metadata, t.startCount, t.finishCount, t.cancelCount, t.finishArg, t.cancelArg
}

type fakeTargetWriter struct {
	target *fakeTarget
}

func (w fakeTargetWriter) Write(p []byte) (int, error) {
	w.target.mu.Lock()
	defer w.target.mu.Unlock()

	if w.target.cancelArg != nil {
		return 0, w.target.cancelArg
	}
	return w.target.body.Write(p)
}

func TestStartIngestWritesDirectlyToTargetAndCleansUp(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-success")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	metadata := Metadata{StatusCode: 206, ContentType: "text/plain", ETag: `"etag"`}
	stream, err := reg.StartIngest(req.ID, req.IngestToken, metadata)
	if err != nil {
		t.Fatalf("StartIngest() error = %v", err)
	}
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	copyErr := errors.New("copy failed")
	if err := stream.CloseWithError(copyErr); err != nil {
		t.Fatalf("CloseWithError() error = %v", err)
	}
	if err := stream.CloseWithError(errors.New("second close")); err != nil {
		t.Fatalf("second CloseWithError() error = %v", err)
	}

	body, gotMetadata, starts, finishes, cancels, finishArg, _ := target.snapshot()
	if body != "hello" {
		t.Fatalf("target body = %q, want hello", body)
	}
	if gotMetadata != metadata {
		t.Fatalf("target metadata = %+v, want %+v", gotMetadata, metadata)
	}
	if starts != 1 || finishes != 1 || cancels != 0 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 1/1/0", starts, finishes, cancels)
	}
	if !errors.Is(finishArg, copyErr) {
		t.Fatalf("Finish() arg = %v, want %v", finishArg, copyErr)
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() after finish error = %v, want ErrNotFound", err)
	}
}

func TestStartIngestRejectsWrongTokenWithoutClaiming(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-token")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if _, err := reg.StartIngest(req.ID, "wrong-token", Metadata{}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("StartIngest() error = %v, want ErrInvalidToken", err)
	}
	_, _, starts, finishes, cancels, _, _ := target.snapshot()
	if starts != 0 || finishes != 0 || cancels != 0 {
		t.Fatalf("target lifecycle after wrong token starts=%d finishes=%d cancels=%d, want 0/0/0", starts, finishes, cancels)
	}

	stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{})
	if err != nil {
		t.Fatalf("StartIngest() after wrong token error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestStartIngestRejectsReplayDuringActiveStream(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-replay")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{})
	if err != nil {
		t.Fatalf("StartIngest() error = %v", err)
	}
	defer stream.Close()

	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrReplayed) {
		t.Fatalf("StartIngest() replay error = %v, want ErrReplayed", err)
	}
	_, _, starts, _, _, _, _ := target.snapshot()
	if starts != 1 {
		t.Fatalf("Start() count = %d, want 1", starts)
	}
}

func TestCancelBeforeClaimRejectsLaterIngest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-cancel-before")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	cancelErr := errors.New("public request canceled")
	if !reg.Cancel(req.ID, cancelErr) {
		t.Fatal("Cancel() = false, want true")
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() error = %v, want ErrNotFound", err)
	}
	_, _, starts, finishes, cancels, _, cancelArg := target.snapshot()
	if starts != 0 || finishes != 0 || cancels != 1 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 0/0/1", starts, finishes, cancels)
	}
	if !errors.Is(cancelArg, cancelErr) {
		t.Fatalf("Cancel() arg = %v, want %v", cancelArg, cancelErr)
	}
}

func TestCancelAfterClaimFailsWritesAndFinishAndCleansUp(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-cancel-after")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{})
	if err != nil {
		t.Fatalf("StartIngest() error = %v", err)
	}
	if _, err := stream.Write([]byte("before cancel")); err != nil {
		t.Fatalf("Write() before cancel error = %v", err)
	}

	cancelErr := errors.New("client disconnected")
	if !reg.Cancel(req.ID, cancelErr) {
		t.Fatal("Cancel() = false, want true")
	}
	if _, err := stream.Write([]byte("after cancel")); !errors.Is(err, cancelErr) {
		t.Fatalf("Write() after cancel error = %v, want %v", err, cancelErr)
	}
	if err := stream.Close(); !errors.Is(err, cancelErr) {
		t.Fatalf("Close() after cancel error = %v, want %v", err, cancelErr)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() after cancel error = %v, want ErrNotFound", err)
	}
	if reg.Cancel(req.ID, cancelErr) {
		t.Fatal("Cancel() after cleanup = true, want false")
	}

	body, _, starts, finishes, cancels, _, cancelArg := target.snapshot()
	if body != "before cancel" {
		t.Fatalf("target body = %q, want only pre-cancel bytes", body)
	}
	if starts != 1 || finishes != 1 || cancels != 1 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 1/1/1", starts, finishes, cancels)
	}
	if !errors.Is(cancelArg, cancelErr) {
		t.Fatalf("Cancel() arg = %v, want %v", cancelArg, cancelErr)
	}
}

func TestRegistryExpiresPendingRequests(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-expire")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	now = now.Add(2 * time.Minute)
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("StartIngest() error = %v, want ErrExpired", err)
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() after expiry cleanup error = %v, want ErrNotFound", err)
	}
	_, _, starts, finishes, cancels, _, cancelArg := target.snapshot()
	if starts != 0 || finishes != 0 || cancels != 1 {
		t.Fatalf("target lifecycle starts=%d finishes=%d cancels=%d, want 0/0/1", starts, finishes, cancels)
	}
	if !errors.Is(cancelArg, ErrExpired) {
		t.Fatalf("Cancel() arg = %v, want ErrExpired", cancelArg)
	}
}

func TestExpireCancelsWaitingTargets(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-expire-sweep")
	target := &fakeTarget{}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if expired := reg.Expire(now.Add(2 * time.Minute)); expired != 1 {
		t.Fatalf("Expire() = %d, want 1", expired)
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() after Expire() error = %v, want ErrNotFound", err)
	}
	_, _, _, _, cancels, _, cancelArg := target.snapshot()
	if cancels != 1 || !errors.Is(cancelArg, ErrExpired) {
		t.Fatalf("target cancel count=%d arg=%v, want 1/ErrExpired", cancels, cancelArg)
	}
}

func TestConcurrentPendingRequestsAreIsolated(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})

	const count = 20
	targets := make([]*fakeTarget, count)
	for i := 0; i < count; i++ {
		req := baseRequest(now, "req-"+strconv.Itoa(i))
		targets[i] = &fakeTarget{}
		if err := reg.Register(req, targets[i]); err != nil {
			t.Fatalf("Register(%s) error = %v", req.ID, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		i := i
		req := baseRequest(now, "req-"+strconv.Itoa(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{ETag: "etag-" + strconv.Itoa(i)})
			if err != nil {
				t.Errorf("StartIngest(%s) error = %v", req.ID, err)
				return
			}
			if _, err := stream.Write([]byte("body-" + strconv.Itoa(i))); err != nil {
				t.Errorf("Write(%s) error = %v", req.ID, err)
				return
			}
			if err := stream.Close(); err != nil {
				t.Errorf("Close(%s) error = %v", req.ID, err)
			}
		}()
	}
	wg.Wait()

	for i, target := range targets {
		body, metadata, starts, finishes, cancels, _, _ := target.snapshot()
		wantBody := "body-" + strconv.Itoa(i)
		wantETag := "etag-" + strconv.Itoa(i)
		if body != wantBody || metadata.ETag != wantETag {
			t.Fatalf("target %d body=%q etag=%q, want %q/%q", i, body, metadata.ETag, wantBody, wantETag)
		}
		if starts != 1 || finishes != 1 || cancels != 0 {
			t.Fatalf("target %d lifecycle starts=%d finishes=%d cancels=%d, want 1/1/0", i, starts, finishes, cancels)
		}
	}
}

func TestStartIngestStreamsWithoutFullBuffering(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-stream")
	writer := newBlockingWriter()
	target := &fakeTarget{writer: writer}
	if err := reg.Register(req, target); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("StartIngest() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("first"))
		done <- err
	}()

	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("Write() did not reach target writer")
	}
	select {
	case err := <-done:
		t.Fatalf("Write() returned before target accepted bytes: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(writer.release)
	if err := <-done; err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := writer.body(); got != "first" {
		t.Fatalf("blocking writer body = %q, want first", got)
	}
}

func TestRegisterRejectsInvalidAndExpiredRequests(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	target := &fakeTarget{}
	expired := baseRequest(now, "req-old")
	expired.Deadline = now
	if err := reg.Register(expired, target); !errors.Is(err, ErrExpired) {
		t.Fatalf("Register(expired) error = %v, want ErrExpired", err)
	}

	bad := baseRequest(now, "bad id")
	if err := reg.Register(bad, target); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Register(invalid) error = %v, want ErrInvalidRequest", err)
	}
	if err := reg.Register(baseRequest(now, "req-nil-target"), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Register(nil target) error = %v, want ErrInvalidRequest", err)
	}
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
