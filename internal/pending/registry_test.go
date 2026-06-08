package pending

import (
	"context"
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

func TestRegistryExpiresPendingRequests(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-expire")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	now = now.Add(2 * time.Minute)
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("StartIngest() error = %v, want ErrExpired", err)
	}
	if _, err := reg.Wait(context.Background(), req.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Wait() error = %v, want ErrNotFound after expiry cleanup", err)
	}
}

func TestWaitContextCancellationCancelsPendingRequest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-cancel")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reg.Wait(ctx, req.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() error = %v, want ErrNotFound", err)
	}
}

func TestCancelRejectsLaterIngest(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-cancel-direct")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if !reg.Cancel(req.ID) {
		t.Fatal("Cancel() = false, want true")
	}
	if _, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StartIngest() error = %v, want ErrNotFound", err)
	}
}

func TestStartIngestRejectsTokenReplay(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-replay")
	if err := reg.Register(req); err != nil {
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
}

func TestConcurrentPendingRequestsAreIsolated(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})

	const count = 20
	for i := 0; i < count; i++ {
		req := baseRequest(now, "req-"+strconv.Itoa(i))
		if err := reg.Register(req); err != nil {
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
			resp, err := reg.Wait(context.Background(), req.ID)
			if err != nil {
				t.Errorf("Wait(%s) error = %v", req.ID, err)
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("ReadAll(%s) error = %v", req.ID, err)
				return
			}
			want := "body-" + strconv.Itoa(i)
			if string(body) != want || resp.Metadata.ETag != "etag-"+strconv.Itoa(i) {
				t.Errorf("response %s body=%q etag=%q", req.ID, body, resp.Metadata.ETag)
			}
		}()
	}

	for i := 0; i < count; i++ {
		req := baseRequest(now, "req-"+strconv.Itoa(i))
		stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{ETag: "etag-" + strconv.Itoa(i)})
		if err != nil {
			t.Fatalf("StartIngest(%s) error = %v", req.ID, err)
		}
		if _, err := stream.Write([]byte("body-" + strconv.Itoa(i))); err != nil {
			t.Fatalf("Write(%s) error = %v", req.ID, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close(%s) error = %v", req.ID, err)
		}
	}
	wg.Wait()
}

func TestStartIngestStreamsWithoutFullBuffering(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	req := baseRequest(now, "req-stream")
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	stream, err := reg.StartIngest(req.ID, req.IngestToken, Metadata{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("StartIngest() error = %v", err)
	}

	resp, err := reg.Wait(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	defer resp.Body.Close()

	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		_, _ = stream.Write([]byte("first"))
		<-time.After(50 * time.Millisecond)
		_, _ = stream.Write([]byte("second"))
		_ = stream.Close()
	}()

	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	if string(buf) != "first" {
		t.Fatalf("first chunk = %q", buf)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	<-wrote
	if string(rest) != "second" {
		t.Fatalf("rest = %q", rest)
	}
}

func TestRegisterRejectsInvalidAndExpiredRequests(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(Options{Now: func() time.Time { return now }})
	expired := baseRequest(now, "req-old")
	expired.Deadline = now
	if err := reg.Register(expired); !errors.Is(err, ErrExpired) {
		t.Fatalf("Register(expired) error = %v, want ErrExpired", err)
	}

	bad := baseRequest(now, "bad id")
	if err := reg.Register(bad); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Register(invalid) error = %v, want ErrInvalidRequest", err)
	}
}
