package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/pending"
)

type sinkResponseWriter struct {
	header  http.Header
	status  int
	body    bytes.Buffer
	flushes int
}

func newSinkResponseWriter() *sinkResponseWriter {
	return &sinkResponseWriter{header: make(http.Header)}
}

func (w *sinkResponseWriter) Header() http.Header {
	return w.header
}

func (w *sinkResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *sinkResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *sinkResponseWriter) Flush() {
	w.flushes++
}

type panicWriteResponseWriter struct {
	*sinkResponseWriter
}

func (w *panicWriteResponseWriter) Write([]byte) (int, error) {
	panic("write failed")
}

type panicFlushResponseWriter struct {
	*sinkResponseWriter
	beforePanic func()
}

func (w *panicFlushResponseWriter) Flush() {
	if w.beforePanic != nil {
		w.beforePanic()
	}
	panic("flush failed")
}

type cancelOnWriteResponseWriter struct {
	*sinkResponseWriter
	cancel      context.CancelFunc
	flushCalled bool
}

func (w *cancelOnWriteResponseWriter) Write(p []byte) (int, error) {
	n, err := w.sinkResponseWriter.Write(p)
	w.cancel()
	return n, err
}

func (w *cancelOnWriteResponseWriter) Flush() {
	w.flushCalled = true
	panic("flush should have been skipped")
}

func TestResponseSinkMetadataAllowlistAndDefaultStatus(t *testing.T) {
	resp := newSinkResponseWriter()
	sink := newResponseSink(resp, http.MethodGet, context.Background())
	writer, err := sink.Start(pending.Metadata{
		ContentType:   " text/plain ",
		ContentLength: " 42 ",
		ContentRange:  "   ",
		ETag:          ` "abc" `,
		LastModified:  " Tue, 09 Jun 2026 12:00:00 GMT ",
		AcceptRanges:  " bytes ",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if writer == nil {
		t.Fatal("Start() returned nil writer")
	}
	if resp.status != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.status, http.StatusOK)
	}
	wantHeaders := map[string]string{
		"Content-Type":   "text/plain",
		"Content-Length": "42",
		"ETag":           `"abc"`,
		"Last-Modified":  "Tue, 09 Jun 2026 12:00:00 GMT",
		"Accept-Ranges":  "bytes",
	}
	for name, want := range wantHeaders {
		if got := resp.header.Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	if got := resp.header.Get("Content-Range"); got != "" {
		t.Fatalf("Content-Range = %q, want empty", got)
	}
	if got := resp.header.Get("X-Air3-Status-Code"); got != "" {
		t.Fatalf("status leaked as header: %q", got)
	}
}

func TestResponseSinkFinishIsIdempotent(t *testing.T) {
	finishErr := errors.New("finish failed")
	errSink := newResponseSink(newSinkResponseWriter(), http.MethodGet, context.Background())
	if err := errSink.Finish(finishErr); !errors.Is(err, finishErr) {
		t.Fatalf("Finish(error) error = %v, want %v", err, finishErr)
	}
	if err := errSink.Finish(nil); !errors.Is(err, finishErr) {
		t.Fatalf("second Finish(nil) error = %v, want original %v", err, finishErr)
	}

	resp := newSinkResponseWriter()
	sink := newResponseSink(resp, http.MethodGet, context.Background())
	writer, err := sink.Start(pending.Metadata{StatusCode: http.StatusPartialContent})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := sink.Finish(nil); err != nil {
		t.Fatalf("Finish(nil) error = %v", err)
	}
	if err := sink.Finish(nil); err != nil {
		t.Fatalf("second Finish(nil) error = %v", err)
	}
	if err := sink.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	sink.Cancel(errors.New("late cancel"))
	if resp.status != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", resp.status, http.StatusPartialContent)
	}
	if body := resp.body.String(); body != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late Write() error = %v, want ErrClosedPipe", err)
	}
}

func TestResponseSinkCancelBeforeStart(t *testing.T) {
	boom := errors.New("client left")
	resp := newSinkResponseWriter()
	sink := newResponseSink(resp, http.MethodGet, context.Background())
	sink.Cancel(boom)
	sink.Cancel(errors.New("second cancel"))

	if err := sink.Wait(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Wait() error = %v, want %v", err, boom)
	}
	if resp.status != 0 {
		t.Fatalf("status = %d, want uncommitted", resp.status)
	}
	if len(resp.header) != 0 {
		t.Fatalf("headers = %#v, want none", resp.header)
	}
	if _, err := sink.Start(pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, boom) {
		t.Fatalf("Start() error = %v, want %v", err, boom)
	}
}

func TestResponseSinkCancelAfterStart(t *testing.T) {
	boom := errors.New("client left")
	resp := newSinkResponseWriter()
	sink := newResponseSink(resp, http.MethodGet, context.Background())
	writer, err := sink.Start(pending.Metadata{StatusCode: http.StatusNotFound, ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	sink.Cancel(boom)
	sink.Cancel(errors.New("second cancel"))

	if err := sink.Wait(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Wait() error = %v, want %v", err, boom)
	}
	if resp.status != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.status, http.StatusNotFound)
	}
	if _, err := writer.Write([]byte("late body")); !errors.Is(err, boom) {
		t.Fatalf("Write() error = %v, want %v", err, boom)
	}
	if body := resp.body.String(); body != "" {
		t.Fatalf("body = %q, want empty", body)
	}
}

func TestResponseSinkGETFlushesAndHEADDiscards(t *testing.T) {
	getResp := newSinkResponseWriter()
	getSink := newResponseSink(getResp, http.MethodGet, context.Background())
	getWriter, err := getSink.Start(pending.Metadata{})
	if err != nil {
		t.Fatalf("GET Start() error = %v", err)
	}
	if n, err := getWriter.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("GET Write() = %d, %v; want 5, nil", n, err)
	}
	if body := getResp.body.String(); body != "hello" {
		t.Fatalf("GET body = %q, want hello", body)
	}
	if getResp.flushes == 0 {
		t.Fatal("GET Write() did not flush")
	}

	headResp := newSinkResponseWriter()
	headSink := newResponseSink(headResp, http.MethodHead, context.Background())
	headWriter, err := headSink.Start(pending.Metadata{ContentLength: "5"})
	if err != nil {
		t.Fatalf("HEAD Start() error = %v", err)
	}
	if n, err := headWriter.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("HEAD Write() = %d, %v; want 5, nil", n, err)
	}
	if body := headResp.body.String(); body != "" {
		t.Fatalf("HEAD body = %q, want empty", body)
	}
}

func TestResponseSinkWriteRecoversResponseWriterPanics(t *testing.T) {
	t.Run("write panic", func(t *testing.T) {
		resp := &panicWriteResponseWriter{sinkResponseWriter: newSinkResponseWriter()}
		sink := newResponseSink(resp, http.MethodGet, context.Background())
		writer, err := sink.Start(pending.Metadata{})
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}

		n, err := writer.Write([]byte("hello"))
		if n != 0 {
			t.Fatalf("Write() n = %d, want 0", n)
		}
		if !errors.Is(err, errResponseWriterPanic) {
			t.Fatalf("Write() error = %v, want response writer panic", err)
		}
	})

	t.Run("flush panic", func(t *testing.T) {
		resp := &panicFlushResponseWriter{sinkResponseWriter: newSinkResponseWriter()}
		sink := newResponseSink(resp, http.MethodGet, context.Background())
		writer, err := sink.Start(pending.Metadata{})
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}

		n, err := writer.Write([]byte("hello"))
		if n != 5 {
			t.Fatalf("Write() n = %d, want 5", n)
		}
		if !errors.Is(err, errResponseWriterPanic) {
			t.Fatalf("Write() error = %v, want response writer panic", err)
		}
		if body := resp.body.String(); body != "hello" {
			t.Fatalf("body = %q, want hello", body)
		}
	})
}

func TestResponseSinkWriteTreatsFlushRaceAsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resp := &panicFlushResponseWriter{sinkResponseWriter: newSinkResponseWriter(), beforePanic: cancel}
	sink := newResponseSink(resp, http.MethodGet, ctx)
	writer, err := sink.Start(pending.Metadata{})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	n, err := writer.Write([]byte("hello"))
	if n != 5 {
		t.Fatalf("Write() n = %d, want 5", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() error = %v, want context.Canceled", err)
	}
}

func TestResponseSinkWriteSkipsFlushAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resp := &cancelOnWriteResponseWriter{sinkResponseWriter: newSinkResponseWriter(), cancel: cancel}
	sink := newResponseSink(resp, http.MethodGet, ctx)
	writer, err := sink.Start(pending.Metadata{})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	n, err := writer.Write([]byte("hello"))
	if n != 5 {
		t.Fatalf("Write() n = %d, want 5", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() error = %v, want context.Canceled", err)
	}
	if resp.flushCalled {
		t.Fatal("Flush() was called after context cancellation")
	}
}

func TestResponseSinkWriterHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resp := newSinkResponseWriter()
	sink := newResponseSink(resp, http.MethodHead, ctx)
	writer, err := sink.Start(pending.Metadata{})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	cancel()
	if _, err := writer.Write([]byte("hello")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() error = %v, want context.Canceled", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer waitCancel()
	if err := sink.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait(timeout) error = %v, want context deadline exceeded", err)
	}
}
