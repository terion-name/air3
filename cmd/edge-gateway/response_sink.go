package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/terion-name/air3/internal/pending"
)

type responseSink struct {
	w      http.ResponseWriter
	method string
	ctx    context.Context
	done   chan struct{}

	mu        sync.Mutex
	started   bool
	completed bool
	err       error
}

func newResponseSink(w http.ResponseWriter, method string, ctx context.Context) *responseSink {
	return &responseSink{w: w, method: method, ctx: ctx, done: make(chan struct{})}
}

func (s *responseSink) Start(metadata pending.Metadata) (io.Writer, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return nil, s.completionErrorLocked()
	}
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	copyPublicMetadata(s.w.Header(), metadata)
	s.w.WriteHeader(publicStatusCode(metadata))
	s.started = true
	if s.method == http.MethodHead {
		return &headResponseWriter{sink: s}, nil
	}
	return &getResponseWriter{sink: s}, nil
}

func (s *responseSink) Finish(err error) error {
	return s.complete(err)
}

func (s *responseSink) Cancel(err error) {
	if err == nil {
		err = pending.ErrCanceled
	}
	_ = s.complete(err)
}

func (s *responseSink) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *responseSink) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *responseSink) complete(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return s.err
	}
	s.completed = true
	s.err = err
	close(s.done)
	return err
}

func (s *responseSink) completionErrorLocked() error {
	if s.err != nil {
		return s.err
	}
	return io.ErrClosedPipe
}

func (s *responseSink) write(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	if s.completed {
		err := s.completionErrorLocked()
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()

	n, err := s.w.Write(p)
	if err != nil {
		return n, err
	}
	if n > 0 {
		if flusher, ok := s.w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	return n, nil
}

func (s *responseSink) discard(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	if s.completed {
		err := s.completionErrorLocked()
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()
	return len(p), nil
}

type getResponseWriter struct {
	sink *responseSink
}

func (w *getResponseWriter) Write(p []byte) (int, error) {
	return w.sink.write(p)
}

type headResponseWriter struct {
	sink *responseSink
}

func (w *headResponseWriter) Write(p []byte) (int, error) {
	return w.sink.discard(p)
}

func copyPublicMetadata(dst http.Header, metadata pending.Metadata) {
	setTrimmedHeader(dst, "Content-Type", metadata.ContentType)
	setTrimmedHeader(dst, "Content-Length", metadata.ContentLength)
	setTrimmedHeader(dst, "Content-Range", metadata.ContentRange)
	setTrimmedHeader(dst, "ETag", metadata.ETag)
	setTrimmedHeader(dst, "Last-Modified", metadata.LastModified)
	setTrimmedHeader(dst, "Accept-Ranges", metadata.AcceptRanges)
}

func setTrimmedHeader(h http.Header, name, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	h.Set(name, value)
}

func publicStatusCode(metadata pending.Metadata) int {
	if metadata.StatusCode == 0 {
		return http.StatusOK
	}
	return metadata.StatusCode
}
