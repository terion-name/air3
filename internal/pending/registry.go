package pending

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/terion-name/air3/internal/tickets"
)

var (
	ErrAlreadyExists  = errors.New("pending request already exists")
	ErrInvalidRequest = errors.New("invalid pending request")
	ErrInvalidToken   = errors.New("invalid ingest token")
	ErrNotFound       = errors.New("pending request not found")
	ErrExpired        = errors.New("pending request expired")
	ErrCanceled       = errors.New("pending request canceled")
	ErrReplayed       = errors.New("pending request already completed")
)

// Request is one public edge request held while a private connector fetches the
// object and streams it back to the edge ingest endpoint.
type Request struct {
	ID          string
	Deadline    time.Time
	IngestToken string
	Method      string
	Bucket      string
	Key         string
	Range       string
}

// Metadata is the small allowlisted response metadata accepted from the
// connector before object bytes begin streaming.
type Metadata struct {
	StatusCode    int
	ContentType   string
	ContentLength string
	ContentRange  string
	ETag          string
	LastModified  string
	AcceptRanges  string
}

// Header returns Metadata encoded as safe HTTP response headers. It only emits
// the object metadata fields this package knows how to carry.
func (m Metadata) Header() http.Header {
	h := make(http.Header)
	setHeader(h, "Content-Type", m.ContentType)
	setHeader(h, "Content-Length", m.ContentLength)
	setHeader(h, "Content-Range", m.ContentRange)
	setHeader(h, "ETag", m.ETag)
	setHeader(h, "Last-Modified", m.LastModified)
	setHeader(h, "Accept-Ranges", m.AcceptRanges)
	return h
}

// Response is delivered to the held public request once connector ingest starts.
// The caller owns Body and must close it when the public response is done.
type Response struct {
	Request  Request
	Metadata Metadata
	Body     io.ReadCloser
}

// Registry tracks pending requests in memory for one edge gateway process.
type Registry struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]*entry
}

type Options struct {
	Now func() time.Time
}

type entry struct {
	req  Request
	ch   chan result
	done bool
	err  error
}

type result struct {
	resp Response
	err  error
}

// IngestStream is the connector-side writer for an accepted ingest body.
type IngestStream struct {
	requestID string
	registry  *Registry
	writer    *io.PipeWriter
	once      sync.Once
}

func NewRegistry(opts Options) *Registry {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{now: now, entries: make(map[string]*entry)}
}

// Register creates or replaces no state outside this process. Request IDs must
// be unique until the request expires, is canceled, or its ingest stream closes.
func (r *Registry) Register(req Request) error {
	if err := validateRequest(req, r.now()); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[req.ID]; exists {
		return ErrAlreadyExists
	}
	r.entries[req.ID] = &entry{req: req, ch: make(chan result, 1)}
	return nil
}

// Wait blocks until ingest metadata/body is available, the pending request is
// canceled or expired, or ctx is canceled. If ctx is canceled before ingest
// starts, the pending request is canceled so a late connector POST cannot win.
func (r *Registry) Wait(ctx context.Context, requestID string) (Response, error) {
	e, err := r.lookupWaiting(requestID)
	if err != nil {
		return Response{}, err
	}

	select {
	case res := <-e.ch:
		return res.resp, res.err
	case <-ctx.Done():
		r.Cancel(requestID)
		return Response{}, ctx.Err()
	}
}

// StartIngest validates request state and one-time token, then hands a streaming
// body reader to Wait callers. The returned stream must be closed to release the
// registry entry.
func (r *Registry) StartIngest(requestID, token string, metadata Metadata) (*IngestStream, error) {
	now := r.now()

	r.mu.Lock()
	e, ok := r.entries[requestID]
	if !ok {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	if err := e.currentError(now); err != nil {
		r.finishLocked(requestID, e, err, true)
		r.mu.Unlock()
		return nil, err
	}
	if e.done {
		r.mu.Unlock()
		return nil, ErrReplayed
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(e.req.IngestToken)) != 1 {
		r.mu.Unlock()
		return nil, ErrInvalidToken
	}

	reader, writer := io.Pipe()
	stream := &IngestStream{requestID: requestID, registry: r, writer: writer}
	e.done = true
	e.ch <- result{resp: Response{Request: e.req, Metadata: metadata, Body: reader}}
	r.mu.Unlock()
	return stream, nil
}

func (s *IngestStream) Write(p []byte) (int, error) {
	return s.writer.Write(p)
}

func (s *IngestStream) Close() error {
	return s.CloseWithError(nil)
}

func (s *IngestStream) CloseWithError(err error) error {
	var closeErr error
	s.once.Do(func() {
		if err != nil {
			closeErr = s.writer.CloseWithError(err)
		} else {
			closeErr = s.writer.Close()
		}
		s.registry.remove(s.requestID)
	})
	return closeErr
}

// Cancel makes a pending request unavailable to future ingest attempts and wakes
// any waiter. It returns false when the request is already unknown.
func (r *Registry) Cancel(requestID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[requestID]
	if !ok {
		return false
	}
	if e.done {
		delete(r.entries, requestID)
		return true
	}
	r.finishLocked(requestID, e, ErrCanceled, true)
	return true
}

// Expire cancels all pending entries whose deadlines are not after now and
// returns the number of entries expired.
func (r *Registry) Expire(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	expired := 0
	for id, e := range r.entries {
		if e.req.Deadline.After(now) {
			continue
		}
		if e.done {
			delete(r.entries, id)
		} else {
			r.finishLocked(id, e, ErrExpired, true)
		}
		expired++
	}
	return expired
}

func (r *Registry) lookupWaiting(requestID string) (*entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[requestID]
	if !ok {
		return nil, ErrNotFound
	}
	if err := e.currentError(r.now()); err != nil {
		r.finishLocked(requestID, e, err, true)
		return nil, err
	}
	return e, nil
}

func (r *Registry) finishLocked(requestID string, e *entry, err error, remove bool) {
	if !e.done {
		e.done = true
		e.err = err
		e.ch <- result{err: err}
	}
	if remove {
		delete(r.entries, requestID)
	}
}

func (r *Registry) remove(requestID string) {
	r.mu.Lock()
	delete(r.entries, requestID)
	r.mu.Unlock()
}

func (e *entry) currentError(now time.Time) error {
	if e.err != nil {
		return e.err
	}
	if !e.req.Deadline.After(now) {
		return ErrExpired
	}
	return nil
}

func validateRequest(req Request, now time.Time) error {
	if !safeToken(req.ID) {
		return fmt.Errorf("%w: request id is required and may contain only safe token characters", ErrInvalidRequest)
	}
	if !safeToken(req.IngestToken) {
		return fmt.Errorf("%w: ingest token is required and may contain only safe token characters", ErrInvalidRequest)
	}
	if req.Method != "GET" && req.Method != "HEAD" {
		return fmt.Errorf("%w: method must be GET or HEAD", ErrInvalidRequest)
	}
	if err := tickets.ValidateBucket(req.Bucket); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := tickets.ValidateKey(req.Key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if req.Range != "" {
		ticket := tickets.Ticket{Version: tickets.Version, RequestID: req.ID, Bucket: req.Bucket, Key: req.Key, Method: req.Method, Range: req.Range, DeadlineUnixMS: req.Deadline.UnixMilli(), IngestURL: "https://edge.invalid/_ingest/" + req.ID, IngestToken: req.IngestToken}
		if err := ticket.Validate(now.Add(-time.Nanosecond)); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	}
	if req.Deadline.IsZero() || !req.Deadline.After(now) {
		return ErrExpired
	}
	return nil
}

func safeToken(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':' {
			continue
		}
		return false
	}
	return true
}

func setHeader(h http.Header, name, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	h.Set(name, value)
}
