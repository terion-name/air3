package pending

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
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
	Operation   tickets.Operation
	Bucket      string
	Key         string
	Range       string
	List        *tickets.ListRequest
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

// Target is the public response sink for a pending request. Start commits the
// response metadata and returns the body writer used by connector ingest.
type Target interface {
	Start(Metadata) (io.Writer, error)
	Finish(error) error
	Cancel(error)
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
	req     Request
	target  Target
	claimed bool
	stream  *IngestStream
	err     error
}

// IngestStream is the connector-side writer for an accepted ingest body.
type IngestStream struct {
	requestID string
	registry  *Registry
	target    Target
	writer    io.Writer

	once sync.Once
	mu   sync.Mutex
	done bool
	err  error
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
func (r *Registry) Register(req Request, target Target) error {
	if target == nil {
		return fmt.Errorf("%w: target is required", ErrInvalidRequest)
	}
	if err := validateRequest(req, r.now()); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[req.ID]; exists {
		return ErrAlreadyExists
	}
	r.entries[req.ID] = &entry{req: req, target: target}
	return nil
}

// StartIngest validates request state and one-time token, then claims the target
// writer. The returned stream must be closed to finish the target and release the
// registry entry.
func (r *Registry) StartIngest(requestID, token string, metadata Metadata) (*IngestStream, error) {
	now := r.now()
	var cancelTarget Target
	var cancelErr error

	r.mu.Lock()
	e, ok := r.entries[requestID]
	if !ok {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	if err := e.currentError(now); err != nil {
		delete(r.entries, requestID)
		cancelTarget = e.target
		cancelErr = err
		r.mu.Unlock()
		cancelTarget.Cancel(cancelErr)
		return nil, err
	}
	if e.claimed {
		r.mu.Unlock()
		return nil, ErrReplayed
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(e.req.IngestToken)) != 1 {
		r.mu.Unlock()
		return nil, ErrInvalidToken
	}

	e.claimed = true
	writer, err := e.target.Start(metadata)
	if err != nil {
		delete(r.entries, requestID)
		r.mu.Unlock()
		e.target.Cancel(err)
		return nil, err
	}
	stream := &IngestStream{requestID: requestID, registry: r, target: e.target, writer: writer}
	e.stream = stream
	r.mu.Unlock()
	return stream, nil
}

func (s *IngestStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return 0, err
	}
	if s.done {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	s.mu.Unlock()

	n, err := s.writer.Write(p)
	if err != nil {
		return n, err
	}

	s.mu.Lock()
	cancelErr := s.err
	s.mu.Unlock()
	if cancelErr != nil {
		return n, cancelErr
	}
	return n, nil
}

func (s *IngestStream) Close() error {
	return s.CloseWithError(nil)
}

func (s *IngestStream) CloseWithError(err error) error {
	var closeErr error
	s.once.Do(func() {
		s.mu.Lock()
		s.done = true
		s.mu.Unlock()

		closeErr = s.target.Finish(err)
		s.registry.remove(s.requestID)
	})
	return closeErr
}

// Cancel makes a pending request unavailable to future ingest attempts and
// notifies its target. It returns false when the request is already unknown.
func (r *Registry) Cancel(requestID string, err error) bool {
	if err == nil {
		err = ErrCanceled
	}

	r.mu.Lock()
	e, ok := r.entries[requestID]
	if !ok {
		r.mu.Unlock()
		return false
	}
	delete(r.entries, requestID)
	e.err = err
	stream := e.stream
	target := e.target
	r.mu.Unlock()

	if stream != nil {
		stream.cancel(err)
	}
	target.Cancel(err)
	return true
}

// Expire cancels all pending entries whose deadlines are not after now and
// returns the number of entries expired.
func (r *Registry) Expire(now time.Time) int {
	type expiredEntry struct {
		target Target
		stream *IngestStream
	}

	r.mu.Lock()
	expired := make([]expiredEntry, 0)
	for id, e := range r.entries {
		if e.req.Deadline.After(now) {
			continue
		}
		delete(r.entries, id)
		e.err = ErrExpired
		expired = append(expired, expiredEntry{target: e.target, stream: e.stream})
	}
	r.mu.Unlock()

	for _, e := range expired {
		if e.stream != nil {
			e.stream.cancel(ErrExpired)
		}
		e.target.Cancel(ErrExpired)
	}
	return len(expired)
}

func (r *Registry) remove(requestID string) {
	r.mu.Lock()
	delete(r.entries, requestID)
	r.mu.Unlock()
}

func (s *IngestStream) cancel(err error) {
	if err == nil {
		err = ErrCanceled
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
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
	operation, err := tickets.ResolveOperation(req.Method, req.Operation)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := tickets.ValidateBucket(req.Bucket); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if req.Deadline.IsZero() || !req.Deadline.After(now) {
		return ErrExpired
	}

	switch operation {
	case tickets.OperationGetObject, tickets.OperationHeadObject:
		if req.List != nil {
			return fmt.Errorf("%w: list metadata must be omitted for object requests", ErrInvalidRequest)
		}
		if err := tickets.ValidateKey(req.Key); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		if err := tickets.ValidateByteRange(req.Range); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	case tickets.OperationPutObject, tickets.OperationDeleteObject:
		if req.List != nil {
			return fmt.Errorf("%w: list metadata must be omitted for object requests", ErrInvalidRequest)
		}
		if req.Range != "" {
			return fmt.Errorf("%w: range must be omitted for mutation requests", ErrInvalidRequest)
		}
		if err := tickets.ValidateKey(req.Key); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	case tickets.OperationListObjectsV2:
		if req.Key != "" {
			return fmt.Errorf("%w: key must be empty for ListObjectsV2", ErrInvalidRequest)
		}
		if req.Range != "" {
			return fmt.Errorf("%w: range must be omitted for ListObjectsV2", ErrInvalidRequest)
		}
		if err := tickets.ValidateListRequest(req.List); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	default:
		return fmt.Errorf("%w: unsupported operation", ErrInvalidRequest)
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
