package uploadsource

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	PathPrefix  = "/_upload-source/"
	TokenHeader = "X-Air3-Upload-Token"
)

var (
	ErrAlreadyExists = errors.New("upload source already exists")
	ErrInvalidSource = errors.New("invalid upload source")
	ErrInvalidToken  = errors.New("invalid upload token")
	ErrNotFound      = errors.New("upload source not found")
	ErrExpired       = errors.New("upload source expired")
	ErrCanceled      = errors.New("upload source canceled")
	ErrReplayed      = errors.New("upload source already claimed")
)

type Options struct {
	Now func() time.Time
}

type Source struct {
	RequestID     string
	Token         string
	Body          io.ReadCloser
	ContentLength *int64
	ContentType   string
	Deadline      time.Time
	Context       context.Context
}

type Registry struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]*entry
}

type entry struct {
	src     Source
	claimed bool
	done    chan struct{}

	once        sync.Once
	terminalErr error
}

type Claim struct {
	requestID string
	registry  *Registry
	entry     *entry
	body      io.ReadCloser

	contentLength *int64
	contentType   string
}

func NewRegistry(opts Options) *Registry {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{now: now, entries: make(map[string]*entry)}
}

func URLForRequest(base string, requestID string) (string, error) {
	if !safeToken(requestID) {
		return "", fmt.Errorf("%w: request id is required and may contain only safe token characters", ErrInvalidSource)
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", fmt.Errorf("%w: base must be an absolute https URL", ErrInvalidSource)
	}
	if u.User != nil {
		return "", fmt.Errorf("%w: base must not contain credentials", ErrInvalidSource)
	}

	prefix := strings.TrimSuffix(PathPrefix, "/")
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		path = prefix
	} else if !strings.HasSuffix(path, prefix) {
		path += PathPrefix[:len(PathPrefix)-1]
	}
	u.Path = path + "/" + url.PathEscape(requestID)
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (r *Registry) Register(src Source) error {
	if err := validateSource(src, r.now()); err != nil {
		return err
	}
	if src.Context != nil {
		select {
		case <-src.Context.Done():
			return fmt.Errorf("%w: %w", ErrCanceled, src.Context.Err())
		default:
		}
	}

	e := &entry{src: src, done: make(chan struct{})}

	r.mu.Lock()
	if _, exists := r.entries[src.RequestID]; exists {
		r.mu.Unlock()
		return ErrAlreadyExists
	}
	r.entries[src.RequestID] = e
	r.mu.Unlock()

	if src.Context != nil {
		go r.watchSourceContext(src.RequestID, src.Context, e.done)
	}
	return nil
}

func (r *Registry) Claim(requestID, token string) (*Claim, error) {
	now := r.now()

	r.mu.Lock()
	e, ok := r.entries[requestID]
	if !ok {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	if !e.src.Deadline.After(now) {
		delete(r.entries, requestID)
		r.mu.Unlock()
		r.terminate(e, ErrExpired)
		return nil, ErrExpired
	}
	if e.claimed {
		r.mu.Unlock()
		return nil, ErrReplayed
	}
	if !safeToken(token) || subtle.ConstantTimeCompare([]byte(token), []byte(e.src.Token)) != 1 {
		r.mu.Unlock()
		return nil, ErrInvalidToken
	}

	e.claimed = true
	claim := &Claim{
		requestID:     requestID,
		registry:      r,
		entry:         e,
		body:          e.src.Body,
		contentLength: copyInt64Ptr(e.src.ContentLength),
		contentType:   e.src.ContentType,
	}
	r.mu.Unlock()
	return claim, nil
}

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
	r.mu.Unlock()
	return r.terminate(e, err)
}

func (r *Registry) Expire(now time.Time) int {
	r.mu.Lock()
	expired := make([]*entry, 0)
	for id, e := range r.entries {
		if e.src.Deadline.After(now) {
			continue
		}
		delete(r.entries, id)
		expired = append(expired, e)
	}
	r.mu.Unlock()

	for _, e := range expired {
		r.terminate(e, ErrExpired)
	}
	return len(expired)
}

func (c *Claim) ContentLength() *int64 {
	return copyInt64Ptr(c.contentLength)
}

func (c *Claim) ContentType() string {
	return c.contentType
}

func (c *Claim) Read(p []byte) (int, error) {
	return c.body.Read(p)
}

func (c *Claim) Close() error {
	return c.CloseWithError(nil)
}

func (c *Claim) CloseWithError(err error) error {
	if c == nil || c.registry == nil || c.entry == nil {
		return nil
	}
	return c.registry.closeClaim(c, err)
}

func (r *Registry) closeClaim(c *Claim, err error) error {
	var closeErr error
	r.terminate(c.entry, err, func() {
		closeErr = c.body.Close()
	})
	return closeErr
}

func (r *Registry) watchSourceContext(requestID string, ctx context.Context, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		r.Cancel(requestID, ctx.Err())
	case <-done:
	}
}

func (r *Registry) terminate(e *entry, err error, closeFns ...func()) bool {
	terminated := false
	e.once.Do(func() {
		terminated = true
		e.terminalErr = err

		r.mu.Lock()
		if current := r.entries[e.src.RequestID]; current == e {
			delete(r.entries, e.src.RequestID)
		}
		r.mu.Unlock()

		close(e.done)
		if len(closeFns) > 0 {
			for _, closeFn := range closeFns {
				closeFn()
			}
		} else if e.src.Body != nil {
			_ = e.src.Body.Close()
		}
	})
	return terminated
}

func validateSource(src Source, now time.Time) error {
	if src.Body == nil {
		return fmt.Errorf("%w: body is required", ErrInvalidSource)
	}
	if !safeToken(src.RequestID) {
		return fmt.Errorf("%w: request id is required and may contain only safe token characters", ErrInvalidSource)
	}
	if !safeToken(src.Token) {
		return fmt.Errorf("%w: upload token is required and may contain only safe token characters", ErrInvalidSource)
	}
	if src.Deadline.IsZero() {
		return fmt.Errorf("%w: deadline is required", ErrInvalidSource)
	}
	if !src.Deadline.After(now) {
		return ErrExpired
	}
	if src.ContentLength != nil && *src.ContentLength < 0 {
		return fmt.Errorf("%w: content length must not be negative", ErrInvalidSource)
	}
	if err := validateContentType(src.ContentType); err != nil {
		return err
	}
	return nil
}

func validateContentType(contentType string) error {
	if len(contentType) > 255 {
		return fmt.Errorf("%w: content type is too long", ErrInvalidSource)
	}
	for _, r := range contentType {
		if r == 0 || unicode.IsControl(r) {
			return fmt.Errorf("%w: content type must not contain control characters", ErrInvalidSource)
		}
	}
	return nil
}

func safeToken(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == ':' {
			continue
		}
		return false
	}
	return true
}

func copyInt64Ptr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}
