package tickets

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/terion-name/air3/internal/publicpath"
)

const Version = 1

var (
	ErrInvalidTicket = errors.New("invalid ticket")
	bucketNameRE     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	rangeRE          = regexp.MustCompile(`^bytes=(\d+)-(\d*)$|^bytes=-(\d+)$`)
)

// Ticket is the NATS control-plane message for one live HTTP work item.
//
// The JSON schema is intentionally small and closed. Tickets carry only the
// object identity, deadline, ingest callback, one-time ingest token, and safe
// tracing metadata. They must never contain S3 credentials, object bytes,
// public client secrets, or raw untrusted request headers.
type Ticket struct {
	Version        int    `json:"version"`
	RequestID      string `json:"request_id"`
	Bucket         string `json:"bucket"`
	Key            string `json:"key"`
	Method         string `json:"method"`
	Range          string `json:"range,omitempty"`
	Server         string `json:"server,omitempty"`
	DeadlineUnixMS int64  `json:"deadline_unix_ms"`
	IngestURL      string `json:"ingest_url"`
	IngestToken    string `json:"ingest_token"`
	TraceID        string `json:"trace_id,omitempty"`
}

// Marshal validates t against now before returning its canonical JSON encoding.
func Marshal(t Ticket, now time.Time) ([]byte, error) {
	if err := t.Validate(now); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

// Unmarshal decodes a ticket from a closed JSON schema and validates it against now.
func Unmarshal(data []byte, now time.Time) (Ticket, error) {
	if err := rejectForbiddenFields(data); err != nil {
		return Ticket{}, err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var t Ticket
	if err := dec.Decode(&t); err != nil {
		return Ticket{}, fmt.Errorf("%w: decoding ticket: %v", ErrInvalidTicket, err)
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Ticket{}, fmt.Errorf("%w: trailing ticket data", ErrInvalidTicket)
	}
	if err := t.Validate(now); err != nil {
		return Ticket{}, err
	}
	return t, nil
}

// Validate checks that t is a live, safe control-plane work item.
func (t Ticket) Validate(now time.Time) error {
	if t.Version != Version {
		return fieldError("version", "must be 1")
	}
	if !saneToken(t.RequestID) {
		return fieldError("request_id", "is required and may contain only safe token characters")
	}
	if err := ValidateBucket(t.Bucket); err != nil {
		return err
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	if t.Method != "GET" && t.Method != "HEAD" {
		return fieldError("method", "must be GET or HEAD")
	}
	if t.Range != "" && !validRange(t.Range) {
		return fieldError("range", "must be a single HTTP byte range")
	}
	if t.Server != "" {
		if err := publicpath.ValidateAlias(t.Server); err != nil {
			return fieldError("server", "must be a valid server alias")
		}
	}
	if t.DeadlineUnixMS <= 0 {
		return fieldError("deadline_unix_ms", "is required")
	}
	if !now.IsZero() && !time.UnixMilli(t.DeadlineUnixMS).After(now) {
		return fieldError("deadline_unix_ms", "is expired")
	}
	if err := validateIngestURL(t.IngestURL); err != nil {
		return err
	}
	if !saneToken(t.IngestToken) {
		return fieldError("ingest_token", "is required and may contain only safe token characters")
	}
	if t.TraceID != "" && !saneTraceID(t.TraceID) {
		return fieldError("trace_id", "contains unsafe characters")
	}
	return nil
}

func ValidateBucket(bucket string) error {
	if !bucketNameRE.MatchString(bucket) {
		return fieldError("bucket", "must be a valid DNS-style S3 bucket name")
	}
	if strings.Contains(bucket, "..") || strings.Contains(bucket, ".-") || strings.Contains(bucket, "-.") {
		return fieldError("bucket", "contains invalid dot/hyphen placement")
	}
	if net.ParseIP(bucket) != nil {
		return fieldError("bucket", "must not look like an IP address")
	}
	return nil
}

func ValidateKey(key string) error {
	if key == "" {
		return fieldError("key", "is required")
	}
	if len(key) > 1024 {
		return fieldError("key", "is too long")
	}
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return fieldError("key", "must not start or end with slash")
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return fieldError("key", "must not contain empty or traversal path segments")
		}
	}
	for _, r := range key {
		if r == 0 || unicode.IsControl(r) {
			return fieldError("key", "must not contain control characters")
		}
	}
	return nil
}

func fieldError(field, message string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidTicket, field, message)
}

func saneToken(s string) bool {
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

func saneTraceID(s string) bool {
	if len(s) > 128 {
		return false
	}
	return saneToken(s)
}

func validRange(s string) bool {
	m := rangeRE.FindStringSubmatch(s)
	if m == nil {
		return false
	}
	if m[1] == "" || m[2] == "" {
		return true
	}
	start, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return false
	}
	end, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return false
	}
	return end >= start
}

func validateIngestURL(raw string) error {
	if raw == "" {
		return fieldError("ingest_url", "is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fieldError("ingest_url", "must be an absolute https URL")
	}
	if u.User != nil {
		return fieldError("ingest_url", "must not contain credentials")
	}
	return nil
}

func rejectForbiddenFields(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: decoding ticket object: %v", ErrInvalidTicket, err)
	}
	for name := range raw {
		normalized := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
		switch normalized {
		case "access_key", "access_key_id", "secret_key", "secret_access_key", "session_token", "s3_credentials", "credentials", "object_bytes", "bytes", "body", "payload", "public_secret", "client_secret", "headers", "raw_headers":
			return fmt.Errorf("%w: forbidden ticket field %q", ErrInvalidTicket, name)
		}
	}
	return nil
}
