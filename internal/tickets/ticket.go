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

type Operation string

const (
	OperationGetObject               Operation = "GetObject"
	OperationHeadObject              Operation = "HeadObject"
	OperationListObjectsV2           Operation = "ListObjectsV2"
	OperationPutObject               Operation = "PutObject"
	OperationDeleteObject            Operation = "DeleteObject"
	OperationCreateMultipartUpload   Operation = "CreateMultipartUpload"
	OperationUploadPart              Operation = "UploadPart"
	OperationCompleteMultipartUpload Operation = "CompleteMultipartUpload"
	OperationAbortMultipartUpload    Operation = "AbortMultipartUpload"
)

// MaxCompleteMultipartBodyBytes caps the CompleteMultipartUpload part-list XML
// accepted from clients. S3 allows at most 10,000 parts, which stays well
// under this limit.
const MaxCompleteMultipartBodyBytes = 1 << 20

// MaxPartNumber is the highest S3 multipart part number.
const MaxPartNumber = 10000

var (
	ErrInvalidTicket = errors.New("invalid ticket")
	bucketNameRE     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	rangeRE          = regexp.MustCompile(`^bytes=(\d+)-(\d*)$|^bytes=-(\d+)$`)
)

// ListRequest carries the safe, normalized ListObjectsV2 parameters needed by a
// connector to perform a bucket listing and rewrite the public response paths.
type ListRequest struct {
	Prefix            string      `json:"prefix,omitempty"`
	Delimiter         string      `json:"delimiter,omitempty"`
	ContinuationToken string      `json:"continuation_token,omitempty"`
	StartAfter        string      `json:"start_after,omitempty"`
	MaxKeys           int         `json:"max_keys"`
	EncodingType      string      `json:"encoding_type,omitempty"`
	FetchOwner        bool        `json:"fetch_owner,omitempty"`
	Rewrite           ListRewrite `json:"rewrite"`
}

// ListRewrite describes the public bucket/prefix shape emitted for list results.
type ListRewrite struct {
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	KeyPrefix string `json:"key_prefix,omitempty"`
}

// MultipartRequest carries the multipart-upload identity for one ticket and
// the public bucket/key names echoed in multipart response XML. Part lists
// never ride tickets; the CompleteMultipartUpload XML streams through the
// upload source instead.
type MultipartRequest struct {
	UploadID   string           `json:"upload_id,omitempty"`
	PartNumber int32            `json:"part_number,omitempty"`
	Rewrite    MultipartRewrite `json:"rewrite"`
}

// MultipartRewrite describes the public bucket/key names for multipart
// response XML.
type MultipartRewrite struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// Ticket is the NATS control-plane message for one live HTTP work item.
//
// The JSON schema is intentionally small and closed. Tickets carry only the
// object/list identity, mutation upload envelope, deadline, ingest callback,
// one-time ingest token, and safe tracing metadata. They must never contain S3
// credentials, object bytes, public client secrets, or raw untrusted request
// headers.
type Ticket struct {
	Version         int               `json:"version"`
	RequestID       string            `json:"request_id"`
	Bucket          string            `json:"bucket"`
	Key             string            `json:"key"`
	Method          string            `json:"method"`
	Operation       Operation         `json:"operation,omitempty"`
	Range           string            `json:"range,omitempty"`
	List            *ListRequest      `json:"list,omitempty"`
	Multipart       *MultipartRequest `json:"multipart,omitempty"`
	UploadSourceURL string            `json:"upload_source_url,omitempty"`
	UploadToken     string            `json:"upload_token,omitempty"`
	ContentLength   *int64            `json:"content_length,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	Server          string            `json:"server,omitempty"`
	DeadlineUnixMS  int64             `json:"deadline_unix_ms"`
	IngestURL       string            `json:"ingest_url"`
	IngestToken     string            `json:"ingest_token"`
	TraceID         string            `json:"trace_id,omitempty"`
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
	op, err := ResolveOperation(t.Method, t.Operation)
	if err != nil {
		return err
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

	switch op {
	case OperationGetObject, OperationHeadObject:
		return t.validateObjectOperation()
	case OperationListObjectsV2:
		return t.validateListOperation()
	case OperationPutObject:
		return t.validatePutObjectOperation()
	case OperationDeleteObject:
		return t.validateDeleteObjectOperation()
	case OperationCreateMultipartUpload:
		return t.validateCreateMultipartOperation()
	case OperationUploadPart:
		return t.validateUploadPartOperation()
	case OperationCompleteMultipartUpload:
		return t.validateCompleteMultipartOperation()
	case OperationAbortMultipartUpload:
		return t.validateAbortMultipartOperation()
	default:
		return fieldError("operation", "is unsupported")
	}
}

// ResolveOperation resolves the ticket operation from an explicit operation or,
// for legacy object tickets, from the HTTP method.
func ResolveOperation(method string, op Operation) (Operation, error) {
	if op == "" {
		switch method {
		case "GET":
			return OperationGetObject, nil
		case "HEAD":
			return OperationHeadObject, nil
		default:
			return "", fieldError("method", "must be GET or HEAD")
		}
	}

	switch op {
	case OperationGetObject:
		if method != "GET" {
			return "", fieldError("operation", "GetObject requires method GET")
		}
	case OperationHeadObject:
		if method != "HEAD" {
			return "", fieldError("operation", "HeadObject requires method HEAD")
		}
	case OperationListObjectsV2:
		if method != "GET" {
			return "", fieldError("operation", "ListObjectsV2 requires method GET")
		}
	case OperationPutObject:
		if method != "PUT" {
			return "", fieldError("operation", "PutObject requires method PUT")
		}
	case OperationDeleteObject:
		if method != "DELETE" {
			return "", fieldError("operation", "DeleteObject requires method DELETE")
		}
	case OperationCreateMultipartUpload:
		if method != "POST" {
			return "", fieldError("operation", "CreateMultipartUpload requires method POST")
		}
	case OperationUploadPart:
		if method != "PUT" {
			return "", fieldError("operation", "UploadPart requires method PUT")
		}
	case OperationCompleteMultipartUpload:
		if method != "POST" {
			return "", fieldError("operation", "CompleteMultipartUpload requires method POST")
		}
	case OperationAbortMultipartUpload:
		if method != "DELETE" {
			return "", fieldError("operation", "AbortMultipartUpload requires method DELETE")
		}
	default:
		return "", fieldError("operation", "is unsupported")
	}
	return op, nil
}

func (t Ticket) validateObjectOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for object operations")
	}
	if t.Multipart != nil {
		return fieldError("multipart", "must be omitted for object operations")
	}
	if err := t.validateUploadEnvelopeOmitted(); err != nil {
		return err
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	return ValidateByteRange(t.Range)
}

func (t Ticket) validateListOperation() error {
	if t.Key != "" {
		return fieldError("key", "must be empty for ListObjectsV2")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for ListObjectsV2")
	}
	if t.Multipart != nil {
		return fieldError("multipart", "must be omitted for ListObjectsV2")
	}
	if err := t.validateUploadEnvelopeOmitted(); err != nil {
		return err
	}
	return ValidateListRequest(t.List)
}

func (t Ticket) validatePutObjectOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for PutObject")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for PutObject")
	}
	if t.Multipart != nil {
		return fieldError("multipart", "must be omitted for PutObject")
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	if err := t.validateUploadEnvelope(); err != nil {
		return err
	}
	return validateContentType(t.ContentType)
}

func (t Ticket) validateDeleteObjectOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for DeleteObject")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for DeleteObject")
	}
	if t.Multipart != nil {
		return fieldError("multipart", "must be omitted for DeleteObject")
	}
	if err := t.validateUploadEnvelopeOmitted(); err != nil {
		return err
	}
	return ValidateKey(t.Key)
}

func (t Ticket) validateCreateMultipartOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for CreateMultipartUpload")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for CreateMultipartUpload")
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	// CreateMultipartUpload carries the object content type but no body, so
	// the upload envelope stays empty.
	if t.UploadSourceURL != "" {
		return fieldError("upload_source_url", "must be omitted for CreateMultipartUpload")
	}
	if t.UploadToken != "" {
		return fieldError("upload_token", "must be omitted for CreateMultipartUpload")
	}
	if t.ContentLength != nil {
		return fieldError("content_length", "must be omitted for CreateMultipartUpload")
	}
	if err := validateContentType(t.ContentType); err != nil {
		return err
	}
	return ValidateMultipartRequest(OperationCreateMultipartUpload, t.Multipart)
}

func (t Ticket) validateUploadPartOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for UploadPart")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for UploadPart")
	}
	if t.ContentType != "" {
		return fieldError("content_type", "must be omitted for UploadPart")
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	if err := t.validateUploadEnvelope(); err != nil {
		return err
	}
	return ValidateMultipartRequest(OperationUploadPart, t.Multipart)
}

func (t Ticket) validateCompleteMultipartOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for CompleteMultipartUpload")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for CompleteMultipartUpload")
	}
	if t.ContentType != "" {
		return fieldError("content_type", "must be omitted for CompleteMultipartUpload")
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	if err := t.validateUploadEnvelope(); err != nil {
		return err
	}
	if *t.ContentLength > MaxCompleteMultipartBodyBytes {
		return fieldError("content_length", "exceeds the CompleteMultipartUpload body limit")
	}
	return ValidateMultipartRequest(OperationCompleteMultipartUpload, t.Multipart)
}

func (t Ticket) validateAbortMultipartOperation() error {
	if t.List != nil {
		return fieldError("list", "must be omitted for AbortMultipartUpload")
	}
	if t.Range != "" {
		return fieldError("range", "must be omitted for AbortMultipartUpload")
	}
	if err := t.validateUploadEnvelopeOmitted(); err != nil {
		return err
	}
	if err := ValidateKey(t.Key); err != nil {
		return err
	}
	return ValidateMultipartRequest(OperationAbortMultipartUpload, t.Multipart)
}

// validateUploadEnvelope checks the upload-source fields required by
// operations whose request body streams through the edge upload source.
func (t Ticket) validateUploadEnvelope() error {
	if err := validateUploadSourceURL(t.UploadSourceURL); err != nil {
		return err
	}
	if !saneToken(t.UploadToken) {
		return fieldError("upload_token", "is required and may contain only safe token characters")
	}
	if t.ContentLength == nil {
		return fieldError("content_length", "is required for upload operations")
	}
	if *t.ContentLength < 0 {
		return fieldError("content_length", "must be non-negative")
	}
	return nil
}

func (t Ticket) validateUploadEnvelopeOmitted() error {
	if t.UploadSourceURL != "" {
		return fieldError("upload_source_url", "must be omitted")
	}
	if t.UploadToken != "" {
		return fieldError("upload_token", "must be omitted")
	}
	if t.ContentLength != nil {
		return fieldError("content_length", "must be omitted")
	}
	if t.ContentType != "" {
		return fieldError("content_type", "must be omitted")
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
	return validateKeyLike("key", key, true)
}

// ValidateByteRange checks an optional HTTP byte range value. Empty ranges are valid.
func ValidateByteRange(byteRange string) error {
	if byteRange == "" {
		return nil
	}
	if !validRange(byteRange) {
		return fieldError("range", "must be a single HTTP byte range")
	}
	return nil
}

// ValidateListRequest checks the safe ListObjectsV2 subset carried by tickets.
func ValidateListRequest(list *ListRequest) error {
	if list == nil {
		return fieldError("list", "is required for ListObjectsV2")
	}
	if list.MaxKeys < 0 || list.MaxKeys > 1000 {
		return fieldError("list.max_keys", "must be between 0 and 1000")
	}
	if list.Delimiter != "" && list.Delimiter != "/" {
		return fieldError("list.delimiter", "must be empty or slash")
	}
	if list.EncodingType != "" && list.EncodingType != "url" {
		return fieldError("list.encoding_type", "must be empty or url")
	}
	if err := validateKeyLike("list.prefix", list.Prefix, false); err != nil {
		return err
	}
	if err := validateKeyLike("list.start_after", list.StartAfter, false); err != nil {
		return err
	}
	if err := validateContinuationToken(list.ContinuationToken); err != nil {
		return err
	}
	if err := ValidateBucket(list.Rewrite.Bucket); err != nil {
		return fieldError("list.rewrite.bucket", "must be a valid DNS-style S3 bucket name")
	}
	if err := validateKeyLike("list.rewrite.prefix", list.Rewrite.Prefix, false); err != nil {
		return err
	}
	if err := validateKeyLike("list.rewrite.key_prefix", list.Rewrite.KeyPrefix, false); err != nil {
		return err
	}
	return nil
}

// ValidateMultipartRequest checks the multipart identity carried by tickets
// for op, which must be one of the four multipart operations.
func ValidateMultipartRequest(op Operation, m *MultipartRequest) error {
	if m == nil {
		return fieldError("multipart", "is required for multipart operations")
	}
	switch op {
	case OperationCreateMultipartUpload:
		if m.UploadID != "" {
			return fieldError("multipart.upload_id", "must be empty for CreateMultipartUpload")
		}
	case OperationUploadPart, OperationCompleteMultipartUpload, OperationAbortMultipartUpload:
		if err := validateUploadID(m.UploadID); err != nil {
			return err
		}
	default:
		return fieldError("multipart", "must be omitted for non-multipart operations")
	}
	if op == OperationUploadPart {
		if m.PartNumber < 1 || m.PartNumber > MaxPartNumber {
			return fieldError("multipart.part_number", "must be between 1 and 10000")
		}
	} else if m.PartNumber != 0 {
		return fieldError("multipart.part_number", "must be set only for UploadPart")
	}
	if err := ValidateBucket(m.Rewrite.Bucket); err != nil {
		return fieldError("multipart.rewrite.bucket", "must be a valid DNS-style S3 bucket name")
	}
	return validateKeyLike("multipart.rewrite.key", m.Rewrite.Key, true)
}

// validateUploadID accepts the printable-ASCII upload IDs minted by S3
// backends (base64-like, sometimes long) without trusting anything wider.
func validateUploadID(id string) error {
	if id == "" || len(id) > 1024 {
		return fieldError("multipart.upload_id", "is required and must be at most 1024 characters")
	}
	for i := 0; i < len(id); i++ {
		if id[i] < '!' || id[i] > '~' {
			return fieldError("multipart.upload_id", "must contain only printable non-space ASCII characters")
		}
	}
	return nil
}

func validateKeyLike(field, value string, requireNonEmpty bool) error {
	if value == "" {
		if requireNonEmpty {
			return fieldError(field, "is required")
		}
		return nil
	}
	if len(value) > 1024 {
		return fieldError(field, "is too long")
	}
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return fieldError(field, "must not start or end with slash")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fieldError(field, "must not contain empty or traversal path segments")
		}
	}
	for _, r := range value {
		if r == 0 || unicode.IsControl(r) {
			return fieldError(field, "must not contain control characters")
		}
	}
	return nil
}

func validateContinuationToken(token string) error {
	if len(token) > 2048 {
		return fieldError("list.continuation_token", "is too long")
	}
	for _, r := range token {
		if r == 0 || unicode.IsControl(r) {
			return fieldError("list.continuation_token", "must not contain control characters")
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

func validateUploadSourceURL(raw string) error {
	if raw == "" {
		return fieldError("upload_source_url", "is required for PutObject")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fieldError("upload_source_url", "must be an absolute https URL")
	}
	if u.User != nil {
		return fieldError("upload_source_url", "must not contain credentials")
	}
	return nil
}

func validateContentType(contentType string) error {
	if len(contentType) > 255 {
		return fieldError("content_type", "is too long")
	}
	for _, r := range contentType {
		if r == 0 || unicode.IsControl(r) {
			return fieldError("content_type", "must not contain control characters")
		}
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
		case "access_key", "access_key_id", "secret_key", "secret_access_key", "session_token", "s3_credentials", "credentials", "object_bytes", "bytes", "body", "payload", "public_secret", "client_secret", "headers", "raw_headers", "authorization", "raw_authorization", "authorization_header", "signed_headers", "x_amz_signedheaders", "x_amz_signature", "x_amz_credential", "x_amz_security_token":
			return fmt.Errorf("%w: forbidden ticket field %q", ErrInvalidTicket, name)
		}
	}
	return nil
}
