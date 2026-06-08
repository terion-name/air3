package s3fetch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/tickets"
)

var (
	ErrNotFound       = errors.New("s3 object not found")
	ErrInvalidRequest = errors.New("invalid s3 fetch request")
)

type Request struct {
	Method string
	Bucket string
	Key    string
	Range  string
}

type Object struct {
	StatusCode    int
	ContentType   string
	ContentLength int64
	ContentRange  string
	ETag          string
	LastModified  string
	AcceptRanges  string
	Body          io.ReadCloser
}

type Fetcher struct {
	client *s3.Client
}

func New(ctx context.Context, cfg config.S3Config) (*Fetcher, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Endpoint == "" || cfg.Region == "" {
		return nil, errors.New("s3 endpoint and region are required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("s3 access key id and secret access key are required")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.UsePathStyle
		if cfg.InsecureSkipVerify {
			o.HTTPClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // Explicit local-demo compatibility option.
		}
	})
	return &Fetcher{client: client}, nil
}

func NewWithClient(client *s3.Client) *Fetcher {
	return &Fetcher{client: client}
}

func (f *Fetcher) Fetch(ctx context.Context, req Request) (*Object, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if f == nil || f.client == nil {
		return nil, errors.New("s3 client is required")
	}
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	if req.Method == http.MethodHead {
		return f.head(ctx, req)
	}
	return f.get(ctx, req)
}

func (f *Fetcher) get(ctx context.Context, req Request) (*Object, error) {
	input := &s3.GetObjectInput{Bucket: aws.String(req.Bucket), Key: aws.String(req.Key)}
	if req.Range != "" {
		input.Range = aws.String(req.Range)
	}
	out, err := f.client.GetObject(ctx, input)
	if err != nil {
		return nil, mapError(err)
	}
	status := http.StatusOK
	if out.ContentRange != nil && *out.ContentRange != "" {
		status = http.StatusPartialContent
	}
	return &Object{
		StatusCode:    status,
		ContentType:   value(out.ContentType),
		ContentLength: int64Value(out.ContentLength, -1),
		ContentRange:  value(out.ContentRange),
		ETag:          value(out.ETag),
		LastModified:  formatHTTPTime(out.LastModified),
		AcceptRanges:  value(out.AcceptRanges),
		Body:          out.Body,
	}, nil
}

func (f *Fetcher) head(ctx context.Context, req Request) (*Object, error) {
	input := &s3.HeadObjectInput{Bucket: aws.String(req.Bucket), Key: aws.String(req.Key)}
	if req.Range != "" {
		input.Range = aws.String(req.Range)
	}
	out, err := f.client.HeadObject(ctx, input)
	if err != nil {
		return nil, mapError(err)
	}
	status := http.StatusOK
	if req.Range != "" {
		status = http.StatusPartialContent
	}
	return &Object{
		StatusCode:    status,
		ContentType:   value(out.ContentType),
		ContentLength: int64Value(out.ContentLength, -1),
		ETag:          value(out.ETag),
		LastModified:  formatHTTPTime(out.LastModified),
		AcceptRanges:  value(out.AcceptRanges),
		Body:          http.NoBody,
	}, nil
}

func validateRequest(req Request) error {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return fmt.Errorf("%w: method must be GET or HEAD", ErrInvalidRequest)
	}
	if err := tickets.ValidateBucket(req.Bucket); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := tickets.ValidateKey(req.Key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if req.Range != "" {
		t := tickets.Ticket{Version: tickets.Version, RequestID: "range-check", Bucket: req.Bucket, Key: req.Key, Method: req.Method, Range: req.Range, DeadlineUnixMS: time.Now().Add(time.Minute).UnixMilli(), IngestURL: "https://edge.invalid/_ingest/range-check", IngestToken: "range-check-token"}
		if err := t.Validate(time.Now()); err != nil {
			return fmt.Errorf("%w: invalid range", ErrInvalidRequest)
		}
	}
	return nil
}

func mapError(err error) error {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "NotFound", "404":
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
	}
	return err
}

func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func int64Value(n *int64, fallback int64) int64 {
	if n == nil {
		return fallback
	}
	return *n
}

func formatHTTPTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(http.TimeFormat)
}
