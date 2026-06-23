package s3fetch

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/s3api"
	"github.com/terion-name/air3/internal/tickets"
)

var (
	ErrNotFound       = errors.New("s3 object not found")
	ErrInvalidRequest = errors.New("invalid s3 fetch request")
)

type Request struct {
	Method        string
	Operation     tickets.Operation
	Bucket        string
	Key           string
	Range         string
	List          *tickets.ListRequest
	Body          io.Reader
	ContentLength *int64
	ContentType   string
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
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Explicit local-demo compatibility option.
			transport.DisableCompression = true
			o.HTTPClient = &http.Client{Transport: transport}
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
	operation, err := validateRequest(req)
	if err != nil {
		return nil, err
	}
	switch operation {
	case tickets.OperationHeadObject:
		return f.head(ctx, req)
	case tickets.OperationListObjectsV2:
		return f.list(ctx, req)
	case tickets.OperationPutObject:
		return f.put(ctx, req)
	case tickets.OperationDeleteObject:
		return f.delete(ctx, req)
	default:
		return f.get(ctx, req)
	}
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

func (f *Fetcher) put(ctx context.Context, req Request) (*Object, error) {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(req.Bucket),
		Key:           aws.String(req.Key),
		Body:          req.Body,
		ContentLength: req.ContentLength,
	}
	if req.ContentType != "" {
		input.ContentType = aws.String(req.ContentType)
	}
	out, err := f.client.PutObject(ctx, input)
	if err != nil {
		return nil, mapError(err)
	}
	return &Object{
		StatusCode: http.StatusOK,
		ETag:       value(out.ETag),
		Body:       http.NoBody,
	}, nil
}

func (f *Fetcher) delete(ctx context.Context, req Request) (*Object, error) {
	_, err := f.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(req.Bucket), Key: aws.String(req.Key)})
	if err != nil {
		return nil, mapError(err)
	}
	return &Object{
		StatusCode: http.StatusNoContent,
		Body:       http.NoBody,
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

func (f *Fetcher) list(ctx context.Context, req Request) (*Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(req.Bucket),
		MaxKeys: aws.Int32(int32(req.List.MaxKeys)),
	}
	if req.List.Prefix != "" {
		input.Prefix = aws.String(req.List.Prefix)
	}
	if req.List.Delimiter != "" {
		input.Delimiter = aws.String(req.List.Delimiter)
	}
	if req.List.ContinuationToken != "" {
		input.ContinuationToken = aws.String(req.List.ContinuationToken)
	}
	if req.List.StartAfter != "" {
		input.StartAfter = aws.String(req.List.StartAfter)
	}
	if req.List.EncodingType != "" {
		input.EncodingType = types.EncodingType(req.List.EncodingType)
	}
	if req.List.FetchOwner {
		input.FetchOwner = aws.Bool(true)
	}

	out, err := f.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, mapError(err)
	}

	result := listBucketResult(req, out)
	s3api.ApplyListPublicPrefix(&result, req.List.Rewrite.KeyPrefix)
	xmlBody, err := s3api.RenderListBucketResult(result)
	if err != nil {
		return nil, err
	}

	return &Object{
		StatusCode:    http.StatusOK,
		ContentType:   "application/xml",
		ContentLength: int64(len(xmlBody)),
		Body:          io.NopCloser(bytes.NewReader(xmlBody)),
	}, nil
}

func listBucketResult(req Request, out *s3.ListObjectsV2Output) s3api.ListBucketResult {
	result := s3api.ListBucketResult{
		Name:                  firstNonEmpty(req.List.Rewrite.Bucket, req.Bucket),
		Prefix:                firstNonEmpty(req.List.Rewrite.Prefix, value(out.Prefix), req.List.Prefix),
		Delimiter:             firstNonEmpty(value(out.Delimiter), req.List.Delimiter),
		KeyCount:              int32Value(out.KeyCount, 0),
		MaxKeys:               int32Value(out.MaxKeys, req.List.MaxKeys),
		IsTruncated:           boolValue(out.IsTruncated),
		ContinuationToken:     firstNonEmpty(value(out.ContinuationToken), req.List.ContinuationToken),
		NextContinuationToken: value(out.NextContinuationToken),
		StartAfter:            firstNonEmpty(value(out.StartAfter), req.List.StartAfter),
		EncodingType:          firstNonEmpty(string(out.EncodingType), req.List.EncodingType),
	}

	for _, object := range out.Contents {
		result.Contents = append(result.Contents, s3api.ListObject{
			Key:          value(object.Key),
			LastModified: formatS3Time(object.LastModified),
			ETag:         value(object.ETag),
			Size:         int64Value(object.Size, 0),
			StorageClass: string(object.StorageClass),
		})
	}
	for _, prefix := range out.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, value(prefix.Prefix))
	}
	return result
}

func validateRequest(req Request) (tickets.Operation, error) {
	operation, err := tickets.ResolveOperation(req.Method, req.Operation)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := tickets.ValidateBucket(req.Bucket); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}

	switch operation {
	case tickets.OperationGetObject, tickets.OperationHeadObject:
		if err := validateMutationFieldsOmitted(req); err != nil {
			return "", err
		}
		if req.List != nil {
			return "", fmt.Errorf("%w: list metadata must be omitted for object requests", ErrInvalidRequest)
		}
		if err := tickets.ValidateKey(req.Key); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		if err := tickets.ValidateByteRange(req.Range); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	case tickets.OperationListObjectsV2:
		if err := validateMutationFieldsOmitted(req); err != nil {
			return "", err
		}
		if req.Key != "" {
			return "", fmt.Errorf("%w: key must be empty for ListObjectsV2", ErrInvalidRequest)
		}
		if req.Range != "" {
			return "", fmt.Errorf("%w: range must be omitted for ListObjectsV2", ErrInvalidRequest)
		}
		if err := tickets.ValidateListRequest(req.List); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	case tickets.OperationPutObject:
		if req.List != nil {
			return "", fmt.Errorf("%w: list metadata must be omitted for PutObject", ErrInvalidRequest)
		}
		if req.Range != "" {
			return "", fmt.Errorf("%w: range must be omitted for PutObject", ErrInvalidRequest)
		}
		if err := tickets.ValidateKey(req.Key); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		if req.Body == nil {
			return "", fmt.Errorf("%w: body is required for PutObject", ErrInvalidRequest)
		}
		if req.ContentLength == nil {
			return "", fmt.Errorf("%w: content length is required for PutObject", ErrInvalidRequest)
		}
		if *req.ContentLength < 0 {
			return "", fmt.Errorf("%w: content length must be non-negative", ErrInvalidRequest)
		}
		if err := validateContentType(req.ContentType); err != nil {
			return "", err
		}
	case tickets.OperationDeleteObject:
		if err := validateMutationFieldsOmitted(req); err != nil {
			return "", err
		}
		if req.List != nil {
			return "", fmt.Errorf("%w: list metadata must be omitted for DeleteObject", ErrInvalidRequest)
		}
		if req.Range != "" {
			return "", fmt.Errorf("%w: range must be omitted for DeleteObject", ErrInvalidRequest)
		}
		if err := tickets.ValidateKey(req.Key); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	default:
		return "", fmt.Errorf("%w: unsupported operation", ErrInvalidRequest)
	}
	return operation, nil
}

func validateMutationFieldsOmitted(req Request) error {
	if req.Body != nil {
		return fmt.Errorf("%w: body must be omitted", ErrInvalidRequest)
	}
	if req.ContentLength != nil {
		return fmt.Errorf("%w: content length must be omitted", ErrInvalidRequest)
	}
	if req.ContentType != "" {
		return fmt.Errorf("%w: content type must be omitted", ErrInvalidRequest)
	}
	return nil
}

func validateContentType(contentType string) error {
	if len(contentType) > 255 {
		return fmt.Errorf("%w: content type is too long", ErrInvalidRequest)
	}
	for _, r := range contentType {
		if r == 0 || unicode.IsControl(r) {
			return fmt.Errorf("%w: content type must not contain control characters", ErrInvalidRequest)
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

func int32Value(n *int32, fallback int) int {
	if n == nil {
		return fallback
	}
	return int(*n)
}

func boolValue(b *bool) bool {
	return b != nil && *b
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func formatS3Time(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatHTTPTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(http.TimeFormat)
}
