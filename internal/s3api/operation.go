package s3api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type RoutingMode int

const (
	RoutingSingleServer RoutingMode = iota
	RoutingMultiServer
)

type ClassifyOptions struct {
	Mode                   RoutingMode
	DefaultBucketForServer func(server string) (bucket string, ok bool)
	ValidateBucket         func(bucket string) error
	ValidateServer         func(server string) error
}

type Operation string

const (
	OperationGetObject               Operation = "GetObject"
	OperationHeadObject              Operation = "HeadObject"
	OperationHeadBucket              Operation = "HeadBucket"
	OperationListObjectsV2           Operation = "ListObjectsV2"
	OperationPutObject               Operation = "PutObject"
	OperationDeleteObject            Operation = "DeleteObject"
	OperationCreateMultipartUpload   Operation = "CreateMultipartUpload"
	OperationUploadPart              Operation = "UploadPart"
	OperationCompleteMultipartUpload Operation = "CompleteMultipartUpload"
	OperationAbortMultipartUpload    Operation = "AbortMultipartUpload"
)

type RequestMapping struct {
	Operation     Operation
	Server        string
	S3Bucket      string
	S3Key         string
	BackendBucket string
	BackendKey    string
	List          ListMapping
	Multipart     MultipartMapping
}

// MultipartMapping carries the multipart identity parsed from the request
// query for the multipart operations.
type MultipartMapping struct {
	UploadID   string
	PartNumber int
}

type ListMapping struct {
	Prefix            string
	BackendPrefix     string
	Delimiter         string
	ContinuationToken string
	StartAfter        string
	MaxKeys           int
	EncodingType      string
	FetchOwner        bool
	PublicKeyPrefix   string
}

func Classify(r *http.Request, opts ClassifyOptions) (RequestMapping, error) {
	if r == nil {
		return RequestMapping{}, fmt.Errorf("classify s3 operation: nil request")
	}
	if !operationSupportedMethod(r.Method) {
		return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported method %s", r.Method)
	}

	segments, err := operationPathSegments(r.URL)
	if err != nil {
		return RequestMapping{}, err
	}
	if len(segments) == 0 || segments[0] == "" {
		return RequestMapping{}, fmt.Errorf("classify s3 operation: missing bucket or server in path")
	}

	switch opts.Mode {
	case RoutingSingleServer:
		return operationClassifySingleServer(r, segments, opts)
	case RoutingMultiServer:
		return operationClassifyMultiServer(r, segments, opts)
	default:
		return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported routing mode %d", opts.Mode)
	}
}

func operationClassifySingleServer(r *http.Request, segments []string, opts ClassifyOptions) (RequestMapping, error) {
	bucket := segments[0]
	if err := operationValidateBucket(opts, bucket); err != nil {
		return RequestMapping{}, err
	}

	if r.Method == http.MethodHead && len(segments) == 1 {
		return RequestMapping{
			Operation:     OperationHeadBucket,
			S3Bucket:      bucket,
			BackendBucket: bucket,
		}, nil
	}

	if r.Method == http.MethodGet && operationIsListObjectsV2(r) {
		if len(segments) != 1 {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: ListObjectsV2 path must name only a bucket")
		}
		list, err := operationParseListMapping(r)
		if err != nil {
			return RequestMapping{}, err
		}
		list.BackendPrefix = list.Prefix
		return RequestMapping{
			Operation:     OperationListObjectsV2,
			S3Bucket:      bucket,
			BackendBucket: bucket,
			List:          list,
		}, nil
	}

	operation, multipart, ok, err := operationObjectOperationForRequest(r)
	if err != nil {
		return RequestMapping{}, err
	}
	if ok {
		if len(segments) < 2 {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported bucket-level %s operation", r.Method)
		}
		if err := operationValidateObjectMutationQuery(r, operation); err != nil {
			return RequestMapping{}, err
		}
		key := strings.Join(segments[1:], "/")
		if key == "" {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: missing object key")
		}
		return RequestMapping{
			Operation:     operation,
			S3Bucket:      bucket,
			S3Key:         key,
			BackendBucket: bucket,
			BackendKey:    key,
			Multipart:     multipart,
		}, nil
	}

	return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported method %s", r.Method)
}

func operationClassifyMultiServer(r *http.Request, segments []string, opts ClassifyOptions) (RequestMapping, error) {
	server := segments[0]
	if err := operationValidateServer(opts, server); err != nil {
		return RequestMapping{}, err
	}

	if r.Method == http.MethodHead && len(segments) == 1 {
		return RequestMapping{
			Operation: OperationHeadBucket,
			Server:    server,
			S3Bucket:  server,
		}, nil
	}

	defaultBucket, directPrefix := "", false
	if opts.DefaultBucketForServer != nil {
		defaultBucket, directPrefix = opts.DefaultBucketForServer(server)
	}

	if r.Method == http.MethodGet && operationIsListObjectsV2(r) {
		if len(segments) != 1 {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: ListObjectsV2 path must name only a server")
		}
		list, err := operationParseListMapping(r)
		if err != nil {
			return RequestMapping{}, err
		}

		if directPrefix {
			if err := operationValidateBucket(opts, defaultBucket); err != nil {
				return RequestMapping{}, err
			}
			list.BackendPrefix = list.Prefix
			return RequestMapping{
				Operation:     OperationListObjectsV2,
				Server:        server,
				S3Bucket:      server,
				BackendBucket: defaultBucket,
				List:          list,
			}, nil
		}

		backendBucket, backendPrefix, err := operationSplitStandardListPrefix(list.Prefix)
		if err != nil {
			return RequestMapping{}, err
		}
		if err := operationValidateBucket(opts, backendBucket); err != nil {
			return RequestMapping{}, err
		}
		list.BackendPrefix = backendPrefix
		list.PublicKeyPrefix = backendBucket + "/"
		return RequestMapping{
			Operation:     OperationListObjectsV2,
			Server:        server,
			S3Bucket:      server,
			BackendBucket: backendBucket,
			List:          list,
		}, nil
	}

	operation, multipart, ok, err := operationObjectOperationForRequest(r)
	if err != nil {
		return RequestMapping{}, err
	}
	if ok {
		if err := operationValidateObjectMutationQuery(r, operation); err != nil {
			return RequestMapping{}, err
		}

		if directPrefix {
			if len(segments) < 2 {
				return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported server-level %s operation", r.Method)
			}
			if err := operationValidateBucket(opts, defaultBucket); err != nil {
				return RequestMapping{}, err
			}
			key := strings.Join(segments[1:], "/")
			if key == "" {
				return RequestMapping{}, fmt.Errorf("classify s3 operation: missing object key")
			}
			return RequestMapping{
				Operation:     operation,
				Server:        server,
				S3Bucket:      server,
				S3Key:         key,
				BackendBucket: defaultBucket,
				BackendKey:    key,
				Multipart:     multipart,
			}, nil
		}

		if len(segments) < 3 {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: missing backend bucket or object key")
		}
		backendBucket := segments[1]
		if backendBucket == "" {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: missing backend bucket")
		}
		if err := operationValidateBucket(opts, backendBucket); err != nil {
			return RequestMapping{}, err
		}
		backendKey := strings.Join(segments[2:], "/")
		if backendKey == "" {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: missing object key")
		}
		return RequestMapping{
			Operation:     operation,
			Server:        server,
			S3Bucket:      server,
			S3Key:         strings.Join(segments[1:], "/"),
			BackendBucket: backendBucket,
			BackendKey:    backendKey,
			Multipart:     multipart,
		}, nil
	}

	return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported method %s", r.Method)
}

func operationSupportedMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete:
		return true
	default:
		return false
	}
}

// operationObjectOperationForRequest resolves the object-level operation from
// the method and the multipart query parameters (?uploads, ?uploadId,
// ?partNumber).
func operationObjectOperationForRequest(r *http.Request) (Operation, MultipartMapping, bool, error) {
	query := r.URL.Query()
	_, hasUploads := query["uploads"]
	_, hasUploadID := query["uploadId"]
	_, hasPartNumber := query["partNumber"]
	uploadID := query.Get("uploadId")

	switch {
	case r.Method == http.MethodPost && hasUploads:
		if hasUploadID || hasPartNumber {
			return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: uploads query conflicts with uploadId or partNumber")
		}
		return OperationCreateMultipartUpload, MultipartMapping{}, true, nil
	case r.Method == http.MethodPut && hasPartNumber:
		if uploadID == "" {
			return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: UploadPart requires a non-empty uploadId")
		}
		partNumber, err := strconv.Atoi(query.Get("partNumber"))
		if err != nil || partNumber < 1 || partNumber > 10000 {
			return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: partNumber must be an integer between 1 and 10000")
		}
		return OperationUploadPart, MultipartMapping{UploadID: uploadID, PartNumber: partNumber}, true, nil
	case r.Method == http.MethodPost && hasUploadID:
		if uploadID == "" {
			return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: CompleteMultipartUpload requires a non-empty uploadId")
		}
		return OperationCompleteMultipartUpload, MultipartMapping{UploadID: uploadID}, true, nil
	case r.Method == http.MethodDelete && hasUploadID:
		if uploadID == "" {
			return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: AbortMultipartUpload requires a non-empty uploadId")
		}
		return OperationAbortMultipartUpload, MultipartMapping{UploadID: uploadID}, true, nil
	case r.Method == http.MethodPost:
		return "", MultipartMapping{}, false, fmt.Errorf("classify s3 operation: unsupported method %s", r.Method)
	}

	operation, ok := operationObjectOperationForMethod(r.Method)
	return operation, MultipartMapping{}, ok, nil
}

func operationObjectOperationForMethod(method string) (Operation, bool) {
	switch method {
	case http.MethodGet:
		return OperationGetObject, true
	case http.MethodHead:
		return OperationHeadObject, true
	case http.MethodPut:
		return OperationPutObject, true
	case http.MethodDelete:
		return OperationDeleteObject, true
	default:
		return "", false
	}
}

func operationValidateObjectMutationQuery(r *http.Request, operation Operation) error {
	for _, key := range operationUnsupportedMutationQueryKeys(operation) {
		if _, ok := r.URL.Query()[key]; ok {
			return fmt.Errorf("classify s3 operation: unsupported %s query parameter %q", operation, key)
		}
	}
	return nil
}

func operationUnsupportedMutationQueryKeys(operation Operation) []string {
	switch operation {
	case OperationPutObject:
		return []string{"acl", "tagging", "uploads", "uploadId", "partNumber"}
	case OperationDeleteObject:
		return []string{"versionId"}
	case OperationCreateMultipartUpload:
		return []string{"acl", "tagging"}
	default:
		return nil
	}
}

func operationPathSegments(u *url.URL) ([]string, error) {
	if u == nil {
		return nil, fmt.Errorf("classify s3 operation: nil request URL")
	}

	escapedPath := u.EscapedPath()
	if escapedPath == "" || escapedPath == "/" {
		return nil, nil
	}

	rawSegments := strings.Split(escapedPath, "/")
	if len(rawSegments) > 0 && rawSegments[0] == "" {
		rawSegments = rawSegments[1:]
	}

	segments := make([]string, len(rawSegments))
	for i, raw := range rawSegments {
		segment, err := url.PathUnescape(raw)
		if err != nil {
			return nil, fmt.Errorf("classify s3 operation: invalid escaped path segment %q: %w", raw, err)
		}
		segments[i] = segment
	}
	return segments, nil
}

func operationIsListObjectsV2(r *http.Request) bool {
	return r.URL.Query().Get("list-type") == "2"
}

func operationParseListMapping(r *http.Request) (ListMapping, error) {
	query := r.URL.Query()
	maxKeys := 1000
	if rawMaxKeys := query.Get("max-keys"); rawMaxKeys != "" {
		parsed, err := strconv.Atoi(rawMaxKeys)
		if err != nil {
			return ListMapping{}, fmt.Errorf("classify s3 operation: invalid max-keys %q: %w", rawMaxKeys, err)
		}
		if parsed < 0 || parsed > 1000 {
			return ListMapping{}, fmt.Errorf("classify s3 operation: max-keys %d out of range 0..1000", parsed)
		}
		maxKeys = parsed
	}

	fetchOwner := false
	if rawFetchOwner := query.Get("fetch-owner"); rawFetchOwner != "" {
		parsed, err := strconv.ParseBool(rawFetchOwner)
		if err != nil {
			return ListMapping{}, fmt.Errorf("classify s3 operation: invalid fetch-owner %q: %w", rawFetchOwner, err)
		}
		fetchOwner = parsed
	}

	return ListMapping{
		Prefix:            query.Get("prefix"),
		Delimiter:         query.Get("delimiter"),
		ContinuationToken: query.Get("continuation-token"),
		StartAfter:        query.Get("start-after"),
		MaxKeys:           maxKeys,
		EncodingType:      query.Get("encoding-type"),
		FetchOwner:        fetchOwner,
	}, nil
}

func operationSplitStandardListPrefix(prefix string) (backendBucket string, backendPrefix string, err error) {
	if prefix == "" || strings.HasPrefix(prefix, "/") {
		return "", "", fmt.Errorf("classify s3 operation: list prefix must start with a backend bucket segment")
	}

	backendBucket, backendPrefix, found := strings.Cut(prefix, "/")
	if !found {
		backendBucket = prefix
		backendPrefix = ""
	}
	if backendBucket == "" {
		return "", "", fmt.Errorf("classify s3 operation: list prefix must start with a backend bucket segment")
	}
	return backendBucket, backendPrefix, nil
}

func operationValidateBucket(opts ClassifyOptions, bucket string) error {
	if bucket == "" {
		return fmt.Errorf("classify s3 operation: missing bucket")
	}
	if opts.ValidateBucket == nil {
		return nil
	}
	if err := opts.ValidateBucket(bucket); err != nil {
		return fmt.Errorf("classify s3 operation: validate bucket %q: %w", bucket, err)
	}
	return nil
}

func operationValidateServer(opts ClassifyOptions, server string) error {
	if server == "" {
		return fmt.Errorf("classify s3 operation: missing server")
	}
	if opts.ValidateServer == nil {
		return nil
	}
	if err := opts.ValidateServer(server); err != nil {
		return fmt.Errorf("classify s3 operation: validate server %q: %w", server, err)
	}
	return nil
}
