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
	OperationGetObject     Operation = "GetObject"
	OperationHeadObject    Operation = "HeadObject"
	OperationHeadBucket    Operation = "HeadBucket"
	OperationListObjectsV2 Operation = "ListObjectsV2"
)

type RequestMapping struct {
	Operation     Operation
	Server        string
	S3Bucket      string
	S3Key         string
	BackendBucket string
	BackendKey    string
	List          ListMapping
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
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
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

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if len(segments) < 2 {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported bucket-level %s operation", r.Method)
		}
		key := strings.Join(segments[1:], "/")
		if key == "" {
			return RequestMapping{}, fmt.Errorf("classify s3 operation: missing object key")
		}
		operation := OperationGetObject
		if r.Method == http.MethodHead {
			operation = OperationHeadObject
		}
		return RequestMapping{
			Operation:     operation,
			S3Bucket:      bucket,
			S3Key:         key,
			BackendBucket: bucket,
			BackendKey:    key,
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

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		operation := OperationGetObject
		if r.Method == http.MethodHead {
			operation = OperationHeadObject
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
		}, nil
	}

	return RequestMapping{}, fmt.Errorf("classify s3 operation: unsupported method %s", r.Method)
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
