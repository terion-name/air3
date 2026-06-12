package s3api

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestClassifyRejectsUnsupportedMethodsBeforeValidation(t *testing.T) {
	boom := errors.New("validation should not run")
	got, err := Classify(operationRequest(t, http.MethodPut, "/photos/2024/cat.jpg"), ClassifyOptions{
		Mode:           RoutingSingleServer,
		ValidateBucket: func(bucket string) error { return boom },
	})
	if err == nil {
		t.Fatalf("Classify() = %#v, want error", got)
	}
	if errors.Is(err, boom) {
		t.Fatalf("Classify() error = %v, want unsupported method before validation", err)
	}
}

func TestClassifySingleServerOperations(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want RequestMapping
	}{
		{
			name: "get object",
			req:  operationRequest(t, http.MethodGet, "/photos/2024/cat.jpg"),
			want: RequestMapping{
				Operation:     OperationGetObject,
				S3Bucket:      "photos",
				S3Key:         "2024/cat.jpg",
				BackendBucket: "photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
		{
			name: "head object",
			req:  operationRequest(t, http.MethodHead, "/photos/2024/cat.jpg"),
			want: RequestMapping{
				Operation:     OperationHeadObject,
				S3Bucket:      "photos",
				S3Key:         "2024/cat.jpg",
				BackendBucket: "photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
		{
			name: "head bucket",
			req:  operationRequest(t, http.MethodHead, "/photos"),
			want: RequestMapping{
				Operation:     OperationHeadBucket,
				S3Bucket:      "photos",
				BackendBucket: "photos",
			},
		},
		{
			name: "list objects v2",
			req:  operationRequest(t, http.MethodGet, "/photos?list-type=2&prefix=2024/&delimiter=/"),
			want: RequestMapping{
				Operation:     OperationListObjectsV2,
				S3Bucket:      "photos",
				BackendBucket: "photos",
				List: ListMapping{
					Prefix:        "2024/",
					BackendPrefix: "2024/",
					Delimiter:     "/",
					MaxKeys:       1000,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Classify(tt.req, ClassifyOptions{Mode: RoutingSingleServer})
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Classify() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestClassifySingleServerRejectsShortObjectAndV1List(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
	}{
		{
			name: "does not map short get to default bucket object",
			req:  operationRequest(t, http.MethodGet, "/lonely-key"),
		},
		{
			name: "rejects unsupported v1 list",
			req:  operationRequest(t, http.MethodGet, "/photos"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := Classify(tt.req, ClassifyOptions{Mode: RoutingSingleServer}); err == nil {
				t.Fatalf("Classify() = %#v, want error", got)
			}
		})
	}
}

func TestClassifyMultiServerStandardObjectMapping(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want RequestMapping
	}{
		{
			name: "get object",
			url:  "/edge-a/photos/2024/cat.jpg",
			want: RequestMapping{
				Operation:     OperationGetObject,
				Server:        "edge-a",
				S3Bucket:      "edge-a",
				S3Key:         "photos/2024/cat.jpg",
				BackendBucket: "photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
		{
			name: "head object",
			url:  "/edge-a/photos/2024/cat.jpg",
			want: RequestMapping{
				Operation:     OperationHeadObject,
				Server:        "edge-a",
				S3Bucket:      "edge-a",
				S3Key:         "photos/2024/cat.jpg",
				BackendBucket: "photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := http.MethodGet
			if strings.HasPrefix(tt.name, "head") {
				method = http.MethodHead
			}
			got, err := Classify(operationRequest(t, method, tt.url), ClassifyOptions{Mode: RoutingMultiServer})
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Classify() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestClassifyMultiServerStandardListMapping(t *testing.T) {
	got, err := Classify(
		operationRequest(t, http.MethodGet, "/edge-a?list-type=2&prefix=photos/2024/&delimiter=/"),
		ClassifyOptions{Mode: RoutingMultiServer},
	)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}

	want := RequestMapping{
		Operation:     OperationListObjectsV2,
		Server:        "edge-a",
		S3Bucket:      "edge-a",
		BackendBucket: "photos",
		List: ListMapping{
			Prefix:          "photos/2024/",
			BackendPrefix:   "2024/",
			Delimiter:       "/",
			MaxKeys:         1000,
			PublicKeyPrefix: "photos/",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Classify() = %#v, want %#v", got, want)
	}
}

func TestClassifyMultiServerStandardListRejectsMissingBackendBucketPrefix(t *testing.T) {
	tests := []string{
		"/edge-a?list-type=2",
		"/edge-a?list-type=2&prefix=/photos",
	}

	for _, url := range tests {
		t.Run(url, func(t *testing.T) {
			if got, err := Classify(operationRequest(t, http.MethodGet, url), ClassifyOptions{Mode: RoutingMultiServer}); err == nil {
				t.Fatalf("Classify() = %#v, want error", got)
			}
		})
	}
}

func TestClassifyMultiServerDefaultBucketDirectPrefix(t *testing.T) {
	opts := ClassifyOptions{
		Mode: RoutingMultiServer,
		DefaultBucketForServer: func(server string) (string, bool) {
			if server == "edge-a" {
				return "default-photos", true
			}
			return "", false
		},
	}

	tests := []struct {
		name string
		req  *http.Request
		want RequestMapping
	}{
		{
			name: "get object",
			req:  operationRequest(t, http.MethodGet, "/edge-a/2024/cat.jpg"),
			want: RequestMapping{
				Operation:     OperationGetObject,
				Server:        "edge-a",
				S3Bucket:      "edge-a",
				S3Key:         "2024/cat.jpg",
				BackendBucket: "default-photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
		{
			name: "head object",
			req:  operationRequest(t, http.MethodHead, "/edge-a/2024/cat.jpg"),
			want: RequestMapping{
				Operation:     OperationHeadObject,
				Server:        "edge-a",
				S3Bucket:      "edge-a",
				S3Key:         "2024/cat.jpg",
				BackendBucket: "default-photos",
				BackendKey:    "2024/cat.jpg",
			},
		},
		{
			name: "list direct prefix",
			req:  operationRequest(t, http.MethodGet, "/edge-a?list-type=2&prefix=2024/"),
			want: RequestMapping{
				Operation:     OperationListObjectsV2,
				Server:        "edge-a",
				S3Bucket:      "edge-a",
				BackendBucket: "default-photos",
				List: ListMapping{
					Prefix:        "2024/",
					BackendPrefix: "2024/",
					MaxKeys:       1000,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Classify(tt.req, opts)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Classify() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestClassifyMultiServerHeadBucketValidationHook(t *testing.T) {
	req := operationRequest(t, http.MethodHead, "/edge-a")

	got, err := Classify(req, ClassifyOptions{
		Mode: RoutingMultiServer,
		ValidateServer: func(server string) error {
			if server != "edge-a" {
				t.Fatalf("ValidateServer(%q), want edge-a", server)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if got.Operation != OperationHeadBucket || got.Server != "edge-a" || got.S3Bucket != "edge-a" {
		t.Fatalf("Classify() = %#v, want multi-server HeadBucket", got)
	}

	boom := errors.New("boom")
	_, err = Classify(req, ClassifyOptions{
		Mode:           RoutingMultiServer,
		ValidateServer: func(server string) error { return boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Classify() error = %v, want wrapping boom", err)
	}
}

func TestClassifyBucketValidationHook(t *testing.T) {
	req := operationRequest(t, http.MethodHead, "/photos")

	got, err := Classify(req, ClassifyOptions{
		Mode: RoutingSingleServer,
		ValidateBucket: func(bucket string) error {
			if bucket != "photos" {
				t.Fatalf("ValidateBucket(%q), want photos", bucket)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if got.Operation != OperationHeadBucket || got.S3Bucket != "photos" {
		t.Fatalf("Classify() = %#v, want single-server HeadBucket", got)
	}

	boom := errors.New("boom")
	_, err = Classify(req, ClassifyOptions{
		Mode:           RoutingSingleServer,
		ValidateBucket: func(bucket string) error { return boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Classify() error = %v, want wrapping boom", err)
	}
}

func TestClassifyListParameters(t *testing.T) {
	got, err := Classify(
		operationRequest(t, http.MethodGet, "/photos?list-type=2&prefix=2024/&delimiter=%2F&continuation-token=next-token&start-after=old-key&max-keys=25&encoding-type=url&fetch-owner=true"),
		ClassifyOptions{Mode: RoutingSingleServer},
	)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	wantList := ListMapping{
		Prefix:            "2024/",
		BackendPrefix:     "2024/",
		Delimiter:         "/",
		ContinuationToken: "next-token",
		StartAfter:        "old-key",
		MaxKeys:           25,
		EncodingType:      "url",
		FetchOwner:        true,
	}
	if !reflect.DeepEqual(got.List, wantList) {
		t.Fatalf("List = %#v, want %#v", got.List, wantList)
	}
}

func TestClassifyListMaxKeysDefaultsAndBounds(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		got, err := Classify(operationRequest(t, http.MethodGet, "/photos?list-type=2"), ClassifyOptions{Mode: RoutingSingleServer})
		if err != nil {
			t.Fatalf("Classify() error = %v", err)
		}
		if got.List.MaxKeys != 1000 {
			t.Fatalf("MaxKeys = %d, want 1000", got.List.MaxKeys)
		}
	})

	tests := []string{
		"/photos?list-type=2&max-keys=-1",
		"/photos?list-type=2&max-keys=1001",
		"/photos?list-type=2&max-keys=not-an-int",
	}
	for _, url := range tests {
		t.Run(url, func(t *testing.T) {
			if got, err := Classify(operationRequest(t, http.MethodGet, url), ClassifyOptions{Mode: RoutingSingleServer}); err == nil {
				t.Fatalf("Classify() = %#v, want error", got)
			}
		})
	}
}

func TestClassifyEncodedPathSegmentsWithoutNormalization(t *testing.T) {
	got, err := Classify(
		operationRequest(t, http.MethodGet, "/photos/a%2Fb/./c//d%20e"),
		ClassifyOptions{Mode: RoutingSingleServer},
	)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}

	wantKey := "a/b/./c//d e"
	if got.S3Key != wantKey || got.BackendKey != wantKey {
		t.Fatalf("keys = (%q, %q), want %q without path normalization", got.S3Key, got.BackendKey, wantKey)
	}
}

func operationRequest(t *testing.T, method string, target string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://s3.test"+target, nil)
	if err != nil {
		t.Fatalf("NewRequest(%q, %q) error = %v", method, target, err)
	}
	return req
}
