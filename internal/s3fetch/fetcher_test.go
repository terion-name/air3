package s3fetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/tickets"
)

func TestFetcherGetObjectPathStyleWithRangeAndMetadata(t *testing.T) {
	var gotMethod, gotPath, gotRange string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", "Mon, 08 Jun 2026 12:00:00 GMT")
		w.Header().Set("Content-Range", "bytes 0-4/11")
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("hello"))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{Method: http.MethodGet, Bucket: "demo-bucket", Key: "objects/file.txt", Range: "bytes=0-4"})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	_ = obj.Body.Close()

	if gotMethod != http.MethodGet || gotPath != "/demo-bucket/objects/file.txt" || gotRange != "bytes=0-4" {
		t.Fatalf("request method=%q path=%q range=%q", gotMethod, gotPath, gotRange)
	}
	if string(body) != "hello" || obj.StatusCode != http.StatusPartialContent || obj.ContentLength != 5 || obj.ContentType != "text/plain" || obj.ContentRange != "bytes 0-4/11" || obj.AcceptRanges != "bytes" {
		t.Fatalf("object = %#v body=%q", obj, body)
	}
}

func TestFetcherHeadObjectUsesHEADAndNoBody(t *testing.T) {
	var gotMethod string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "11")
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{Method: http.MethodHead, Bucket: "demo-bucket", Key: "objects/file.txt"})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if gotMethod != http.MethodHead || obj.Body != http.NoBody || obj.ContentLength != 11 || obj.ContentType != "text/plain" {
		t.Fatalf("method=%q object=%#v", gotMethod, obj)
	}
}

func TestFetcherListObjectsV2RequestShapeAndXMLMetadata(t *testing.T) {
	var gotMethod, gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		query := r.URL.Query()
		checks := map[string]string{
			"list-type":          "2",
			"prefix":             "photos/2026",
			"delimiter":          "/",
			"continuation-token": "opaque-token",
			"start-after":        "photos/2025/last.jpg",
			"max-keys":           "25",
			"encoding-type":      "url",
			"fetch-owner":        "true",
		}
		for name, want := range checks {
			if got := query.Get(name); got != want {
				t.Fatalf("query %s = %q, want %q (raw query %q)", name, got, want, r.URL.RawQuery)
			}
		}

		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(listObjectsV2XML("demo-bucket")))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:    http.MethodGet,
		Operation: tickets.OperationListObjectsV2,
		Bucket:    "demo-bucket",
		List: &tickets.ListRequest{
			Prefix:            "photos/2026",
			Delimiter:         "/",
			ContinuationToken: "opaque-token",
			StartAfter:        "photos/2025/last.jpg",
			MaxKeys:           25,
			EncodingType:      "url",
			FetchOwner:        true,
			Rewrite:           tickets.ListRewrite{Bucket: "public-bucket"},
		},
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	_ = obj.Body.Close()

	if gotMethod != http.MethodGet || gotPath != "/demo-bucket" {
		t.Fatalf("request method=%q path=%q", gotMethod, gotPath)
	}
	if obj.StatusCode != http.StatusOK || obj.ContentType != "application/xml" || obj.ContentLength != int64(len(body)) {
		t.Fatalf("object = %#v body length = %d", obj, len(body))
	}
	assertContains(t, string(body), `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
}

func TestFetcherListObjectsV2RendersRewriteXML(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(listObjectsV2XML("demo-bucket")))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:    http.MethodGet,
		Operation: tickets.OperationListObjectsV2,
		Bucket:    "demo-bucket",
		List: &tickets.ListRequest{
			Prefix:            "photos/2026",
			Delimiter:         "/",
			ContinuationToken: "opaque-token",
			StartAfter:        "photos/2025/last.jpg",
			MaxKeys:           25,
			EncodingType:      "url",
			FetchOwner:        true,
			Rewrite: tickets.ListRewrite{
				Bucket:    "public-bucket",
				Prefix:    "shared/photos",
				KeyPrefix: "cdn/photos",
			},
		},
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	_ = obj.Body.Close()
	got := string(body)

	for _, want := range []string{
		"<Name>public-bucket</Name>",
		"<Prefix>shared/photos</Prefix>",
		"<Key>cdn/photosphotos/2026/a.jpg</Key>",
		"<CommonPrefixes><Prefix>cdn/photosphotos/2026/archive/</Prefix></CommonPrefixes>",
		"<IsTruncated>true</IsTruncated>",
		"<NextContinuationToken>next-token</NextContinuationToken>",
		"<ContinuationToken>opaque-token</ContinuationToken>",
		"<StartAfter>photos/2025/last.jpg</StartAfter>",
		"<KeyCount>2</KeyCount>",
		"<MaxKeys>25</MaxKeys>",
		"<StorageClass>STANDARD</StorageClass>",
		"<ETag>&#34;etag-a&#34;</ETag>",
		"<Size>123</Size>",
		"<LastModified>2026-06-08T12:34:56Z</LastModified>",
	} {
		assertContains(t, got, want)
	}
}

func TestNewInsecureSkipVerifyUsesClonedDefaultTransport(t *testing.T) {
	cfg := testConfig("https://s3.local")
	cfg.InsecureSkipVerify = true

	fetcher, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	httpClient, ok := fetcher.client.Options().HTTPClient.(*http.Client)
	if !ok {
		t.Fatalf("HTTPClient type = %T, want *http.Client", fetcher.client.Options().HTTPClient)
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport type = %T, want *http.Transport", httpClient.Transport)
	}
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport type = %T, want *http.Transport", http.DefaultTransport)
	}

	if transport == defaultTransport {
		t.Fatal("transport uses http.DefaultTransport directly, want cloned transport")
	}
	if transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("TLSClientConfig = %#v, want InsecureSkipVerify", transport.TLSClientConfig)
	}
	if !transport.DisableCompression {
		t.Fatal("DisableCompression = false, want true")
	}
	if (transport.Proxy == nil) != (defaultTransport.Proxy == nil) {
		t.Fatalf("Proxy nilness = %v, want %v", transport.Proxy == nil, defaultTransport.Proxy == nil)
	}
	if (transport.DialContext == nil) != (defaultTransport.DialContext == nil) {
		t.Fatalf("DialContext nilness = %v, want %v", transport.DialContext == nil, defaultTransport.DialContext == nil)
	}
	if transport.ForceAttemptHTTP2 != defaultTransport.ForceAttemptHTTP2 ||
		transport.MaxIdleConns != defaultTransport.MaxIdleConns ||
		transport.MaxIdleConnsPerHost != defaultTransport.MaxIdleConnsPerHost ||
		transport.MaxConnsPerHost != defaultTransport.MaxConnsPerHost ||
		transport.IdleConnTimeout != defaultTransport.IdleConnTimeout ||
		transport.TLSHandshakeTimeout != defaultTransport.TLSHandshakeTimeout ||
		transport.ExpectContinueTimeout != defaultTransport.ExpectContinueTimeout {
		t.Fatalf("transport defaults were not preserved: got %#v want defaults from %#v", transport, defaultTransport)
	}
}

func TestFetcherMapsMissingObject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = fetcher.Fetch(context.Background(), Request{Method: http.MethodGet, Bucket: "demo-bucket", Key: "missing.txt"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Fetch() error = %v, want ErrNotFound", err)
	}
}

func TestFetcherRejectsInvalidRangeBeforeS3(t *testing.T) {
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = fetcher.Fetch(context.Background(), Request{Method: http.MethodGet, Bucket: "demo-bucket", Key: "objects/file.txt", Range: "items=0-1"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Fetch() error = %v, want ErrInvalidRequest", err)
	}
	if called {
		t.Fatal("S3 server was called for invalid range")
	}
}

func TestFetcherRejectsInvalidListRequestBeforeS3(t *testing.T) {
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	baseList := &tickets.ListRequest{MaxKeys: 10, Rewrite: tickets.ListRewrite{Bucket: "public-bucket"}}
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "non-empty key",
			req:  Request{Method: http.MethodGet, Operation: tickets.OperationListObjectsV2, Bucket: "demo-bucket", Key: "objects/file.txt", List: baseList},
		},
		{
			name: "range",
			req:  Request{Method: http.MethodGet, Operation: tickets.OperationListObjectsV2, Bucket: "demo-bucket", Range: "bytes=0-1", List: baseList},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fetcher.Fetch(context.Background(), tc.req)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Fetch() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
	if called {
		t.Fatal("S3 server was called for invalid list request")
	}
}

func TestFetcherMapsMissingBucketForListObjectsV2(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchBucket</Code><Message>missing bucket</Message></Error>`))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = fetcher.Fetch(context.Background(), Request{
		Method:    http.MethodGet,
		Operation: tickets.OperationListObjectsV2,
		Bucket:    "demo-bucket",
		List:      &tickets.ListRequest{MaxKeys: 10, Rewrite: tickets.ListRewrite{Bucket: "public-bucket"}},
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Fetch() error = %v, want ErrNotFound", err)
	}
}

func listObjectsV2XML(bucket string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>` + bucket + `</Name>
  <Prefix>photos/2026</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>25</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>photos/2026/a.jpg</Key>
    <LastModified>2026-06-08T12:34:56.000Z</LastModified>
    <ETag>&quot;etag-a&quot;</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes><Prefix>photos/2026/archive/</Prefix></CommonPrefixes>
  <NextContinuationToken>next-token</NextContinuationToken>
  <ContinuationToken>opaque-token</ContinuationToken>
  <StartAfter>photos/2025/last.jpg</StartAfter>
  <Delimiter>/</Delimiter>
  <EncodingType>url</EncodingType>
</ListBucketResult>`
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("body missing %q in:\n%s", want, got)
	}
}

func testConfig(endpoint string) config.S3Config {
	return config.S3Config{Endpoint: endpoint, Region: "us-east-1", AccessKeyID: "access", SecretAccessKey: "secret", UsePathStyle: true}
}
