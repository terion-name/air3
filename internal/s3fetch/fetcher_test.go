package s3fetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/terion-name/air3/internal/config"
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

func testConfig(endpoint string) config.S3Config {
	return config.S3Config{Endpoint: endpoint, Region: "us-east-1", AccessKeyID: "access", SecretAccessKey: "secret", UsePathStyle: true}
}
