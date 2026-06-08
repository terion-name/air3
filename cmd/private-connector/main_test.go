package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
)

type fakeFetcher struct {
	requests []s3fetch.Request
	object   *s3fetch.Object
	err      error
}

func (f *fakeFetcher) Fetch(ctx context.Context, req s3fetch.Request) (*s3fetch.Object, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	return f.object, nil
}

func connectorConfig() config.ConnectorConfig {
	return config.ConnectorConfig{
		AllowedBuckets: []string{"demo-bucket"},
		S3: config.S3Config{
			AllowedBuckets:  []string{"demo-bucket"},
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	}
}

func validTicket(url, method string) tickets.Ticket {
	return tickets.Ticket{Version: tickets.Version, RequestID: "req-1", Bucket: "demo-bucket", Key: "objects/file.txt", Method: method, DeadlineUnixMS: time.Now().Add(time.Minute).UnixMilli(), IngestURL: url, IngestToken: "ingest-token"}
}

func TestConnectorStreamsFetchedObjectToIngest(t *testing.T) {
	var gotToken, gotStatus, gotContentType, gotBody string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get(ingest.TokenHeader)
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 11, ETag: `"abc"`, Body: io.NopCloser(strings.NewReader("hello world"))}}
	worker := newConnector(connectorConfig(), fetcher, ts.Client(), nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotToken != "ingest-token" || gotStatus != "200" || gotContentType != "text/plain" || gotBody != "hello world" {
		t.Fatalf("ingest token=%q status=%q content-type=%q body=%q", gotToken, gotStatus, gotContentType, gotBody)
	}
	if len(fetcher.requests) != 1 || fetcher.requests[0].Bucket != "demo-bucket" || fetcher.requests[0].Key != "objects/file.txt" {
		t.Fatalf("fetch requests = %#v", fetcher.requests)
	}
}

func TestConnectorHEADPostsMetadataWithoutBody(t *testing.T) {
	var gotBody string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		if r.Header.Get(ingest.StatusCodeHeader) != "200" || r.Header.Get(ingest.ObjectContentLengthHeader) != "5" {
			t.Fatalf("headers = %#v", r.Header)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 5, Body: io.NopCloser(strings.NewReader("hello"))}}
	worker := newConnector(connectorConfig(), fetcher, ts.Client(), nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodHead)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotBody != "" {
		t.Fatalf("HEAD body = %q, want empty", gotBody)
	}
}

func TestConnectorMissingObjectPosts404Metadata(t *testing.T) {
	var gotStatus, gotBody string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{err: s3fetch.ErrNotFound}
	worker := newConnector(connectorConfig(), fetcher, ts.Client(), nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotStatus != "404" || gotBody != "" {
		t.Fatalf("status=%q body=%q, want 404 empty", gotStatus, gotBody)
	}
}

func TestConnectorRejectsDisallowedBucketBeforeFetch(t *testing.T) {
	fetcher := &fakeFetcher{object: &s3fetch.Object{Body: io.NopCloser(strings.NewReader("body"))}}
	worker := newConnector(connectorConfig(), fetcher, http.DefaultClient, nil)
	ticket := validTicket("https://edge.internal/_ingest/req-1", http.MethodGet)
	ticket.Bucket = "other-bucket"
	if err := worker.handleTicket(context.Background(), ticket); err == nil {
		t.Fatal("handleTicket() error = nil, want disallowed bucket error")
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
	}
}

func TestConnectorMapsBackendErrorTo503Metadata(t *testing.T) {
	var gotStatus string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.Header.Get(ingest.StatusCodeHeader)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher := &fakeFetcher{err: errors.New("backend down")}
	worker := newConnector(connectorConfig(), fetcher, ts.Client(), nil)
	if err := worker.handleTicket(context.Background(), validTicket(ts.URL, http.MethodGet)); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if gotStatus != "503" {
		t.Fatalf("status = %q, want 503", gotStatus)
	}
}
