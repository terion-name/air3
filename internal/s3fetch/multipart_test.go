package s3fetch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/tickets"
)

func testMultipart(uploadID string, partNumber int32) *tickets.MultipartRequest {
	return &tickets.MultipartRequest{
		UploadID:   uploadID,
		PartNumber: partNumber,
		Rewrite:    tickets.MultipartRewrite{Bucket: "public-bucket", Key: "public/file.bin"},
	}
}

func TestFetcherCreateMultipartUploadRendersPublicXML(t *testing.T) {
	var gotMethod, gotQuery, gotContentType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotQuery = r.URL.RawQuery
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><Bucket>demo-bucket</Bucket><Key>objects/file.bin</Key><UploadId>upload-42</UploadId></InitiateMultipartUploadResult>`))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:      http.MethodPost,
		Operation:   tickets.OperationCreateMultipartUpload,
		Bucket:      "demo-bucket",
		Key:         "objects/file.bin",
		ContentType: "application/octet-stream",
		Multipart:   testMultipart("", 0),
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	body, _ := io.ReadAll(obj.Body)
	if gotMethod != http.MethodPost || !strings.Contains(gotQuery, "uploads") || gotContentType != "application/octet-stream" {
		t.Fatalf("request method=%q query=%q contentType=%q", gotMethod, gotQuery, gotContentType)
	}
	if obj.StatusCode != http.StatusOK || obj.ContentType != "application/xml" {
		t.Fatalf("object = %#v", obj)
	}
	want := `<Bucket>public-bucket</Bucket><Key>public/file.bin</Key><UploadId>upload-42</UploadId>`
	if !strings.Contains(string(body), want) {
		t.Fatalf("body = %q, want it to contain %q", body, want)
	}
}

func TestFetcherUploadPartStreamsUnseekableBodyUnsigned(t *testing.T) {
	var gotMethod, gotQuery, gotPayloadHash, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotQuery = r.URL.RawQuery
		gotPayloadHash = r.Header.Get("X-Amz-Content-Sha256")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("ETag", `"part-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	contentLength := int64(len("part body"))
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:        http.MethodPut,
		Operation:     tickets.OperationUploadPart,
		Bucket:        "demo-bucket",
		Key:           "objects/file.bin",
		Body:          io.MultiReader(strings.NewReader("part body")),
		ContentLength: &contentLength,
		Multipart:     testMultipart("upload-42", 3),
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if gotMethod != http.MethodPut || !strings.Contains(gotQuery, "partNumber=3") || !strings.Contains(gotQuery, "uploadId=upload-42") {
		t.Fatalf("request method=%q query=%q", gotMethod, gotQuery)
	}
	if gotPayloadHash != "UNSIGNED-PAYLOAD" || gotBody != "part body" {
		t.Fatalf("payload hash=%q body=%q", gotPayloadHash, gotBody)
	}
	if obj.StatusCode != http.StatusOK || obj.ETag != `"part-etag"` || obj.Body != http.NoBody {
		t.Fatalf("object = %#v", obj)
	}
}

func TestFetcherCompleteMultipartParsesPartsAndRendersPublicXML(t *testing.T) {
	var gotMethod, gotQuery, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotQuery = r.URL.RawQuery
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>demo-bucket</Bucket><Key>objects/file.bin</Key><ETag>"final-etag"</ETag></CompleteMultipartUploadResult>`))
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	clientXML := `<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>"e2"</ETag></Part><Part><PartNumber>1</PartNumber><ETag>"e1"</ETag></Part></CompleteMultipartUpload>`
	contentLength := int64(len(clientXML))
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:        http.MethodPost,
		Operation:     tickets.OperationCompleteMultipartUpload,
		Bucket:        "demo-bucket",
		Key:           "objects/file.bin",
		Body:          io.MultiReader(strings.NewReader(clientXML)),
		ContentLength: &contentLength,
		Multipart:     testMultipart("upload-42", 0),
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	body, _ := io.ReadAll(obj.Body)
	if gotMethod != http.MethodPost || !strings.Contains(gotQuery, "uploadId=upload-42") {
		t.Fatalf("request method=%q query=%q", gotMethod, gotQuery)
	}
	// Parts must reach the backend sorted by part number.
	if first, second := strings.Index(gotBody, "<PartNumber>1</PartNumber>"), strings.Index(gotBody, "<PartNumber>2</PartNumber>"); first == -1 || second == -1 || first > second {
		t.Fatalf("backend body = %q, want sorted parts", gotBody)
	}
	want := `<Bucket>public-bucket</Bucket><Key>public/file.bin</Key>`
	if obj.StatusCode != http.StatusOK || obj.ContentType != "application/xml" || !strings.Contains(string(body), want) {
		t.Fatalf("object = %#v body = %q", obj, body)
	}
}

func TestFetcherCompleteMultipartRejectsMalformedPartsBeforeBackendCall(t *testing.T) {
	backendCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	body := `<CompleteMultipartUpload></CompleteMultipartUpload>`
	contentLength := int64(len(body))
	_, err = fetcher.Fetch(context.Background(), Request{
		Method:        http.MethodPost,
		Operation:     tickets.OperationCompleteMultipartUpload,
		Bucket:        "demo-bucket",
		Key:           "objects/file.bin",
		Body:          io.MultiReader(strings.NewReader(body)),
		ContentLength: &contentLength,
		Multipart:     testMultipart("upload-42", 0),
	})
	if err == nil {
		t.Fatal("Fetch() error = nil, want invalid request")
	}
	if backendCalled {
		t.Fatal("backend was called for malformed CompleteMultipartUpload body")
	}
}

func TestFetcherAbortMultipartReturns204(t *testing.T) {
	var gotMethod, gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	fetcher, err := New(context.Background(), testConfig(ts.URL))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	obj, err := fetcher.Fetch(context.Background(), Request{
		Method:    http.MethodDelete,
		Operation: tickets.OperationAbortMultipartUpload,
		Bucket:    "demo-bucket",
		Key:       "objects/file.bin",
		Multipart: testMultipart("upload-42", 0),
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if gotMethod != http.MethodDelete || !strings.Contains(gotQuery, "uploadId=upload-42") {
		t.Fatalf("request method=%q query=%q", gotMethod, gotQuery)
	}
	if obj.StatusCode != http.StatusNoContent || obj.Body != http.NoBody {
		t.Fatalf("object = %#v", obj)
	}
}

func TestFetcherMultipartValidationRejectsBadRequests(t *testing.T) {
	fetcher, err := New(context.Background(), testConfig("http://backend.invalid"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	contentLength := int64(4)
	oversized := int64(tickets.MaxCompleteMultipartBodyBytes + 1)
	tests := []struct {
		name string
		req  Request
	}{
		{name: "create with body", req: Request{Method: http.MethodPost, Operation: tickets.OperationCreateMultipartUpload, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("x"), Multipart: testMultipart("", 0)}},
		{name: "create with upload id", req: Request{Method: http.MethodPost, Operation: tickets.OperationCreateMultipartUpload, Bucket: "demo-bucket", Key: "k", Multipart: testMultipart("upload", 0)}},
		{name: "upload part missing multipart", req: Request{Method: http.MethodPut, Operation: tickets.OperationUploadPart, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("body"), ContentLength: &contentLength}},
		{name: "upload part number zero", req: Request{Method: http.MethodPut, Operation: tickets.OperationUploadPart, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("body"), ContentLength: &contentLength, Multipart: testMultipart("upload", 0)}},
		{name: "upload part with content type", req: Request{Method: http.MethodPut, Operation: tickets.OperationUploadPart, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("body"), ContentLength: &contentLength, ContentType: "text/plain", Multipart: testMultipart("upload", 1)}},
		{name: "complete oversized body", req: Request{Method: http.MethodPost, Operation: tickets.OperationCompleteMultipartUpload, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("body"), ContentLength: &oversized, Multipart: testMultipart("upload", 0)}},
		{name: "abort missing upload id", req: Request{Method: http.MethodDelete, Operation: tickets.OperationAbortMultipartUpload, Bucket: "demo-bucket", Key: "k", Multipart: testMultipart("", 0)}},
		{name: "abort with body", req: Request{Method: http.MethodDelete, Operation: tickets.OperationAbortMultipartUpload, Bucket: "demo-bucket", Key: "k", Body: strings.NewReader("x"), Multipart: testMultipart("upload", 0)}},
		{name: "get with multipart metadata", req: Request{Method: http.MethodGet, Bucket: "demo-bucket", Key: "k", Multipart: testMultipart("upload", 0)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := fetcher.Fetch(context.Background(), tc.req); err == nil {
				t.Fatal("Fetch() error = nil, want validation error")
			}
		})
	}
}
