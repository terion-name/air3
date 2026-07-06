package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
	"github.com/terion-name/air3/internal/uploadsource"
)

func s3AWSChunkedBody(payload, trailer string) string {
	var b strings.Builder
	if payload != "" {
		fmt.Fprintf(&b, "%x\r\n%s\r\n", len(payload), payload)
	}
	b.WriteString("0\r\n")
	b.WriteString(trailer)
	b.WriteString("\r\n")
	return b.String()
}

func claimUploadAndRespond(t *testing.T, edge *edgeServer, reg *pending.Registry, metadata pending.Metadata, responseBody string) (func(tickets.Ticket), *string) {
	t.Helper()
	uploaded := new(string)
	return func(ticket tickets.Ticket) {
		claim, err := edge.uploadSources.Claim(ticket.RequestID, ticket.UploadToken)
		if err != nil {
			t.Errorf("Claim() error = %v", err)
			return
		}
		body, err := io.ReadAll(claim)
		if err != nil {
			t.Errorf("ReadAll(claim) error = %v", err)
		}
		*uploaded = string(body)
		if err := claim.Close(); err != nil {
			t.Errorf("claim.Close() error = %v", err)
		}
		stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, metadata)
		if err != nil {
			t.Errorf("StartIngest() error = %v", err)
			return
		}
		if responseBody != "" {
			if _, err := stream.Write([]byte(responseBody)); err != nil {
				t.Errorf("stream.Write() error = %v", err)
			}
		}
		_ = stream.Close()
	}, uploaded
}

func TestS3APIRoutedPutDecodesAWSChunkedBody(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
	onPublish, uploaded := claimUploadAndRespond(t, edge, reg, pending.Metadata{StatusCode: http.StatusOK, ETag: `"stored"`}, "")
	pub.on = onPublish

	payload := "chunked routed body"
	framed := s3AWSChunkedBody(payload, "x-amz-checksum-crc32:AAAAAA==\r\n")
	req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/file.txt", io.NopCloser(strings.NewReader(framed)), int64(len(framed)), map[string]string{
		"x-amz-content-sha256":         "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		"X-Amz-Decoded-Content-Length": strconv.Itoa(len(payload)),
		"Content-Encoding":             "aws-chunked",
		"X-Amz-Trailer":                "x-amz-checksum-crc32",
		"Content-Type":                 "text/plain",
	})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
	}
	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	ticket := published[0]
	if ticket.Operation != tickets.OperationPutObject || ticket.ContentLength == nil || *ticket.ContentLength != int64(len(payload)) {
		t.Fatalf("ticket = %#v, want decoded content length %d", ticket, len(payload))
	}
	if *uploaded != payload {
		t.Fatalf("upload source body = %q, want decoded %q", *uploaded, payload)
	}
}

func TestS3APIRoutedPutAcceptsSignedPayloadHash(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
	onPublish, uploaded := claimUploadAndRespond(t, edge, reg, pending.Metadata{StatusCode: http.StatusOK, ETag: `"stored"`}, "")
	pub.on = onPublish

	payload := "signed routed body"
	payloadHash := sha256.Sum256([]byte(payload))
	req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/file.txt", io.NopCloser(strings.NewReader(payload)), int64(len(payload)), map[string]string{
		"x-amz-content-sha256": hex.EncodeToString(payloadHash[:]),
	})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
	}
	if *uploaded != payload {
		t.Fatalf("upload source body = %q, want %q", *uploaded, payload)
	}
}

func TestS3APIRoutedMultipartLifecyclePublishesTickets(t *testing.T) {
	const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	wantRewrite := tickets.MultipartRewrite{Bucket: "demo-bucket", Key: "big/file.bin"}

	t.Run("create", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		createXML := `<InitiateMultipartUploadResult><UploadId>upload-42</UploadId></InitiateMultipartUploadResult>`
		pub.on = func(ticket tickets.Ticket) {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ContentType: "application/xml"})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_, _ = stream.Write([]byte(createXML))
			_ = stream.Close()
		}
		req := s3SignHeaderRequestWithBody(t, http.MethodPost, "/demo-bucket/big/file.bin?uploads", http.NoBody, 0, map[string]string{"x-amz-content-sha256": emptySHA, "Content-Type": "application/octet-stream"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK || resp.Body.String() != createXML {
			t.Fatalf("status = %d body = %q, want 200 with create XML", got, resp.Body.String())
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		ticket := published[0]
		if ticket.Operation != tickets.OperationCreateMultipartUpload || ticket.ContentType != "application/octet-stream" || ticket.UploadSourceURL != "" || ticket.ContentLength != nil {
			t.Fatalf("create ticket = %#v, want content type without upload envelope", ticket)
		}
		if ticket.Multipart == nil || ticket.Multipart.UploadID != "" || ticket.Multipart.PartNumber != 0 || ticket.Multipart.Rewrite != wantRewrite {
			t.Fatalf("create ticket multipart = %#v, want rewrite %#v", ticket.Multipart, wantRewrite)
		}
	})

	t.Run("upload part", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		onPublish, uploaded := claimUploadAndRespond(t, edge, reg, pending.Metadata{StatusCode: http.StatusOK, ETag: `"part-etag"`}, "")
		pub.on = onPublish
		payload := "part payload"
		req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/big/file.bin?partNumber=2&uploadId=upload-42", io.NopCloser(strings.NewReader(payload)), int64(len(payload)), map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
		}
		if got := resp.Result().Header.Get("ETag"); got != `"part-etag"` {
			t.Fatalf("ETag = %q, want part etag", got)
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		ticket := published[0]
		if ticket.Operation != tickets.OperationUploadPart || ticket.UploadSourceURL == "" || ticket.UploadToken == "" || ticket.ContentLength == nil || *ticket.ContentLength != int64(len(payload)) || ticket.ContentType != "" {
			t.Fatalf("part ticket = %#v, want upload envelope without content type", ticket)
		}
		if ticket.Multipart == nil || ticket.Multipart.UploadID != "upload-42" || ticket.Multipart.PartNumber != 2 || ticket.Multipart.Rewrite != wantRewrite {
			t.Fatalf("part ticket multipart = %#v", ticket.Multipart)
		}
		if *uploaded != payload {
			t.Fatalf("upload source body = %q, want %q", *uploaded, payload)
		}
	})

	t.Run("complete", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		completeXML := `<CompleteMultipartUploadResult><ETag>"final"</ETag></CompleteMultipartUploadResult>`
		onPublish, uploaded := claimUploadAndRespond(t, edge, reg, pending.Metadata{StatusCode: http.StatusOK, ContentType: "application/xml"}, completeXML)
		pub.on = onPublish
		partsXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"e1"</ETag></Part></CompleteMultipartUpload>`
		req := s3SignHeaderRequestWithBody(t, http.MethodPost, "/demo-bucket/big/file.bin?uploadId=upload-42", io.NopCloser(strings.NewReader(partsXML)), int64(len(partsXML)), map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK || resp.Body.String() != completeXML {
			t.Fatalf("status = %d body = %q, want 200 with complete XML", got, resp.Body.String())
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		ticket := published[0]
		if ticket.Operation != tickets.OperationCompleteMultipartUpload || ticket.UploadSourceURL == "" || ticket.ContentLength == nil || *ticket.ContentLength != int64(len(partsXML)) {
			t.Fatalf("complete ticket = %#v, want upload envelope", ticket)
		}
		if ticket.Multipart == nil || ticket.Multipart.UploadID != "upload-42" || ticket.Multipart.PartNumber != 0 {
			t.Fatalf("complete ticket multipart = %#v", ticket.Multipart)
		}
		if *uploaded != partsXML {
			t.Fatalf("upload source body = %q, want parts XML", *uploaded)
		}
	})

	t.Run("abort", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		pub.on = func(ticket tickets.Ticket) {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusNoContent})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_ = stream.Close()
		}
		req := s3SignHeaderRequestWithBody(t, http.MethodDelete, "/demo-bucket/big/file.bin?uploadId=upload-42", http.NoBody, 0, map[string]string{"x-amz-content-sha256": emptySHA})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusNoContent, resp.Body.String())
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		ticket := published[0]
		if ticket.Operation != tickets.OperationAbortMultipartUpload || ticket.UploadSourceURL != "" || ticket.ContentLength != nil {
			t.Fatalf("abort ticket = %#v, want no upload envelope", ticket)
		}
		if ticket.Multipart == nil || ticket.Multipart.UploadID != "upload-42" {
			t.Fatalf("abort ticket multipart = %#v", ticket.Multipart)
		}
		if _, err := edge.uploadSources.Claim(ticket.RequestID, "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
			t.Fatalf("abort registered upload source: %v", err)
		}
	})
}

func TestS3APIMultipartDisabledRejectedBeforeSideEffects(t *testing.T) {
	const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	tests := []struct {
		name    string
		method  string
		target  string
		body    string
		headers map[string]string
	}{
		{name: "create", method: http.MethodPost, target: "/demo-bucket/file.bin?uploads", headers: map[string]string{"x-amz-content-sha256": emptySHA}},
		{name: "upload part", method: http.MethodPut, target: "/demo-bucket/file.bin?partNumber=1&uploadId=u", body: "x", headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}},
		{name: "complete", method: http.MethodPost, target: "/demo-bucket/file.bin?uploadId=u", body: "<CompleteMultipartUpload/>", headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}},
		{name: "abort", method: http.MethodDelete, target: "/demo-bucket/file.bin?uploadId=u", headers: map[string]string{"x-amz-content-sha256": emptySHA}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}}, nil)
			edge.newToken = func() (string, error) { t.Fatal("disabled multipart allocated a connector token"); return "", nil }
			var body io.ReadCloser = http.NoBody
			if tt.body != "" {
				body = io.NopCloser(strings.NewReader(tt.body))
			}
			req := s3SignHeaderRequestWithBody(t, tt.method, tt.target, body, int64(len(tt.body)), tt.headers)

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d; body=%q", got, http.StatusMethodNotAllowed, resp.Body.String())
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
		})
	}
}

func TestS3APIDirectMultipartOperations(t *testing.T) {
	newDirectEdge := func(fetcher *fakeFetcher) *edgeServer {
		cfg := config.EdgeConfig{
			MultiServer:      true,
			MutationsEnabled: true,
			AllowedBuckets:   []string{"demo-bucket"},
			DirectServers:    map[string]config.S3Config{"blue": {AllowedBuckets: []string{"demo-bucket"}}},
		}
		edge, _ := testS3Edge(&fakePublisher{}, cfg, map[string]objectFetcher{"blue": fetcher})
		return edge
	}

	t.Run("upload part returns etag and passes multipart identity", func(t *testing.T) {
		fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ETag: `"direct-part"`, Body: http.NoBody}}
		edge := newDirectEdge(fetcher)
		payload := "direct part payload"
		req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/blue/demo-bucket/big/file.bin?partNumber=4&uploadId=upload-77", io.NopCloser(strings.NewReader(payload)), int64(len(payload)), map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
		}
		if got := resp.Result().Header.Get("ETag"); got != `"direct-part"` {
			t.Fatalf("ETag = %q, want direct part etag", got)
		}
		requests := fetcher.snapshot()
		if len(requests) != 1 {
			t.Fatalf("fetch requests = %#v, want one", requests)
		}
		got := requests[0]
		if got.Operation != tickets.OperationUploadPart || got.Multipart == nil || got.Multipart.UploadID != "upload-77" || got.Multipart.PartNumber != 4 || got.ContentLength == nil || *got.ContentLength != int64(len(payload)) {
			t.Fatalf("fetch request = %#v, want upload part with multipart identity", got)
		}
	})

	t.Run("abort returns 204", func(t *testing.T) {
		fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusNoContent, Body: http.NoBody}}
		edge := newDirectEdge(fetcher)
		req := s3SignHeaderRequestWithBody(t, http.MethodDelete, "/blue/demo-bucket/big/file.bin?uploadId=upload-77", http.NoBody, 0, map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusNoContent, resp.Body.String())
		}
		if resp.Body.Len() != 0 {
			t.Fatalf("abort body = %q, want empty", resp.Body.String())
		}
	})

	t.Run("create streams xml response", func(t *testing.T) {
		createXML := `<InitiateMultipartUploadResult><UploadId>upload-77</UploadId></InitiateMultipartUploadResult>`
		fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "application/xml", ContentLength: int64(len(createXML)), Body: io.NopCloser(strings.NewReader(createXML))}}
		edge := newDirectEdge(fetcher)
		req := s3SignHeaderRequestWithBody(t, http.MethodPost, "/blue/demo-bucket/big/file.bin?uploads", http.NoBody, 0, map[string]string{"x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK || resp.Body.String() != createXML {
			t.Fatalf("status = %d body = %q, want 200 with create XML", got, resp.Body.String())
		}
	})
}
