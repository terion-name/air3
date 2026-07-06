package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
)

func multipartRewrite() tickets.MultipartRewrite {
	return tickets.MultipartRewrite{Bucket: "public-bucket", Key: "public/file.bin"}
}

func validCreateMultipartTicket(ingestURL string) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodPost)
	ticket.RequestID = "req-create"
	ticket.Operation = tickets.OperationCreateMultipartUpload
	ticket.ContentType = "application/octet-stream"
	ticket.Multipart = &tickets.MultipartRequest{Rewrite: multipartRewrite()}
	return ticket
}

func validUploadPartTicket(ingestURL string, contentLength int64) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodPut)
	ticket.RequestID = "req-part"
	ticket.Operation = tickets.OperationUploadPart
	ticket.UploadSourceURL = "https://edge.internal/_upload-source/req-part"
	ticket.UploadToken = "upload-token"
	ticket.ContentLength = &contentLength
	ticket.Multipart = &tickets.MultipartRequest{UploadID: "upload-42", PartNumber: 3, Rewrite: multipartRewrite()}
	return ticket
}

func validCompleteMultipartTicket(ingestURL string, contentLength int64) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodPost)
	ticket.RequestID = "req-complete"
	ticket.Operation = tickets.OperationCompleteMultipartUpload
	ticket.UploadSourceURL = "https://edge.internal/_upload-source/req-complete"
	ticket.UploadToken = "upload-token"
	ticket.ContentLength = &contentLength
	ticket.Multipart = &tickets.MultipartRequest{UploadID: "upload-42", Rewrite: multipartRewrite()}
	return ticket
}

func validAbortMultipartTicket(ingestURL string) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodDelete)
	ticket.RequestID = "req-abort"
	ticket.Operation = tickets.OperationAbortMultipartUpload
	ticket.Multipart = &tickets.MultipartRequest{UploadID: "upload-42", Rewrite: multipartRewrite()}
	return ticket
}

func TestConnectorMultipartDisabledSendsS3ErrorWithoutSideEffects(t *testing.T) {
	contentLength := int64(len("part body"))
	tests := []struct {
		name   string
		ticket tickets.Ticket
	}{
		{name: "create", ticket: validCreateMultipartTicket("https://edge.internal/_ingest/req-create")},
		{name: "upload part", ticket: validUploadPartTicket("https://edge.internal/_ingest/req-part", contentLength)},
		{name: "complete", ticket: validCompleteMultipartTicket("https://edge.internal/_ingest/req-complete", contentLength)},
		{name: "abort", ticket: validAbortMultipartTicket("https://edge.internal/_ingest/req-abort")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &mutationFetcher{}
			sender := &fakeIngestSender{}
			opener := &fakeUploadSourceOpener{}
			worker := newConnectorWithUploadSourceOpener(connectorConfig(), fetcher, sender, opener, nil)

			if err := worker.handleTicket(context.Background(), tc.ticket); err != nil {
				t.Fatalf("handleTicket() error = %v", err)
			}
			if len(opener.tickets) != 0 || len(fetcher.requests) != 0 {
				t.Fatalf("side effects: opener=%d fetches=%d, want none", len(opener.tickets), len(fetcher.requests))
			}
			if len(sender.sends) != 1 || sender.sends[0].metadata.StatusCode != http.StatusMethodNotAllowed {
				t.Fatalf("sender sends = %#v, want one 405", sender.sends)
			}
		})
	}
}

func TestConnectorUploadPartStreamsUploadSourceWithMultipartIdentity(t *testing.T) {
	contentLength := int64(len("part body"))
	uploadBody := newCloseTrackingReadCloser("part body")
	fetcher := &mutationFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ETag: `"part-etag"`, Body: newCloseTrackingReadCloser("")}}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{opened: &openedUploadSource{Body: uploadBody, ContentLength: contentLength}}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validUploadPartTicket("https://edge.internal/_ingest/req-part", contentLength)

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(opener.tickets) != 1 || len(fetcher.requests) != 1 {
		t.Fatalf("opener calls = %d fetches = %d, want one each", len(opener.tickets), len(fetcher.requests))
	}
	req := fetcher.requests[0]
	if req.Operation != tickets.OperationUploadPart || req.Multipart == nil || req.Multipart.UploadID != "upload-42" || req.Multipart.PartNumber != 3 || fetcher.body != "part body" {
		t.Fatalf("fetch request = %#v body=%q", req, fetcher.body)
	}
	if !uploadBody.closed {
		t.Fatal("upload source body was not closed")
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.ETag != `"part-etag"` {
		t.Fatalf("sender sends = %#v, want part ETag ingest", sender.sends)
	}
}

func TestConnectorCreateMultipartFetchesWithoutUploadSourceAndStreamsXML(t *testing.T) {
	xmlBody := `<InitiateMultipartUploadResult><UploadId>upload-42</UploadId></InitiateMultipartUploadResult>`
	fetcher := &mutationFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "application/xml", ContentLength: int64(len(xmlBody)), Body: newCloseTrackingReadCloser(xmlBody)}}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validCreateMultipartTicket("https://edge.internal/_ingest/req-create")

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(opener.tickets) != 0 {
		t.Fatalf("upload opener calls = %#v, want none", opener.tickets)
	}
	req := fetcher.requests[0]
	if req.Operation != tickets.OperationCreateMultipartUpload || req.ContentType != "application/octet-stream" || req.Multipart == nil || req.Multipart.Rewrite != multipartRewrite() {
		t.Fatalf("fetch request = %#v", req)
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.ContentType != "application/xml" || sender.sends[0].body != xmlBody {
		t.Fatalf("sender sends = %#v, want XML ingest", sender.sends)
	}
}

func TestConnectorAbortMultipartSends204WithoutUploadSource(t *testing.T) {
	fetcher := &mutationFetcher{object: &s3fetch.Object{StatusCode: http.StatusNoContent, Body: newCloseTrackingReadCloser("")}}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validAbortMultipartTicket("https://edge.internal/_ingest/req-abort")

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(opener.tickets) != 0 {
		t.Fatalf("upload opener calls = %#v, want none", opener.tickets)
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.StatusCode != http.StatusNoContent {
		t.Fatalf("sender sends = %#v, want one 204", sender.sends)
	}
}
