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
	"github.com/terion-name/air3/internal/s3api"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/tickets"
	"github.com/terion-name/air3/internal/uploadsource"
)

type fakeUploadSourceOpener struct {
	tickets []tickets.Ticket
	opened  *openedUploadSource
	err     error
}

func (o *fakeUploadSourceOpener) Open(ctx context.Context, ticket tickets.Ticket) (*openedUploadSource, error) {
	o.tickets = append(o.tickets, ticket)
	return o.opened, o.err
}

type mutationFetcher struct {
	requests []s3fetch.Request
	body     string
	object   *s3fetch.Object
	err      error
}

func (f *mutationFetcher) Fetch(ctx context.Context, req s3fetch.Request) (*s3fetch.Object, error) {
	f.requests = append(f.requests, req)
	if req.Body != nil {
		data, _ := io.ReadAll(req.Body)
		f.body = string(data)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.object, nil
}

type closeTrackingReadCloser struct {
	reader *strings.Reader
	closed bool
}

func newCloseTrackingReadCloser(body string) *closeTrackingReadCloser {
	return &closeTrackingReadCloser{reader: strings.NewReader(body)}
}

func (r *closeTrackingReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *closeTrackingReadCloser) Close() error {
	r.closed = true
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func validPutTicket(ingestURL string, contentLength int64) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodPut)
	ticket.RequestID = "req-put"
	ticket.Operation = tickets.OperationPutObject
	ticket.UploadSourceURL = "https://edge.internal/_uploads/req-put"
	ticket.UploadToken = "upload-token"
	ticket.ContentLength = &contentLength
	ticket.ContentType = "text/plain"
	return ticket
}

func validDeleteTicket(ingestURL string) tickets.Ticket {
	ticket := validTicket(ingestURL, http.MethodDelete)
	ticket.RequestID = "req-delete"
	ticket.Operation = tickets.OperationDeleteObject
	return ticket
}

func enabledMutationConfig() config.ConnectorConfig {
	cfg := connectorConfig()
	cfg.MutationsEnabled = true
	return cfg
}

func wantS3ErrorXML(t *testing.T, ticket tickets.Ticket, code, message string) string {
	t.Helper()
	body, err := s3api.RenderErrorXML(s3api.ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  s3ErrorResource(ticket),
		RequestID: ticket.RequestID,
	})
	if err != nil {
		t.Fatalf("RenderErrorXML() error = %v", err)
	}
	return string(body)
}

func TestConnectorMutationDisabledSendsDeterministicS3ErrorWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		ticket tickets.Ticket
	}{
		{name: "put", ticket: validPutTicket("https://edge.internal/_ingest/req-put", int64(len("payload")))},
		{name: "delete", ticket: validDeleteTicket("https://edge.internal/_ingest/req-delete")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &mutationFetcher{}
			sender := &fakeIngestSender{}
			opener := &fakeUploadSourceOpener{opened: &openedUploadSource{Body: newCloseTrackingReadCloser("payload"), ContentLength: int64(len("payload"))}}
			worker := newConnectorWithUploadSourceOpener(connectorConfig(), fetcher, sender, opener, nil)

			if err := worker.handleTicket(context.Background(), tc.ticket); err != nil {
				t.Fatalf("handleTicket() error = %v", err)
			}
			if len(opener.tickets) != 0 {
				t.Fatalf("upload opener calls = %#v, want none", opener.tickets)
			}
			if len(fetcher.requests) != 0 {
				t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
			}
			if len(sender.sends) != 1 {
				t.Fatalf("sender sends = %#v, want one", sender.sends)
			}
			wantBody := wantS3ErrorXML(t, tc.ticket, "MethodNotAllowed", "mutations are disabled")
			sent := sender.sends[0]
			if sent.metadata.StatusCode != http.StatusMethodNotAllowed || sent.metadata.ContentType != "application/xml" || sent.metadata.ContentLength != int64(len(wantBody)) || sent.body != wantBody {
				t.Fatalf("sent ingest = %#v, want status 405 XML body %q", sent, wantBody)
			}
		})
	}
}

func TestConnectorPutObjectStreamsUploadSourceToBackendAndIngestsResult(t *testing.T) {
	contentLength := int64(len("hello mutation"))
	uploadBody := newCloseTrackingReadCloser("hello mutation")
	objectBody := newCloseTrackingReadCloser("")
	fetcher := &mutationFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentLength: 0, ETag: `"etag-1"`, Body: objectBody}}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{opened: &openedUploadSource{Body: uploadBody, ContentLength: contentLength}}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validPutTicket("https://edge.internal/_ingest/req-put", contentLength)

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(opener.tickets) != 1 {
		t.Fatalf("upload opener calls = %#v, want one", opener.tickets)
	}
	if len(fetcher.requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", fetcher.requests)
	}
	req := fetcher.requests[0]
	if req.Method != http.MethodPut || req.Operation != tickets.OperationPutObject || req.Bucket != ticket.Bucket || req.Key != ticket.Key || req.ContentLength == nil || *req.ContentLength != contentLength || req.ContentType != "text/plain" || fetcher.body != "hello mutation" {
		t.Fatalf("fetch request = %#v body=%q", req, fetcher.body)
	}
	if !uploadBody.closed {
		t.Fatal("upload source body was not closed")
	}
	if !objectBody.closed {
		t.Fatal("backend object body was not closed")
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.StatusCode != http.StatusOK || sender.sends[0].metadata.ETag != `"etag-1"` {
		t.Fatalf("sender sends = %#v, want OK ETag ingest", sender.sends)
	}
}

func TestHTTPUploadSourceOpenerUsesPrivateGETTokenAndStreamsBody(t *testing.T) {
	var gotMethod, gotToken string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotToken = r.Header.Get(uploadsource.TokenHeader)
		w.Header().Set("Content-Length", "12")
		_, _ = w.Write([]byte("upload bytes"))
	}))
	defer ts.Close()

	opener := httpUploadSourceOpener{client: ts.Client()}
	ticket := validPutTicket("https://edge.internal/_ingest/req-put", 12)
	ticket.UploadSourceURL = ts.URL
	opened, err := opener.Open(context.Background(), ticket)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer opened.Body.Close()
	body, _ := io.ReadAll(opened.Body)
	if gotMethod != http.MethodGet || gotToken != "upload-token" || opened.ContentLength != 12 || string(body) != "upload bytes" {
		t.Fatalf("method=%q token=%q contentLength=%d body=%q", gotMethod, gotToken, opened.ContentLength, string(body))
	}
}

func TestHTTPUploadSourceOpenerRejectsNon2xxAndTransportErrors(t *testing.T) {
	non2xxBody := newCloseTrackingReadCloser("nope")
	non2xxOpener := httpUploadSourceOpener{client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Body: non2xxBody}, nil
	})}}
	if opened, err := non2xxOpener.Open(context.Background(), validPutTicket("https://edge.internal/_ingest/req-put", 4)); err == nil || opened != nil {
		t.Fatalf("Open() = %#v, %v; want non-2xx error", opened, err)
	}
	if !non2xxBody.closed {
		t.Fatal("non-2xx upload source body was not closed")
	}

	transportErr := errors.New("dial failed")
	transportOpener := httpUploadSourceOpener{client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, transportErr
	})}}
	if opened, err := transportOpener.Open(context.Background(), validPutTicket("https://edge.internal/_ingest/req-put", 4)); err == nil || opened != nil {
		t.Fatalf("Open() = %#v, %v; want transport error", opened, err)
	}
}

func TestConnectorPutObjectUploadSourceFailuresSendS3ErrorsWithoutBackendFetch(t *testing.T) {
	tests := []struct {
		name       string
		opener     *fakeUploadSourceOpener
		wantStatus int
		wantCode   string
		wantMsg    string
	}{
		{
			name:       "open error",
			opener:     &fakeUploadSourceOpener{err: errors.New("upload unavailable")},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "ServiceUnavailable",
			wantMsg:    "upload source is unavailable",
		},
		{
			name:       "nil source",
			opener:     &fakeUploadSourceOpener{},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "ServiceUnavailable",
			wantMsg:    "upload source is unavailable",
		},
		{
			name: "content length mismatch",
			opener: func() *fakeUploadSourceOpener {
				body := newCloseTrackingReadCloser("toolong")
				return &fakeUploadSourceOpener{opened: &openedUploadSource{Body: body, ContentLength: 7}}
			}(),
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidRequest",
			wantMsg:    "upload source content length does not match ticket content length",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &mutationFetcher{}
			sender := &fakeIngestSender{}
			worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, tc.opener, nil)
			ticket := validPutTicket("https://edge.internal/_ingest/req-put", 4)

			if err := worker.handleTicket(context.Background(), ticket); err != nil {
				t.Fatalf("handleTicket() error = %v", err)
			}
			if len(fetcher.requests) != 0 {
				t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
			}
			if len(sender.sends) != 1 {
				t.Fatalf("sender sends = %#v, want one", sender.sends)
			}
			wantBody := wantS3ErrorXML(t, ticket, tc.wantCode, tc.wantMsg)
			sent := sender.sends[0]
			if sent.metadata.StatusCode != tc.wantStatus || sent.metadata.ContentType != "application/xml" || sent.metadata.ContentLength != int64(len(wantBody)) || sent.body != wantBody {
				t.Fatalf("sent ingest = %#v, want status %d body %q", sent, tc.wantStatus, wantBody)
			}
			if tc.opener.opened != nil {
				body, ok := tc.opener.opened.Body.(*closeTrackingReadCloser)
				if !ok || !body.closed {
					t.Fatalf("opened body closed = %v, want true", ok && body.closed)
				}
			}
		})
	}
}

func TestConnectorPutObjectBackendErrorKeepsStatusOnlyIngestAndClosesUpload(t *testing.T) {
	uploadBody := newCloseTrackingReadCloser("bad")
	fetcher := &mutationFetcher{err: s3fetch.ErrInvalidRequest}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{opened: &openedUploadSource{Body: uploadBody, ContentLength: 3}}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validPutTicket("https://edge.internal/_ingest/req-put", 3)

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(fetcher.requests) != 1 || fetcher.body != "bad" {
		t.Fatalf("fetch requests = %#v body=%q", fetcher.requests, fetcher.body)
	}
	if !uploadBody.closed {
		t.Fatal("upload source body was not closed")
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.StatusCode != http.StatusBadRequest || sender.sends[0].body != "" {
		t.Fatalf("sender sends = %#v, want 400 status-only", sender.sends)
	}
}

func TestConnectorDeleteObjectFetchesBackendAndSends204Ingest(t *testing.T) {
	fetcher := &mutationFetcher{object: &s3fetch.Object{StatusCode: http.StatusNoContent, ContentLength: 0, Body: http.NoBody}}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{}
	worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
	ticket := validDeleteTicket("https://edge.internal/_ingest/req-delete")

	if err := worker.handleTicket(context.Background(), ticket); err != nil {
		t.Fatalf("handleTicket() error = %v", err)
	}
	if len(opener.tickets) != 0 {
		t.Fatalf("upload opener calls = %#v, want none", opener.tickets)
	}
	if len(fetcher.requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", fetcher.requests)
	}
	req := fetcher.requests[0]
	if req.Method != http.MethodDelete || req.Operation != tickets.OperationDeleteObject || req.Body != nil || req.ContentLength != nil || req.ContentType != "" {
		t.Fatalf("fetch request = %#v, want delete without upload fields", req)
	}
	if len(sender.sends) != 1 || sender.sends[0].metadata.StatusCode != http.StatusNoContent || sender.sends[0].body != "" {
		t.Fatalf("sender sends = %#v, want empty 204", sender.sends)
	}
}

func TestConnectorRejectsInvalidMutationTicketsBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tickets.Ticket)
	}{
		{name: "put missing upload source", mutate: func(ticket *tickets.Ticket) { ticket.UploadSourceURL = "" }},
		{name: "put missing content length", mutate: func(ticket *tickets.Ticket) { ticket.ContentLength = nil }},
		{name: "put wrong method", mutate: func(ticket *tickets.Ticket) { ticket.Method = http.MethodGet }},
		{name: "delete upload token present", mutate: func(ticket *tickets.Ticket) {
			ticket.Operation = tickets.OperationDeleteObject
			ticket.Method = http.MethodDelete
			ticket.UploadToken = "upload-token"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &mutationFetcher{}
			sender := &fakeIngestSender{}
			opener := &fakeUploadSourceOpener{opened: &openedUploadSource{Body: newCloseTrackingReadCloser("payload"), ContentLength: 7}}
			worker := newConnectorWithUploadSourceOpener(enabledMutationConfig(), fetcher, sender, opener, nil)
			ticket := validPutTicket("https://edge.internal/_ingest/req-put", 7)
			tc.mutate(&ticket)

			if err := worker.handleTicket(context.Background(), ticket); err == nil {
				t.Fatal("handleTicket() error = nil, want invalid ticket error")
			}
			if len(opener.tickets) != 0 {
				t.Fatalf("upload opener calls = %#v, want none", opener.tickets)
			}
			if len(fetcher.requests) != 0 {
				t.Fatalf("fetch requests = %#v, want none", fetcher.requests)
			}
			if len(sender.sends) != 0 {
				t.Fatalf("sender sends = %#v, want none", sender.sends)
			}
		})
	}
}

func TestConnectorMutationValidationDoesNotDependOnMutationGate(t *testing.T) {
	fetcher := &mutationFetcher{}
	sender := &fakeIngestSender{}
	opener := &fakeUploadSourceOpener{}
	worker := newConnectorWithUploadSourceOpener(connectorConfig(), fetcher, sender, opener, nil)
	ticket := validPutTicket("https://edge.internal/_ingest/req-put", 0)
	ticket.DeadlineUnixMS = time.Now().Add(time.Minute).UnixMilli()

	if err := worker.validateTicket(ticket); err != nil {
		t.Fatalf("validateTicket() error = %v", err)
	}
	if len(opener.tickets) != 0 || len(fetcher.requests) != 0 || len(sender.sends) != 0 {
		t.Fatalf("side effects opener=%#v fetch=%#v sends=%#v, want none", opener.tickets, fetcher.requests, sender.sends)
	}
}
