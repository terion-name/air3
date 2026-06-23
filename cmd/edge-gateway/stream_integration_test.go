package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/tickets"
	"github.com/terion-name/air3/internal/uploadsource"
)

const fullPathStreamBufferBytes = 256 * 1024

type fullPathHarness struct {
	publicURL    string
	publicClient *http.Client
	ingestClient *http.Client
	publisher    *fakePublisher
}

func newFullPathHarness(tb testing.TB, ingestHTTP2 bool) *fullPathHarness {
	tb.Helper()

	reg := pending.NewRegistry(pending.Options{})
	ingestHandler, err := ingest.NewHandler(ingest.Options{Registry: reg, StreamCopyBufferBytes: fullPathStreamBufferBytes})
	if err != nil {
		tb.Fatalf("NewHandler() error = %v", err)
	}
	ingestServer := httptest.NewUnstartedServer(ingestHandler)
	ingestServer.EnableHTTP2 = ingestHTTP2
	ingestServer.StartTLS()
	tb.Cleanup(ingestServer.Close)

	publisher := &fakePublisher{}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:             ingestServer.URL + strings.TrimSuffix(ingest.PathPrefix, "/"),
		AllowedBuckets:        []string{"demo-bucket"},
		Signing:               config.SigningConfig{Disabled: true},
		Timeouts:              config.TimeoutConfig{PendingRequestTTL: 30 * time.Second, StreamTimeout: 30 * time.Second},
		StreamCopyBufferBytes: fullPathStreamBufferBytes,
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), publisher, nil)
	var nextID atomic.Uint64
	edge.newToken = func() (string, error) {
		id := nextID.Add(1)
		return fmt.Sprintf("stream-token-%d", id), nil
	}

	publicServer := httptest.NewServer(edge)
	tb.Cleanup(publicServer.Close)

	return &fullPathHarness{publicURL: publicServer.URL, publicClient: publicServer.Client(), ingestClient: ingestServer.Client(), publisher: publisher}
}

func TestFullPathStreamsBeforeIngestUploadCompletes(t *testing.T) {
	h := newFullPathHarness(t, false)
	first := strings.Repeat("a", 64*1024)
	rest := "second chunk\n"
	releaseRest := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRest) }) }
	t.Cleanup(release)
	uploadDone := make(chan error, 1)
	h.publisher.on = func(ticket tickets.Ticket) {
		body := &gatedReader{first: strings.NewReader(first), rest: strings.NewReader(rest), gate: releaseRest}
		go func() {
			uploadDone <- postFullPathIngest(context.Background(), h.ingestClient, ticket, body, int64(len(first)+len(rest)))
		}()
	}

	resp, err := h.publicClient.Get(h.publicURL + "/demo-bucket/file.txt")
	if err != nil {
		t.Fatalf("public GET error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("public status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("content-type = %q, want application/octet-stream", ct)
	}
	if length := resp.Header.Get("Content-Length"); length != strconv.Itoa(len(first)+len(rest)) {
		t.Fatalf("content-length = %q, want %d", length, len(first)+len(rest))
	}

	gotFirst := readFullWithTimeout(t, resp.Body, len(first), 2*time.Second)
	if string(gotFirst) != first {
		t.Fatalf("first bytes = %q, want %q", gotFirst, first)
	}
	select {
	case err := <-uploadDone:
		t.Fatalf("ingest upload completed before response consumed first chunk: %v", err)
	default:
	}

	release()
	gotRest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response remainder: %v", err)
	}
	if string(gotRest) != rest {
		t.Fatalf("remaining bytes = %q, want %q", gotRest, rest)
	}
	if err := <-uploadDone; err != nil {
		t.Fatalf("ingest POST error = %v", err)
	}
}

func BenchmarkFullPathStreaming(b *testing.B) {
	const bodySize = 8 << 20
	cases := []struct {
		name        string
		fixedLength bool
		ingestHTTP2 bool
	}{
		{name: "http1_fixed_length", fixedLength: true},
		{name: "http1_unknown_length", fixedLength: false},
		{name: "http2_fixed_length", fixedLength: true, ingestHTTP2: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			h := newFullPathHarness(b, tc.ingestHTTP2)
			uploadDoneCh := make(chan chan error, 1)
			h.publisher.on = func(ticket tickets.Ticket) {
				uploadDone := <-uploadDoneCh
				body := &repeatingReader{remaining: bodySize}
				contentLength := int64(-1)
				if tc.fixedLength {
					contentLength = bodySize
				}
				go func() {
					uploadDone <- postFullPathIngest(context.Background(), h.ingestClient, ticket, body, contentLength)
				}()
			}

			b.SetBytes(bodySize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				uploadDone := make(chan error, 1)
				uploadDoneCh <- uploadDone
				resp, err := h.publicClient.Get(h.publicURL + "/demo-bucket/file.txt")
				if err != nil {
					b.Fatalf("public GET error = %v", err)
				}
				_, copyErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					b.Fatalf("public status = %d, want %d", resp.StatusCode, http.StatusOK)
				}
				if copyErr != nil {
					b.Fatalf("copy public response: %v", copyErr)
				}
				if closeErr != nil {
					b.Fatalf("close public response: %v", closeErr)
				}
				if err := <-uploadDone; err != nil {
					b.Fatalf("ingest POST error = %v", err)
				}
			}
		})
	}
}

func postFullPathIngest(ctx context.Context, client *http.Client, ticket tickets.Ticket, body io.Reader, contentLength int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ticket.IngestURL, body)
	if err != nil {
		return err
	}
	req.Header.Set(ingest.TokenHeader, ticket.IngestToken)
	req.Header.Set(ingest.StatusCodeHeader, strconv.Itoa(http.StatusOK))
	req.Header.Set("Content-Type", "application/octet-stream")
	if contentLength >= 0 {
		req.Header.Set(ingest.ObjectContentLengthHeader, strconv.FormatInt(contentLength, 10))
		req.ContentLength = contentLength
	} else {
		req.ContentLength = -1
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("ingest status = %d", resp.StatusCode)
	}
	return nil
}

type gatedReader struct {
	first *strings.Reader
	rest  *strings.Reader
	gate  <-chan struct{}
	open  bool
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.first.Len() > 0 {
		return r.first.Read(p)
	}
	if !r.open {
		<-r.gate
		r.open = true
	}
	return r.rest.Read(p)
}

type repeatingReader struct {
	remaining int64
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = 'a'
	}
	r.remaining -= int64(len(p))
	return len(p), nil
}

func readFullWithTimeout(t *testing.T, r io.Reader, n int, timeout time.Duration) []byte {
	t.Helper()
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		buf := make([]byte, n)
		_, err := io.ReadFull(r, buf)
		ch <- result{data: buf, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("read %d bytes: %v", n, res.err)
		}
		return res.data
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %d streamed bytes", n)
		return nil
	}
}
