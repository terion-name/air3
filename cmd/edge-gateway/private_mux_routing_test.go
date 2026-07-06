package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/uploadsource"
)

// The ingest URL and the upload-source URL are derived from the same
// AIR3_INGEST_URL base, and both must resolve on the edge's private listener.
// This pins the seam that unit tests of either derivation alone cannot cover.
func TestDerivedPrivateURLsRoutableThroughPrivateMux(t *testing.T) {
	bases := []string{
		"https://edge-gateway:9443/ingest", // compose default
		"https://edge-gateway:9443",
		"https://edge-gateway:9443/_ingest",
	}

	reg := pending.NewRegistry(pending.Options{})
	uploadReg := uploadsource.NewRegistry(uploadsource.Options{})
	ingestHandler, err := ingest.NewHandler(ingest.Options{Registry: reg})
	if err != nil {
		t.Fatalf("ingest.NewHandler() error = %v", err)
	}
	uploadHandler, err := uploadsource.NewHandler(uploadsource.HandlerOptions{Registry: uploadReg})
	if err != nil {
		t.Fatalf("uploadsource.NewHandler() error = %v", err)
	}
	mux := newPrivateIngestHandler(ingestHandler, uploadHandler)

	for i, base := range bases {
		requestID := fmt.Sprintf("req-%d", i)
		ingestURL, err := ingestURLForRequest(base, requestID)
		if err != nil {
			t.Fatalf("ingestURLForRequest(%q) error = %v", base, err)
		}
		uploadURL, err := uploadsource.URLForRequest(base, requestID)
		if err != nil {
			t.Fatalf("uploadsource.URLForRequest(%q) error = %v", base, err)
		}

		// The ingest handler answers GET with 405; the mux itself would 404.
		ingestPath := mustPath(t, ingestURL)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ingestPath, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("base %q: GET %q = %d, want %d (ingest handler reached)", base, ingestPath, rec.Code, http.StatusMethodNotAllowed)
		}

		// A registered upload source must be claimable through the mux.
		source := uploadsource.Source{
			RequestID: requestID,
			Token:     "upload-token",
			Body:      io.NopCloser(strings.NewReader("hello")),
			Deadline:  time.Now().Add(time.Minute),
		}
		if err := uploadReg.Register(source); err != nil {
			t.Fatalf("register upload source: %v", err)
		}
		uploadPath := mustPath(t, uploadURL)
		req := httptest.NewRequest(http.MethodGet, uploadPath, nil)
		req.Header.Set(uploadsource.TokenHeader, "upload-token")
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Body.String() != "hello" {
			t.Errorf("base %q: GET %q = %d body %q, want 200 with upload body", base, uploadPath, rec.Code, rec.Body.String())
		}
	}
}

func mustPath(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return u.Path
}
