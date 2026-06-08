package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/signing"
)

func TestRunPrintsURLAcceptedByValidator(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-method", "GET",
		"-base-url", "https://files.example.com",
		"-bucket", "demo",
		"-key", "dir/object.txt",
		"-secret", "secret",
		"-expiration", "1m",
	}, &stdout, &stderr, func() time.Time { return now })
	if code != 0 {
		t.Fatalf("run() code = %d, stderr = %s", code, stderr.String())
	}
	raw := strings.TrimSpace(stdout.String())
	claims, err := signing.ValidateURL("GET", raw, signing.ValidationConfig{Secret: "secret"}, now)
	if err != nil {
		t.Fatalf("ValidateURL() error = %v for %s", err, raw)
	}
	if claims.Bucket != "demo" || claims.Key != "dir/object.txt" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestRunRejectsMissingRequiredFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-bucket", "demo"}, &stdout, &stderr, time.Now)
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "bucket, key, and secret") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
