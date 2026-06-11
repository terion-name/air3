package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/publicpath"
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

func TestRunPrintsMultiServerURLAcceptedByValidator(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-method", "GET",
		"-base-url", "https://files.example.com",
		"-server", "edge-a",
		"-bucket", "demo",
		"-key", "dir/object.txt",
		"-secret", "secret",
		"-expiration", "1m",
	}, &stdout, &stderr, func() time.Time { return now })
	if code != 0 {
		t.Fatalf("run() code = %d, stderr = %s", code, stderr.String())
	}
	raw := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(raw, "https://files.example.com/edge-a/demo/dir/object.txt?") {
		t.Fatalf("signed URL = %q, want multi-server path", raw)
	}
	claims, err := signing.ValidateURLForMode("GET", raw, signing.ValidationConfig{Secret: "secret"}, now, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("ValidateURLForMode() error = %v for %s", err, raw)
	}
	if claims.Server != "edge-a" || claims.Bucket != "demo" || claims.Key != "dir/object.txt" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestRunPrintsDefaultBucketPathURLAcceptedByValidator(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-method", "GET",
		"-base-url", "https://files.example.com",
		"-server", "blue",
		"-default-bucket-path",
		"-bucket", "demo",
		"-key", "file.txt",
		"-secret", "secret",
		"-expiration", "1m",
	}, &stdout, &stderr, func() time.Time { return now })
	if code != 0 {
		t.Fatalf("run() code = %d, stderr = %s", code, stderr.String())
	}
	raw := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(raw, "https://files.example.com/blue/file.txt?") {
		t.Fatalf("signed URL = %q, want default-bucket path", raw)
	}
	resolver := func(server string) (string, bool) {
		if server == "blue" {
			return "demo", true
		}
		return "", false
	}
	claims, err := signing.ValidateURLForModeWithOptions("GET", raw, signing.ValidationConfig{Secret: "secret"}, now, publicpath.ModeMulti, signing.ValidationOptions{DefaultBucket: resolver})
	if err != nil {
		t.Fatalf("ValidateURLForModeWithOptions() error = %v for %s", err, raw)
	}
	if claims.Server != "blue" || claims.Bucket != "demo" || claims.Key != "file.txt" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestRunRejectsDefaultBucketPathWithoutServer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-base-url", "https://files.example.com",
		"-default-bucket-path",
		"-bucket", "demo",
		"-key", "file.txt",
		"-secret", "secret",
	}, &stdout, &stderr, time.Now)
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "default-bucket-path requires -server") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunRejectsInvalidServerFlag(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-base-url", "https://files.example.com",
		"-server", "bad/alias",
		"-bucket", "demo",
		"-key", "dir/object.txt",
		"-secret", "secret",
	}, &stdout, &stderr, func() time.Time { return now })
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "server alias") {
		t.Fatalf("stderr = %q, want server alias error", stderr.String())
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
