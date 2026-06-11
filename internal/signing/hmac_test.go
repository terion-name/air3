package signing

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/publicpath"
)

func TestSignURLValidates(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURL(SignInput{
		Method:              "GET",
		BaseURL:             "https://files.example.com",
		Server:              "ignored-in-single-mode",
		Bucket:              "demo-bucket",
		Key:                 "dir/object.txt",
		Range:               "bytes=0-9",
		ResponseContentType: "text/plain",
		Expires:             now.Add(time.Minute),
		Secret:              "top-secret",
	})
	if err != nil {
		t.Fatalf("SignURL() error = %v", err)
	}
	u := mustParseURL(t, raw)
	if got, want := u.EscapedPath(), "/demo-bucket/dir/object.txt"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
	claims, err := ValidateURL("GET", raw, ValidationConfig{Secret: "top-secret"}, now)
	if err != nil {
		t.Fatalf("ValidateURL() error = %v", err)
	}
	if claims.Server != "" || claims.Bucket != "demo-bucket" || claims.Key != "dir/object.txt" || claims.Range != "bytes=0-9" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestCanonicalStringIsStable(t *testing.T) {
	claims := Claims{
		Method:                     "get",
		Bucket:                     "demo",
		Key:                        "a/b.txt",
		Range:                      "bytes=1-2",
		ResponseContentType:        "text/plain",
		ResponseContentDisposition: "attachment",
		Expires:                    time.Unix(1780934400, 0),
	}
	want := "GET\ndemo\na/b.txt\n1780934400\nbytes=1-2\ntext/plain\nattachment"
	if got := canonicalString(claims); got != want {
		t.Fatalf("canonicalString() = %q, want %q", got, want)
	}
}

func TestCanonicalStringIncludesServerWhenPresent(t *testing.T) {
	claims := Claims{
		Method:                     "get",
		Server:                     "edge-1",
		Bucket:                     "demo",
		Key:                        "a/b.txt",
		Range:                      "bytes=1-2",
		ResponseContentType:        "text/plain",
		ResponseContentDisposition: "attachment",
		Expires:                    time.Unix(1780934400, 0),
	}
	want := "GET\nedge-1\ndemo\na/b.txt\n1780934400\nbytes=1-2\ntext/plain\nattachment"
	if got := canonicalString(claims); got != want {
		t.Fatalf("canonicalString() = %q, want %q", got, want)
	}
}

func TestSignURLForModeMultiValidates(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURLForMode(SignInput{
		Method:                     "GET",
		BaseURL:                    "https://files.example.com/base",
		Server:                     "edge-1",
		Bucket:                     "demo-bucket",
		Key:                        "dir/object.txt",
		Range:                      "bytes=0-9",
		ResponseContentType:        "text/plain",
		ResponseContentDisposition: "attachment; filename=object.txt",
		Expires:                    now.Add(time.Minute),
		Secret:                     "top-secret",
	}, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("SignURLForMode() error = %v", err)
	}
	u := mustParseURL(t, raw)
	if got, want := u.EscapedPath(), "/base/edge-1/demo-bucket/dir/object.txt"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
	q := u.Query()
	if got, want := q.Get(ParamRange), "bytes=0-9"; got != want {
		t.Fatalf("range query = %q, want %q", got, want)
	}
	if got, want := q.Get(ParamResponseContentType), "text/plain"; got != want {
		t.Fatalf("response content type = %q, want %q", got, want)
	}
	if got, want := q.Get(ParamResponseContentDisposition), "attachment; filename=object.txt"; got != want {
		t.Fatalf("response content disposition = %q, want %q", got, want)
	}

	// Validate the public multi-server path itself; signing above intentionally
	// preserves legacy BaseURL path prefix behavior before the public path.
	publicRaw := strings.Replace(raw, "/base/edge-1/", "/edge-1/", 1)
	claims, err := ValidateURLForMode("GET", publicRaw, ValidationConfig{Secret: "top-secret"}, now, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("ValidateURLForMode() error = %v", err)
	}
	if claims.Server != "edge-1" || claims.Bucket != "demo-bucket" || claims.Key != "dir/object.txt" || claims.Range != "bytes=0-9" || claims.ResponseContentType != "text/plain" || claims.ResponseContentDisposition != "attachment; filename=object.txt" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestSignURLForModeMultiRejectsMissingOrInvalidServer(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	for _, server := range []string{"", "bad.alias", "bad alias", "/bad"} {
		t.Run(server, func(t *testing.T) {
			_, err := SignURLForMode(SignInput{
				Method:  "GET",
				BaseURL: "https://files.example.com",
				Server:  server,
				Bucket:  "demo-bucket",
				Key:     "object.txt",
				Expires: now.Add(time.Minute),
				Secret:  "top-secret",
			}, publicpath.ModeMulti)
			if err == nil {
				t.Fatal("SignURLForMode() error = nil, want server alias error")
			}
		})
	}
}

func TestValidateURLForModeMultiRejectsInvalidPathBeforeDisabled(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	for _, raw := range []string{
		"https://files.example.com//demo-bucket/object.txt?expires=1780934400",
		"https://files.example.com/bad.alias/demo-bucket/object.txt?expires=1780934400",
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := ValidateURLForMode("GET", raw, ValidationConfig{Disabled: true}, now, publicpath.ModeMulti)
			if err == nil {
				t.Fatal("ValidateURLForMode() error = nil, want path/server error")
			}
		})
	}
}

func TestValidateURLForModeMultiRejectsServerTampering(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURLForMode(SignInput{Method: "GET", BaseURL: "https://files.example.com", Server: "edge-1", Bucket: "demo-bucket", Key: "object.txt", Expires: now.Add(time.Minute), Secret: "secret"}, publicpath.ModeMulti)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(raw, "/edge-1/", "/edge-2/", 1)
	if _, err := ValidateURLForMode("GET", tampered, ValidationConfig{Secret: "secret"}, now, publicpath.ModeMulti); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("ValidateURLForMode() error = %v, want ErrInvalidSignature", err)
	}
}

func TestDisabledValidationParsesMultiServerClaims(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw := "https://files.example.com/edge-1/demo-bucket/object.txt?expires=1"
	claims, err := ValidateURLForMode("GET", raw, ValidationConfig{Disabled: true}, now, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("ValidateURLForMode() error = %v", err)
	}
	if claims.Server != "edge-1" || claims.Bucket != "demo-bucket" || claims.Key != "object.txt" {
		t.Fatalf("claims = %#v", claims)
	}

	malformedExpires := "https://files.example.com/edge-1/demo-bucket/object.txt?expires=abc"
	if _, err := ValidateURLForMode("GET", malformedExpires, ValidationConfig{Disabled: true}, now, publicpath.ModeMulti); err == nil {
		t.Fatal("ValidateURLForMode() error = nil, want malformed timestamp error")
	}
}

func TestModeAwareFunctionsRejectUnknownMode(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	unknown := publicpath.Mode(99)
	if _, err := SignURLForMode(SignInput{Method: "GET", BaseURL: "https://files.example.com", Bucket: "demo-bucket", Key: "object.txt", Expires: now.Add(time.Minute), Secret: "secret"}, unknown); err == nil {
		t.Fatal("SignURLForMode() error = nil, want unknown mode error")
	}
	if _, err := ValidateURLForMode("GET", "https://files.example.com/demo-bucket/object.txt?expires=1780934400", ValidationConfig{Disabled: true}, now, unknown); err == nil {
		t.Fatal("ValidateURLForMode() error = nil, want unknown mode error")
	}
}

func TestValidateURLRejectsTamperingAndExpiry(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURL(SignInput{Method: "HEAD", BaseURL: "https://files.example.com", Bucket: "demo", Key: "object", Expires: now.Add(time.Minute), Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		url  string
		meth string
		want error
	}{
		{"method", raw, "GET", ErrInvalidSignature},
		{"bucket", strings.Replace(raw, "/demo/", "/other/", 1), "HEAD", ErrInvalidSignature},
		{"key", strings.Replace(raw, "/object", "/other", 1), "HEAD", ErrInvalidSignature},
		{"expired", raw, "HEAD", ErrExpired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			when := now
			if tc.want == ErrExpired {
				when = now.Add(2 * time.Minute)
			}
			_, err := ValidateURL(tc.meth, tc.url, ValidationConfig{Secret: "secret"}, when)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ValidateURL() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateURLRejectsMalformedTimestamp(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw := "https://files.example.com/demo/object?expires=abc&sig=00"
	if _, err := ValidateURL("GET", raw, ValidationConfig{Secret: "secret"}, now); err == nil {
		t.Fatal("ValidateURL() error = nil, want malformed timestamp error")
	}
}

func TestDisabledValidationSkipsSignature(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw := "https://files.example.com/demo/object?expires=1"
	claims, err := ValidateURL("GET", raw, ValidationConfig{Disabled: true}, now)
	if err != nil {
		t.Fatalf("ValidateURL() error = %v", err)
	}
	if claims.Server != "" || claims.Bucket != "demo" || claims.Key != "object" {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestSignatureComparePathRejectsDifferentLengthSignature(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURL(SignInput{Method: "GET", BaseURL: "https://files.example.com", Bucket: "demo", Key: "object", Expires: now.Add(time.Minute), Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set(ParamSig, "00")
	u.RawQuery = q.Encode()
	if _, err := ValidateURL("GET", u.String(), ValidationConfig{Secret: "secret"}, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("ValidateURL() error = %v, want ErrInvalidSignature", err)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
