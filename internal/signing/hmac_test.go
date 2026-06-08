package signing

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSignURLValidates(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURL(SignInput{
		Method:              "GET",
		BaseURL:             "https://files.example.com",
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
	claims, err := ValidateURL("GET", raw, ValidationConfig{Secret: "top-secret"}, now)
	if err != nil {
		t.Fatalf("ValidateURL() error = %v", err)
	}
	if claims.Bucket != "demo-bucket" || claims.Key != "dir/object.txt" || claims.Range != "bytes=0-9" {
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
	if claims.Bucket != "demo" || claims.Key != "object" {
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
