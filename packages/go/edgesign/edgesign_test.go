package edgesign

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

type vector struct {
	Name  string `json:"name"`
	Input struct {
		Method                     string `json:"method"`
		BaseURL                    string `json:"baseUrl"`
		Bucket                     string `json:"bucket"`
		Key                        string `json:"key"`
		Secret                     string `json:"secret"`
		Expires                    int64  `json:"expires"`
		Range                      string `json:"range"`
		ResponseContentType        string `json:"responseContentType"`
		ResponseContentDisposition string `json:"responseContentDisposition"`
	} `json:"input"`
	Now         int64  `json:"now"`
	Canonical   string `json:"canonical"`
	URL         string `json:"url"`
	VerifyRange string `json:"verifyRange"`
}

func TestVectors(t *testing.T) {
	for _, vector := range loadVectors(t) {
		t.Run(vector.Name, func(t *testing.T) {
			expires := time.Unix(vector.Input.Expires, 0).UTC()
			raw, err := SignURL(SignInput{
				Method:                     vector.Input.Method,
				BaseURL:                    vector.Input.BaseURL,
				Bucket:                     vector.Input.Bucket,
				Key:                        vector.Input.Key,
				Secret:                     vector.Input.Secret,
				Expires:                    expires,
				Range:                      vector.Input.Range,
				ResponseContentType:        vector.Input.ResponseContentType,
				ResponseContentDisposition: vector.Input.ResponseContentDisposition,
			})
			if err != nil {
				t.Fatalf("SignURL() error = %v", err)
			}
			if raw != vector.URL {
				t.Fatalf("SignURL() = %q, want %q", raw, vector.URL)
			}

			claims, err := VerifyURL(VerifyInput{Method: vector.Input.Method, URL: raw, Secret: vector.Input.Secret, Now: time.Unix(vector.Now, 0).UTC(), Range: vector.VerifyRange})
			if err != nil {
				t.Fatalf("VerifyURL() error = %v", err)
			}
			if got := CanonicalString(claims); got != vector.Canonical {
				t.Fatalf("CanonicalString() = %q, want %q", got, vector.Canonical)
			}
		})
	}
}

func TestVerifyRangeHeaderHandling(t *testing.T) {
	now := time.Unix(1780934400, 0).UTC()
	raw, err := SignURL(SignInput{Method: "GET", BaseURL: "https://files.example.com", Bucket: "demo-bucket", Key: "object.txt", Secret: "secret", Expires: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = VerifyURL(VerifyInput{Method: "GET", URL: raw, Secret: "secret", Now: now, Range: "bytes=0-9"})
	if !errors.Is(err, ErrUnsignedRange) {
		t.Fatalf("VerifyURL() error = %v, want ErrUnsignedRange", err)
	}

	ranged, err := SignURL(SignInput{Method: "GET", BaseURL: "https://files.example.com", Bucket: "demo-bucket", Key: "object.txt", Secret: "secret", Expires: now.Add(time.Minute), Range: "bytes=0-9"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = VerifyURL(VerifyInput{Method: "GET", URL: ranged, Secret: "secret", Now: now, Range: "bytes=1-9"})
	if !errors.Is(err, ErrRangeMismatch) {
		t.Fatalf("VerifyURL() error = %v, want ErrRangeMismatch", err)
	}
}

func TestMultiServerSignAndVerify(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	expires := now.Add(time.Minute)
	raw, err := SignURL(SignInput{
		Method:                     "GET",
		BaseURL:                    "https://files.example.com",
		Server:                     "edge-1",
		Bucket:                     "demo-bucket",
		Key:                        "dir/object.txt",
		Secret:                     "top-secret",
		Expires:                    expires,
		Range:                      "bytes=0-9",
		ResponseContentType:        "text/plain",
		ResponseContentDisposition: "attachment; filename=object.txt",
	})
	if err != nil {
		t.Fatalf("SignURL() error = %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse signed URL: %v", err)
	}
	if got, want := u.EscapedPath(), "/edge-1/demo-bucket/dir/object.txt"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}

	claims, err := VerifyURL(VerifyInput{Method: "GET", URL: raw, Server: "edge-1", Secret: "top-secret", Now: now, Range: "bytes=0-9"})
	if err != nil {
		t.Fatalf("VerifyURL() error = %v", err)
	}
	if claims.Server != "edge-1" || claims.Bucket != "demo-bucket" || claims.Key != "dir/object.txt" {
		t.Fatalf("claims = %#v", claims)
	}
	wantCanonical := "GET\nedge-1\ndemo-bucket\ndir/object.txt\n" + formatUnixSeconds(expires) + "\nbytes=0-9\ntext/plain\nattachment; filename=object.txt"
	if got := CanonicalString(claims); got != wantCanonical {
		t.Fatalf("CanonicalString() = %q, want %q", got, wantCanonical)
	}
}

func TestVerifyURLMultiServerRejectsTamperAndExpectedServerMismatch(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	raw, err := SignURL(SignInput{Method: "GET", BaseURL: "https://files.example.com", Server: "edge-1", Bucket: "demo-bucket", Key: "object.txt", Secret: "secret", Expires: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}

	tampered := strings.Replace(raw, "/edge-1/", "/edge-2/", 1)
	_, err = VerifyURL(VerifyInput{Method: "GET", URL: tampered, Server: "edge-1", Secret: "secret", Now: now})
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifyURL() tamper error = %v, want ErrInvalidSignature", err)
	}

	_, err = VerifyURL(VerifyInput{Method: "GET", URL: raw, Server: "edge-2", Secret: "secret", Now: now})
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifyURL() server mismatch error = %v, want ErrInvalidSignature", err)
	}
}

func TestNoServerUsesLegacySingleServerBehavior(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	expires := now.Add(time.Minute)
	raw, err := SignURL(SignInput{Method: "GET", BaseURL: "https://files.example.com", Bucket: "demo-bucket", Key: "object.txt", Secret: "secret", Expires: expires})
	if err != nil {
		t.Fatalf("SignURL() error = %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse signed URL: %v", err)
	}
	if got, want := u.EscapedPath(), "/demo-bucket/object.txt"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}

	claims, err := VerifyURL(VerifyInput{Method: "GET", URL: raw, Secret: "secret", Now: now})
	if err != nil {
		t.Fatalf("VerifyURL() error = %v", err)
	}
	if claims.Server != "" {
		t.Fatalf("claims.Server = %q, want empty", claims.Server)
	}
	wantCanonical := "GET\ndemo-bucket\nobject.txt\n" + formatUnixSeconds(expires) + "\n\n\n"
	if got := CanonicalString(claims); got != wantCanonical {
		t.Fatalf("CanonicalString() = %q, want %q", got, wantCanonical)
	}
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	data, err := os.ReadFile("../../testdata/edge-signing-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []vector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}
