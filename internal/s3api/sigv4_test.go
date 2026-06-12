package s3api

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testRegion    = "us-east-1"
	testHost      = "bucket.example.test"
)

var (
	testSignedAt = time.Date(2026, 6, 12, 10, 30, 45, 0, time.UTC)
	testNow      = testSignedAt.Add(5 * time.Minute)
)

type testRawParam struct {
	key   string
	value string
}

func testVerifyOptions() VerifyOptions {
	return VerifyOptions{
		Credentials: Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey},
		Region:      testRegion,
		Now:         func() time.Time { return testNow },
	}
}

func testNewRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	u, err := url.Parse("https://" + testHost + target)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	r, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	r.URL = u
	r.Host = testHost
	return r
}

func testSignHeaderRequest(t *testing.T, method, target string, signedHeaders []string, headers map[string]string) *http.Request {
	t.Helper()
	return testSignHeaderRequestWith(t, method, target, signedHeaders, headers, testAccessKey, testSecretKey, testRegion)
}

func testSignHeaderRequestWith(t *testing.T, method, target string, signedHeaders []string, headers map[string]string, accessKey, secretKey, region string) *http.Request {
	t.Helper()
	r := testNewRequest(t, method, target)
	r.Header.Set("x-amz-date", testSignedAt.Format(sigv4DateTimeLayout))
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	if len(signedHeaders) == 0 {
		signedHeaders = []string{"host", "x-amz-date"}
	}
	payloadHash, err := sigv4HeaderPayloadHash(r, signedHeaders)
	if err != nil {
		t.Fatalf("payload hash: %v", err)
	}
	canonicalRequest, err := sigv4CanonicalRequest(r, signedHeaders, payloadHash, false)
	if err != nil {
		t.Fatalf("canonical request: %v", err)
	}
	date := testSignedAt.Format(sigv4DateLayout)
	scope := sigv4CredentialScopeString(date, region)
	stringToSign := sigv4StringToSign(testSignedAt.Format(sigv4DateTimeLayout), scope, canonicalRequest)
	signature := sigv4Signature(secretKey, date, region, stringToSign)
	r.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigv4Algorithm, accessKey, scope, strings.Join(signedHeaders, ";"), signature))
	return r
}

func testSignPresignedRequest(t *testing.T, method, target string, signedHeaders []string, headers map[string]string, extra []testRawParam, expires int) *http.Request {
	t.Helper()
	return testSignPresignedRequestWith(t, method, target, signedHeaders, headers, extra, expires, testAccessKey, testSecretKey, testRegion)
}

func testSignPresignedRequestWith(t *testing.T, method, target string, signedHeaders []string, headers map[string]string, extra []testRawParam, expires int, accessKey, secretKey, region string) *http.Request {
	t.Helper()
	if len(signedHeaders) == 0 {
		signedHeaders = []string{"host"}
	}
	r := testNewRequest(t, method, target)
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	date := testSignedAt.Format(sigv4DateLayout)
	scope := sigv4CredentialScopeString(date, region)
	params := []testRawParam{
		{key: "X-Amz-Algorithm", value: sigv4Algorithm},
		{key: "X-Amz-Credential", value: accessKey + "/" + scope},
		{key: "X-Amz-Date", value: testSignedAt.Format(sigv4DateTimeLayout)},
		{key: "X-Amz-Expires", value: fmt.Sprintf("%d", expires)},
		{key: "X-Amz-SignedHeaders", value: strings.Join(signedHeaders, ";")},
	}
	params = append(params, extra...)
	r.URL.RawQuery = testRawQuery(params)
	payloadHash, err := sigv4PresignedPayloadHash(r, signedHeaders)
	if err != nil {
		t.Fatalf("payload hash: %v", err)
	}
	canonicalRequest, err := sigv4CanonicalRequest(r, signedHeaders, payloadHash, true)
	if err != nil {
		t.Fatalf("canonical request: %v", err)
	}
	stringToSign := sigv4StringToSign(testSignedAt.Format(sigv4DateTimeLayout), scope, canonicalRequest)
	signature := sigv4Signature(secretKey, date, region, stringToSign)
	r.URL.RawQuery += "&X-Amz-Signature=" + signature
	return r
}

func testRawQuery(params []testRawParam) string {
	parts := make([]string, 0, len(params))
	for _, param := range params {
		parts = append(parts, sigv4URIEncode(param.key, false)+"="+sigv4URIEncode(param.value, false))
	}
	return strings.Join(parts, "&")
}

func TestIsSigV4Request(t *testing.T) {
	r := testNewRequest(t, http.MethodGet, "/object")
	r.Header.Set("Authorization", sigv4Algorithm+" Credential=x")
	if !IsSigV4Request(r) {
		t.Fatal("header SigV4 request was not detected")
	}

	r = testNewRequest(t, http.MethodGet, "/object?X-Amz-Algorithm="+sigv4Algorithm)
	if !IsSigV4Request(r) {
		t.Fatal("presigned SigV4 request was not detected")
	}

	cases := []struct {
		name          string
		authorization string
		rawQuery      string
	}{
		{name: "missing"},
		{name: "lowercase header", authorization: strings.ToLower(sigv4Algorithm) + " Credential=x"},
		{name: "other scheme", authorization: "Bearer token"},
		{name: "unsupported query", rawQuery: "X-Amz-Algorithm=AWS4-HMAC-SHA1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := testNewRequest(t, http.MethodGet, "/object")
			r.Header.Set("Authorization", tc.authorization)
			r.URL.RawQuery = tc.rawQuery
			if IsSigV4Request(r) {
				t.Fatal("unexpected SigV4 detection")
			}
		})
	}

	r = testNewRequest(t, http.MethodGet, "/object")
	r.Header.Set("Authorization", "  "+sigv4Algorithm+"Credential=x")
	if !IsSigV4Request(r) {
		t.Fatal("malformed Authorization beginning with algorithm should be detected")
	}
	_, err := VerifySigV4(r, testVerifyOptions())
	assertErrorContains(t, err, "malformed authorization")
}

func TestVerifySigV4ValidVectors(t *testing.T) {
	cases := []struct {
		name       string
		request    *http.Request
		presigned  bool
		payload    string
		headers    []string
		accessKey  string
		credential string
	}{
		{
			name:      "presigned GET",
			request:   testSignPresignedRequest(t, http.MethodGet, "/photos/puppy.jpg", nil, nil, nil, 300),
			presigned: true,
			payload:   sigv4UnsignedPayload,
			headers:   []string{"host"},
		},
		{
			name:      "presigned HEAD",
			request:   testSignPresignedRequest(t, http.MethodHead, "/photos/puppy.jpg", nil, nil, nil, 300),
			presigned: true,
			payload:   sigv4UnsignedPayload,
			headers:   []string{"host"},
		},
		{
			name:      "header GET",
			request:   testSignHeaderRequest(t, http.MethodGet, "/photos/puppy.jpg", nil, nil),
			presigned: false,
			payload:   sigv4EmptySHA256,
			headers:   []string{"host", "x-amz-date"},
		},
		{
			name:      "header HEAD",
			request:   testSignHeaderRequest(t, http.MethodHead, "/photos/puppy.jpg", nil, nil),
			presigned: false,
			payload:   sigv4EmptySHA256,
			headers:   []string{"host", "x-amz-date"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := VerifySigV4(tc.request, testVerifyOptions())
			if err != nil {
				t.Fatalf("VerifySigV4 returned error: %v", err)
			}
			if ctx.AccessKeyID != testAccessKey || ctx.CredentialDate != testSignedAt.Format(sigv4DateLayout) || ctx.Region != testRegion {
				t.Fatalf("unexpected auth context identity: %#v", ctx)
			}
			if !reflect.DeepEqual(ctx.SignedHeaders, tc.headers) {
				t.Fatalf("signed headers = %#v, want %#v", ctx.SignedHeaders, tc.headers)
			}
			if ctx.PayloadHash != tc.payload {
				t.Fatalf("payload hash = %q, want %q", ctx.PayloadHash, tc.payload)
			}
			if ctx.Presigned != tc.presigned {
				t.Fatalf("Presigned = %v, want %v", ctx.Presigned, tc.presigned)
			}
		})
	}
}

func TestVerifySigV4FailureCases(t *testing.T) {
	validHeader := func() *http.Request {
		return testSignHeaderRequest(t, http.MethodGet, "/photos/puppy.jpg?versionId=1", nil, nil)
	}
	validPresigned := func() *http.Request {
		return testSignPresignedRequest(t, http.MethodGet, "/photos/puppy.jpg?versionId=1", nil, nil, nil, 300)
	}

	cases := []struct {
		name    string
		request *http.Request
		opts    VerifyOptions
		want    string
	}{
		{
			name:    "wrong access key",
			request: testSignHeaderRequestWith(t, http.MethodGet, "/object", nil, nil, "OTHERKEY", testSecretKey, testRegion),
			opts:    testVerifyOptions(),
			want:    "access key",
		},
		{
			name:    "wrong secret causes signature mismatch",
			request: validHeader(),
			opts: VerifyOptions{
				Credentials: Credentials{AccessKeyID: testAccessKey, SecretAccessKey: "wrong"},
				Region:      testRegion,
				Now:         func() time.Time { return testNow },
			},
			want: "signature",
		},
		{
			name:    "wrong region",
			request: testSignHeaderRequestWith(t, http.MethodGet, "/object", nil, nil, testAccessKey, testSecretKey, "us-west-2"),
			opts:    testVerifyOptions(),
			want:    "region",
		},
		{
			name:    "expired presign",
			request: testSignPresignedRequest(t, http.MethodGet, "/object", nil, nil, nil, 10),
			opts: VerifyOptions{
				Credentials: Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey},
				Region:      testRegion,
				Now:         func() time.Time { return testSignedAt.Add(11 * time.Second) },
			},
			want: "expired",
		},
		{
			name:    "header outside skew",
			request: validHeader(),
			opts: VerifyOptions{
				Credentials: Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey},
				Region:      testRegion,
				Now:         func() time.Time { return testSignedAt.Add(16 * time.Minute) },
			},
			want: "skew",
		},
		{
			name: "missing signed header",
			request: func() *http.Request {
				r := testSignHeaderRequest(t, http.MethodGet, "/object", []string{"host", "x-amz-date", "x-amz-meta-test"}, map[string]string{"x-amz-meta-test": "before"})
				r.Header.Del("x-amz-meta-test")
				return r
			}(),
			opts: testVerifyOptions(),
			want: "missing signed header",
		},
		{
			name: "malformed credential scope",
			request: func() *http.Request {
				r := validHeader()
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/s3/aws4_request", "/s3", 1))
				return r
			}(),
			opts: testVerifyOptions(),
			want: "credential scope",
		},
		{
			name: "unsupported algorithm",
			request: func() *http.Request {
				r := testNewRequest(t, http.MethodGet, "/object")
				r.URL.RawQuery = "X-Amz-Algorithm=AWS4-HMAC-SHA1"
				return r
			}(),
			opts: testVerifyOptions(),
			want: "unsupported algorithm",
		},
		{
			name: "tampered path",
			request: func() *http.Request {
				r := validPresigned()
				r.URL.Path = "/photos/kitten.jpg"
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
		{
			name: "tampered query",
			request: func() *http.Request {
				r := validPresigned()
				r.URL.RawQuery += "&extra=value"
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
		{
			name: "tampered signed header value",
			request: func() *http.Request {
				r := testSignHeaderRequest(t, http.MethodGet, "/object", []string{"host", "x-amz-date", "x-amz-meta-test"}, map[string]string{"x-amz-meta-test": "before"})
				r.Header.Set("x-amz-meta-test", "after")
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
		{
			name: "uppercase signature",
			request: func() *http.Request {
				r := validHeader()
				r.Header.Set("Authorization", replaceSignature(r.Header.Get("Authorization"), strings.Repeat("A", 64)))
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
		{
			name: "non-hex signature",
			request: func() *http.Request {
				r := validPresigned()
				r.URL.RawQuery = replaceQueryValue(r.URL.RawQuery, "X-Amz-Signature", strings.Repeat("g", 64))
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
		{
			name: "malformed short signature",
			request: func() *http.Request {
				r := validHeader()
				r.Header.Set("Authorization", replaceSignature(r.Header.Get("Authorization"), "abc"))
				return r
			}(),
			opts: testVerifyOptions(),
			want: "signature",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := VerifySigV4(tc.request, tc.opts)
			assertErrorContains(t, err, tc.want)
		})
	}
}

func TestVerifySigV4PayloadHashRules(t *testing.T) {
	t.Run("header GET accepts signed unsigned payload", func(t *testing.T) {
		r := testSignHeaderRequest(t, http.MethodGet, "/object", []string{"host", "x-amz-content-sha256", "x-amz-date"}, map[string]string{"x-amz-content-sha256": sigv4UnsignedPayload})
		ctx, err := VerifySigV4(r, testVerifyOptions())
		if err != nil {
			t.Fatalf("VerifySigV4 returned error: %v", err)
		}
		if ctx.PayloadHash != sigv4UnsignedPayload {
			t.Fatalf("payload hash = %q", ctx.PayloadHash)
		}
	})

	t.Run("non-GET header requires content hash", func(t *testing.T) {
		r := testNewRequest(t, http.MethodPut, "/object")
		r.Header.Set("x-amz-date", testSignedAt.Format(sigv4DateTimeLayout))
		date := testSignedAt.Format(sigv4DateLayout)
		scope := sigv4CredentialScopeString(date, testRegion)
		r.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=host;x-amz-date, Signature=%s", sigv4Algorithm, testAccessKey, scope, strings.Repeat("0", 64)))
		_, err := VerifySigV4(r, testVerifyOptions())
		assertErrorContains(t, err, "payload hash")
	})

	t.Run("presigned PUT accepts query content hash", func(t *testing.T) {
		r := testSignPresignedRequest(t, http.MethodPut, "/object", nil, nil, []testRawParam{{key: "X-Amz-Content-Sha256", value: sigv4EmptySHA256}}, 300)
		ctx, err := VerifySigV4(r, testVerifyOptions())
		if err != nil {
			t.Fatalf("VerifySigV4 returned error: %v", err)
		}
		if ctx.PayloadHash != sigv4EmptySHA256 {
			t.Fatalf("payload hash = %q", ctx.PayloadHash)
		}
	})
}

func TestSigV4CanonicalQuery(t *testing.T) {
	raw := "z=b&a=2&a=1&space=a b&plus=a+b&pct=%2f%aa&X-Amz-Signature=deadbeef&X-Amz-SignatureX=keep&X-Amz-Meta=1&blank=&novalue"
	got := sigv4CanonicalQuery(raw, true)
	want := "X-Amz-Meta=1&X-Amz-SignatureX=keep&a=1&a=2&blank=&novalue=&pct=%2F%AA&plus=a%2Bb&space=a%20b&z=b"
	if got != want {
		t.Fatalf("canonical query = %q\nwant %q", got, want)
	}
}

func TestSigV4CanonicalURI(t *testing.T) {
	got := sigv4CanonicalURI("/a//b/./c/%2f/%aa/plus+space here/%zz")
	want := "/a//b/./c/%2F/%AA/plus%2Bspace%20here/%25zz"
	if got != want {
		t.Fatalf("canonical URI = %q\nwant %q", got, want)
	}
}

func replaceSignature(authorization, signature string) string {
	idx := strings.LastIndex(authorization, "Signature=")
	if idx < 0 {
		return authorization
	}
	return authorization[:idx+len("Signature=")] + signature
}

func replaceQueryValue(rawQuery, key, value string) string {
	prefix := key + "="
	parts := strings.Split(rawQuery, "&")
	for i, part := range parts {
		if strings.HasPrefix(part, prefix) {
			parts[i] = prefix + value
		}
	}
	return strings.Join(parts, "&")
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want substring %q", err.Error(), want)
	}
}
