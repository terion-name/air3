package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/ingestquic"
	"github.com/terion-name/air3/internal/ingestsmux"
	"github.com/terion-name/air3/internal/ingesttcp"
	"github.com/terion-name/air3/internal/pending"
	"github.com/terion-name/air3/internal/publicpath"
	"github.com/terion-name/air3/internal/s3fetch"
	"github.com/terion-name/air3/internal/signing"
	"github.com/terion-name/air3/internal/tickets"
	"github.com/terion-name/air3/internal/uploadsource"
)

type fakePublisher struct {
	mu       sync.Mutex
	tickets  []tickets.Ticket
	subjects []string
	err      error
	on       func(tickets.Ticket)
}

func (p *fakePublisher) PublishTicketTo(ctx context.Context, subject string, t tickets.Ticket) error {
	p.mu.Lock()
	p.tickets = append(p.tickets, t)
	p.subjects = append(p.subjects, subject)
	p.mu.Unlock()
	if p.on != nil {
		p.on(t)
	}
	return p.err
}

func (p *fakePublisher) snapshot() []tickets.Ticket {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]tickets.Ticket(nil), p.tickets...)
}

func (p *fakePublisher) subjectSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.subjects...)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tickets)
}

type fakeFetcher struct {
	mu       sync.Mutex
	requests []s3fetch.Request
	object   *s3fetch.Object
	err      error
}

func (f *fakeFetcher) Fetch(ctx context.Context, req s3fetch.Request) (*s3fetch.Object, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
	return f.object, f.err
}

func (f *fakeFetcher) snapshot() []s3fetch.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]s3fetch.Request(nil), f.requests...)
}

func testEdge(pub *fakePublisher, ttl time.Duration) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{})
	cfg := config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		NATS:           config.NATSConfig{Subject: "air3.tickets", SubjectTemplate: "air3.{server}"},
		Signing:        config.SigningConfig{Disabled: true},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: ttl, StreamTimeout: time.Minute},
	}
	edge := newEdgeServer(cfg, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	tokens := []string{"req-test", "ingest-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}
	return edge, reg
}

func signedMultiURL(t *testing.T, method, server, bucket, key string, now time.Time) string {
	t.Helper()
	signed, err := signing.SignURLForMode(signing.SignInput{
		Method:  method,
		BaseURL: "https://files.example",
		Server:  server,
		Bucket:  bucket,
		Key:     key,
		Expires: now.Add(time.Minute),
		Secret:  "secret",
	}, publicpath.ModeMulti)
	if err != nil {
		t.Fatalf("SignURLForMode() error = %v", err)
	}
	return signed
}

func signedDefaultBucketMultiURL(t *testing.T, method, server, bucket, key string, now time.Time) string {
	t.Helper()
	signed, err := signing.SignURLForModeWithOptions(signing.SignInput{
		Method:  method,
		BaseURL: "https://files.example",
		Server:  server,
		Bucket:  bucket,
		Key:     key,
		Expires: now.Add(time.Minute),
		Secret:  "secret",
	}, publicpath.ModeMulti, signing.SignOptions{DefaultBucketPath: true})
	if err != nil {
		t.Fatalf("SignURLForModeWithOptions() error = %v", err)
	}
	return signed
}

func signedDefaultBucketSingleURL(t *testing.T, method, bucket, key string, now time.Time) string {
	t.Helper()
	signed, err := signing.SignURLForModeWithOptions(signing.SignInput{
		Method:  method,
		BaseURL: "https://files.example",
		Bucket:  bucket,
		Key:     key,
		Expires: now.Add(time.Minute),
		Secret:  "secret",
	}, publicpath.ModeSingle, signing.SignOptions{DefaultBucketPath: true})
	if err != nil {
		t.Fatalf("SignURLForModeWithOptions() error = %v", err)
	}
	return signed
}

const (
	s3TestAccessKey = "AKIDEXAMPLE"
	s3TestSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	s3TestRegion    = "us-east-1"
	s3TestHost      = "files.example"
)

var (
	s3TestSignedAt = time.Date(2026, 6, 12, 10, 30, 45, 0, time.UTC)
	s3TestNow      = s3TestSignedAt.Add(5 * time.Minute)
)

type s3TestRawParam struct {
	key   string
	value string
}

func testS3Edge(pub *fakePublisher, cfg config.EdgeConfig, fetchers map[string]objectFetcher) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{Now: func() time.Time { return s3TestNow }})
	cfg.IngestURL = "https://edge.internal/_ingest"
	cfg.S3API = config.S3APIConfig{Enabled: true, Region: s3TestRegion, AccessKeyID: s3TestAccessKey, SecretAccessKey: s3TestSecretKey}
	cfg.NATS.Subject = "air3.tickets"
	if cfg.NATS.SubjectTemplate == "" {
		cfg.NATS.SubjectTemplate = "air3.{server}"
	}
	if cfg.Timeouts.PendingRequestTTL == 0 {
		cfg.Timeouts.PendingRequestTTL = time.Second
	}
	edge := newEdgeServer(cfg, reg, uploadsource.NewRegistry(uploadsource.Options{Now: func() time.Time { return s3TestNow }}), pub, nil, fetchers)
	tokens := []string{"req-s3", "ingest-s3-token", "upload-s3-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}
	edge.now = func() time.Time { return s3TestNow }
	return edge, reg
}

func s3SignHeaderRequest(t *testing.T, method, target string, headers map[string]string) *http.Request {
	t.Helper()
	r := s3NewRequest(t, method, target)
	r.Header.Set("x-amz-date", s3TestSignedAt.Format("20060102T150405Z"))
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	signedHeaders := []string{"host", "x-amz-date"}
	if r.Header.Get("x-amz-content-sha256") != "" {
		signedHeaders = []string{"host", "x-amz-content-sha256", "x-amz-date"}
	}
	payloadHash := s3HeaderPayloadHash(r, signedHeaders)
	canonicalRequest := s3CanonicalRequest(r, signedHeaders, payloadHash, false)
	date := s3TestSignedAt.Format("20060102")
	scope := s3CredentialScope(date, s3TestRegion)
	stringToSign := s3StringToSign(s3TestSignedAt.Format("20060102T150405Z"), scope, canonicalRequest)
	signature := s3Signature(s3TestSecretKey, date, s3TestRegion, stringToSign)
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s3TestAccessKey, scope, strings.Join(signedHeaders, ";"), signature))
	return r
}

func s3SignHeaderRequestWithBody(t *testing.T, method, target string, body io.ReadCloser, contentLength int64, headers map[string]string) *http.Request {
	t.Helper()
	r := s3NewRequestWithBody(t, method, target, body, contentLength)
	r.Header.Set("x-amz-date", s3TestSignedAt.Format("20060102T150405Z"))
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	signedHeaders := []string{"host", "x-amz-date"}
	if r.Header.Get("x-amz-content-sha256") != "" {
		signedHeaders = []string{"host", "x-amz-content-sha256", "x-amz-date"}
	}
	payloadHash := s3HeaderPayloadHash(r, signedHeaders)
	canonicalRequest := s3CanonicalRequest(r, signedHeaders, payloadHash, false)
	date := s3TestSignedAt.Format("20060102")
	scope := s3CredentialScope(date, s3TestRegion)
	stringToSign := s3StringToSign(s3TestSignedAt.Format("20060102T150405Z"), scope, canonicalRequest)
	signature := s3Signature(s3TestSecretKey, date, s3TestRegion, stringToSign)
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s3TestAccessKey, scope, strings.Join(signedHeaders, ";"), signature))
	return r
}

func s3SignPresignedRequest(t *testing.T, method, target string, extra []s3TestRawParam) *http.Request {
	t.Helper()
	r := s3NewRequest(t, method, target)
	date := s3TestSignedAt.Format("20060102")
	scope := s3CredentialScope(date, s3TestRegion)
	params := []s3TestRawParam{
		{key: "X-Amz-Algorithm", value: "AWS4-HMAC-SHA256"},
		{key: "X-Amz-Credential", value: s3TestAccessKey + "/" + scope},
		{key: "X-Amz-Date", value: s3TestSignedAt.Format("20060102T150405Z")},
		{key: "X-Amz-Expires", value: "900"},
		{key: "X-Amz-SignedHeaders", value: "host"},
	}
	params = append(params, extra...)
	r.URL.RawQuery = s3RawQuery(params)
	canonicalRequest := s3CanonicalRequest(r, []string{"host"}, "UNSIGNED-PAYLOAD", true)
	stringToSign := s3StringToSign(s3TestSignedAt.Format("20060102T150405Z"), scope, canonicalRequest)
	signature := s3Signature(s3TestSecretKey, date, s3TestRegion, stringToSign)
	r.URL.RawQuery += "&X-Amz-Signature=" + signature
	return r
}

func s3NewRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	u, err := url.Parse("https://" + s3TestHost + target)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	r := httptest.NewRequest(method, u.String(), nil)
	r.URL = u
	r.Host = s3TestHost
	return r
}

func s3NewRequestWithBody(t *testing.T, method, target string, body io.ReadCloser, contentLength int64) *http.Request {
	t.Helper()
	u, err := url.Parse("https://" + s3TestHost + target)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	r := httptest.NewRequest(method, u.String(), body)
	r.URL = u
	r.Host = s3TestHost
	r.ContentLength = contentLength
	if contentLength >= 0 {
		r.Header.Set("Content-Length", fmt.Sprintf("%d", contentLength))
	} else {
		r.Header.Del("Content-Length")
	}
	return r
}

func s3HeaderPayloadHash(r *http.Request, signedHeaders []string) string {
	for _, header := range signedHeaders {
		if header == "x-amz-content-sha256" {
			return r.Header.Get("x-amz-content-sha256")
		}
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	return "UNSIGNED-PAYLOAD"
}

func s3CanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, presigned bool) string {
	return strings.Join([]string{
		strings.ToUpper(r.Method),
		s3CanonicalURI(r.URL.EscapedPath()),
		s3CanonicalQuery(r.URL.RawQuery, presigned),
		s3CanonicalHeaders(r, signedHeaders),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
}

func s3CanonicalHeaders(r *http.Request, signedHeaders []string) string {
	var b strings.Builder
	for _, name := range signedHeaders {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(s3NormalizeHeaderValue(value))
		b.WriteByte('\n')
	}
	return b.String()
}

func s3CanonicalQuery(rawQuery string, presigned bool) string {
	if rawQuery == "" {
		return ""
	}
	type pair struct{ key, value string }
	pairs := []pair{}
	for _, part := range strings.Split(rawQuery, "&") {
		key, value, _ := strings.Cut(part, "=")
		if presigned {
			decodedKey, err := url.QueryUnescape(key)
			if err == nil && decodedKey == "X-Amz-Signature" {
				continue
			}
		}
		pairs = append(pairs, pair{key: s3URIEncode(key, false), value: s3URIEncode(value, false)})
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts = append(parts, pair.key+"="+pair.value)
	}
	return strings.Join(parts, "&")
}

func s3StringToSign(amzDate, scope, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(sum[:])}, "\n")
}

func s3CredentialScope(date, region string) string {
	return date + "/" + region + "/s3/aws4_request"
}

func s3Signature(secret, date, region, stringToSign string) string {
	dateKey := s3HMAC([]byte("AWS4"+secret), []byte(date))
	regionKey := s3HMAC(dateKey, []byte(region))
	serviceKey := s3HMAC(regionKey, []byte("s3"))
	signingKey := s3HMAC(serviceKey, []byte("aws4_request"))
	return hex.EncodeToString(s3HMAC(signingKey, []byte(stringToSign)))
}

func s3HMAC(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func s3RawQuery(params []s3TestRawParam) string {
	parts := make([]string, 0, len(params))
	for _, param := range params {
		parts = append(parts, s3URIEncode(param.key, false)+"="+s3URIEncode(param.value, false))
	}
	return strings.Join(parts, "&")
}

func s3CanonicalURI(path string) string {
	if path == "" {
		path = "/"
	}
	return s3URIEncode(path, true)
}

func s3URIEncode(value string, allowSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '%' && i+2 < len(value) && isS3Hex(value[i+1]) && isS3Hex(value[i+2]) {
			b.WriteByte('%')
			b.WriteByte(s3UpperHex(value[i+1]))
			b.WriteByte(s3UpperHex(value[i+2]))
			i += 2
			continue
		}
		if isS3Unreserved(c) || c == '/' && allowSlash {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte("0123456789ABCDEF"[c>>4])
		b.WriteByte("0123456789ABCDEF"[c&0x0f])
	}
	return b.String()
}

func s3NormalizeHeaderValue(value string) string {
	value = strings.TrimSpace(value)
	return strings.Join(strings.Fields(value), " ")
}

func isS3Unreserved(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~'
}

func isS3Hex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func s3UpperHex(c byte) byte {
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 'A'
	}
	return c
}

func testMultiEdge(pub *fakePublisher, cfg config.EdgeConfig, fetchers map[string]objectFetcher) (*edgeServer, *pending.Registry) {
	reg := pending.NewRegistry(pending.Options{})
	cfg.IngestURL = "https://edge.internal/_ingest"
	cfg.MultiServer = true
	if cfg.NATS.SubjectTemplate == "" {
		cfg.NATS.SubjectTemplate = "air3.{server}"
	}
	if cfg.Timeouts.PendingRequestTTL == 0 {
		cfg.Timeouts.PendingRequestTTL = time.Second
	}
	edge := newEdgeServer(cfg, reg, uploadsource.NewRegistry(uploadsource.Options{Now: func() time.Time { return s3TestNow }}), pub, nil, fetchers)
	tokens := []string{"req-multi", "ingest-multi-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}
	return edge, reg
}

func TestS3APIDisabledPreservesAir3PathForSigV4ShapedRequest(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testEdge(pub, time.Second)
	req := s3SignPresignedRequest(t, http.MethodGet, "/demo-bucket/file.txt", nil)

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Bucket != "demo-bucket" || published[0].Key != "file.txt" || published[0].Operation != "" {
		t.Fatalf("ticket = %#v, want legacy Air3 object ticket", published[0])
	}
}

func TestS3APIPresignedGETPublishesObjectTicketWithoutAuthMaterial(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}}, nil)
	req := s3SignPresignedRequest(t, http.MethodGet, "/demo-bucket/photos/cat.jpg", nil)
	req.Header.Set("Range", "bytes=0-9")

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	ticket := published[0]
	if ticket.Bucket != "demo-bucket" || ticket.Key != "photos/cat.jpg" || ticket.Operation != tickets.OperationGetObject || ticket.Range != "bytes=0-9" || ticket.List != nil {
		t.Fatalf("ticket = %#v, want GetObject demo-bucket/photos/cat.jpg with range and no list", ticket)
	}
	encoded, err := tickets.Marshal(ticket, s3TestNow)
	if err != nil {
		t.Fatalf("Marshal(ticket) error = %v", err)
	}
	for _, secret := range []string{"Authorization", "X-Amz", s3TestAccessKey, s3TestSecretKey, "Signature"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("ticket leaked auth material %q in %s", secret, encoded)
		}
	}
}

func TestS3APIHeaderHEADPublishesObjectTicketAndSuppressesBody(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}}, nil)
	req := s3SignHeaderRequest(t, http.MethodHead, "/demo-bucket/photos/cat.jpg", nil)

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Operation != tickets.OperationHeadObject || published[0].Bucket != "demo-bucket" || published[0].Key != "photos/cat.jpg" {
		t.Fatalf("ticket = %#v, want HeadObject demo-bucket/photos/cat.jpg", published[0])
	}
	if resp.Body.Len() != 0 {
		t.Fatalf("HEAD response body = %q, want empty", resp.Body.String())
	}
}

func TestS3APIAuthAndClassificationFailuresHaveXMLNoSideEffects(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.EdgeConfig
		req  func(t *testing.T) *http.Request
		code string
	}{
		{
			name: "invalid signature",
			cfg:  config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}},
			req: func(t *testing.T) *http.Request {
				r := s3SignPresignedRequest(t, http.MethodGet, "/demo-bucket/file.txt", nil)
				r.URL.RawQuery = strings.TrimSuffix(r.URL.RawQuery, "0") + "1"
				return r
			},
			code: "SignatureDoesNotMatch",
		},
		{
			name: "malformed list query",
			cfg:  config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}},
			req: func(t *testing.T) *http.Request {
				return s3SignPresignedRequest(t, http.MethodGet, "/demo-bucket", []s3TestRawParam{{key: "list-type", value: "2"}, {key: "max-keys", value: "abc"}})
			},
			code: "InvalidRequest",
		},
		{
			name: "single default short object path",
			cfg:  config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, DefaultBucket: "demo-bucket"},
			req: func(t *testing.T) *http.Request {
				return s3SignPresignedRequest(t, http.MethodGet, "/lonely-key", nil)
			},
			code: "InvalidRequest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			edge, reg := testS3Edge(pub, tt.cfg, nil)
			edge.newToken = func() (string, error) { t.Fatal("failure allocated a connector token"); return "", nil }

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, tt.req(t))

			if got := resp.Result().Header.Get("Content-Type"); got != "application/xml" {
				t.Fatalf("Content-Type = %q, want application/xml", got)
			}
			if !strings.Contains(resp.Body.String(), "<Code>"+tt.code+"</Code>") {
				t.Fatalf("body = %q, want S3 code %s", resp.Body.String(), tt.code)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("registered pending request: %v", err)
			}
		})
	}
}

func TestS3APIMutationsDisabledBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		direct   bool
		body     string
		headers  map[string]string
		wantCode int
	}{
		{name: "routed put", method: http.MethodPut, body: "hello", headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, wantCode: http.StatusMethodNotAllowed},
		{name: "routed delete", method: http.MethodDelete, headers: map[string]string{"x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}, wantCode: http.StatusMethodNotAllowed},
		{name: "direct put", method: http.MethodPut, direct: true, body: "hello", headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, wantCode: http.StatusMethodNotAllowed},
		{name: "direct delete", method: http.MethodDelete, direct: true, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, wantCode: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			fetcher := &fakeFetcher{object: &s3fetch.Object{}}
			cfg := config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}}
			fetchers := map[string]objectFetcher(nil)
			target := "/demo-bucket/file.txt"
			if tt.direct {
				cfg.MultiServer = true
				cfg.DirectServers = map[string]config.S3Config{"blue": {AllowedBuckets: []string{"demo-bucket"}}}
				fetchers = map[string]objectFetcher{"blue": fetcher}
				target = "/blue/demo-bucket/file.txt"
			}
			edge, reg := testS3Edge(pub, cfg, fetchers)
			edge.newToken = func() (string, error) { t.Fatal("disabled mutation allocated a connector token"); return "", nil }
			var body io.ReadCloser = http.NoBody
			if tt.body != "" {
				body = io.NopCloser(strings.NewReader(tt.body))
			}
			req := s3SignHeaderRequestWithBody(t, tt.method, target, body, int64(len(tt.body)), tt.headers)

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != tt.wantCode {
				t.Fatalf("status = %d, want %d; body=%q", got, tt.wantCode, resp.Body.String())
			}
			if got := resp.Result().Header.Get("Allow"); got != "GET, HEAD" {
				t.Fatalf("Allow = %q, want GET, HEAD", got)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if got := len(fetcher.snapshot()); got != 0 {
				t.Fatalf("fetch calls = %d, want 0", got)
			}
			if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("registered pending request: %v", err)
			}
			if _, err := edge.uploadSources.Claim("req-s3", "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
				t.Fatalf("registered upload source: %v", err)
			}
		})
	}
}

func TestS3APIUnsupportedMethodAllowHeaderReflectsMutationGate(t *testing.T) {
	const emptyPayloadSHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	tests := []struct {
		name             string
		mutationsEnabled bool
		wantAllow        string
	}{
		{name: "mutations disabled", wantAllow: "GET, HEAD"},
		{name: "mutations enabled", mutationsEnabled: true, wantAllow: "GET, HEAD, PUT, POST, DELETE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: tt.mutationsEnabled}, nil)
			req := s3SignHeaderRequestWithBody(t, http.MethodPost, "/demo-bucket/file.txt", http.NoBody, 0, map[string]string{"x-amz-content-sha256": emptyPayloadSHA})

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d; body=%q", got, http.StatusMethodNotAllowed, resp.Body.String())
			}
			if got := resp.Result().Header.Get("Allow"); got != tt.wantAllow {
				t.Fatalf("Allow = %q, want %q", got, tt.wantAllow)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
		})
	}
}

func TestS3APIMutationPolicyRejectsBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		body    string
		length  int64
		headers map[string]string
		mutate  func(*http.Request)
	}{
		{name: "signed streaming put", method: http.MethodPut, body: "hello", length: 5, headers: map[string]string{"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}},
		{name: "streaming put without decoded length", method: http.MethodPut, body: "hello", length: 5, headers: map[string]string{"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"}},
		{name: "aws chunked put", method: http.MethodPut, body: "hello", length: 5, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Content-Encoding": "aws-chunked"}},
		{name: "unknown length put", method: http.MethodPut, body: "hello", length: -1, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}},
		{name: "delete body", method: http.MethodDelete, body: "x", length: 1, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}},
		{name: "delete chunked marker", method: http.MethodDelete, length: 0, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, mutate: func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }},
		{name: "delete streaming marker", method: http.MethodDelete, length: 0, headers: map[string]string{"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"}},
		{name: "range put", method: http.MethodPut, body: "hello", length: 5, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Range": "bytes=0-1"}},
		{name: "range delete", method: http.MethodDelete, length: 0, headers: map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Range": "bytes=0-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
			edge.newToken = func() (string, error) { t.Fatal("invalid mutation allocated a connector token"); return "", nil }
			var body io.ReadCloser = http.NoBody
			if tt.body != "" {
				body = io.NopCloser(strings.NewReader(tt.body))
			}
			req := s3SignHeaderRequestWithBody(t, tt.method, "/demo-bucket/file.txt", body, tt.length, tt.headers)
			if tt.mutate != nil {
				tt.mutate(req)
			}

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%q", got, http.StatusBadRequest, resp.Body.String())
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("registered pending request: %v", err)
			}
			if _, err := edge.uploadSources.Claim("req-s3", "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
				t.Fatalf("registered upload source: %v", err)
			}
		})
	}
}

func TestS3APISingleServerListPublishesTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}}, nil)
	req := s3SignPresignedRequest(t, http.MethodGet, "/demo-bucket", []s3TestRawParam{{key: "list-type", value: "2"}, {key: "prefix", value: "photos"}, {key: "delimiter", value: "/"}, {key: "max-keys", value: "10"}, {key: "start-after", value: "old"}, {key: "continuation-token", value: "token"}, {key: "encoding-type", value: "url"}, {key: "fetch-owner", value: "true"}})
	req.Header.Set("Range", "bytes=0-9")

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	ticket := published[0]
	if ticket.Operation != tickets.OperationListObjectsV2 || ticket.Key != "" || ticket.Range != "" || ticket.List == nil {
		t.Fatalf("ticket = %#v, want ListObjectsV2 with empty key/range", ticket)
	}
	if ticket.List.Prefix != "photos" || ticket.List.Delimiter != "/" || ticket.List.MaxKeys != 10 || ticket.List.StartAfter != "old" || ticket.List.ContinuationToken != "token" || ticket.List.EncodingType != "url" || !ticket.List.FetchOwner {
		t.Fatalf("list = %#v, want requested list fields", ticket.List)
	}
	if ticket.List.Rewrite.Bucket != "demo-bucket" || ticket.List.Rewrite.Prefix != "photos" || ticket.List.Rewrite.KeyPrefix != "" {
		t.Fatalf("rewrite = %#v, want public bucket demo-bucket prefix photos", ticket.List.Rewrite)
	}
}

func TestS3APISingleDefaultBucketShortObjectPathRejected(t *testing.T) {
	pub := &fakePublisher{}
	edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, DefaultBucket: "demo-bucket"}, nil)
	edge.newToken = func() (string, error) { t.Fatal("short path allocated a connector token"); return "", nil }

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, s3SignPresignedRequest(t, http.MethodGet, "/file.txt", nil))

	if got := resp.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", got, http.StatusBadRequest)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestS3APIMultiStandardListPrefixRewrite(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testS3Edge(pub, config.EdgeConfig{MultiServer: true, AllowedBuckets: []string{"photos-bucket"}}, nil)
	req := s3SignPresignedRequest(t, http.MethodGet, "/blue", []s3TestRawParam{{key: "list-type", value: "2"}, {key: "prefix", value: "photos-bucket/2024"}})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	list := published[0].List
	if published[0].Server != "blue" || published[0].Bucket != "photos-bucket" || list == nil || list.Prefix != "2024" || list.Rewrite.Bucket != "blue" || list.Rewrite.Prefix != "photos-bucket/2024" || list.Rewrite.KeyPrefix != "photos-bucket" {
		t.Fatalf("ticket = %#v, want rewritten multi-standard list", published[0])
	}
}

func TestS3APIMultiStandardListMissingBackendBucketPrefixRejected(t *testing.T) {
	pub := &fakePublisher{}
	edge, _ := testS3Edge(pub, config.EdgeConfig{MultiServer: true, AllowedBuckets: []string{"photos-bucket"}}, nil)
	edge.newToken = func() (string, error) { t.Fatal("bad list allocated a connector token"); return "", nil }
	req := s3SignPresignedRequest(t, http.MethodGet, "/blue", []s3TestRawParam{{key: "list-type", value: "2"}})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	if got := resp.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", got, http.StatusBadRequest)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestS3APIMultiDefaultListMapsToConfiguredBackendBucket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testS3Edge(pub, config.EdgeConfig{MultiServer: true, AllowedBuckets: []string{"default-bucket"}, ServerDefaultBuckets: map[string]string{"BLUE": "default-bucket"}}, nil)
	req := s3SignPresignedRequest(t, http.MethodGet, "/blue", []s3TestRawParam{{key: "list-type", value: "2"}, {key: "prefix", value: "2024"}})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	list := published[0].List
	if published[0].Bucket != "default-bucket" || list == nil || list.Prefix != "2024" || list.Rewrite.Bucket != "blue" || list.Rewrite.Prefix != "2024" || list.Rewrite.KeyPrefix != "" {
		t.Fatalf("ticket = %#v, want multi-default list mapped to default-bucket", published[0])
	}
}

func TestS3APIDirectAliasListBypassesNATSAndStreamsXML(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "application/xml", ContentLength: 41, Body: io.NopCloser(strings.NewReader("<ListBucketResult></ListBucketResult>"))}}
	edge, reg := testS3Edge(pub, config.EdgeConfig{
		MultiServer:           true,
		AllowedBuckets:        []string{"other-bucket"},
		ServerDefaultBuckets:  map[string]string{"BLUE": "default-bucket"},
		DirectServers:         map[string]config.S3Config{"blue": {AllowedBuckets: []string{"default-bucket"}}},
		StreamCopyBufferBytes: 1024,
	}, map[string]objectFetcher{"blue": fetcher})
	edge.newToken = func() (string, error) { t.Fatal("direct list allocated a connector token"); return "", nil }
	req := s3SignPresignedRequest(t, http.MethodGet, "/blue", []s3TestRawParam{{key: "list-type", value: "2"}, {key: "prefix", value: "2024"}})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)

	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
	}
	if resp.Body.String() != "<ListBucketResult></ListBucketResult>" {
		t.Fatalf("body = %q, want direct XML", resp.Body.String())
	}
	requests := fetcher.snapshot()
	if len(requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", requests)
	}
	if requests[0].Operation != tickets.OperationListObjectsV2 || requests[0].Bucket != "default-bucket" || requests[0].Key != "" || requests[0].Range != "" || requests[0].List == nil || requests[0].List.Prefix != "2024" {
		t.Fatalf("fetch request = %#v, want direct ListObjectsV2 for default-bucket prefix 2024", requests[0])
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("direct list registered pending request: %v", err)
	}
}

func TestS3APIDirectAliasMutationsBypassNATSAndUploadSource(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{ETag: "\"put-etag\""}}
	edge, reg := testS3Edge(pub, config.EdgeConfig{
		MultiServer:      true,
		MutationsEnabled: true,
		DirectServers:    map[string]config.S3Config{"blue": {AllowedBuckets: []string{"demo-bucket"}}},
	}, map[string]objectFetcher{"blue": fetcher})
	edge.newToken = func() (string, error) { t.Fatal("direct mutation allocated a connector token"); return "", nil }
	body := io.NopCloser(strings.NewReader("direct body"))
	putReq := s3SignHeaderRequestWithBody(t, http.MethodPut, "/blue/demo-bucket/file.txt", body, int64(len("direct body")), map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Content-Type": "text/plain"})

	putResp := httptest.NewRecorder()
	edge.ServeHTTP(putResp, putReq)

	if got := putResp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d; body=%q", got, http.StatusOK, putResp.Body.String())
	}
	if got := putResp.Result().Header.Get("ETag"); got != "\"put-etag\"" {
		t.Fatalf("PUT ETag = %q, want put-etag", got)
	}
	requests := fetcher.snapshot()
	if len(requests) != 1 {
		t.Fatalf("fetch requests after PUT = %#v, want one", requests)
	}
	if requests[0].Operation != tickets.OperationPutObject || requests[0].Body != putReq.Body || requests[0].ContentLength == nil || *requests[0].ContentLength != int64(len("direct body")) || requests[0].ContentType != "text/plain" {
		t.Fatalf("PUT fetch request = %#v, want original body and content metadata", requests[0])
	}

	deleteReq := s3SignHeaderRequestWithBody(t, http.MethodDelete, "/blue/demo-bucket/file.txt", http.NoBody, 0, map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})
	deleteResp := httptest.NewRecorder()
	edge.ServeHTTP(deleteResp, deleteReq)
	if got := deleteResp.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d; body=%q", got, http.StatusNoContent, deleteResp.Body.String())
	}
	if deleteResp.Body.Len() != 0 {
		t.Fatalf("DELETE body = %q, want empty", deleteResp.Body.String())
	}
	requests = fetcher.snapshot()
	if len(requests) != 2 || requests[1].Operation != tickets.OperationDeleteObject || requests[1].Body != nil || requests[1].ContentLength != nil || requests[1].ContentType != "" {
		t.Fatalf("DELETE fetch request = %#v, want no body/content fields", requests)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("direct mutation registered pending request: %v", err)
	}
	if _, err := edge.uploadSources.Claim("req-s3", "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
		t.Fatalf("direct mutation registered upload source: %v", err)
	}
}

func TestS3APIRoutedMutationsPublishTicketsAndCleanup(t *testing.T) {
	t.Run("put", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		var uploaded string
		var uploadedLength *int64
		var uploadedType string
		pub.on = func(ticket tickets.Ticket) {
			claim, err := edge.uploadSources.Claim(ticket.RequestID, ticket.UploadToken)
			if err != nil {
				t.Errorf("Claim() error = %v", err)
				return
			}
			uploadedLength = claim.ContentLength()
			uploadedType = claim.ContentType()
			body, err := io.ReadAll(claim)
			if err != nil {
				t.Errorf("ReadAll(claim) error = %v", err)
			}
			uploaded = string(body)
			if err := claim.Close(); err != nil {
				t.Errorf("claim.Close() error = %v", err)
			}
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ETag: "\"stored\""})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_ = stream.Close()
		}
		req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/file.txt", io.NopCloser(strings.NewReader("routed body")), int64(len("routed body")), map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Content-Type": "text/plain"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusOK, resp.Body.String())
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		ticket := published[0]
		if ticket.Operation != tickets.OperationPutObject || ticket.UploadSourceURL == "" || ticket.UploadToken != "upload-s3-token" || ticket.ContentLength == nil || *ticket.ContentLength != int64(len("routed body")) || ticket.ContentType != "text/plain" {
			t.Fatalf("PUT ticket = %#v, want upload envelope", ticket)
		}
		encoded, err := tickets.Marshal(ticket, s3TestNow)
		if err != nil {
			t.Fatalf("Marshal(PUT ticket) error = %v", err)
		}
		for _, secret := range []string{"Authorization", "X-Amz", s3TestAccessKey, s3TestSecretKey, "routed body"} {
			if strings.Contains(string(encoded), secret) {
				t.Fatalf("ticket leaked %q in %s", secret, encoded)
			}
		}
		if uploaded != "routed body" || uploadedLength == nil || *uploadedLength != int64(len("routed body")) || uploadedType != "text/plain" {
			t.Fatalf("upload source = body %q length %v type %q", uploaded, uploadedLength, uploadedType)
		}
		if _, err := edge.uploadSources.Claim(ticket.RequestID, ticket.UploadToken); !errors.Is(err, uploadsource.ErrNotFound) {
			t.Fatalf("upload source remained after completion: %v", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		pub.on = func(ticket tickets.Ticket) {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusNoContent})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_ = stream.Close()
		}
		req := s3SignHeaderRequestWithBody(t, http.MethodDelete, "/demo-bucket/file.txt", http.NoBody, 0, map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD"})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusNoContent, resp.Body.String())
		}
		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		if published[0].Operation != tickets.OperationDeleteObject || published[0].UploadSourceURL != "" || published[0].UploadToken != "" || published[0].ContentLength != nil || published[0].ContentType != "" {
			t.Fatalf("DELETE ticket = %#v, want no upload envelope", published[0])
		}
		if _, err := edge.uploadSources.Claim(published[0].RequestID, "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
			t.Fatalf("DELETE registered upload source: %v", err)
		}
	})
}

func TestS3APIRoutedPUTCleanupOnPublishFailureAndCancellation(t *testing.T) {
	bodyHeaders := map[string]string{"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Content-Type": "text/plain"}
	t.Run("publish failure", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("nats down")}
		edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/file.txt", io.NopCloser(strings.NewReader("body")), 4, bodyHeaders)

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d; body=%q", got, http.StatusServiceUnavailable, resp.Body.String())
		}
		if _, err := edge.uploadSources.Claim("req-s3", "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
			t.Fatalf("upload source remained after publish failure: %v", err)
		}
		if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("pending request remained after publish failure: %v", err)
		}
	})

	t.Run("client cancellation", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, _ := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: []string{"demo-bucket"}, MutationsEnabled: true}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		pub.on = func(tickets.Ticket) { cancel() }
		req := s3SignHeaderRequestWithBody(t, http.MethodPut, "/demo-bucket/file.txt", io.NopCloser(strings.NewReader("body")), 4, bodyHeaders).WithContext(ctx)

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if _, err := edge.uploadSources.Claim("req-s3", "upload-s3-token"); !errors.Is(err, uploadsource.ErrNotFound) {
			t.Fatalf("upload source remained after cancellation: %v", err)
		}
	})
}

func TestPrivateIngestMuxMountsUploadSourceAndPublicDoesNot(t *testing.T) {
	pendingReg := pending.NewRegistry(pending.Options{Now: func() time.Time { return s3TestNow }})
	uploadReg := uploadsource.NewRegistry(uploadsource.Options{Now: func() time.Time { return s3TestNow }})
	ingestHandler, err := ingest.NewHandler(ingest.Options{Registry: pendingReg})
	if err != nil {
		t.Fatalf("New ingest handler: %v", err)
	}
	uploadHandler, err := uploadsource.NewHandler(uploadsource.HandlerOptions{Registry: uploadReg})
	if err != nil {
		t.Fatalf("New upload handler: %v", err)
	}
	private := newPrivateIngestHandler(ingestHandler, uploadHandler)
	length := int64(len("upload-body"))
	if err := uploadReg.Register(uploadsource.Source{RequestID: "upload-req", Token: "upload-token", Body: io.NopCloser(strings.NewReader("upload-body")), ContentLength: &length, ContentType: "text/plain", Deadline: s3TestNow.Add(time.Minute), Context: context.Background()}); err != nil {
		t.Fatalf("Register upload source: %v", err)
	}
	uploadReq := httptest.NewRequest(http.MethodGet, uploadsource.PathPrefix+"upload-req", nil)
	uploadReq.Header.Set(uploadsource.TokenHeader, "upload-token")
	uploadResp := httptest.NewRecorder()
	private.ServeHTTP(uploadResp, uploadReq)
	if got := uploadResp.Result().StatusCode; got != http.StatusOK || uploadResp.Body.String() != "upload-body" {
		t.Fatalf("private upload response status=%d body=%q", got, uploadResp.Body.String())
	}

	publicSinkResp := httptest.NewRecorder()
	if err := pendingReg.Register(pending.Request{ID: "ingest-req", Deadline: s3TestNow.Add(time.Minute), IngestToken: "ingest-token", Method: http.MethodGet, Operation: tickets.OperationGetObject, Bucket: "demo-bucket", Key: "file.txt"}, newResponseSink(publicSinkResp, http.MethodGet, context.Background())); err != nil {
		t.Fatalf("Register pending request: %v", err)
	}
	ingestReq := httptest.NewRequest(http.MethodPost, ingest.PathPrefix+"ingest-req", strings.NewReader("ingest-body"))
	ingestReq.Header.Set(ingest.TokenHeader, "ingest-token")
	ingestReq.Header.Set(ingest.StatusCodeHeader, "200")
	ingestResp := httptest.NewRecorder()
	private.ServeHTTP(ingestResp, ingestReq)
	if got := ingestResp.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("private ingest response status=%d body=%q", got, ingestResp.Body.String())
	}

	pub := &fakePublisher{}
	edge, _ := testEdge(pub, time.Second)
	publicResp := httptest.NewRecorder()
	edge.ServeHTTP(publicResp, httptest.NewRequest(http.MethodGet, uploadsource.PathPrefix+"upload-req", nil))
	if got := publicResp.Result().StatusCode; got == http.StatusOK && publicResp.Body.String() == "upload-body" {
		t.Fatalf("public edge exposed upload source route")
	}
}

func TestS3APIHeadBucketIsEdgeOnly(t *testing.T) {
	for _, tt := range []struct {
		name       string
		allowlist  []string
		wantStatus int
	}{
		{name: "success", allowlist: []string{"demo-bucket"}, wantStatus: http.StatusOK},
		{name: "failure", allowlist: []string{"other-bucket"}, wantStatus: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
			edge, reg := testS3Edge(pub, config.EdgeConfig{AllowedBuckets: tt.allowlist}, map[string]objectFetcher{"unused": fetcher})
			edge.newToken = func() (string, error) { t.Fatal("HeadBucket allocated a connector token"); return "", nil }
			req := s3SignHeaderRequest(t, http.MethodHead, "/demo-bucket", nil)

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != tt.wantStatus {
				t.Fatalf("status = %d, want %d", got, tt.wantStatus)
			}
			if resp.Body.Len() != 0 {
				t.Fatalf("HeadBucket body = %q, want empty", resp.Body.String())
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if got := len(fetcher.snapshot()); got != 0 {
				t.Fatalf("fetch calls = %d, want 0", got)
			}
			if _, err := reg.StartIngest("req-s3", "ingest-s3-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("HeadBucket registered pending request: %v", err)
			}
		})
	}
}

func TestTCPIngestListenerDisabledForHTTPTransports(t *testing.T) {
	for _, transport := range []config.IngestTransport{config.IngestTransportHTTP, config.IngestTransportHTTP1, config.IngestTransportHTTP2, config.IngestTransportHTTP3} {
		t.Run(string(transport), func(t *testing.T) {
			tcpIngest, err := newTCPIngestListener(config.EdgeConfig{
				IngestTransport:     transport,
				IngestTCPListenAddr: "not a valid listen address",
			}, pending.NewRegistry(pending.Options{}), nil)
			if err != nil {
				t.Fatalf("newTCPIngestListener() error = %v, want nil", err)
			}
			if tcpIngest != nil {
				t.Fatalf("newTCPIngestListener() = %#v, want nil for %s transport", tcpIngest, transport)
			}
		})
	}
}

func TestTCPIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newTCPIngestListener(config.EdgeConfig{
		IngestTransport:     config.IngestTransportTCP,
		IngestTCPListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newTCPIngestListener() error = %v, want TLS config error", err)
	}
}

func TestTCPIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	tcpIngest, err := newTCPIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportTCP,
		IngestTCPListenAddr:   "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newTCPIngestListener() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- tcpIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = tcpIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("tcpIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("tcpIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingesttcp.DialAndSend(context.Background(), "tcp", tcpIngest.listener.Addr().String(), clientTLS, ingesttcp.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "8"},
				Body:        strings.NewReader("tcp-body"),
				BodyLength:  8,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:           "https://edge.internal/_ingest",
		IngestTransport:     config.IngestTransportTCP,
		IngestTCPListenAddr: tcpIngest.listener.Addr().String(),
		AllowedBuckets:      []string{"demo-bucket"},
		Signing:             config.SigningConfig{Disabled: true},
		Timeouts:            config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	tokens := []string{"req-tcp", "ingest-tcp-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		select {
		case err := <-uploadDone:
			t.Fatalf("status = %d, want %d; upload error = %v", got, http.StatusOK, err)
		default:
			t.Fatalf("status = %d, want %d", got, http.StatusOK)
		}
	}
	if body := resp.Body.String(); body != "tcp-body" {
		t.Fatalf("body = %q, want tcp-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("TCP ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TCP ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-tcp" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, tcpIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains TCP listener address", published[0].IngestURL)
	}
}

func TestNonHTTPIngestListenerDisabledForHTTPTransports(t *testing.T) {
	for _, transport := range []config.IngestTransport{config.IngestTransportHTTP, config.IngestTransportHTTP1, config.IngestTransportHTTP2, config.IngestTransportHTTP3} {
		t.Run(string(transport), func(t *testing.T) {
			listener, err := newNonHTTPIngestListener(config.EdgeConfig{
				IngestTransport:     transport,
				IngestTCPListenAddr: "not a valid listen address",
			}, pending.NewRegistry(pending.Options{}), nil)
			if err != nil {
				t.Fatalf("newNonHTTPIngestListener() error = %v, want nil", err)
			}
			if listener != nil {
				t.Fatalf("newNonHTTPIngestListener() = %#v, want nil for %s transport", listener, transport)
			}
		})
	}
}

func TestSMUXIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newSMUXIngestListener(config.EdgeConfig{
		IngestTransport:     config.IngestTransportSMUX,
		IngestTCPListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newSMUXIngestListener() error = %v, want TLS config error", err)
	}
}

func TestSMUXIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	smuxIngest, err := newSMUXIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportSMUX,
		IngestTCPListenAddr:   "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newSMUXIngestListener() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- smuxIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = smuxIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("smuxIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("smuxIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingestsmux.DialAndSend(context.Background(), "tcp", smuxIngest.listener.Addr().String(), clientTLS, ingestsmux.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "9"},
				Body:        strings.NewReader("smux-body"),
				BodyLength:  9,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:           "https://edge.internal/_ingest",
		IngestTransport:     config.IngestTransportSMUX,
		IngestTCPListenAddr: smuxIngest.listener.Addr().String(),
		AllowedBuckets:      []string{"demo-bucket"},
		Signing:             config.SigningConfig{Disabled: true},
		Timeouts:            config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	tokens := []string{"req-smux", "ingest-smux-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		select {
		case err := <-uploadDone:
			t.Fatalf("status = %d, want %d; upload error = %v", got, http.StatusOK, err)
		default:
			t.Fatalf("status = %d, want %d", got, http.StatusOK)
		}
	}
	if body := resp.Body.String(); body != "smux-body" {
		t.Fatalf("body = %q, want smux-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("SMUX ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SMUX ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-smux" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, smuxIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains SMUX listener address", published[0].IngestURL)
	}
}

func TestHTTP3IngestServerSelectedAndNonHTTPDisabled(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	ingestServer := newIngestHTTPServer(config.EdgeConfig{IngestTransport: config.IngestTransportHTTP3, IngestListenAddr: "127.0.0.1:0"}, http.NotFoundHandler(), tlsCfg)
	http3Server, ok := ingestServer.(*http3IngestServer)
	if !ok {
		t.Fatalf("newIngestHTTPServer() = %T, want *http3IngestServer", ingestServer)
	}
	if http3Server.server.TLSConfig != tlsCfg {
		t.Fatalf("HTTP/3 ingest TLS config = %#v, want provided config", http3Server.server.TLSConfig)
	}

	listener, err := newNonHTTPIngestListener(config.EdgeConfig{
		IngestTransport:      config.IngestTransportHTTP3,
		IngestTCPListenAddr:  "not a valid listen address",
		IngestQUICListenAddr: "not a valid listen address",
	}, pending.NewRegistry(pending.Options{}), tlsCfg)
	if err != nil {
		t.Fatalf("newNonHTTPIngestListener() error = %v, want nil", err)
	}
	if listener != nil {
		t.Fatalf("newNonHTTPIngestListener() = %#v, want nil for http3 transport", listener)
	}
}

func TestIngestTLSRequiredForTLSDirectQUICAndHTTP3(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.EdgeConfig
		want bool
	}{
		{name: "plain http", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP}, want: false},
		{name: "explicit mtls", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP, MTLS: config.MTLSPaths{CertFile: "cert.pem"}}, want: true},
		{name: "tcp", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportTCP}, want: true},
		{name: "smux", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportSMUX}, want: true},
		{name: "quic", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportQUIC}, want: true},
		{name: "http3", cfg: config.EdgeConfig{IngestTransport: config.IngestTransportHTTP3}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ingestTLSRequired(tt.cfg); got != tt.want {
				t.Fatalf("ingestTLSRequired() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestQUICIngestListenerRequiresTLSConfig(t *testing.T) {
	_, err := newQUICIngestListener(config.EdgeConfig{
		IngestTransport:      config.IngestTransportQUIC,
		IngestQUICListenAddr: "127.0.0.1:0",
	}, pending.NewRegistry(pending.Options{}), nil)
	if err == nil || !strings.Contains(err.Error(), "TLS config is required") {
		t.Fatalf("newQUICIngestListener() error = %v, want TLS config error", err)
	}
}

func TestQUICIngestListenerSharesRegistryAndTicketKeepsHTTPSURL(t *testing.T) {
	tlsTemplate := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsTemplate.Close)
	clientTLS := tlsTemplate.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: tlsTemplate.TLS.Certificates}

	reg := pending.NewRegistry(pending.Options{})
	listener, err := newNonHTTPIngestListener(config.EdgeConfig{
		IngestTransport:       config.IngestTransportQUIC,
		IngestQUICListenAddr:  "127.0.0.1:0",
		StreamCopyBufferBytes: 1024,
	}, reg, serverTLS)
	if err != nil {
		t.Fatalf("newNonHTTPIngestListener() error = %v", err)
	}
	quicIngest, ok := listener.(*quicIngestListener)
	if !ok {
		t.Fatalf("newNonHTTPIngestListener() = %T, want *quicIngestListener", listener)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- quicIngest.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = quicIngest.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("quicIngest.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("quicIngest.Serve() did not stop")
		}
	})

	uploadDone := make(chan error, 1)
	pub := &fakePublisher{}
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			uploadDone <- ingestquic.DialAndSend(context.Background(), quicIngest.listener.Addr().String(), clientTLS, ingestquic.ClientRequest{
				RequestID:   ticket.RequestID,
				IngestToken: ticket.IngestToken,
				Metadata:    pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "9"},
				Body:        strings.NewReader("quic-body"),
				BodyLength:  9,
			})
		}()
	}
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:            "https://edge.internal/_ingest",
		IngestTransport:      config.IngestTransportQUIC,
		IngestQUICListenAddr: quicIngest.listener.Addr().String(),
		AllowedBuckets:       []string{"demo-bucket"},
		Signing:              config.SigningConfig{Disabled: true},
		Timeouts:             config.TimeoutConfig{PendingRequestTTL: 2 * time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	tokens := []string{"req-quic", "ingest-quic-token"}
	edge.newToken = func() (string, error) {
		v := tokens[0]
		tokens = tokens[1:]
		return v, nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		select {
		case err := <-uploadDone:
			t.Fatalf("status = %d, want %d; upload error = %v", got, http.StatusOK, err)
		default:
			t.Fatalf("status = %d, want %d", got, http.StatusOK)
		}
	}
	if body := resp.Body.String(); body != "quic-body" {
		t.Fatalf("body = %q, want quic-body", body)
	}
	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("QUIC ingest upload error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for QUIC ingest upload")
	}

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].IngestURL != "https://edge.internal/_ingest/req-quic" {
		t.Fatalf("ticket ingest URL = %q, want HTTPS ingest URL", published[0].IngestURL)
	}
	if strings.Contains(published[0].IngestURL, quicIngest.listener.Addr().String()) {
		t.Fatalf("ticket ingest URL %q unexpectedly contains QUIC listener address", published[0].IngestURL)
	}
}

func TestBadSignatureAndDisallowedPathFailBeforePublish(t *testing.T) {
	pub := &fakePublisher{}
	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)

	badSigReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt?expires=9999999999&sig=bad", nil)
	badSigResp := httptest.NewRecorder()
	edge.ServeHTTP(badSigResp, badSigReq)
	if got := badSigResp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("bad signature status = %d, want %d", got, http.StatusForbidden)
	}

	disallowedReq := httptest.NewRequest(http.MethodGet, "/other-bucket/file.txt", nil)
	disallowedResp := httptest.NewRecorder()
	edge.ServeHTTP(disallowedResp, disallowedReq)
	if got := disallowedResp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("disallowed status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestSignedURLAcceptedAndPublishesTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	reg := pending.NewRegistry(pending.Options{})
	now := time.Now()
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	edge.now = func() time.Time { return now }
	edge.newToken = func() (string, error) { return "signed-token", nil }

	signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Expires: now.Add(time.Minute), Secret: "secret"})
	if err != nil {
		t.Fatalf("SignURL() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, signed, nil)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, req)
	if got := pub.count(); got != 1 {
		t.Fatalf("published %d tickets, want 1", got)
	}
}

func TestSingleServerPublishesDefaultSubjectAndEmptyServer(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testEdge(pub, time.Second)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "" {
		t.Fatalf("ticket server = %q, want empty", published[0].Server)
	}
	subjects := pub.subjectSnapshot()
	if len(subjects) != 1 || subjects[0] != "air3.tickets" {
		t.Fatalf("subjects = %#v, want [air3.tickets]", subjects)
	}
}

func TestSingleServerDefaultBucketShortPathPublishesTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		DefaultBucket:  "demo",
		AllowedBuckets: []string{"demo"},
		NATS:           config.NATSConfig{Subject: "air3.tickets"},
		Signing:        config.SigningConfig{Disabled: true},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	edge.newToken = func() (string, error) { return "single-default-token", nil }

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "" || published[0].Bucket != "demo" || published[0].Key != "file.txt" {
		t.Fatalf("published ticket = %#v, want empty server bucket demo key file.txt", published[0])
	}
}

func TestSingleServerDefaultBucketShortFormWinsOverBucketPrefix(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		DefaultBucket:  "demo",
		AllowedBuckets: []string{"demo"},
		Signing:        config.SigningConfig{Disabled: true},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
	edge.newToken = func() (string, error) { return "single-explicit-token", nil }

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "" || published[0].Bucket != "demo" || published[0].Key != "demo/file.txt" {
		t.Fatalf("published ticket = %#v, want empty server bucket demo key demo/file.txt", published[0])
	}
}

func TestSingleServerDefaultBucketShortPathAllowlistUsesResolvedBucket(t *testing.T) {
	pub := &fakePublisher{}
	reg := pending.NewRegistry(pending.Options{})
	edge := newEdgeServer(config.EdgeConfig{
		IngestURL:      "https://edge.internal/_ingest",
		DefaultBucket:  "demo",
		AllowedBuckets: []string{"other"},
		Signing:        config.SigningConfig{Disabled: true},
		Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
	}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestSignedSingleServerDefaultBucketShortURLAcceptedAndTamperingRejected(t *testing.T) {
	now := time.Now()
	signed := signedDefaultBucketSingleURL(t, http.MethodGet, "demo", "file.txt", now)

	t.Run("accepted", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		reg := pending.NewRegistry(pending.Options{})
		edge := newEdgeServer(config.EdgeConfig{
			IngestURL:      "https://edge.internal/_ingest",
			DefaultBucket:  "demo",
			AllowedBuckets: []string{"demo"},
			Signing:        config.SigningConfig{Secret: "secret"},
			Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
		}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
		edge.now = func() time.Time { return now }
		edge.newToken = func() (string, error) { return "signed-single-default-token", nil }

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, signed, nil))

		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		if published[0].Server != "" || published[0].Bucket != "demo" || published[0].Key != "file.txt" {
			t.Fatalf("published ticket = %#v, want empty server bucket demo key file.txt", published[0])
		}
	})

	for _, tt := range []struct {
		name string
		raw  string
	}{
		{name: "path", raw: strings.Replace(signed, "/file.txt", "/other.txt", 1)},
		{name: "signature", raw: strings.Replace(signed, "sig=", "sig=bad", 1)},
	} {
		t.Run("tampered "+tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			reg := pending.NewRegistry(pending.Options{})
			edge := newEdgeServer(config.EdgeConfig{
				IngestURL:      "https://edge.internal/_ingest",
				DefaultBucket:  "demo",
				AllowedBuckets: []string{"demo"},
				Signing:        config.SigningConfig{Secret: "secret"},
				Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
			}, reg, uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
			edge.now = func() time.Time { return now }

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tt.raw, nil))
			if got := resp.Result().StatusCode; got != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
		})
	}
}

func TestMultiServerConnectorPublishesRoutedSubjectAndTicket(t *testing.T) {
	pub := &fakePublisher{err: errors.New("stop after publish")}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Disabled: true},
	}, nil)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))

	published := pub.snapshot()
	if len(published) != 1 {
		t.Fatalf("published tickets = %#v, want one", published)
	}
	if published[0].Server != "blue" {
		t.Fatalf("ticket server = %q, want blue", published[0].Server)
	}
	subjects := pub.subjectSnapshot()
	if len(subjects) != 1 || subjects[0] != "air3.blue" {
		t.Fatalf("subjects = %#v, want [air3.blue]", subjects)
	}
}

func TestMultiServerDefaultBucketShortPathPublishesRoutedTicket(t *testing.T) {
	for _, tt := range []struct {
		name    string
		path    string
		wantKey string
	}{
		{name: "single segment key", path: "/blue/file.txt", wantKey: "file.txt"},
		{name: "additional path segments", path: "/blue/archive/file.txt", wantKey: "archive/file.txt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{err: errors.New("stop after publish")}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets:       []string{"demo"},
				ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
				Signing:              config.SigningConfig{Disabled: true},
			}, nil)
			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tt.path, nil))

			published := pub.snapshot()
			if len(published) != 1 {
				t.Fatalf("published tickets = %#v, want one", published)
			}
			if published[0].Server != "blue" || published[0].Bucket != "demo" || published[0].Key != tt.wantKey {
				t.Fatalf("published ticket = %#v, want server blue bucket demo key %q", published[0], tt.wantKey)
			}
			subjects := pub.subjectSnapshot()
			if len(subjects) != 1 || subjects[0] != "air3.blue" {
				t.Fatalf("subjects = %#v, want [air3.blue]", subjects)
			}
		})
	}
}

func TestDirectDefaultBucketShortPathBypassesNATS(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: 5, Body: io.NopCloser(strings.NewReader("hello"))}}
	edge, reg := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets:       []string{"connector-bucket"},
		ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"demo"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})
	edge.newToken = func() (string, error) {
		t.Fatal("direct default-bucket path allocated a connector token")
		return "", nil
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))

	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	requests := fetcher.snapshot()
	if len(requests) != 1 {
		t.Fatalf("fetch requests = %#v, want one", requests)
	}
	if requests[0].Bucket != "demo" || requests[0].Key != "file.txt" {
		t.Fatalf("fetch request = %#v, want bucket demo key file.txt", requests[0])
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if _, err := reg.StartIngest("req-multi", "ingest-multi-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("direct alias registered pending request: %v", err)
	}
}

func TestDefaultBucketShortPathAllowlistUsesResolvedBucket(t *testing.T) {
	t.Run("routed", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"other"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			Signing:              config.SigningConfig{Disabled: true},
		}, nil)

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})

	t.Run("direct", func(t *testing.T) {
		pub := &fakePublisher{}
		fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"demo"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			DirectServers: map[string]config.S3Config{
				"blue": {AllowedBuckets: []string{"other"}},
			},
			Signing: config.SigningConfig{Disabled: true},
		}, map[string]objectFetcher{"blue": fetcher})

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
		if got := len(fetcher.snapshot()); got != 0 {
			t.Fatalf("fetch calls = %d, want 0", got)
		}
	})
}

func TestSignedDefaultBucketShortURLAcceptedAndTamperingRejected(t *testing.T) {
	now := time.Now()
	signed := signedDefaultBucketMultiURL(t, http.MethodGet, "blue", "demo", "file.txt", now)

	t.Run("accepted", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		edge, _ := testMultiEdge(pub, config.EdgeConfig{
			AllowedBuckets:       []string{"demo"},
			ServerDefaultBuckets: map[string]string{"BLUE": "demo"},
			Signing:              config.SigningConfig{Secret: "secret"},
		}, nil)
		edge.now = func() time.Time { return now }
		edge.newToken = func() (string, error) { return "signed-default-token", nil }

		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, signed, nil))

		published := pub.snapshot()
		if len(published) != 1 {
			t.Fatalf("published tickets = %#v, want one", published)
		}
		if published[0].Server != "blue" || published[0].Bucket != "demo" || published[0].Key != "file.txt" {
			t.Fatalf("published ticket = %#v, want server blue bucket demo key file.txt", published[0])
		}
	})

	for _, tt := range []struct {
		name string
		raw  string
	}{
		{name: "server", raw: strings.Replace(signed, "/blue/", "/green/", 1)},
		{name: "key", raw: strings.Replace(signed, "/file.txt", "/other.txt", 1)},
	} {
		t.Run("tampered "+tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			blueFetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("blue"))}}
			greenFetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("green"))}}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets:       []string{"demo"},
				ServerDefaultBuckets: map[string]string{"BLUE": "demo", "GREEN": "demo"},
				DirectServers: map[string]config.S3Config{
					"blue":  {AllowedBuckets: []string{"demo"}},
					"green": {AllowedBuckets: []string{"demo"}},
				},
				Signing: config.SigningConfig{Secret: "secret"},
			}, map[string]objectFetcher{"blue": blueFetcher, "green": greenFetcher})
			edge.now = func() time.Time { return now }

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tt.raw, nil))
			if got := resp.Result().StatusCode; got != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if got := len(blueFetcher.snapshot()) + len(greenFetcher.snapshot()); got != 0 {
				t.Fatalf("fetch calls = %d, want 0", got)
			}
		})
	}
}

func TestMultiServerSignedURLRejectsServerPathTampering(t *testing.T) {
	pub := &fakePublisher{}
	now := time.Now()
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		Signing:        config.SigningConfig{Secret: "secret"},
	}, nil)
	edge.now = func() time.Time { return now }

	signed := signedMultiURL(t, http.MethodGet, "blue", "demo-bucket", "file.txt", now)
	tampered := strings.Replace(signed, "/blue/", "/red/", 1)
	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tampered, nil))

	if got := resp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
}

func TestInvalidMultiServerPathFailsBeforePublishOrFetch(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"demo-bucket"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})

	for _, path := range []string{"/bad%2Falias/demo-bucket/file.txt", "/blue/demo-bucket", "/blue/file.txt"} {
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
		if got := resp.Result().StatusCode; got != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", path, got, http.StatusBadRequest)
		}
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if got := len(fetcher.snapshot()); got != 0 {
		t.Fatalf("fetch calls = %d, want 0", got)
	}
}

func TestDirectAliasGETHEADAndRangeBypassNATS(t *testing.T) {
	for _, tt := range []struct {
		name     string
		method   string
		rangeHdr string
		body     string
		wantBody string
	}{
		{name: "get range", method: http.MethodGet, rangeHdr: "bytes=0-4", body: "hello", wantBody: "hello"},
		{name: "head", method: http.MethodHead, body: "ignored", wantBody: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusPartialContent, ContentType: "text/plain", ContentLength: int64(len(tt.body)), ContentRange: "bytes 0-4/11", ETag: `"abc"`, AcceptRanges: "bytes", Body: io.NopCloser(strings.NewReader(tt.body))}}
			edge, reg := testMultiEdge(pub, config.EdgeConfig{
				AllowedBuckets: []string{"connector-bucket"},
				DirectServers: map[string]config.S3Config{
					"blue": {AllowedBuckets: []string{"demo-bucket"}},
				},
				Signing: config.SigningConfig{Disabled: true},
			}, map[string]objectFetcher{"blue": fetcher})
			edge.newToken = func() (string, error) { t.Fatal("direct alias allocated a connector token"); return "", nil }

			req := httptest.NewRequest(tt.method, "/blue/demo-bucket/file.txt", nil)
			if tt.rangeHdr != "" {
				req.Header.Set("Range", tt.rangeHdr)
			}
			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, req)

			if got := resp.Result().StatusCode; got != http.StatusPartialContent {
				t.Fatalf("status = %d, want %d", got, http.StatusPartialContent)
			}
			if body := resp.Body.String(); body != tt.wantBody {
				t.Fatalf("body = %q, want %q", body, tt.wantBody)
			}
			if ct := resp.Result().Header.Get("Content-Type"); ct != "text/plain" {
				t.Fatalf("content-type = %q, want text/plain", ct)
			}
			requests := fetcher.snapshot()
			if len(requests) != 1 {
				t.Fatalf("fetch requests = %#v, want one", requests)
			}
			if requests[0].Method != tt.method || requests[0].Bucket != "demo-bucket" || requests[0].Key != "file.txt" || requests[0].Range != tt.rangeHdr {
				t.Fatalf("fetch request = %#v", requests[0])
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			if _, err := reg.StartIngest("req-multi", "ingest-multi-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
				t.Fatalf("direct alias registered pending request: %v", err)
			}
		})
	}
}

func TestDirectAliasUsesAliasSpecificAllowlist(t *testing.T) {
	pub := &fakePublisher{}
	fetcher := &fakeFetcher{object: &s3fetch.Object{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unused"))}}
	edge, _ := testMultiEdge(pub, config.EdgeConfig{
		AllowedBuckets: []string{"demo-bucket"},
		DirectServers: map[string]config.S3Config{
			"blue": {AllowedBuckets: []string{"other-bucket"}},
		},
		Signing: config.SigningConfig{Disabled: true},
	}, map[string]objectFetcher{"blue": fetcher})

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d tickets, want 0", pub.count())
	}
	if got := len(fetcher.snapshot()); got != 0 {
		t.Fatalf("fetch calls = %d, want 0", got)
	}
}

func TestDirectAliasErrorMapping(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{
		{name: "not found", err: s3fetch.ErrNotFound, want: http.StatusNotFound},
		{name: "invalid request", err: s3fetch.ErrInvalidRequest, want: http.StatusBadRequest},
		{name: "unavailable", err: errors.New("s3 secret=topsecret endpoint private.internal"), want: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			var logBuffer bytes.Buffer
			fetcher := &fakeFetcher{err: tt.err}
			edge, _ := testMultiEdge(pub, config.EdgeConfig{
				DirectServers: map[string]config.S3Config{
					"blue": {AllowedBuckets: []string{"demo-bucket"}},
				},
				Signing: config.SigningConfig{Disabled: true},
			}, map[string]objectFetcher{"blue": fetcher})
			edge.logger = slog.New(slog.NewTextHandler(&logBuffer, nil))

			resp := httptest.NewRecorder()
			edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/blue/demo-bucket/file.txt", nil))
			if got := resp.Result().StatusCode; got != tt.want {
				t.Fatalf("status = %d, want %d", got, tt.want)
			}
			if pub.count() != 0 {
				t.Fatalf("published %d tickets, want 0", pub.count())
			}
			for _, secret := range []string{"topsecret", "private.internal"} {
				if strings.Contains(resp.Body.String(), secret) || strings.Contains(logBuffer.String(), secret) {
					t.Fatalf("direct error leaked %q; body=%q logs=%q", secret, resp.Body.String(), logBuffer.String())
				}
			}
		})
	}
}

func TestRangeRequestValidationAndTicketPropagation(t *testing.T) {
	t.Run("valid unsigned range is included in ticket", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		edge, _ := testEdge(pub, time.Second)
		req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil)
		req.Header.Set("Range", "bytes=0-4")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		tickets := pub.snapshot()
		if len(tickets) != 1 || tickets[0].Range != "bytes=0-4" {
			t.Fatalf("published tickets = %#v, want one ticket with range", tickets)
		}
	})

	t.Run("invalid range is rejected before publish", func(t *testing.T) {
		pub := &fakePublisher{}
		edge, _ := testEdge(pub, time.Second)
		req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil)
		req.Header.Set("Range", "bytes=10-1")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", got, http.StatusBadRequest)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})

	t.Run("signed range query is included in ticket", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("stop after publish")}
		now := time.Now()
		edge := newEdgeServer(config.EdgeConfig{
			IngestURL:      "https://edge.internal/_ingest",
			AllowedBuckets: []string{"demo-bucket"},
			Signing:        config.SigningConfig{Secret: "secret"},
			Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
		}, pending.NewRegistry(pending.Options{}), uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
		edge.now = func() time.Time { return now }
		edge.newToken = func() (string, error) { return "signed-range-token", nil }

		signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Range: "bytes=0-4", Expires: now.Add(time.Minute), Secret: "secret"})
		if err != nil {
			t.Fatalf("SignURL() error = %v", err)
		}
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, signed, nil))

		tickets := pub.snapshot()
		if len(tickets) != 1 || tickets[0].Range != "bytes=0-4" {
			t.Fatalf("published tickets = %#v, want one ticket with signed range", tickets)
		}
	})

	t.Run("signed range must be part of signature", func(t *testing.T) {
		pub := &fakePublisher{}
		now := time.Now()
		edge := newEdgeServer(config.EdgeConfig{
			IngestURL:      "https://edge.internal/_ingest",
			AllowedBuckets: []string{"demo-bucket"},
			Signing:        config.SigningConfig{Secret: "secret"},
			Timeouts:       config.TimeoutConfig{PendingRequestTTL: time.Second},
		}, pending.NewRegistry(pending.Options{}), uploadsource.NewRegistry(uploadsource.Options{}), pub, nil)
		edge.now = func() time.Time { return now }

		signed, err := signing.SignURL(signing.SignInput{Method: http.MethodGet, BaseURL: "https://files.example", Bucket: "demo-bucket", Key: "file.txt", Expires: now.Add(time.Minute), Secret: "secret"})
		if err != nil {
			t.Fatalf("SignURL() error = %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, signed, nil)
		req.Header.Set("Range", "bytes=0-4")
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, req)

		if got := resp.Result().StatusCode; got != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
		}
		if pub.count() != 0 {
			t.Fatalf("published %d tickets, want 0", pub.count())
		}
	})
}

func TestPublicErrorAndRequestLogsDoNotLeakSensitiveDetails(t *testing.T) {
	leakyErr := errors.New("dial nats://edge:password@private-nats.internal:4222 subject air3.tickets secret=topsecret")
	var logBuffer bytes.Buffer
	edge, _ := testEdge(&fakePublisher{err: leakyErr}, time.Second)
	edge.logger = slog.New(slog.NewTextHandler(&logBuffer, nil))

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))

	if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
	}
	body := resp.Body.String()
	logs := logBuffer.String()
	for _, secret := range []string{"password", "private-nats.internal", "air3.tickets", "topsecret"} {
		if strings.Contains(body, secret) {
			t.Fatalf("public body leaked %q: %q", secret, body)
		}
		if strings.Contains(logs, secret) {
			t.Fatalf("logs leaked %q: %q", secret, logs)
		}
	}
	if !strings.Contains(body, "backend unavailable") {
		t.Fatalf("public body = %q, want generic backend unavailable message", body)
	}
}

func TestGETStreamsIngestToPublicResponse(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "11", ETag: `"abc"`})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_, _ = io.Copy(stream, strings.NewReader("hello world"))
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	if body := resp.Body.String(); body != "hello world" {
		t.Fatalf("body = %q", body)
	}
	if ct := resp.Result().Header.Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestHEADReturnsMetadataWithoutBody(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusOK, ContentType: "text/plain", ContentLength: "5"})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_, _ = stream.Write([]byte("hello"))
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodHead, "/demo-bucket/file.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	if body := resp.Body.String(); body != "" {
		t.Fatalf("HEAD body = %q, want empty", body)
	}
	if length := resp.Result().Header.Get("Content-Length"); length != "5" {
		t.Fatalf("content-length = %q", length)
	}
}

func TestMissingObjectMetadataMapsToPublic404(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Second)
	pub.on = func(ticket tickets.Ticket) {
		go func() {
			stream, err := reg.StartIngest(ticket.RequestID, ticket.IngestToken, pending.Metadata{StatusCode: http.StatusNotFound})
			if err != nil {
				t.Errorf("StartIngest() error = %v", err)
				return
			}
			_ = stream.Close()
		}()
	}

	resp := httptest.NewRecorder()
	edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/missing.txt", nil))
	if got := resp.Result().StatusCode; got != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", got, http.StatusNotFound)
	}
	if body := resp.Body.String(); body != "" {
		t.Fatalf("body = %q, want empty", body)
	}
}

func TestPublishFailureAndConnectorTimeoutMapping(t *testing.T) {
	t.Run("publish failure", func(t *testing.T) {
		edge, reg := testEdge(&fakePublisher{err: errors.New("nats unavailable")}, time.Second)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
		}
		if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("connector timeout", func(t *testing.T) {
		edge, reg := testEdge(&fakePublisher{}, 5*time.Millisecond)
		resp := httptest.NewRecorder()
		edge.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil))
		if got := resp.Result().StatusCode; got != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want %d", got, http.StatusGatewayTimeout)
		}
		if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
			t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
		}
	})
}

func TestClientDisconnectCancelsPendingState(t *testing.T) {
	pub := &fakePublisher{}
	edge, reg := testEdge(pub, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/demo-bucket/file.txt", nil).WithContext(ctx)
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		edge.ServeHTTP(resp, req)
	}()
	for pub.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if _, err := reg.StartIngest("req-test", "ingest-token", pending.Metadata{StatusCode: http.StatusOK}); !errors.Is(err, pending.ErrNotFound) {
		t.Fatalf("late StartIngest() error = %v, want ErrNotFound", err)
	}
}
