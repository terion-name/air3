package s3api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sigv4Algorithm                             = "AWS4-HMAC-SHA256"
	sigv4Service                               = "s3"
	sigv4TerminalScope                         = "aws4_request"
	sigv4DateLayout                            = "20060102"
	sigv4DateTimeLayout                        = "20060102T150405Z"
	sigv4EmptySHA256                           = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	sigv4UnsignedPayload                       = "UNSIGNED-PAYLOAD"
	sigv4StreamingAWS4HMACSHA256Payload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	sigv4StreamingAWS4HMACSHA256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	sigv4StreamingUnsignedPayloadTrailer       = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

// Credentials are the AWS SigV4 access key pair accepted by VerifySigV4.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// VerifyOptions configures AWS SigV4 verification for S3 requests.
type VerifyOptions struct {
	Credentials       Credentials
	Region            string
	Now               func() time.Time
	HeaderSkew        time.Duration
	MaxPresignExpires time.Duration
}

// AuthContext describes the verified SigV4 identity and canonical inputs.
type AuthContext struct {
	AccessKeyID    string
	CredentialDate string
	Region         string
	SignedHeaders  []string
	PayloadHash    string
	Presigned      bool
}

// PayloadHashMode identifies the canonical SigV4 payload hash form after verification.
type PayloadHashMode string

const (
	PayloadHashModeUnknown   PayloadHashMode = "Unknown"
	PayloadHashModeUnsigned  PayloadHashMode = "UnsignedPayload"
	PayloadHashModeEmpty     PayloadHashMode = "EmptySHA256"
	PayloadHashModeSigned    PayloadHashMode = "SignedSHA256"
	PayloadHashModeStreaming PayloadHashMode = "Streaming"
)

// PayloadHashMode reports the verified request's canonical payload hash mode.
func (ctx AuthContext) PayloadHashMode() PayloadHashMode {
	return PayloadHashModeForValue(ctx.PayloadHash)
}

// PayloadHashModeForValue classifies a canonical SigV4 payload hash value.
func PayloadHashModeForValue(value string) PayloadHashMode {
	switch {
	case value == sigv4UnsignedPayload:
		return PayloadHashModeUnsigned
	case value == sigv4EmptySHA256:
		return PayloadHashModeEmpty
	case sigv4IsStreamingPayloadHash(value):
		return PayloadHashModeStreaming
	case sigv4IsSHA256PayloadHash(value):
		return PayloadHashModeSigned
	default:
		return PayloadHashModeUnknown
	}
}

// ValidatePayloadHashForOperation applies the S3 API v1 mutation payload-hash policy.
func ValidatePayloadHashForOperation(operation Operation, auth AuthContext) error {
	mode := auth.PayloadHashMode()
	switch operation {
	case OperationPutObject:
		if mode == PayloadHashModeUnsigned {
			return nil
		}
	case OperationDeleteObject:
		if mode == PayloadHashModeUnsigned || mode == PayloadHashModeEmpty {
			return nil
		}
	default:
		return nil
	}
	return fmt.Errorf("payload hash: %s does not allow %s", operation, mode)
}

type sigv4CredentialScope struct {
	AccessKeyID string
	Date        string
	Region      string
}

type sigv4Authorization struct {
	Credential    sigv4CredentialScope
	SignedHeaders []string
	Signature     string
}

type sigv4QueryPair struct {
	RawKey   string
	RawValue string
}

// IsSigV4Request reports whether r appears to use AWS SigV4 header or query authentication.
func IsSigV4Request(r *http.Request) bool {
	if r == nil {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(r.Header.Get("Authorization")), sigv4Algorithm) {
		return true
	}
	algorithm, ok := sigv4QueryValue(r.URL.RawQuery, "X-Amz-Algorithm")
	return ok && algorithm == sigv4Algorithm
}

// VerifySigV4 verifies an AWS SigV4 Authorization header or presigned query for S3.
func VerifySigV4(r *http.Request, opts VerifyOptions) (AuthContext, error) {
	if r == nil {
		return AuthContext{}, errors.New("missing request")
	}
	if opts.Credentials.AccessKeyID == "" {
		return AuthContext{}, errors.New("access key is required")
	}
	if opts.Credentials.SecretAccessKey == "" {
		return AuthContext{}, errors.New("credentials secret access key is required")
	}
	if opts.Region == "" {
		return AuthContext{}, errors.New("region is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.HeaderSkew == 0 {
		opts.HeaderSkew = 15 * time.Minute
	}
	if opts.MaxPresignExpires == 0 {
		opts.MaxPresignExpires = 7 * 24 * time.Hour
	}

	if _, ok := sigv4QueryValue(r.URL.RawQuery, "X-Amz-Algorithm"); ok {
		return sigv4VerifyPresigned(r, opts)
	}
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		return sigv4VerifyHeader(r, opts)
	}
	return AuthContext{}, errors.New("missing authorization")
}

func sigv4VerifyHeader(r *http.Request, opts VerifyOptions) (AuthContext, error) {
	auth, err := sigv4ParseAuthorizationHeader(r.Header.Get("Authorization"))
	if err != nil {
		return AuthContext{}, err
	}
	if err := sigv4ValidateCredentialScope(auth.Credential, opts); err != nil {
		return AuthContext{}, err
	}

	amzDate := r.Header.Get("x-amz-date")
	if amzDate == "" {
		return AuthContext{}, errors.New("missing signed header: x-amz-date")
	}
	signedAt, err := sigv4ParseRequestTime(amzDate, auth.Credential.Date)
	if err != nil {
		return AuthContext{}, err
	}
	if sigv4AbsDuration(opts.Now().UTC().Sub(signedAt)) > opts.HeaderSkew {
		return AuthContext{}, errors.New("skew: x-amz-date is outside allowed header skew")
	}

	payloadHash, err := sigv4HeaderPayloadHash(r, auth.SignedHeaders)
	if err != nil {
		return AuthContext{}, err
	}
	if err := sigv4CheckSignature(auth.Signature); err != nil {
		return AuthContext{}, err
	}

	canonicalRequest, err := sigv4CanonicalRequest(r, auth.SignedHeaders, payloadHash, false)
	if err != nil {
		return AuthContext{}, err
	}
	scope := sigv4CredentialScopeString(auth.Credential.Date, opts.Region)
	stringToSign := sigv4StringToSign(amzDate, scope, canonicalRequest)
	expected := sigv4Signature(opts.Credentials.SecretAccessKey, auth.Credential.Date, opts.Region, stringToSign)
	if err := sigv4CompareSignatures(auth.Signature, expected); err != nil {
		return AuthContext{}, err
	}

	return AuthContext{
		AccessKeyID:    auth.Credential.AccessKeyID,
		CredentialDate: auth.Credential.Date,
		Region:         auth.Credential.Region,
		SignedHeaders:  append([]string(nil), auth.SignedHeaders...),
		PayloadHash:    payloadHash,
		Presigned:      false,
	}, nil
}

func sigv4VerifyPresigned(r *http.Request, opts VerifyOptions) (AuthContext, error) {
	algorithm, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-Algorithm")
	if !ok {
		return AuthContext{}, errors.New("malformed authorization: missing X-Amz-Algorithm")
	}
	if algorithm != sigv4Algorithm {
		return AuthContext{}, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
	credentialValue, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-Credential")
	if !ok {
		return AuthContext{}, errors.New("malformed authorization: missing X-Amz-Credential")
	}
	credential, err := sigv4ParseCredential(credentialValue)
	if err != nil {
		return AuthContext{}, err
	}
	if err := sigv4ValidateCredentialScope(credential, opts); err != nil {
		return AuthContext{}, err
	}
	amzDate, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-Date")
	if !ok {
		return AuthContext{}, errors.New("malformed authorization: missing X-Amz-Date")
	}
	signedAt, err := sigv4ParseRequestTime(amzDate, credential.Date)
	if err != nil {
		return AuthContext{}, err
	}
	expiresValue, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-Expires")
	if !ok {
		return AuthContext{}, errors.New("expired: missing X-Amz-Expires")
	}
	expiresSeconds, err := strconv.Atoi(expiresValue)
	if err != nil || expiresSeconds <= 0 {
		return AuthContext{}, errors.New("expired: X-Amz-Expires must be positive whole seconds")
	}
	expires := time.Duration(expiresSeconds) * time.Second
	if expires > opts.MaxPresignExpires {
		return AuthContext{}, errors.New("expired: X-Amz-Expires exceeds maximum")
	}
	if opts.Now().UTC().After(signedAt.Add(expires)) {
		return AuthContext{}, errors.New("expired: presigned URL has expired")
	}
	signedHeaderValue, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-SignedHeaders")
	if !ok {
		return AuthContext{}, errors.New("malformed authorization: missing X-Amz-SignedHeaders")
	}
	signedHeaders, err := sigv4ParseSignedHeaders(signedHeaderValue)
	if err != nil {
		return AuthContext{}, err
	}
	signature, ok := sigv4RequiredQueryValue(r.URL.RawQuery, "X-Amz-Signature")
	if !ok {
		return AuthContext{}, errors.New("signature: missing X-Amz-Signature")
	}
	if err := sigv4CheckSignature(signature); err != nil {
		return AuthContext{}, err
	}

	payloadHash, err := sigv4PresignedPayloadHash(r, signedHeaders)
	if err != nil {
		return AuthContext{}, err
	}
	canonicalRequest, err := sigv4CanonicalRequest(r, signedHeaders, payloadHash, true)
	if err != nil {
		return AuthContext{}, err
	}
	scope := sigv4CredentialScopeString(credential.Date, opts.Region)
	stringToSign := sigv4StringToSign(amzDate, scope, canonicalRequest)
	expected := sigv4Signature(opts.Credentials.SecretAccessKey, credential.Date, opts.Region, stringToSign)
	if err := sigv4CompareSignatures(signature, expected); err != nil {
		return AuthContext{}, err
	}

	return AuthContext{
		AccessKeyID:    credential.AccessKeyID,
		CredentialDate: credential.Date,
		Region:         credential.Region,
		SignedHeaders:  append([]string(nil), signedHeaders...),
		PayloadHash:    payloadHash,
		Presigned:      true,
	}, nil
}

func sigv4ParseAuthorizationHeader(header string) (sigv4Authorization, error) {
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, sigv4Algorithm) {
		return sigv4Authorization{}, fmt.Errorf("unsupported algorithm: authorization is not %s", sigv4Algorithm)
	}
	if len(header) == len(sigv4Algorithm) || header[len(sigv4Algorithm)] != ' ' {
		return sigv4Authorization{}, errors.New("malformed authorization: missing algorithm delimiter")
	}
	attrs := map[string]string{}
	seenRequired := map[string]bool{}
	for _, part := range strings.Split(strings.TrimSpace(header[len(sigv4Algorithm):]), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return sigv4Authorization{}, errors.New("malformed authorization: empty attribute")
		}
		key, value, ok := strings.Cut(part, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok || key == "" {
			return sigv4Authorization{}, errors.New("malformed authorization: attribute must be key=value")
		}
		switch key {
		case "Credential", "SignedHeaders", "Signature":
			if value == "" {
				return sigv4Authorization{}, fmt.Errorf("malformed authorization: empty %s", key)
			}
			if seenRequired[key] {
				return sigv4Authorization{}, fmt.Errorf("malformed authorization: duplicate %s", key)
			}
			seenRequired[key] = true
			attrs[key] = value
		}
	}
	for _, key := range []string{"Credential", "SignedHeaders", "Signature"} {
		if attrs[key] == "" {
			return sigv4Authorization{}, fmt.Errorf("malformed authorization: missing %s", key)
		}
	}
	credential, err := sigv4ParseCredential(attrs["Credential"])
	if err != nil {
		return sigv4Authorization{}, err
	}
	signedHeaders, err := sigv4ParseSignedHeaders(attrs["SignedHeaders"])
	if err != nil {
		return sigv4Authorization{}, err
	}
	return sigv4Authorization{Credential: credential, SignedHeaders: signedHeaders, Signature: attrs["Signature"]}, nil
}

func sigv4ParseCredential(value string) (sigv4CredentialScope, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 5 || parts[0] == "" || parts[1] == "" || parts[2] == "" || parts[3] == "" || parts[4] == "" {
		return sigv4CredentialScope{}, errors.New("credential scope: malformed credential")
	}
	if len(parts[1]) != len(sigv4DateLayout) || !sigv4AllDigits(parts[1]) {
		return sigv4CredentialScope{}, errors.New("credential scope: malformed credential date")
	}
	if parts[3] != sigv4Service || parts[4] != sigv4TerminalScope {
		return sigv4CredentialScope{}, errors.New("credential scope: service or terminal scope mismatch")
	}
	return sigv4CredentialScope{AccessKeyID: parts[0], Date: parts[1], Region: parts[2]}, nil
}

func sigv4ValidateCredentialScope(scope sigv4CredentialScope, opts VerifyOptions) error {
	if scope.AccessKeyID != opts.Credentials.AccessKeyID {
		return errors.New("access key: credential access key mismatch")
	}
	if scope.Region != opts.Region {
		return errors.New("region: credential scope region mismatch")
	}
	return nil
}

func sigv4ParseSignedHeaders(value string) ([]string, error) {
	parts := strings.Split(value, ";")
	if len(parts) == 0 {
		return nil, errors.New("missing signed header: empty signed headers")
	}
	headers := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		if part == "" {
			return nil, errors.New("missing signed header: empty signed header")
		}
		lower := strings.ToLower(part)
		if part != lower {
			return nil, errors.New("missing signed header: signed headers must be lowercase")
		}
		if seen[lower] {
			return nil, errors.New("missing signed header: duplicate signed header")
		}
		seen[lower] = true
		headers = append(headers, lower)
	}
	if !sort.StringsAreSorted(headers) {
		return nil, errors.New("missing signed header: signed headers must be sorted")
	}
	if !seen["host"] {
		return nil, errors.New("missing signed header: host is required")
	}
	return headers, nil
}

func sigv4ParseRequestTime(value string, credentialDate string) (time.Time, error) {
	signedAt, err := time.Parse(sigv4DateTimeLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("skew: malformed x-amz-date: %w", err)
	}
	if signedAt.Format(sigv4DateLayout) != credentialDate {
		return time.Time{}, errors.New("credential scope: credential date does not match x-amz-date")
	}
	return signedAt.UTC(), nil
}

func sigv4HeaderPayloadHash(r *http.Request, signedHeaders []string) (string, error) {
	if sigv4HasSignedHeader(signedHeaders, "x-amz-content-sha256") {
		value, err := sigv4CanonicalHeaderValue(r, "x-amz-content-sha256")
		if err != nil {
			return "", err
		}
		if sigv4IsReadOnlyMethod(r.Method) && value != sigv4UnsignedPayload && value != sigv4EmptySHA256 {
			return "", errors.New("payload hash: GET/HEAD signed payload hash must be UNSIGNED-PAYLOAD or empty SHA256")
		}
		if !sigv4ValidPayloadHash(value) {
			return "", errors.New("payload hash: malformed x-amz-content-sha256")
		}
		return value, nil
	}
	if sigv4IsReadOnlyMethod(r.Method) {
		return sigv4EmptySHA256, nil
	}
	return "", errors.New("payload hash: non-GET/HEAD requests require signed x-amz-content-sha256")
}

func sigv4PresignedPayloadHash(r *http.Request, signedHeaders []string) (string, error) {
	headerHash := ""
	if sigv4HasSignedHeader(signedHeaders, "x-amz-content-sha256") {
		value, err := sigv4CanonicalHeaderValue(r, "x-amz-content-sha256")
		if err != nil {
			return "", err
		}
		headerHash = value
	}
	queryHash, hasQueryHash := sigv4QueryValue(r.URL.RawQuery, "X-Amz-Content-Sha256")
	if headerHash != "" && hasQueryHash && headerHash != queryHash {
		return "", errors.New("payload hash: signed header and query payload hashes differ")
	}
	payloadHash := headerHash
	if payloadHash == "" && hasQueryHash {
		payloadHash = queryHash
	}
	if payloadHash == "" {
		return sigv4UnsignedPayload, nil
	}
	if sigv4IsReadOnlyMethod(r.Method) && payloadHash != sigv4UnsignedPayload && payloadHash != sigv4EmptySHA256 {
		return "", errors.New("payload hash: GET/HEAD presigned payload hash must be UNSIGNED-PAYLOAD or empty SHA256")
	}
	if !sigv4ValidPayloadHash(payloadHash) {
		return "", errors.New("payload hash: malformed presigned payload hash")
	}
	return payloadHash, nil
}

func sigv4CanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, presigned bool) (string, error) {
	canonicalHeaders, err := sigv4CanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}
	lines := []string{
		strings.ToUpper(r.Method),
		sigv4CanonicalURI(r.URL.EscapedPath()),
		sigv4CanonicalQuery(r.URL.RawQuery, presigned),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}
	return strings.Join(lines, "\n"), nil
}

func sigv4CanonicalURI(path string) string {
	if path == "" {
		path = "/"
	}
	return sigv4URIEncode(path, true)
}

func sigv4CanonicalQuery(rawQuery string, presigned bool) string {
	if rawQuery == "" {
		return ""
	}
	type encodedPair struct {
		key   string
		value string
	}
	encoded := []encodedPair{}
	for _, pair := range sigv4RawQueryPairs(rawQuery) {
		if presigned {
			decodedKey, err := url.QueryUnescape(pair.RawKey)
			if err == nil && decodedKey == "X-Amz-Signature" {
				continue
			}
		}
		encoded = append(encoded, encodedPair{
			key:   sigv4URIEncode(pair.RawKey, false),
			value: sigv4URIEncode(pair.RawValue, false),
		})
	}
	sort.SliceStable(encoded, func(i, j int) bool {
		if encoded[i].key == encoded[j].key {
			return encoded[i].value < encoded[j].value
		}
		return encoded[i].key < encoded[j].key
	})
	parts := make([]string, 0, len(encoded))
	for _, pair := range encoded {
		parts = append(parts, pair.key+"="+pair.value)
	}
	return strings.Join(parts, "&")
}

func sigv4CanonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	lines := make([]string, 0, len(signedHeaders))
	for _, name := range signedHeaders {
		value, err := sigv4CanonicalHeaderValue(r, name)
		if err != nil {
			return "", err
		}
		lines = append(lines, name+":"+value+"\n")
	}
	return strings.Join(lines, ""), nil
}

func sigv4CanonicalHeaderValue(r *http.Request, name string) (string, error) {
	if name == "host" {
		host := r.Host
		if host == "" && r.URL != nil {
			host = r.URL.Host
		}
		if host == "" {
			return "", errors.New("missing signed header: host")
		}
		return sigv4NormalizeHeaderValue(host), nil
	}
	values, ok := r.Header[http.CanonicalHeaderKey(name)]
	if !ok || len(values) == 0 {
		return "", fmt.Errorf("missing signed header: %s", name)
	}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, sigv4NormalizeHeaderValue(value))
	}
	return strings.Join(normalized, ","), nil
}

func sigv4StringToSign(amzDate string, credentialScope string, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		sigv4Algorithm,
		amzDate,
		credentialScope,
		hex.EncodeToString(sum[:]),
	}, "\n")
}

func sigv4CredentialScopeString(date string, region string) string {
	return strings.Join([]string{date, region, sigv4Service, sigv4TerminalScope}, "/")
}

func sigv4Signature(secret string, date string, region string, stringToSign string) string {
	key := sigv4SigningKey(secret, date, region)
	signature := sigv4HMAC(key, []byte(stringToSign))
	return hex.EncodeToString(signature)
}

func sigv4SigningKey(secret string, date string, region string) []byte {
	dateKey := sigv4HMAC([]byte("AWS4"+secret), []byte(date))
	regionKey := sigv4HMAC(dateKey, []byte(region))
	serviceKey := sigv4HMAC(regionKey, []byte(sigv4Service))
	return sigv4HMAC(serviceKey, []byte(sigv4TerminalScope))
}

func sigv4HMAC(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func sigv4CheckSignature(signature string) error {
	if len(signature) != 64 {
		return errors.New("signature: must be 64 lowercase hex characters")
	}
	for _, r := range signature {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return errors.New("signature: must be 64 lowercase hex characters")
		}
	}
	return nil
}

func sigv4CompareSignatures(actual string, expected string) error {
	actualBytes, err := hex.DecodeString(actual)
	if err != nil {
		return fmt.Errorf("signature: malformed signature: %w", err)
	}
	expectedBytes, err := hex.DecodeString(expected)
	if err != nil {
		return fmt.Errorf("signature: malformed expected signature: %w", err)
	}
	if !hmac.Equal(actualBytes, expectedBytes) {
		return errors.New("signature: signature mismatch")
	}
	return nil
}

func sigv4URIEncode(value string, allowSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '%' && i+2 < len(value) && sigv4IsHex(value[i+1]) && sigv4IsHex(value[i+2]) {
			b.WriteByte('%')
			b.WriteByte(sigv4UpperHex(value[i+1]))
			b.WriteByte(sigv4UpperHex(value[i+2]))
			i += 2
			continue
		}
		if sigv4IsUnreserved(c) || c == '/' && allowSlash {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte("0123456789ABCDEF"[c>>4])
		b.WriteByte("0123456789ABCDEF"[c&0x0f])
	}
	return b.String()
}

func sigv4NormalizeHeaderValue(value string) string {
	value = strings.TrimFunc(value, sigv4IsASCIIWhitespaceRune)
	var b strings.Builder
	inWhitespace := false
	for _, r := range value {
		if sigv4IsASCIIWhitespaceRune(r) {
			inWhitespace = true
			continue
		}
		if inWhitespace && b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		inWhitespace = false
	}
	return b.String()
}

func sigv4RawQueryPairs(rawQuery string) []sigv4QueryPair {
	parts := strings.Split(rawQuery, "&")
	pairs := make([]sigv4QueryPair, 0, len(parts))
	for _, part := range parts {
		key, value, hasEqual := strings.Cut(part, "=")
		if !hasEqual {
			value = ""
		}
		pairs = append(pairs, sigv4QueryPair{RawKey: key, RawValue: value})
	}
	return pairs
}

func sigv4QueryValue(rawQuery string, key string) (string, bool) {
	values := sigv4QueryValues(rawQuery, key)
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func sigv4RequiredQueryValue(rawQuery string, key string) (string, bool) {
	values := sigv4QueryValues(rawQuery, key)
	if len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func sigv4QueryValues(rawQuery string, key string) []string {
	if rawQuery == "" {
		return nil
	}
	values := []string{}
	for _, pair := range sigv4RawQueryPairs(rawQuery) {
		decodedKey, err := url.QueryUnescape(pair.RawKey)
		if err != nil || decodedKey != key {
			continue
		}
		decodedValue, err := url.QueryUnescape(pair.RawValue)
		if err != nil {
			continue
		}
		values = append(values, decodedValue)
	}
	return values
}

func sigv4HasSignedHeader(signedHeaders []string, header string) bool {
	for _, signedHeader := range signedHeaders {
		if signedHeader == header {
			return true
		}
	}
	return false
}

func sigv4ValidPayloadHash(value string) bool {
	return value == sigv4UnsignedPayload || sigv4IsSHA256PayloadHash(value) || sigv4IsStreamingPayloadHash(value)
}

func sigv4IsSHA256PayloadHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func sigv4IsStreamingPayloadHash(value string) bool {
	switch value {
	case sigv4StreamingAWS4HMACSHA256Payload, sigv4StreamingAWS4HMACSHA256PayloadTrailer, sigv4StreamingUnsignedPayloadTrailer:
		return true
	default:
		return false
	}
}

func sigv4IsReadOnlyMethod(method string) bool {
	method = strings.ToUpper(method)
	return method == http.MethodGet || method == http.MethodHead
}

func sigv4IsUnreserved(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~'
}

func sigv4IsHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func sigv4UpperHex(c byte) byte {
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 'A'
	}
	return c
}

func sigv4AllDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func sigv4IsASCIIWhitespaceRune(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
}

func sigv4AbsDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
