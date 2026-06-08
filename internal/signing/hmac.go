package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/terion-name/air3/internal/tickets"
)

const (
	ParamExpires = "expires"
	ParamSig     = "sig"
	ParamRange   = "range"

	ParamResponseContentType        = "response-content-type"
	ParamResponseContentDisposition = "response-content-disposition"
)

var (
	ErrInvalidSignature = errors.New("invalid signature")
	ErrExpired          = errors.New("signature expired")
	ErrSigningDisabled  = errors.New("signing disabled")
)

// SignInput describes one public object URL to sign.
type SignInput struct {
	Method                     string
	BaseURL                    string
	Bucket                     string
	Key                        string
	Range                      string
	ResponseContentType        string
	ResponseContentDisposition string
	Expires                    time.Time
	Secret                     string
}

type Claims struct {
	Method                     string
	Bucket                     string
	Key                        string
	Range                      string
	ResponseContentType        string
	ResponseContentDisposition string
	Expires                    time.Time
}

type ValidationConfig struct {
	Secret   string
	Disabled bool
}

// SignURL returns BaseURL/{bucket}/{key}?expires=...&sig=... using the same
// canonical form that ValidateURL verifies.
func SignURL(input SignInput) (string, error) {
	if input.Secret == "" {
		return "", errors.New("signing secret is required")
	}
	claims, err := claimsFromInput(input)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(input.BaseURL)
	if err != nil {
		return "", fmt.Errorf("parse base url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", errors.New("base url must be absolute")
	}
	u.Path = appendObjectPath(u.Path, input.Bucket, input.Key)

	q := u.Query()
	q.Set(ParamExpires, fmt.Sprintf("%d", input.Expires.Unix()))
	if input.Range != "" {
		q.Set(ParamRange, input.Range)
	}
	if input.ResponseContentType != "" {
		q.Set(ParamResponseContentType, input.ResponseContentType)
	}
	if input.ResponseContentDisposition != "" {
		q.Set(ParamResponseContentDisposition, input.ResponseContentDisposition)
	}
	q.Set(ParamSig, signatureHex(claims, input.Secret))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// ValidateURL verifies the HMAC signature embedded in rawURL for method.
//
// The canonical form is deterministic UTF-8 text with one field per line:
//
//	METHOD
//	bucket
//	key
//	expires_unix_seconds
//	range
//	response-content-type
//	response-content-disposition
//
// The signature is lowercase hex HMAC-SHA256 over that exact string. The
// comparison uses hmac.Equal so callers do not leak timing information.
func ValidateURL(method, rawURL string, cfg ValidationConfig, now time.Time) (Claims, error) {
	claims, sig, err := claimsFromURL(method, rawURL)
	if err != nil {
		return Claims{}, err
	}
	if cfg.Disabled {
		return claims, nil
	}
	if cfg.Secret == "" {
		return Claims{}, errors.New("signing secret is required")
	}
	if sig == "" {
		return Claims{}, ErrInvalidSignature
	}
	if !claims.Expires.After(now) {
		return Claims{}, ErrExpired
	}
	supplied, err := hex.DecodeString(sig)
	if err != nil {
		return Claims{}, ErrInvalidSignature
	}
	expected := signatureBytes(claims, cfg.Secret)
	if !hmac.Equal(supplied, expected) {
		return Claims{}, ErrInvalidSignature
	}
	return claims, nil
}

func claimsFromInput(input SignInput) (Claims, error) {
	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if method != "GET" && method != "HEAD" {
		return Claims{}, errors.New("method must be GET or HEAD")
	}
	if err := tickets.ValidateBucket(input.Bucket); err != nil {
		return Claims{}, err
	}
	if err := tickets.ValidateKey(input.Key); err != nil {
		return Claims{}, err
	}
	if input.Expires.IsZero() {
		return Claims{}, errors.New("expiration is required")
	}
	return Claims{
		Method:                     method,
		Bucket:                     input.Bucket,
		Key:                        input.Key,
		Range:                      input.Range,
		ResponseContentType:        input.ResponseContentType,
		ResponseContentDisposition: input.ResponseContentDisposition,
		Expires:                    time.Unix(input.Expires.Unix(), 0).UTC(),
	}, nil
}

func claimsFromURL(method, rawURL string) (Claims, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return Claims{}, "", fmt.Errorf("parse signed url: %w", err)
	}
	bucket, key, err := objectFromPath(u.EscapedPath())
	if err != nil {
		return Claims{}, "", err
	}
	q := u.Query()
	expiryText := q.Get(ParamExpires)
	if expiryText == "" {
		return Claims{}, "", errors.New("expires query parameter is required")
	}
	expiresUnix, err := parseUnixSeconds(expiryText)
	if err != nil {
		return Claims{}, "", err
	}
	claims, err := claimsFromInput(SignInput{
		Method:                     method,
		Bucket:                     bucket,
		Key:                        key,
		Range:                      q.Get(ParamRange),
		ResponseContentType:        q.Get(ParamResponseContentType),
		ResponseContentDisposition: q.Get(ParamResponseContentDisposition),
		Expires:                    time.Unix(expiresUnix, 0).UTC(),
	})
	if err != nil {
		return Claims{}, "", err
	}
	return claims, q.Get(ParamSig), nil
}

func signatureHex(claims Claims, secret string) string {
	return hex.EncodeToString(signatureBytes(claims, secret))
}

func signatureBytes(claims Claims, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(canonicalString(claims)))
	return mac.Sum(nil)
}

func canonicalString(c Claims) string {
	return strings.Join([]string{
		strings.ToUpper(c.Method),
		c.Bucket,
		c.Key,
		fmt.Sprintf("%d", c.Expires.Unix()),
		c.Range,
		c.ResponseContentType,
		c.ResponseContentDisposition,
	}, "\n")
}

func appendObjectPath(basePath, bucket, key string) string {
	parts := []string{strings.Trim(basePath, "/"), url.PathEscape(bucket)}
	for _, segment := range strings.Split(key, "/") {
		parts = append(parts, url.PathEscape(segment))
	}
	return "/" + strings.Join(nonEmpty(parts), "/")
}

func objectFromPath(escapedPath string) (string, string, error) {
	cleaned := strings.TrimPrefix(path.Clean("/"+escapedPath), "/")
	if cleaned == "." || cleaned == "" {
		return "", "", errors.New("signed url path must include bucket and key")
	}
	parts := strings.SplitN(cleaned, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("signed url path must include bucket and key")
	}
	bucket, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("decode bucket path: %w", err)
	}
	key, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("decode key path: %w", err)
	}
	return bucket, key, nil
}

func parseUnixSeconds(text string) (int64, error) {
	var n int64
	for _, r := range text {
		if r < '0' || r > '9' {
			return 0, errors.New("expires query parameter must be unix seconds")
		}
		n = n*10 + int64(r-'0')
	}
	if n <= 0 {
		return 0, errors.New("expires query parameter must be positive")
	}
	return n, nil
}

func nonEmpty(parts []string) []string {
	out := parts[:0]
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
