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

	"github.com/terion-name/air3/internal/publicpath"
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
	Server                     string
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
	Server                     string
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

// SignOptions controls opt-in signing behavior. The zero value preserves the
// strict public path layout.
type SignOptions struct {
	DefaultBucketPath bool
}

// ValidationOptions controls opt-in signed URL validation behavior. The zero
// value preserves strict public path parsing.
type ValidationOptions struct {
	DefaultBucket publicpath.DefaultBucketResolver
}

// SignURL returns BaseURL/{bucket}/{key}?expires=...&sig=... using the same
// canonical form that ValidateURL verifies.
func SignURL(input SignInput) (string, error) {
	return SignURLForModeWithOptions(input, publicpath.ModeSingle, SignOptions{})
}

// SignURLForMode returns a signed URL using the public path layout for mode.
func SignURLForMode(input SignInput, mode publicpath.Mode) (string, error) {
	return SignURLForModeWithOptions(input, mode, SignOptions{})
}

// SignURLForModeWithOptions returns a signed URL using the public path layout
// for mode and opt-in signing behavior.
func SignURLForModeWithOptions(input SignInput, mode publicpath.Mode, opts SignOptions) (string, error) {
	if err := validateMode(mode); err != nil {
		return "", err
	}
	if input.Secret == "" {
		return "", errors.New("signing secret is required")
	}
	claims, err := claimsFromInput(input, mode)
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
	object := publicpath.Object{
		Server: claims.Server,
		Bucket: claims.Bucket,
		Key:    claims.Key,
	}
	objectPath, err := appendObjectPath(u.Path, object, mode)
	if opts.DefaultBucketPath {
		objectPath, err = appendDefaultBucketObjectPath(u.Path, object)
	}
	if err != nil {
		return "", err
	}
	u.Path = objectPath

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
	return ValidateURLForModeWithOptions(method, rawURL, cfg, now, publicpath.ModeSingle, ValidationOptions{})
}

// ValidateURLWithOptions verifies the HMAC signature embedded in rawURL for
// method and opt-in validation behavior.
func ValidateURLWithOptions(method, rawURL string, cfg ValidationConfig, now time.Time, opts ValidationOptions) (Claims, error) {
	return ValidateURLForModeWithOptions(method, rawURL, cfg, now, publicpath.ModeSingle, opts)
}

// ValidateURLForMode verifies the HMAC signature embedded in rawURL for method
// using the public path layout for mode.
func ValidateURLForMode(method, rawURL string, cfg ValidationConfig, now time.Time, mode publicpath.Mode) (Claims, error) {
	return ValidateURLForModeWithOptions(method, rawURL, cfg, now, mode, ValidationOptions{})
}

// ValidateURLForModeWithOptions verifies the HMAC signature embedded in rawURL
// for method using the public path layout for mode and opt-in validation
// behavior.
func ValidateURLForModeWithOptions(method, rawURL string, cfg ValidationConfig, now time.Time, mode publicpath.Mode, opts ValidationOptions) (Claims, error) {
	if err := validateMode(mode); err != nil {
		return Claims{}, err
	}
	claims, sig, err := claimsFromURL(method, rawURL, mode, opts)
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

func claimsFromInput(input SignInput, mode publicpath.Mode) (Claims, error) {
	if err := validateMode(mode); err != nil {
		return Claims{}, err
	}
	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if method != "GET" && method != "HEAD" {
		return Claims{}, errors.New("method must be GET or HEAD")
	}
	server := ""
	if mode == publicpath.ModeMulti {
		if err := publicpath.ValidateAlias(input.Server); err != nil {
			return Claims{}, fmt.Errorf("server alias: %w", err)
		}
		server = input.Server
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
		Server:                     server,
		Bucket:                     input.Bucket,
		Key:                        input.Key,
		Range:                      input.Range,
		ResponseContentType:        input.ResponseContentType,
		ResponseContentDisposition: input.ResponseContentDisposition,
		Expires:                    time.Unix(input.Expires.Unix(), 0).UTC(),
	}, nil
}

func claimsFromURL(method, rawURL string, mode publicpath.Mode, opts ValidationOptions) (Claims, string, error) {
	if err := validateMode(mode); err != nil {
		return Claims{}, "", err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return Claims{}, "", fmt.Errorf("parse signed url: %w", err)
	}
	object, err := objectFromEscapedPath(u.EscapedPath(), mode, opts)
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
		Server:                     object.Server,
		Bucket:                     object.Bucket,
		Key:                        object.Key,
		Range:                      q.Get(ParamRange),
		ResponseContentType:        q.Get(ParamResponseContentType),
		ResponseContentDisposition: q.Get(ParamResponseContentDisposition),
		Expires:                    time.Unix(expiresUnix, 0).UTC(),
	}, mode)
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
	fields := []string{strings.ToUpper(c.Method)}
	if c.Server != "" {
		fields = append(fields, c.Server)
	}
	fields = append(fields,
		c.Bucket,
		c.Key,
		fmt.Sprintf("%d", c.Expires.Unix()),
		c.Range,
		c.ResponseContentType,
		c.ResponseContentDisposition,
	)
	return strings.Join(fields, "\n")
}

func appendObjectPath(basePath string, object publicpath.Object, mode publicpath.Mode) (string, error) {
	parts := []string{strings.Trim(basePath, "/")}
	switch mode {
	case publicpath.ModeSingle:
	case publicpath.ModeMulti:
		parts = append(parts, url.PathEscape(object.Server))
	default:
		return "", unknownModeError(mode)
	}
	parts = append(parts, url.PathEscape(object.Bucket))
	for _, segment := range strings.Split(object.Key, "/") {
		parts = append(parts, url.PathEscape(segment))
	}
	return "/" + strings.Join(nonEmpty(parts), "/"), nil
}

func objectFromEscapedPath(escapedPath string, mode publicpath.Mode, opts ValidationOptions) (publicpath.Object, error) {
	if opts.DefaultBucket != nil {
		return publicpath.ParseEscapedPathWithDefaultBucket(escapedPath, mode, opts.DefaultBucket)
	}
	switch mode {
	case publicpath.ModeSingle:
		bucket, key, err := objectFromPath(escapedPath)
		if err != nil {
			return publicpath.Object{}, err
		}
		return publicpath.Object{Bucket: bucket, Key: key}, nil
	case publicpath.ModeMulti:
		return publicpath.ParseEscapedPath(escapedPath, mode)
	default:
		return publicpath.Object{}, unknownModeError(mode)
	}
}

func appendDefaultBucketObjectPath(basePath string, object publicpath.Object) (string, error) {
	parts := []string{strings.Trim(basePath, "/"), url.PathEscape(object.Server)}
	for _, segment := range strings.Split(object.Key, "/") {
		parts = append(parts, url.PathEscape(segment))
	}
	return "/" + strings.Join(nonEmpty(parts), "/"), nil
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

func validateMode(mode publicpath.Mode) error {
	switch mode {
	case publicpath.ModeSingle, publicpath.ModeMulti:
		return nil
	default:
		return unknownModeError(mode)
	}
}

func unknownModeError(mode publicpath.Mode) error {
	return fmt.Errorf("unknown public path mode %d", mode)
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
