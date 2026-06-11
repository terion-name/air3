package edgesign

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/terion-name/air3/internal/publicpath"
	"github.com/terion-name/air3/internal/signing"
)

const (
	ParamExpires                    = signing.ParamExpires
	ParamSig                        = signing.ParamSig
	ParamRange                      = signing.ParamRange
	ParamResponseContentType        = signing.ParamResponseContentType
	ParamResponseContentDisposition = signing.ParamResponseContentDisposition
)

var (
	ErrInvalidSignature = signing.ErrInvalidSignature
	ErrExpired          = signing.ErrExpired
	ErrUnsignedRange    = errors.New("range header is not signed")
	ErrRangeMismatch    = errors.New("range header does not match signed range")
)

// SignInput describes one public edge object URL to sign.
//
// DefaultBucketPath applies in single-server and multi-server modes. When set,
// SignURL emits /{key} in single-server mode or /{server}/{key} in
// multi-server mode while signing the canonical claims with the real Bucket.
type SignInput struct {
	Method                     string
	BaseURL                    string
	Server                     string
	Bucket                     string
	Key                        string
	Secret                     string
	Expires                    time.Time
	Range                      string
	ResponseContentType        string
	ResponseContentDisposition string
	DefaultBucketPath          bool
}

// Claims are the signed fields decoded from an edge URL.
type Claims struct {
	Method                     string
	Server                     string
	Bucket                     string
	Key                        string
	Expires                    time.Time
	Range                      string
	ResponseContentType        string
	ResponseContentDisposition string
}

// VerifyInput describes one signed edge URL verification.
//
// Range is the optional HTTP Range header supplied with the request. When it is
// set, the URL must include the same signed range claim. When it is empty, a
// signed range claim remains valid and is returned in Claims.Range.
//
// DefaultBucket applies in single-server and multi-server modes. When set,
// VerifyURL also accepts /{key} in single-server mode or /{server}/{key} in
// multi-server mode and verifies the signature against DefaultBucket as the real
// signed bucket claim.
type VerifyInput struct {
	Method        string
	URL           string
	Server        string
	Secret        string
	Now           time.Time
	Range         string
	DefaultBucket string
}

// SignURL returns BaseURL/{bucket}/{key}?expires=...&sig=... using the edge
// gateway HMAC canonical form unless DefaultBucketPath requests the short
// /{key} form. When Server is set, it returns
// BaseURL/{server}/{bucket}/{key}?expires=...&sig=... using multi-server mode
// unless DefaultBucketPath requests the short /{server}/{key} form.
func SignURL(input SignInput) (string, error) {
	signingInput := signing.SignInput{
		Method:                     input.Method,
		BaseURL:                    input.BaseURL,
		Server:                     input.Server,
		Bucket:                     input.Bucket,
		Key:                        input.Key,
		Range:                      input.Range,
		ResponseContentType:        input.ResponseContentType,
		ResponseContentDisposition: input.ResponseContentDisposition,
		Expires:                    input.Expires,
		Secret:                     input.Secret,
	}
	if input.Server == "" {
		if input.DefaultBucketPath {
			return signing.SignURLForModeWithOptions(signingInput, publicpath.ModeSingle, signing.SignOptions{DefaultBucketPath: true})
		}
		return signing.SignURL(signingInput)
	}
	raw, err := signing.SignURLForMode(signingInput, publicpath.ModeMulti)
	if err != nil || !input.DefaultBucketPath {
		return raw, err
	}
	return withDefaultBucketPath(raw, input.BaseURL, input.Server, input.Key)
}

// VerifyURL validates the signature and expiration in input.URL for input.Method.
func VerifyURL(input VerifyInput) (Claims, error) {
	claims, err := validateURL(input)
	if err != nil {
		return Claims{}, err
	}
	if input.Server != "" && claims.Server != input.Server {
		return Claims{}, ErrInvalidSignature
	}
	if strings.TrimSpace(input.Range) != "" {
		rangeHeader := strings.TrimSpace(input.Range)
		if claims.Range == "" {
			return Claims{}, ErrUnsignedRange
		}
		if claims.Range != rangeHeader {
			return Claims{}, ErrRangeMismatch
		}
	}
	return Claims{
		Method:                     claims.Method,
		Server:                     claims.Server,
		Bucket:                     claims.Bucket,
		Key:                        claims.Key,
		Expires:                    claims.Expires,
		Range:                      claims.Range,
		ResponseContentType:        claims.ResponseContentType,
		ResponseContentDisposition: claims.ResponseContentDisposition,
	}, nil
}

func validateURL(input VerifyInput) (signing.Claims, error) {
	cfg := signing.ValidationConfig{Secret: input.Secret}
	if input.Server == "" {
		if input.DefaultBucket != "" {
			return signing.ValidateURLForModeWithOptions(input.Method, input.URL, cfg, input.Now, publicpath.ModeSingle, signing.ValidationOptions{
				DefaultBucket: func(server string) (string, bool) {
					return input.DefaultBucket, server == ""
				},
			})
		}
		return signing.ValidateURL(input.Method, input.URL, cfg, input.Now)
	}
	claims, err := signing.ValidateURLForMode(input.Method, input.URL, cfg, input.Now, publicpath.ModeMulti)
	if err == nil || input.DefaultBucket == "" {
		return claims, err
	}
	return validateDefaultBucketURL(input, cfg)
}

func withDefaultBucketPath(rawURL, baseURL, server, key string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse base url: %w", err)
	}
	u.Path = objectPath(base.Path, publicpath.Object{Server: server, Key: key})
	u.RawPath = ""
	return u.String(), nil
}

func validateDefaultBucketURL(input VerifyInput, cfg signing.ValidationConfig) (signing.Claims, error) {
	u, err := url.Parse(input.URL)
	if err != nil {
		return signing.Claims{}, fmt.Errorf("parse signed url: %w", err)
	}
	object, err := defaultBucketObject(u.EscapedPath(), input.DefaultBucket)
	if err != nil {
		return signing.Claims{}, err
	}
	u.Path = objectPath("", object)
	u.RawPath = ""
	return signing.ValidateURLForMode(input.Method, u.String(), cfg, input.Now, publicpath.ModeMulti)
}

func defaultBucketObject(escapedPath, defaultBucket string) (publicpath.Object, error) {
	if !strings.HasPrefix(escapedPath, "/") {
		return publicpath.Object{}, fmt.Errorf("public path: missing leading slash")
	}
	parts := strings.Split(escapedPath[1:], "/")
	if len(parts) < 2 || parts[0] == "" {
		return publicpath.Object{}, fmt.Errorf("public path: missing server")
	}
	server, err := url.PathUnescape(parts[0])
	if err != nil {
		return publicpath.Object{}, fmt.Errorf("public path: bad escape in server: %w", err)
	}
	if err := publicpath.ValidateAlias(server); err != nil {
		return publicpath.Object{}, fmt.Errorf("public path: invalid server alias %q: %w", server, err)
	}
	keyParts := make([]string, len(parts)-1)
	for i, escapedPart := range parts[1:] {
		part, err := url.PathUnescape(escapedPart)
		if err != nil {
			return publicpath.Object{}, fmt.Errorf("public path: bad escape in key: %w", err)
		}
		keyParts[i] = part
	}
	return publicpath.Object{Server: server, Bucket: defaultBucket, Key: strings.Join(keyParts, "/")}, nil
}

func objectPath(basePath string, object publicpath.Object) string {
	parts := []string{strings.Trim(basePath, "/")}
	if object.Server != "" {
		parts = append(parts, object.Server)
	}
	if object.Bucket != "" {
		parts = append(parts, object.Bucket)
	}
	parts = append(parts, strings.Split(object.Key, "/")...)
	return "/" + strings.Join(nonEmpty(parts), "/")
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

// CanonicalString returns the exact newline-delimited text signed by the edge
// gateway for these claims.
func CanonicalString(claims Claims) string {
	fields := []string{strings.ToUpper(claims.Method)}
	if claims.Server != "" {
		fields = append(fields, claims.Server)
	}
	fields = append(fields,
		claims.Bucket,
		claims.Key,
		formatUnixSeconds(claims.Expires),
		claims.Range,
		claims.ResponseContentType,
		claims.ResponseContentDisposition,
	)
	return strings.Join(fields, "\n")
}

func formatUnixSeconds(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}
