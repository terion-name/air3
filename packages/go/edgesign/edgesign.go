package edgesign

import (
	"errors"
	"strconv"
	"strings"
	"time"

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
type SignInput struct {
	Method                     string
	BaseURL                    string
	Bucket                     string
	Key                        string
	Secret                     string
	Expires                    time.Time
	Range                      string
	ResponseContentType        string
	ResponseContentDisposition string
}

// Claims are the signed fields decoded from an edge URL.
type Claims struct {
	Method                     string
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
type VerifyInput struct {
	Method string
	URL    string
	Secret string
	Now    time.Time
	Range  string
}

// SignURL returns BaseURL/{bucket}/{key}?expires=...&sig=... using the edge
// gateway HMAC canonical form.
func SignURL(input SignInput) (string, error) {
	return signing.SignURL(signing.SignInput{
		Method:                     input.Method,
		BaseURL:                    input.BaseURL,
		Bucket:                     input.Bucket,
		Key:                        input.Key,
		Range:                      input.Range,
		ResponseContentType:        input.ResponseContentType,
		ResponseContentDisposition: input.ResponseContentDisposition,
		Expires:                    input.Expires,
		Secret:                     input.Secret,
	})
}

// VerifyURL validates the signature and expiration in input.URL for input.Method.
func VerifyURL(input VerifyInput) (Claims, error) {
	claims, err := signing.ValidateURL(input.Method, input.URL, signing.ValidationConfig{Secret: input.Secret}, input.Now)
	if err != nil {
		return Claims{}, err
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
		Bucket:                     claims.Bucket,
		Key:                        claims.Key,
		Expires:                    claims.Expires,
		Range:                      claims.Range,
		ResponseContentType:        claims.ResponseContentType,
		ResponseContentDisposition: claims.ResponseContentDisposition,
	}, nil
}

// CanonicalString returns the exact newline-delimited text signed by the edge
// gateway for these claims.
func CanonicalString(claims Claims) string {
	return strings.Join([]string{
		strings.ToUpper(claims.Method),
		claims.Bucket,
		claims.Key,
		formatUnixSeconds(claims.Expires),
		claims.Range,
		claims.ResponseContentType,
		claims.ResponseContentDisposition,
	}, "\n")
}

func formatUnixSeconds(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}
