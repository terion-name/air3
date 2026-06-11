package publicpath

import (
	"fmt"
	"net/url"
	"strings"
)

type Mode int

const (
	ModeSingle Mode = iota
	ModeMulti
)

type Object struct {
	Server string
	Bucket string
	Key    string
}

type DefaultBucketResolver func(server string) (bucket string, ok bool)

func ParseEscapedPath(escapedPath string, mode Mode) (Object, error) {
	if !strings.HasPrefix(escapedPath, "/") {
		return Object{}, fmt.Errorf("public path: missing leading slash")
	}

	parts := strings.Split(escapedPath[1:], "/")
	switch mode {
	case ModeSingle:
		if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
			return Object{}, fmt.Errorf("public path: missing bucket")
		}
		if parts[0] == "" {
			return Object{}, fmt.Errorf("public path: empty bucket")
		}
		if len(parts) < 2 {
			return Object{}, fmt.Errorf("public path: missing key")
		}

		bucket, err := unescapeSegment("bucket", parts[0])
		if err != nil {
			return Object{}, err
		}
		key, err := unescapeKey(parts[1:])
		if err != nil {
			return Object{}, err
		}
		if bucket == "" {
			return Object{}, fmt.Errorf("public path: empty bucket")
		}
		if key == "" {
			return Object{}, fmt.Errorf("public path: empty key")
		}
		return Object{Bucket: bucket, Key: key}, nil

	case ModeMulti:
		if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
			return Object{}, fmt.Errorf("public path: missing server")
		}
		if parts[0] == "" {
			return Object{}, fmt.Errorf("public path: missing server")
		}
		if len(parts) < 2 {
			return Object{}, fmt.Errorf("public path: missing bucket")
		}
		if parts[1] == "" {
			return Object{}, fmt.Errorf("public path: empty bucket")
		}
		if len(parts) < 3 {
			return Object{}, fmt.Errorf("public path: missing key")
		}

		server, err := unescapeSegment("server", parts[0])
		if err != nil {
			return Object{}, err
		}
		if err := ValidateAlias(server); err != nil {
			return Object{}, fmt.Errorf("public path: invalid server alias %q: %w", server, err)
		}
		bucket, err := unescapeSegment("bucket", parts[1])
		if err != nil {
			return Object{}, err
		}
		key, err := unescapeKey(parts[2:])
		if err != nil {
			return Object{}, err
		}
		if bucket == "" {
			return Object{}, fmt.Errorf("public path: empty bucket")
		}
		if key == "" {
			return Object{}, fmt.Errorf("public path: empty key")
		}
		return Object{Server: server, Bucket: bucket, Key: key}, nil

	default:
		return Object{}, fmt.Errorf("public path: unknown mode %d", mode)
	}
}

func ParseEscapedPathWithDefaultBucket(escapedPath string, mode Mode, defaultBucket DefaultBucketResolver) (Object, error) {
	if defaultBucket == nil {
		return ParseEscapedPath(escapedPath, mode)
	}

	switch mode {
	case ModeSingle:
		return parseSingleEscapedPathWithDefaultBucket(escapedPath, defaultBucket)
	case ModeMulti:
		return parseMultiEscapedPathWithDefaultBucket(escapedPath, defaultBucket)
	default:
		return ParseEscapedPath(escapedPath, mode)
	}
}

func parseSingleEscapedPathWithDefaultBucket(escapedPath string, defaultBucket DefaultBucketResolver) (Object, error) {
	bucket, ok := defaultBucket("")
	if !ok {
		return ParseEscapedPath(escapedPath, ModeSingle)
	}
	if bucket == "" {
		return Object{}, fmt.Errorf("public path: empty default bucket for server %q", "")
	}
	if !strings.HasPrefix(escapedPath, "/") {
		return Object{}, fmt.Errorf("public path: missing leading slash")
	}
	if escapedPath == "/" {
		return Object{}, fmt.Errorf("public path: missing key")
	}

	key, err := unescapeKey(strings.Split(escapedPath[1:], "/"))
	if err != nil {
		return Object{}, err
	}
	if key == "" {
		return Object{}, fmt.Errorf("public path: empty key")
	}
	return Object{Bucket: bucket, Key: key}, nil
}

func parseMultiEscapedPathWithDefaultBucket(escapedPath string, defaultBucket DefaultBucketResolver) (Object, error) {
	if !strings.HasPrefix(escapedPath, "/") {
		return Object{}, fmt.Errorf("public path: missing leading slash")
	}

	parts := strings.Split(escapedPath[1:], "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return Object{}, fmt.Errorf("public path: missing server")
	}
	if parts[0] == "" {
		return Object{}, fmt.Errorf("public path: missing server")
	}

	server, err := unescapeSegment("server", parts[0])
	if err != nil {
		return Object{}, err
	}
	if err := ValidateAlias(server); err != nil {
		return Object{}, fmt.Errorf("public path: invalid server alias %q: %w", server, err)
	}

	bucket, ok := defaultBucket(server)
	if !ok {
		return ParseEscapedPath(escapedPath, ModeMulti)
	}
	if bucket == "" {
		return Object{}, fmt.Errorf("public path: empty default bucket for server %q", server)
	}
	if len(parts) < 2 {
		return Object{}, fmt.Errorf("public path: missing key")
	}

	key, err := unescapeKey(parts[1:])
	if err != nil {
		return Object{}, err
	}
	if key == "" {
		return Object{}, fmt.Errorf("public path: empty key")
	}
	return Object{Server: server, Bucket: bucket, Key: key}, nil
}

func ValidateAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias is empty")
	}
	if len(alias) > 63 {
		return fmt.Errorf("alias is too long")
	}
	if !isAliasAlphaNum(alias[0]) {
		return fmt.Errorf("alias must start with an ASCII letter or digit")
	}
	for i := 1; i < len(alias); i++ {
		if !isAliasChar(alias[i]) {
			return fmt.Errorf("alias contains invalid character %q", alias[i])
		}
	}
	return nil
}

func EnvSuffix(alias string) string {
	return strings.ToUpper(strings.ReplaceAll(alias, "-", "_"))
}

func EnvSuffixCollisions(aliases []string) map[string][]string {
	bySuffix := make(map[string][]string)
	for _, alias := range aliases {
		suffix := EnvSuffix(alias)
		bySuffix[suffix] = append(bySuffix[suffix], alias)
	}

	collisions := make(map[string][]string)
	for suffix, suffixAliases := range bySuffix {
		if len(suffixAliases) > 1 {
			collisions[suffix] = suffixAliases
		}
	}
	return collisions
}

func unescapeSegment(name, escaped string) (string, error) {
	value, err := url.PathUnescape(escaped)
	if err != nil {
		return "", fmt.Errorf("public path: bad escape in %s: %w", name, err)
	}
	return value, nil
}

func unescapeKey(escapedSegments []string) (string, error) {
	segments := make([]string, len(escapedSegments))
	for i, escapedSegment := range escapedSegments {
		segment, err := unescapeSegment("key", escapedSegment)
		if err != nil {
			return "", err
		}
		segments[i] = segment
	}
	return strings.Join(segments, "/"), nil
}

func isAliasChar(ch byte) bool {
	return isAliasAlphaNum(ch) || ch == '_' || ch == '-'
}

func isAliasAlphaNum(ch byte) bool {
	return ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9')
}
