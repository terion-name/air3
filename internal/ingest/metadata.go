package ingest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/terion-name/air3/internal/pending"
)

const maxMetadataFieldBytes = 8 * 1024

// ValidateMetadata trims and validates the allowlisted connector response
// metadata before it is committed to a pending response target.
func ValidateMetadata(metadata pending.Metadata) (pending.Metadata, error) {
	var err error
	if metadata.ContentType, err = validateMetadataString("content type", metadata.ContentType); err != nil {
		return pending.Metadata{}, err
	}
	if metadata.ContentLength, err = validateMetadataString("content length", metadata.ContentLength); err != nil {
		return pending.Metadata{}, err
	}
	if metadata.ContentRange, err = validateMetadataString("content range", metadata.ContentRange); err != nil {
		return pending.Metadata{}, err
	}
	if metadata.ETag, err = validateMetadataString("etag", metadata.ETag); err != nil {
		return pending.Metadata{}, err
	}
	if metadata.LastModified, err = validateMetadataString("last modified", metadata.LastModified); err != nil {
		return pending.Metadata{}, err
	}
	if metadata.AcceptRanges, err = validateMetadataString("accept ranges", metadata.AcceptRanges); err != nil {
		return pending.Metadata{}, err
	}

	if metadata.StatusCode != 0 && (metadata.StatusCode < 100 || metadata.StatusCode > 599) {
		return pending.Metadata{}, fmt.Errorf("invalid status code")
	}
	if metadata.ContentLength != "" {
		length, err := strconv.ParseInt(metadata.ContentLength, 10, 64)
		if err != nil || length < 0 {
			return pending.Metadata{}, fmt.Errorf("invalid content length")
		}
	}
	return metadata, nil
}

func validateMetadataString(name, value string) (string, error) {
	if strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("unsafe %s", name)
	}
	value = strings.TrimSpace(value)
	if len(value) > maxMetadataFieldBytes {
		return "", fmt.Errorf("%s is too large", name)
	}
	return value, nil
}
