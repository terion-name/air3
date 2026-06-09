package ingest

import (
	"strconv"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/pending"
)

func TestValidateMetadataAllowsAndTrimsPublicMetadata(t *testing.T) {
	got, err := ValidateMetadata(pending.Metadata{
		StatusCode:    206,
		ContentType:   " application/octet-stream ",
		ContentLength: " 123 ",
		ContentRange:  " bytes 0-122/123 ",
		ETag:          " \"abc\" ",
		LastModified:  " Mon, 08 Jun 2026 12:00:00 GMT ",
		AcceptRanges:  " bytes ",
	})
	if err != nil {
		t.Fatalf("ValidateMetadata() error = %v", err)
	}
	want := pending.Metadata{
		StatusCode:    206,
		ContentType:   "application/octet-stream",
		ContentLength: "123",
		ContentRange:  "bytes 0-122/123",
		ETag:          "\"abc\"",
		LastModified:  "Mon, 08 Jun 2026 12:00:00 GMT",
		AcceptRanges:  "bytes",
	}
	if got != want {
		t.Fatalf("metadata = %#v, want %#v", got, want)
	}
}

func TestValidateMetadataRejectsLineBreaksInStringFields(t *testing.T) {
	tests := []struct {
		name string
		set  func(*pending.Metadata)
	}{
		{name: "content type", set: func(m *pending.Metadata) { m.ContentType = "ok\nbad" }},
		{name: "content length", set: func(m *pending.Metadata) { m.ContentLength = "1\n2" }},
		{name: "content range", set: func(m *pending.Metadata) { m.ContentRange = "ok\rbad" }},
		{name: "etag", set: func(m *pending.Metadata) { m.ETag = "ok\nbad" }},
		{name: "last modified", set: func(m *pending.Metadata) { m.LastModified = "ok\rbad" }},
		{name: "accept ranges", set: func(m *pending.Metadata) { m.AcceptRanges = "ok\nbad" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := pending.Metadata{}
			tt.set(&metadata)
			if _, err := ValidateMetadata(metadata); err == nil {
				t.Fatal("ValidateMetadata() error = nil, want error")
			}
		})
	}
}

func TestValidateMetadataRejectsInvalidStatusValues(t *testing.T) {
	for _, status := range []int{-1, 99, 600} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			if _, err := ValidateMetadata(pending.Metadata{StatusCode: status}); err == nil {
				t.Fatalf("ValidateMetadata(StatusCode=%d) error = nil, want error", status)
			}
		})
	}
	for _, status := range []int{0, 100, 599} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			if _, err := ValidateMetadata(pending.Metadata{StatusCode: status}); err != nil {
				t.Fatalf("ValidateMetadata(StatusCode=%d) error = %v, want nil", status, err)
			}
		})
	}
}

func TestValidateMetadataRejectsInvalidContentLength(t *testing.T) {
	tests := []string{"abc", "-1", strings.Repeat("1", maxMetadataFieldBytes+1), "9223372036854775808"}
	for _, contentLength := range tests {
		t.Run(contentLength[:min(len(contentLength), 32)], func(t *testing.T) {
			if _, err := ValidateMetadata(pending.Metadata{ContentLength: contentLength}); err == nil {
				t.Fatal("ValidateMetadata() error = nil, want error")
			}
		})
	}
}

func TestValidateMetadataRejectsOversizedStringField(t *testing.T) {
	if _, err := ValidateMetadata(pending.Metadata{ContentType: strings.Repeat("a", maxMetadataFieldBytes+1)}); err == nil {
		t.Fatal("ValidateMetadata() error = nil, want error")
	}
}
