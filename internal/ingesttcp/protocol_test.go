package ingesttcp

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/pending"
)

func TestHeaderRoundTrip(t *testing.T) {
	want := Header{
		RequestID:   "req-1",
		IngestToken: "token-1",
		BodyLength:  11,
		Metadata: pending.Metadata{
			StatusCode:    206,
			ContentType:   " text/plain ",
			ContentLength: "11",
			ContentRange:  "bytes 0-10/11",
			ETag:          `"abc"`,
			LastModified:  "Tue, 09 Jun 2026 12:00:00 GMT",
			AcceptRanges:  "bytes",
		},
	}
	var buf bytes.Buffer
	if err := EncodeHeader(&buf, want); err != nil {
		t.Fatalf("EncodeHeader() error = %v", err)
	}
	got, err := DecodeHeader(&buf, DefaultMaxHeaderBytes)
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}
	want.Metadata.ContentType = "text/plain"
	if got != want {
		t.Fatalf("DecodeHeader() = %#v, want %#v", got, want)
	}
}

func TestDecodeHeaderRejectsBadMagic(t *testing.T) {
	data := append(prefixWithLength(2), []byte("{}")...)
	data[0] = 'X'
	if _, err := DecodeHeader(bytes.NewReader(data), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want bad magic error")
	}
}

func TestDecodeHeaderRejectsBadVersion(t *testing.T) {
	data := append(prefixWithLength(2), []byte("{}")...)
	data[4] = 99
	if _, err := DecodeHeader(bytes.NewReader(data), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want bad version error")
	}
}

func TestDecodeHeaderRejectsOversizedHeader(t *testing.T) {
	data := prefixWithLength(100)
	if _, err := DecodeHeader(bytes.NewReader(data), 99); err == nil {
		t.Fatal("DecodeHeader() error = nil, want oversized header error")
	}
}

func TestDecodeHeaderRejectsUnknownJSONFields(t *testing.T) {
	tests := map[string]string{
		"top level": `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{},"extra":"nope"}`,
		"metadata":  `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"x_amz_request_id":"secret"}}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(frame(payload)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want unknown field error")
			}
		})
	}
}

func TestDecodeHeaderRejectsEmptyIdentityFields(t *testing.T) {
	tests := map[string]string{
		"request id":   `{"ingest_token":"tok","body_length":0,"metadata":{}}`,
		"ingest token": `{"request_id":"req","body_length":0,"metadata":{}}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(frame(payload)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want validation error")
			}
		})
	}
}

func TestDecodeHeaderRejectsInvalidLengths(t *testing.T) {
	tests := map[string]string{
		"status low":       `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"status_code":99}}`,
		"status high":      `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"status_code":600}}`,
		"content negative": `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"content_length":"-1"}}`,
		"content invalid":  `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"content_length":"abc"}}`,
		"body invalid":     `{"request_id":"req","ingest_token":"tok","body_length":-2,"metadata":{}}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(frame(payload)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want validation error")
			}
		})
	}
}

func TestDecodeHeaderRejectsUnsafeMetadata(t *testing.T) {
	oversized := strings.Repeat("a", 8*1024+1)
	tests := map[string]string{
		"crlf":      `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"etag":"ok\r\nbad"}}`,
		"oversized": `{"request_id":"req","ingest_token":"tok","body_length":0,"metadata":{"content_type":"` + oversized + `"}}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(frame(payload)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want metadata validation error")
			}
		})
	}
}

func frame(payload string) []byte {
	return append(prefixWithLength(uint32(len(payload))), []byte(payload)...)
}

func prefixWithLength(length uint32) []byte {
	prefix := make([]byte, prefixBytes)
	copy(prefix[:4], protocolMagic[:])
	prefix[4] = protocolVersion
	binary.BigEndian.PutUint32(prefix[5:], length)
	return prefix
}
