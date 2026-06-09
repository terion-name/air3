package ingesttcp

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/terion-name/air3/internal/pending"
	"github.com/vmihailenco/msgpack/v5"
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
	data := frame(mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, validHeaderFields()...) }))
	data[0] = 'X'
	if _, err := DecodeHeader(bytes.NewReader(data), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want bad magic error")
	}
}

func TestDecodeHeaderRejectsBadVersion(t *testing.T) {
	data := frame(mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, validHeaderFields()...) }))
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

func TestDecodeHeaderRejectsTruncatedHeader(t *testing.T) {
	payload := mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, validHeaderFields()...) })
	data := append(prefixWithLength(uint32(len(payload)+1)), payload...)
	if _, err := DecodeHeader(bytes.NewReader(data), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want truncated header error")
	}
}

func TestDecodeHeaderRejectsUnknownMessagePackFields(t *testing.T) {
	tests := map[string][]msgpackField{
		"top level": append(validHeaderFields(), rawField("extra", "nope")),
		"metadata": validHeaderFieldsWithMetadata(
			rawField("x_amz_request_id", "secret"),
		),
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want unknown field error")
			}
		})
	}
}

func TestDecodeHeaderRejectsDuplicateMessagePackFields(t *testing.T) {
	tests := map[string][]msgpackField{
		"top level": append(validHeaderFields(), stringField("request_id", "req-2")),
		"metadata": validHeaderFieldsWithMetadata(
			stringField("content_type", "text/plain"),
			stringField("content_type", "application/json"),
		),
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want duplicate field error")
			}
		})
	}
}

func TestDecodeHeaderRejectsMissingRequiredFields(t *testing.T) {
	tests := map[string][]msgpackField{
		"request_id":   {stringField("ingest_token", "tok"), intField("body_length", 0), metadataField()},
		"ingest_token": {stringField("request_id", "req"), intField("body_length", 0), metadataField()},
		"body_length":  {stringField("request_id", "req"), stringField("ingest_token", "tok"), metadataField()},
		"metadata":     {stringField("request_id", "req"), stringField("ingest_token", "tok"), intField("body_length", 0)},
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want missing field error")
			}
		})
	}
}

func TestDecodeHeaderRejectsInvalidLengths(t *testing.T) {
	tests := map[string][]msgpackField{
		"status low":       validHeaderFieldsWithMetadata(intField("status_code", 99)),
		"status high":      validHeaderFieldsWithMetadata(intField("status_code", 600)),
		"content negative": validHeaderFieldsWithMetadata(stringField("content_length", "-1")),
		"content invalid":  validHeaderFieldsWithMetadata(stringField("content_length", "abc")),
		"body invalid":     validHeaderFieldsWithBodyLength(-2),
		"body out of range": validHeaderFieldsWithBodyLength(
			uint64(1) << 63,
		),
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want validation error")
			}
		})
	}
}

func TestDecodeHeaderRejectsInvalidValueTypes(t *testing.T) {
	tests := map[string][]msgpackField{
		"request id":   {rawField("request_id", 123), stringField("ingest_token", "tok"), intField("body_length", 0), metadataField()},
		"ingest token": {stringField("request_id", "req"), rawField("ingest_token", 123), intField("body_length", 0), metadataField()},
		"body length":  {stringField("request_id", "req"), stringField("ingest_token", "tok"), rawField("body_length", "0"), metadataField()},
		"metadata":     {stringField("request_id", "req"), stringField("ingest_token", "tok"), intField("body_length", 0), rawField("metadata", "nope")},
		"status code":  validHeaderFieldsWithMetadata(rawField("status_code", "200")),
		"metadata bin": validHeaderFieldsWithMetadata(rawField("content_type", []byte("text/plain"))),
		"metadata str": validHeaderFieldsWithMetadata(rawField("content_type", 123)),
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want type error")
			}
		})
	}
}

func TestDecodeHeaderRejectsUnsafeMetadata(t *testing.T) {
	oversized := strings.Repeat("a", 8*1024+1)
	tests := map[string][]msgpackField{
		"crlf":      validHeaderFieldsWithMetadata(stringField("etag", "ok\r\nbad")),
		"oversized": validHeaderFieldsWithMetadata(stringField("content_type", oversized)),
	}
	for name, fields := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeHeader(bytes.NewReader(headerFrame(t, fields...)), DefaultMaxHeaderBytes); err == nil {
				t.Fatal("DecodeHeader() error = nil, want metadata validation error")
			}
		})
	}
}

type msgpackField struct {
	key    string
	encode func(*msgpack.Encoder) error
}

func headerFrame(t *testing.T, fields ...msgpackField) []byte {
	t.Helper()
	payload := mustMsgpack(t, func(enc *msgpack.Encoder) { encodeHeaderMap(t, enc, fields...) })
	return frame(payload)
}

func mustMsgpack(t *testing.T, encode func(*msgpack.Encoder)) []byte {
	t.Helper()
	var buf bytes.Buffer
	encode(msgpack.NewEncoder(&buf))
	return buf.Bytes()
}

func encodeHeaderMap(t *testing.T, enc *msgpack.Encoder, fields ...msgpackField) {
	t.Helper()
	if err := enc.EncodeMapLen(len(fields)); err != nil {
		t.Fatalf("EncodeMapLen() error = %v", err)
	}
	for _, field := range fields {
		if err := enc.EncodeString(field.key); err != nil {
			t.Fatalf("EncodeString(%q) error = %v", field.key, err)
		}
		if err := field.encode(enc); err != nil {
			t.Fatalf("encode %q error = %v", field.key, err)
		}
	}
}

func stringField(key, value string) msgpackField {
	return msgpackField{key: key, encode: func(enc *msgpack.Encoder) error { return enc.EncodeString(value) }}
}

func intField(key string, value int64) msgpackField {
	return msgpackField{key: key, encode: func(enc *msgpack.Encoder) error { return enc.EncodeInt(value) }}
}

func rawField(key string, value any) msgpackField {
	return msgpackField{key: key, encode: func(enc *msgpack.Encoder) error { return enc.Encode(value) }}
}

func metadataField(fields ...msgpackField) msgpackField {
	return msgpackField{key: "metadata", encode: func(enc *msgpack.Encoder) error {
		if err := enc.EncodeMapLen(len(fields)); err != nil {
			return err
		}
		for _, field := range fields {
			if err := enc.EncodeString(field.key); err != nil {
				return err
			}
			if err := field.encode(enc); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validHeaderFields() []msgpackField {
	return []msgpackField{
		stringField("request_id", "req"),
		stringField("ingest_token", "tok"),
		intField("body_length", 0),
		metadataField(),
	}
}

func validHeaderFieldsWithMetadata(fields ...msgpackField) []msgpackField {
	return []msgpackField{
		stringField("request_id", "req"),
		stringField("ingest_token", "tok"),
		intField("body_length", 0),
		metadataField(fields...),
	}
}

func validHeaderFieldsWithBodyLength(value any) []msgpackField {
	return []msgpackField{
		stringField("request_id", "req"),
		stringField("ingest_token", "tok"),
		rawField("body_length", value),
		metadataField(),
	}
}

func frame(payload []byte) []byte {
	return append(prefixWithLength(uint32(len(payload))), payload...)
}

func prefixWithLength(length uint32) []byte {
	prefix := make([]byte, prefixBytes)
	copy(prefix[:4], protocolMagic[:])
	prefix[4] = protocolVersion
	binary.BigEndian.PutUint32(prefix[5:], length)
	return prefix
}
