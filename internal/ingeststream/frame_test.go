package ingeststream

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/terion-name/air3/internal/pending"
	"github.com/vmihailenco/msgpack/v5"
)

func TestValidateHeaderNormalizesMetadata(t *testing.T) {
	got, err := ValidateHeader(Header{
		RequestID:   "req",
		IngestToken: "tok",
		BodyLength:  UnknownBodyLength,
		Metadata: pending.Metadata{
			ContentType: " text/plain ",
		},
	})
	if err != nil {
		t.Fatalf("ValidateHeader() error = %v", err)
	}
	if got.Metadata.ContentType != "text/plain" {
		t.Fatalf("ContentType = %q, want text/plain", got.Metadata.ContentType)
	}
}

func TestDecodeHeaderRejectsZeroLength(t *testing.T) {
	var prefix [PrefixBytes]byte
	copy(prefix[:4], ProtocolMagic[:])
	prefix[4] = ProtocolVersion
	if _, err := DecodeHeader(bytes.NewReader(prefix[:]), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want empty header error")
	}
}

func TestDecodeHeaderRejectsOversizedLengthBeforePayloadRead(t *testing.T) {
	var prefix [PrefixBytes]byte
	copy(prefix[:4], ProtocolMagic[:])
	prefix[4] = ProtocolVersion
	binary.BigEndian.PutUint32(prefix[5:], uint32(DefaultMaxHeaderBytes+1))
	if _, err := DecodeHeader(bytes.NewReader(prefix[:]), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want oversized error")
	}
}

func TestDecodeHeaderRejectsTrailingMessagePack(t *testing.T) {
	payload := headerPayload(t)
	payload = append(payload, 0xc0) // nil value after the header map.
	if _, err := DecodeHeader(bytes.NewReader(framePayload(payload)), DefaultMaxHeaderBytes); err == nil {
		t.Fatal("DecodeHeader() error = nil, want trailing data error")
	}
}

func TestDecodeHeaderRejectsTruncatedPayload(t *testing.T) {
	payload := headerPayload(t)
	framed := framePayload(payload)
	framed = framed[:len(framed)-1]
	if _, err := DecodeHeader(bytes.NewReader(framed), DefaultMaxHeaderBytes); err == nil || err == io.EOF {
		t.Fatalf("DecodeHeader() error = %v, want wrapped truncated payload error", err)
	}
}

func headerPayload(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeMapLen(4); err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		key   string
		value any
	}{
		{key: "request_id", value: "req"},
		{key: "ingest_token", value: "tok"},
		{key: "body_length", value: int64(0)},
	} {
		if err := enc.EncodeString(pair.key); err != nil {
			t.Fatal(err)
		}
		if err := enc.Encode(pair.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.EncodeString("metadata"); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeMapLen(0); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func framePayload(payload []byte) []byte {
	prefix := make([]byte, PrefixBytes)
	copy(prefix[:4], ProtocolMagic[:])
	prefix[4] = ProtocolVersion
	binary.BigEndian.PutUint32(prefix[5:], uint32(len(payload)))
	return append(prefix, payload...)
}
