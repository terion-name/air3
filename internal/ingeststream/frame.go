package ingeststream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/pending"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	UnknownBodyLength     int64 = -1
	DefaultMaxHeaderBytes       = 64 * 1024

	ProtocolVersion byte = 1
	PrefixBytes          = 9
)

var ProtocolMagic = [4]byte{'A', 'I', '3', 'T'}

// Header is the bounded MessagePack control frame that precedes the raw ingest body.
type Header struct {
	RequestID   string
	IngestToken string
	BodyLength  int64
	Metadata    pending.Metadata
}

// EncodeHeader validates h, encodes it as MessagePack, and writes the fixed
// binary prefix followed by the MessagePack header bytes.
func EncodeHeader(w io.Writer, h Header) error {
	h, err := ValidateHeader(h)
	if err != nil {
		return err
	}

	var payload bytes.Buffer
	if err := encodeHeaderPayload(&payload, h); err != nil {
		return fmt.Errorf("encode ingest stream header: %w", err)
	}
	if payload.Len() == 0 || payload.Len() > DefaultMaxHeaderBytes {
		return fmt.Errorf("ingest stream header length %d exceeds limit %d", payload.Len(), DefaultMaxHeaderBytes)
	}

	var prefix [PrefixBytes]byte
	copy(prefix[:4], ProtocolMagic[:])
	prefix[4] = ProtocolVersion
	binary.BigEndian.PutUint32(prefix[5:], uint32(payload.Len()))
	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("write ingest stream header prefix: %w", err)
	}
	if _, err := w.Write(payload.Bytes()); err != nil {
		return fmt.Errorf("write ingest stream header: %w", err)
	}
	return nil
}

// DecodeHeader reads and validates the fixed prefix and bounded MessagePack
// header. It rejects bad magic/version and oversized lengths before allocating
// the payload buffer.
func DecodeHeader(r io.Reader, maxHeaderBytes int) (Header, error) {
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = DefaultMaxHeaderBytes
	}

	var prefix [PrefixBytes]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Header{}, fmt.Errorf("read ingest stream header prefix: %w", err)
	}
	if !bytes.Equal(prefix[:4], ProtocolMagic[:]) {
		return Header{}, errors.New("invalid ingest stream magic")
	}
	if prefix[4] != ProtocolVersion {
		return Header{}, errors.New("unsupported ingest stream version")
	}
	headerLen := binary.BigEndian.Uint32(prefix[5:])
	if headerLen == 0 {
		return Header{}, errors.New("empty ingest stream header")
	}
	if uint64(headerLen) > uint64(maxHeaderBytes) {
		return Header{}, fmt.Errorf("ingest stream header length %d exceeds limit %d", headerLen, maxHeaderBytes)
	}

	payload := make([]byte, int(headerLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return Header{}, fmt.Errorf("read ingest stream header: %w", err)
	}
	return decodeHeaderPayload(payload)
}

// ValidateHeader validates the fields in h and returns h with normalized metadata.
func ValidateHeader(h Header) (Header, error) {
	if h.RequestID == "" {
		return Header{}, errors.New("request_id is required")
	}
	if h.IngestToken == "" {
		return Header{}, errors.New("ingest_token is required")
	}
	if h.BodyLength < UnknownBodyLength {
		return Header{}, errors.New("invalid body_length")
	}
	metadata, err := ingest.ValidateMetadata(h.Metadata)
	if err != nil {
		return Header{}, fmt.Errorf("invalid metadata: %w", err)
	}
	h.Metadata = metadata
	return h, nil
}

func encodeHeaderPayload(w io.Writer, h Header) error {
	enc := msgpack.NewEncoder(w)
	if err := enc.EncodeMapLen(4); err != nil {
		return err
	}
	if err := encodeStringField(enc, "request_id", h.RequestID); err != nil {
		return err
	}
	if err := encodeStringField(enc, "ingest_token", h.IngestToken); err != nil {
		return err
	}
	if err := enc.EncodeString("body_length"); err != nil {
		return err
	}
	if err := enc.EncodeInt(h.BodyLength); err != nil {
		return err
	}
	if err := enc.EncodeString("metadata"); err != nil {
		return err
	}
	return encodeMetadata(enc, h.Metadata)
}

func encodeStringField(enc *msgpack.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

func encodeMetadata(enc *msgpack.Encoder, metadata pending.Metadata) error {
	if err := enc.EncodeMapLen(7); err != nil {
		return err
	}
	if err := enc.EncodeString("status_code"); err != nil {
		return err
	}
	if err := enc.EncodeInt(int64(metadata.StatusCode)); err != nil {
		return err
	}
	if err := encodeStringField(enc, "content_type", metadata.ContentType); err != nil {
		return err
	}
	if err := encodeStringField(enc, "content_length", metadata.ContentLength); err != nil {
		return err
	}
	if err := encodeStringField(enc, "content_range", metadata.ContentRange); err != nil {
		return err
	}
	if err := encodeStringField(enc, "etag", metadata.ETag); err != nil {
		return err
	}
	if err := encodeStringField(enc, "last_modified", metadata.LastModified); err != nil {
		return err
	}
	return encodeStringField(enc, "accept_ranges", metadata.AcceptRanges)
}

func decodeHeaderPayload(payload []byte) (Header, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(payload))
	header, err := decodeHeaderMap(dec)
	if err != nil {
		return Header{}, fmt.Errorf("decode ingest stream header: %w", err)
	}
	if _, err := dec.PeekCode(); err == nil {
		return Header{}, errors.New("ingest stream header contains trailing MessagePack")
	} else if !errors.Is(err, io.EOF) {
		return Header{}, fmt.Errorf("decode ingest stream header trailer: %w", err)
	}
	return ValidateHeader(header)
}

func decodeHeaderMap(dec *msgpack.Decoder) (Header, error) {
	fieldCount, err := dec.DecodeMapLen()
	if err != nil {
		return Header{}, err
	}
	var h Header
	seen := make(map[string]bool)
	for i := 0; i < fieldCount; i++ {
		key, err := decodeMapKey(dec)
		if err != nil {
			return Header{}, err
		}
		if seen[key] {
			return Header{}, fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = true
		switch key {
		case "request_id":
			h.RequestID, err = decodeRequiredString(dec, key)
		case "ingest_token":
			h.IngestToken, err = decodeRequiredString(dec, key)
		case "body_length":
			h.BodyLength, err = decodeInt64(dec, key)
		case "metadata":
			h.Metadata, err = decodeMetadata(dec)
		default:
			return Header{}, fmt.Errorf("unknown field %q", key)
		}
		if err != nil {
			return Header{}, err
		}
	}
	for _, key := range []string{"request_id", "ingest_token", "body_length", "metadata"} {
		if !seen[key] {
			return Header{}, fmt.Errorf("%s is required", key)
		}
	}
	return h, nil
}

func decodeMetadata(dec *msgpack.Decoder) (pending.Metadata, error) {
	fieldCount, err := dec.DecodeMapLen()
	if err != nil {
		return pending.Metadata{}, fmt.Errorf("metadata must be a map: %w", err)
	}
	var metadata pending.Metadata
	seen := make(map[string]bool)
	for i := 0; i < fieldCount; i++ {
		key, err := decodeMapKey(dec)
		if err != nil {
			return pending.Metadata{}, err
		}
		if seen[key] {
			return pending.Metadata{}, fmt.Errorf("duplicate metadata field %q", key)
		}
		seen[key] = true
		switch key {
		case "status_code":
			statusCode, err := decodeInt64(dec, key)
			if err != nil {
				return pending.Metadata{}, err
			}
			if statusCode > math.MaxInt || statusCode < math.MinInt {
				return pending.Metadata{}, fmt.Errorf("%s is out of range", key)
			}
			metadata.StatusCode = int(statusCode)
		case "content_type":
			metadata.ContentType, err = decodeRequiredString(dec, key)
		case "content_length":
			metadata.ContentLength, err = decodeRequiredString(dec, key)
		case "content_range":
			metadata.ContentRange, err = decodeRequiredString(dec, key)
		case "etag":
			metadata.ETag, err = decodeRequiredString(dec, key)
		case "last_modified":
			metadata.LastModified, err = decodeRequiredString(dec, key)
		case "accept_ranges":
			metadata.AcceptRanges, err = decodeRequiredString(dec, key)
		default:
			return pending.Metadata{}, fmt.Errorf("unknown metadata field %q", key)
		}
		if err != nil {
			return pending.Metadata{}, err
		}
	}
	return metadata, nil
}

func decodeMapKey(dec *msgpack.Decoder) (string, error) {
	key, err := decodeStrictString(dec, "map key")
	if err != nil {
		return "", err
	}
	return key, nil
}

func decodeRequiredString(dec *msgpack.Decoder, field string) (string, error) {
	return decodeStrictString(dec, field)
}

func decodeStrictString(dec *msgpack.Decoder, field string) (string, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return "", err
	}
	if !isStringCode(code) {
		return "", fmt.Errorf("%s must be a string", field)
	}
	return dec.DecodeString()
}

func isStringCode(code byte) bool {
	return code&0xe0 == 0xa0 || code == 0xd9 || code == 0xda || code == 0xdb
}

func decodeInt64(dec *msgpack.Decoder, field string) (int64, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return 0, err
	}
	if code <= 0x7f || code >= 0xe0 || code == 0xd0 || code == 0xd1 || code == 0xd2 || code == 0xd3 {
		return dec.DecodeInt64()
	}
	if code == 0xcc || code == 0xcd || code == 0xce || code == 0xcf {
		n, err := dec.DecodeUint64()
		if err != nil {
			return 0, err
		}
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("%s is out of range", field)
		}
		return int64(n), nil
	}
	return 0, fmt.Errorf("%s must be an integer", field)
}
