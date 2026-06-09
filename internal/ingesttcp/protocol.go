package ingesttcp

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/pending"
)

const (
	UnknownBodyLength     int64 = -1
	DefaultMaxHeaderBytes       = 64 * 1024

	protocolVersion byte = 1
	prefixBytes          = 9
)

var protocolMagic = [4]byte{'A', 'I', '3', 'T'}

// Header is the bounded JSON control frame that precedes the raw ingest body.
type Header struct {
	RequestID   string
	IngestToken string
	BodyLength  int64
	Metadata    pending.Metadata
}

type wireHeader struct {
	RequestID   string       `json:"request_id"`
	IngestToken string       `json:"ingest_token"`
	BodyLength  int64        `json:"body_length"`
	Metadata    wireMetadata `json:"metadata"`
}

type wireMetadata struct {
	StatusCode    int    `json:"status_code"`
	ContentType   string `json:"content_type"`
	ContentLength string `json:"content_length"`
	ContentRange  string `json:"content_range"`
	ETag          string `json:"etag"`
	LastModified  string `json:"last_modified"`
	AcceptRanges  string `json:"accept_ranges"`
}

// EncodeHeader validates h, encodes it as JSON, and writes the fixed binary
// prefix followed by the JSON header bytes.
func EncodeHeader(w io.Writer, h Header) error {
	h, err := validateHeader(h)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(toWireHeader(h))
	if err != nil {
		return fmt.Errorf("encode ingest TCP header: %w", err)
	}
	if len(payload) == 0 || len(payload) > DefaultMaxHeaderBytes {
		return fmt.Errorf("ingest TCP header length %d exceeds limit %d", len(payload), DefaultMaxHeaderBytes)
	}

	var prefix [prefixBytes]byte
	copy(prefix[:4], protocolMagic[:])
	prefix[4] = protocolVersion
	binary.BigEndian.PutUint32(prefix[5:], uint32(len(payload)))
	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("write ingest TCP header prefix: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write ingest TCP header: %w", err)
	}
	return nil
}

// DecodeHeader reads and validates the fixed prefix and bounded JSON header. It
// intentionally rejects bad magic/version before allocating a header buffer.
func DecodeHeader(r io.Reader, maxHeaderBytes int) (Header, error) {
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = DefaultMaxHeaderBytes
	}

	var prefix [prefixBytes]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Header{}, fmt.Errorf("read ingest TCP header prefix: %w", err)
	}
	if !bytes.Equal(prefix[:4], protocolMagic[:]) {
		return Header{}, errors.New("invalid ingest TCP magic")
	}
	if prefix[4] != protocolVersion {
		return Header{}, errors.New("unsupported ingest TCP version")
	}
	headerLen := binary.BigEndian.Uint32(prefix[5:])
	if headerLen == 0 {
		return Header{}, errors.New("empty ingest TCP header")
	}
	if headerLen > uint32(maxHeaderBytes) {
		return Header{}, fmt.Errorf("ingest TCP header length %d exceeds limit %d", headerLen, maxHeaderBytes)
	}

	payload := make([]byte, int(headerLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return Header{}, fmt.Errorf("read ingest TCP header: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var wh wireHeader
	if err := dec.Decode(&wh); err != nil {
		return Header{}, fmt.Errorf("decode ingest TCP header: %w", err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return Header{}, errors.New("ingest TCP header contains trailing JSON")
	}
	return validateHeader(fromWireHeader(wh))
}

func validateHeader(h Header) (Header, error) {
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

func toWireHeader(h Header) wireHeader {
	return wireHeader{
		RequestID:   h.RequestID,
		IngestToken: h.IngestToken,
		BodyLength:  h.BodyLength,
		Metadata: wireMetadata{
			StatusCode:    h.Metadata.StatusCode,
			ContentType:   h.Metadata.ContentType,
			ContentLength: h.Metadata.ContentLength,
			ContentRange:  h.Metadata.ContentRange,
			ETag:          h.Metadata.ETag,
			LastModified:  h.Metadata.LastModified,
			AcceptRanges:  h.Metadata.AcceptRanges,
		},
	}
}

func fromWireHeader(h wireHeader) Header {
	return Header{
		RequestID:   h.RequestID,
		IngestToken: h.IngestToken,
		BodyLength:  h.BodyLength,
		Metadata: pending.Metadata{
			StatusCode:    h.Metadata.StatusCode,
			ContentType:   h.Metadata.ContentType,
			ContentLength: h.Metadata.ContentLength,
			ContentRange:  h.Metadata.ContentRange,
			ETag:          h.Metadata.ETag,
			LastModified:  h.Metadata.LastModified,
			AcceptRanges:  h.Metadata.AcceptRanges,
		},
	}
}
