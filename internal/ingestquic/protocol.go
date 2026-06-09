package ingestquic

import (
	"io"

	"github.com/terion-name/air3/internal/ingeststream"
)

const (
	UnknownBodyLength     = ingeststream.UnknownBodyLength
	DefaultMaxHeaderBytes = ingeststream.DefaultMaxHeaderBytes

	protocolVersion = ingeststream.ProtocolVersion
	prefixBytes     = ingeststream.PrefixBytes
)

var protocolMagic = ingeststream.ProtocolMagic

// Header is the bounded stream control frame that precedes the raw ingest body.
type Header = ingeststream.Header

func EncodeHeader(w io.Writer, h Header) error {
	return ingeststream.EncodeHeader(w, h)
}

func DecodeHeader(r io.Reader, maxHeaderBytes int) (Header, error) {
	return ingeststream.DecodeHeader(r, maxHeaderBytes)
}

func validateHeader(h Header) (Header, error) {
	return ingeststream.ValidateHeader(h)
}
