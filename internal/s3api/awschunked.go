package s3api

import (
	"errors"
	"fmt"
	"io"
	"net/http/httputil"
)

// ErrChunkedDecode reports malformed aws-chunked upload framing or a decoded
// body that does not match the declared decoded content length.
var ErrChunkedDecode = errors.New("invalid aws-chunked body")

// NewAWSChunkedReader decodes an unsigned aws-chunked upload body
// (STREAMING-UNSIGNED-PAYLOAD-TRAILER). Unsigned aws-chunked framing is
// HTTP/1.1 chunked framing; checksum trailers after the terminal chunk are
// left unread and discarded, not verified. The reader yields exactly
// decodedLength bytes or fails with ErrChunkedDecode.
func NewAWSChunkedReader(r io.Reader, decodedLength int64) io.Reader {
	return &awsChunkedReader{chunks: httputil.NewChunkedReader(r), remaining: decodedLength}
}

type awsChunkedReader struct {
	chunks    io.Reader
	remaining int64
	err       error
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining <= 0 {
		r.err = r.finish()
		return 0, r.err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.chunks.Read(p)
	r.remaining -= int64(n)
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, io.EOF):
		if r.remaining > 0 {
			r.err = fmt.Errorf("%w: body ended %d bytes short of the declared decoded length", ErrChunkedDecode, r.remaining)
		} else {
			r.err = io.EOF
		}
		return n, r.err
	default:
		r.err = fmt.Errorf("%w: %w", ErrChunkedDecode, err)
		return n, r.err
	}
}

// finish confirms the chunk stream ends exactly at the declared length.
func (r *awsChunkedReader) finish() error {
	var scratch [1]byte
	switch _, err := io.ReadFull(r.chunks, scratch[:]); {
	case err == nil:
		return fmt.Errorf("%w: body exceeds the declared decoded length", ErrChunkedDecode)
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return io.EOF
	default:
		return fmt.Errorf("%w: %w", ErrChunkedDecode, err)
	}
}
