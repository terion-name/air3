package s3api

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func awsChunkedBody(chunks []string, trailer string) string {
	var b strings.Builder
	for _, chunk := range chunks {
		b.WriteString(strconv.FormatInt(int64(len(chunk)), 16))
		b.WriteString("\r\n")
		b.WriteString(chunk)
		b.WriteString("\r\n")
	}
	b.WriteString("0\r\n")
	b.WriteString(trailer)
	b.WriteString("\r\n")
	return b.String()
}

func TestAWSChunkedReaderDecodesFramedBody(t *testing.T) {
	tests := []struct {
		name    string
		framed  string
		decoded string
	}{
		{name: "single chunk", framed: awsChunkedBody([]string{"hello world"}, ""), decoded: "hello world"},
		{name: "multiple chunks", framed: awsChunkedBody([]string{"hello ", "multipart ", "world"}, ""), decoded: "hello multipart world"},
		{name: "checksum trailer discarded", framed: awsChunkedBody([]string{"payload"}, "x-amz-checksum-crc32:sOO8/Q==\r\n"), decoded: "payload"},
		{name: "empty body", framed: awsChunkedBody(nil, ""), decoded: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewAWSChunkedReader(strings.NewReader(tc.framed), int64(len(tc.decoded)))
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if string(got) != tc.decoded {
				t.Fatalf("decoded = %q, want %q", got, tc.decoded)
			}
		})
	}
}

func TestAWSChunkedReaderRejectsMalformedBodies(t *testing.T) {
	tests := []struct {
		name          string
		framed        string
		decodedLength int64
	}{
		{name: "declared length longer than body", framed: awsChunkedBody([]string{"short"}, ""), decodedLength: 10},
		{name: "declared length shorter than body", framed: awsChunkedBody([]string{"a longer payload"}, ""), decodedLength: 4},
		{name: "not chunked at all", framed: "plain body without framing", decodedLength: 26},
		{name: "truncated mid-chunk", framed: "b\r\nhello", decodedLength: 11},
		{name: "garbage chunk size", framed: "zz\r\nhello\r\n0\r\n\r\n", decodedLength: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewAWSChunkedReader(strings.NewReader(tc.framed), tc.decodedLength)
			_, err := io.ReadAll(r)
			if !errors.Is(err, ErrChunkedDecode) {
				t.Fatalf("ReadAll() error = %v, want ErrChunkedDecode", err)
			}
		})
	}
}

func TestAWSChunkedReaderKeepsReturningTerminalError(t *testing.T) {
	r := NewAWSChunkedReader(strings.NewReader("zz\r\n"), 5)
	buf := make([]byte, 8)
	if _, err := r.Read(buf); !errors.Is(err, ErrChunkedDecode) {
		t.Fatalf("first Read() error = %v, want ErrChunkedDecode", err)
	}
	if _, err := r.Read(buf); !errors.Is(err, ErrChunkedDecode) {
		t.Fatalf("second Read() error = %v, want ErrChunkedDecode", err)
	}
}
