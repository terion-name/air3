package s3api

import (
	"strings"
	"testing"
)

func TestRenderInitiateMultipartUploadResult(t *testing.T) {
	body, err := RenderInitiateMultipartUploadResult(InitiateMultipartUploadResult{
		Bucket:   "public-bucket",
		Key:      "objects/file.bin",
		UploadID: "upload-123",
	})
	if err != nil {
		t.Fatalf("RenderInitiateMultipartUploadResult() error = %v", err)
	}
	want := `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>public-bucket</Bucket><Key>objects/file.bin</Key><UploadId>upload-123</UploadId></InitiateMultipartUploadResult>`
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestRenderCompleteMultipartUploadResult(t *testing.T) {
	body, err := RenderCompleteMultipartUploadResult(CompleteMultipartUploadResult{
		Bucket: "public-bucket",
		Key:    "objects/file.bin",
		ETag:   `"complete-etag"`,
	})
	if err != nil {
		t.Fatalf("RenderCompleteMultipartUploadResult() error = %v", err)
	}
	want := `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>public-bucket</Bucket><Key>objects/file.bin</Key><ETag>&#34;complete-etag&#34;</ETag></CompleteMultipartUploadResult>`
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestParseCompleteMultipartUpload(t *testing.T) {
	t.Run("parses parts and ignores checksum members", func(t *testing.T) {
		body := `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag><ChecksumCRC32>sOO8/Q==</ChecksumCRC32></Part>
			<Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
		</CompleteMultipartUpload>`
		parts, err := ParseCompleteMultipartUpload(strings.NewReader(body), 1<<20)
		if err != nil {
			t.Fatalf("ParseCompleteMultipartUpload() error = %v", err)
		}
		if len(parts) != 2 || parts[0].PartNumber != 1 || parts[0].ETag != `"etag-1"` || parts[1].PartNumber != 2 || parts[1].ETag != `"etag-2"` {
			t.Fatalf("parts = %#v, want sorted parts 1 and 2", parts)
		}
	})

	t.Run("rejects invalid bodies", func(t *testing.T) {
		tests := []struct {
			name string
			body string
		}{
			{name: "no parts", body: `<CompleteMultipartUpload></CompleteMultipartUpload>`},
			{name: "not xml", body: `{"parts": []}`},
			{name: "wrong root", body: `<Delete><Object><Key>k</Key></Object></Delete>`},
			{name: "part number zero", body: `<CompleteMultipartUpload><Part><PartNumber>0</PartNumber><ETag>"e"</ETag></Part></CompleteMultipartUpload>`},
			{name: "part number too large", body: `<CompleteMultipartUpload><Part><PartNumber>10001</PartNumber><ETag>"e"</ETag></Part></CompleteMultipartUpload>`},
			{name: "missing etag", body: `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`},
			{name: "duplicate part", body: `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"a"</ETag></Part><Part><PartNumber>1</PartNumber><ETag>"b"</ETag></Part></CompleteMultipartUpload>`},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := ParseCompleteMultipartUpload(strings.NewReader(tc.body), 1<<20); err == nil {
					t.Fatal("ParseCompleteMultipartUpload() error = nil, want error")
				}
			})
		}
	})

	t.Run("caps body size", func(t *testing.T) {
		huge := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"` + strings.Repeat("e", 128) + `"</ETag></Part></CompleteMultipartUpload>`
		if _, err := ParseCompleteMultipartUpload(strings.NewReader(huge), 64); err == nil {
			t.Fatal("ParseCompleteMultipartUpload() error = nil, want truncation error")
		}
	})
}
