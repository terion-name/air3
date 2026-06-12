package s3api

import "testing"

const xmlTestHeader = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"

func TestRenderErrorXML(t *testing.T) {
	tests := []struct {
		name     string
		response ErrorResponse
		want     string
	}{
		{
			name: "required fields only",
			response: ErrorResponse{
				Code:    "AccessDenied",
				Message: "access denied",
			},
			want: xmlTestHeader + `<Error><Code>AccessDenied</Code><Message>access denied</Message></Error>`,
		},
		{
			name: "optional resource and request id",
			response: ErrorResponse{
				Code:      "NoSuchKey",
				Message:   "object not found",
				Resource:  "/bucket/missing.txt",
				RequestID: "req-123",
			},
			want: xmlTestHeader + `<Error><Code>NoSuchKey</Code><Message>object not found</Message><Resource>/bucket/missing.txt</Resource><RequestId>req-123</RequestId></Error>`,
		},
		{
			name: "escaping",
			response: ErrorResponse{
				Code:      "BadRequest",
				Message:   `bad & <tag> "quoted" 'single' ü`,
				Resource:  `/bucket/a&b<q>"'`,
				RequestID: `req&<"'>`,
			},
			want: xmlTestHeader + `<Error><Code>BadRequest</Code><Message>bad &amp; &lt;tag&gt; &#34;quoted&#34; &#39;single&#39; ü</Message><Resource>/bucket/a&amp;b&lt;q&gt;&#34;&#39;</Resource><RequestId>req&amp;&lt;&#34;&#39;&gt;</RequestId></Error>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderErrorXML(tc.response)
			if err != nil {
				t.Fatalf("RenderErrorXML() error = %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("RenderErrorXML() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderListBucketResultEmpty(t *testing.T) {
	got, err := RenderListBucketResult(ListBucketResult{
		Name:    "photos",
		Prefix:  "public/",
		MaxKeys: 1000,
	})
	if err != nil {
		t.Fatalf("RenderListBucketResult() error = %v", err)
	}

	want := xmlTestHeader + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>photos</Name><Prefix>public/</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`
	if string(got) != want {
		t.Fatalf("RenderListBucketResult() = %q, want %q", got, want)
	}
}

func TestRenderListBucketResultWithContentsAndTokens(t *testing.T) {
	got, err := RenderListBucketResult(ListBucketResult{
		Name:                  "photos",
		Prefix:                "public/",
		Delimiter:             "/",
		KeyCount:              4,
		MaxKeys:               2,
		IsTruncated:           true,
		ContinuationToken:     "token-1",
		NextContinuationToken: "token-2",
		StartAfter:            "public/000.jpg",
		EncodingType:          "url",
		Contents: []ListObject{
			{
				Key:          "public/a.jpg",
				LastModified: "2026-06-12T12:00:00Z",
				ETag:         `"etag-a"`,
				Size:         12,
				StorageClass: "STANDARD",
			},
			{
				Key:          "public/b.jpg",
				LastModified: "2026-06-12T12:01:00Z",
				ETag:         `"etag-b"`,
				Size:         34,
				StorageClass: "GLACIER",
			},
		},
		CommonPrefixes: []string{"public/2025/", "public/2026/"},
	})
	if err != nil {
		t.Fatalf("RenderListBucketResult() error = %v", err)
	}

	want := xmlTestHeader + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>photos</Name><Prefix>public/</Prefix><KeyCount>4</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>public/a.jpg</Key><LastModified>2026-06-12T12:00:00Z</LastModified><ETag>&#34;etag-a&#34;</ETag><Size>12</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>public/b.jpg</Key><LastModified>2026-06-12T12:01:00Z</LastModified><ETag>&#34;etag-b&#34;</ETag><Size>34</Size><StorageClass>GLACIER</StorageClass></Contents><CommonPrefixes><Prefix>public/2025/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>public/2026/</Prefix></CommonPrefixes><NextContinuationToken>token-2</NextContinuationToken><ContinuationToken>token-1</ContinuationToken><StartAfter>public/000.jpg</StartAfter><Delimiter>/</Delimiter><EncodingType>url</EncodingType></ListBucketResult>`
	if string(got) != want {
		t.Fatalf("RenderListBucketResult() = %q, want %q", got, want)
	}
}

func TestRenderListBucketResultEscapesXML(t *testing.T) {
	got, err := RenderListBucketResult(ListBucketResult{
		Name:                  "bucket",
		Prefix:                `pre & < > " ' ü+ /`,
		Delimiter:             `&`,
		KeyCount:              1,
		MaxKeys:               1,
		ContinuationToken:     `ct & < > " ' ü+`,
		NextContinuationToken: `next & < > " ' ü+`,
		StartAfter:            `after & < > " ' ü+`,
		Contents: []ListObject{
			{
				Key:          `key & < > " ' ü+ space`,
				LastModified: "2026-06-12T12:00:00Z",
				ETag:         `etag & < > " ' ü+`,
				Size:         1,
				StorageClass: "STANDARD",
			},
		},
		CommonPrefixes: []string{`common & < > " ' ü+`},
	})
	if err != nil {
		t.Fatalf("RenderListBucketResult() error = %v", err)
	}

	want := xmlTestHeader + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>pre &amp; &lt; &gt; &#34; &#39; ü+ /</Prefix><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>key &amp; &lt; &gt; &#34; &#39; ü+ space</Key><LastModified>2026-06-12T12:00:00Z</LastModified><ETag>etag &amp; &lt; &gt; &#34; &#39; ü+</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>common &amp; &lt; &gt; &#34; &#39; ü+</Prefix></CommonPrefixes><NextContinuationToken>next &amp; &lt; &gt; &#34; &#39; ü+</NextContinuationToken><ContinuationToken>ct &amp; &lt; &gt; &#34; &#39; ü+</ContinuationToken><StartAfter>after &amp; &lt; &gt; &#34; &#39; ü+</StartAfter><Delimiter>&amp;</Delimiter></ListBucketResult>`
	if string(got) != want {
		t.Fatalf("RenderListBucketResult() = %q, want %q", got, want)
	}
}

func TestApplyListPublicPrefix(t *testing.T) {
	result := &ListBucketResult{
		Name:                  "photos",
		Prefix:                "internal/",
		ContinuationToken:     "ct-internal",
		NextContinuationToken: "next-internal",
		StartAfter:            "internal/start.jpg",
		Contents: []ListObject{
			{Key: "a.jpg"},
			{Key: "nested/b.jpg"},
		},
		CommonPrefixes: []string{"album/", "archive/"},
	}

	ApplyListPublicPrefix(result, "public/")

	if result.Name != "photos" {
		t.Fatalf("Name = %q, want %q", result.Name, "photos")
	}
	if result.Prefix != "internal/" {
		t.Fatalf("Prefix = %q, want %q", result.Prefix, "internal/")
	}
	if result.ContinuationToken != "ct-internal" {
		t.Fatalf("ContinuationToken = %q, want %q", result.ContinuationToken, "ct-internal")
	}
	if result.NextContinuationToken != "next-internal" {
		t.Fatalf("NextContinuationToken = %q, want %q", result.NextContinuationToken, "next-internal")
	}
	if result.StartAfter != "internal/start.jpg" {
		t.Fatalf("StartAfter = %q, want %q", result.StartAfter, "internal/start.jpg")
	}

	wantKeys := []string{"public/a.jpg", "public/nested/b.jpg"}
	for i, want := range wantKeys {
		if result.Contents[i].Key != want {
			t.Fatalf("Contents[%d].Key = %q, want %q", i, result.Contents[i].Key, want)
		}
	}

	wantPrefixes := []string{"public/album/", "public/archive/"}
	for i, want := range wantPrefixes {
		if result.CommonPrefixes[i] != want {
			t.Fatalf("CommonPrefixes[%d] = %q, want %q", i, result.CommonPrefixes[i], want)
		}
	}
}

func TestApplyListPublicPrefixNilResult(t *testing.T) {
	ApplyListPublicPrefix(nil, "public/")
}
