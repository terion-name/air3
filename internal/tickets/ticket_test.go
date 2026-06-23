package tickets

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func validTicket(now time.Time) Ticket {
	return Ticket{
		Version:        Version,
		RequestID:      "req-123",
		Bucket:         "demo-bucket",
		Key:            "path/to/object.txt",
		Method:         "GET",
		Range:          "bytes=1-10",
		DeadlineUnixMS: now.Add(time.Minute).UnixMilli(),
		IngestURL:      "https://edge-internal:8443/ingest/req-123",
		IngestToken:    "tok_123-abc",
		TraceID:        "trace-123",
	}
}

func validListTicket(now time.Time) Ticket {
	return Ticket{
		Version:        Version,
		RequestID:      "req-list",
		Bucket:         "demo-bucket",
		Key:            "",
		Method:         "GET",
		Operation:      OperationListObjectsV2,
		DeadlineUnixMS: now.Add(time.Minute).UnixMilli(),
		IngestURL:      "https://edge-internal:8443/ingest/req-list",
		IngestToken:    "tok_list-123",
		TraceID:        "trace-list",
		List: &ListRequest{
			Prefix:            "photos/2026",
			Delimiter:         "/",
			ContinuationToken: "opaque-token",
			StartAfter:        "photos/2025/last.jpg",
			MaxKeys:           100,
			EncodingType:      "url",
			FetchOwner:        true,
			Rewrite: ListRewrite{
				Bucket:    "public-bucket",
				Prefix:    "shared/photos",
				KeyPrefix: "cdn/photos",
			},
		},
	}
}

func validPutTicket(now time.Time) Ticket {
	contentLength := int64(11)
	return Ticket{
		Version:         Version,
		RequestID:       "req-put",
		Bucket:          "demo-bucket",
		Key:             "path/to/object.txt",
		Method:          "PUT",
		Operation:       OperationPutObject,
		UploadSourceURL: "https://edge-internal:8443/uploads/req-put",
		UploadToken:     "upload_tok-123",
		ContentLength:   &contentLength,
		ContentType:     "text/plain",
		DeadlineUnixMS:  now.Add(time.Minute).UnixMilli(),
		IngestURL:       "https://edge-internal:8443/ingest/req-put",
		IngestToken:     "tok_put-123",
		TraceID:         "trace-put",
	}
}

func validDeleteTicket(now time.Time) Ticket {
	return Ticket{
		Version:        Version,
		RequestID:      "req-delete",
		Bucket:         "demo-bucket",
		Key:            "path/to/object.txt",
		Method:         "DELETE",
		Operation:      OperationDeleteObject,
		DeadlineUnixMS: now.Add(time.Minute).UnixMilli(),
		IngestURL:      "https://edge-internal:8443/ingest/req-delete",
		IngestToken:    "tok_delete-123",
		TraceID:        "trace-delete",
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}

func TestTicketMarshalUnmarshalValidatesClosedSchema(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data, err := Marshal(validTicket(now), now)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	if bytes.Contains(data, []byte(`"server"`)) {
		t.Fatalf("Marshal() included legacy empty server: %s", data)
	}

	got, err := Unmarshal(data, now)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.RequestID != "req-123" || got.Bucket != "demo-bucket" || got.Key != "path/to/object.txt" || got.Server != "" {
		t.Fatalf("Unmarshal() = %#v", got)
	}
}

func TestTicketServerRoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	ticket := validTicket(now)
	ticket.Server = "edge_west-1"

	data, err := Marshal(ticket, now)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !bytes.Contains(data, []byte(`"server":"edge_west-1"`)) {
		t.Fatalf("Marshal() omitted server: %s", data)
	}

	got, err := Unmarshal(data, now)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.Server != "edge_west-1" {
		t.Fatalf("Unmarshal().Server = %q, want %q", got.Server, "edge_west-1")
	}
}

func TestTicketValidationRejectsUnsafeCases(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		edit func(*Ticket)
	}{
		{"bad version", func(t *Ticket) { t.Version = 2 }},
		{"missing request", func(t *Ticket) { t.RequestID = "" }},
		{"unsupported method", func(t *Ticket) { t.Method = "POST" }},
		{"bad bucket", func(t *Ticket) { t.Bucket = "Bad_Bucket" }},
		{"missing key", func(t *Ticket) { t.Key = "" }},
		{"bad key", func(t *Ticket) { t.Key = "../secret" }},
		{"bad range", func(t *Ticket) { t.Range = "bytes=10-1" }},
		{"object list metadata", func(t *Ticket) { t.List = &ListRequest{MaxKeys: 1, Rewrite: ListRewrite{Bucket: "public-bucket"}} }},
		{"bad server", func(t *Ticket) { t.Server = "-edge" }},
		{"expired", func(t *Ticket) { t.DeadlineUnixMS = now.Add(-time.Second).UnixMilli() }},
		{"plain ingest url", func(t *Ticket) { t.IngestURL = "http://edge/ingest" }},
		{"ingest url credentials", func(t *Ticket) { t.IngestURL = "https://user:pass@edge/ingest" }},
		{"bad ingest token", func(t *Ticket) { t.IngestToken = "secret value" }},
		{"read upload source", func(t *Ticket) { t.UploadSourceURL = "https://edge/uploads/req" }},
		{"read upload token", func(t *Ticket) { t.UploadToken = "upload_tok-123" }},
		{"read content length", func(t *Ticket) { t.ContentLength = int64Ptr(1) }},
		{"read content type", func(t *Ticket) { t.ContentType = "text/plain" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validTicket(now)
			tc.edit(&ticket)
			if err := ticket.Validate(now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestTicketValidationAcceptsExplicitObjectOperations(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		method    string
		operation Operation
	}{
		{"get object", "GET", OperationGetObject},
		{"head object", "HEAD", OperationHeadObject},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validTicket(now)
			ticket.Method = tc.method
			ticket.Operation = tc.operation
			if err := ticket.Validate(now); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestTicketValidationRejectsOperationMismatches(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		method    string
		operation Operation
	}{
		{"get object head", "HEAD", OperationGetObject},
		{"head object get", "GET", OperationHeadObject},
		{"list head", "HEAD", OperationListObjectsV2},
		{"put get", "GET", OperationPutObject},
		{"delete get", "GET", OperationDeleteObject},
		{"unknown operation", "GET", Operation("CopyObject")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validTicket(now)
			ticket.Method = tc.method
			ticket.Operation = tc.operation
			if err := ticket.Validate(now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestTicketValidationAcceptsExplicitMutationOperations(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	for _, ticket := range []Ticket{validPutTicket(now), validDeleteTicket(now)} {
		if err := ticket.Validate(now); err != nil {
			t.Fatalf("Validate(%s) error = %v", ticket.Operation, err)
		}
	}
}

func TestResolveOperationDoesNotInferMutations(t *testing.T) {
	for _, method := range []string{"PUT", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			if _, err := ResolveOperation(method, ""); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("ResolveOperation(%q, empty) error = %v, want ErrInvalidTicket", method, err)
			}
		})
	}
}

func TestMutationTicketMarshalUnmarshal(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)

	putData, err := Marshal(validPutTicket(now), now)
	if err != nil {
		t.Fatalf("Marshal(PutObject) error = %v", err)
	}
	if !bytes.Contains(putData, []byte(`"operation":"PutObject"`)) || !bytes.Contains(putData, []byte(`"upload_source_url":`)) || !bytes.Contains(putData, []byte(`"content_length":11`)) {
		t.Fatalf("Marshal(PutObject) omitted mutation envelope: %s", putData)
	}
	put, err := Unmarshal(putData, now)
	if err != nil {
		t.Fatalf("Unmarshal(PutObject) error = %v", err)
	}
	if put.Operation != OperationPutObject || put.UploadSourceURL == "" || put.UploadToken == "" || put.ContentLength == nil || *put.ContentLength != 11 || put.ContentType != "text/plain" {
		t.Fatalf("Unmarshal(PutObject) = %#v", put)
	}

	deleteData, err := Marshal(validDeleteTicket(now), now)
	if err != nil {
		t.Fatalf("Marshal(DeleteObject) error = %v", err)
	}
	if !bytes.Contains(deleteData, []byte(`"operation":"DeleteObject"`)) || bytes.Contains(deleteData, []byte(`"upload_source_url"`)) || bytes.Contains(deleteData, []byte(`"content_length"`)) {
		t.Fatalf("Marshal(DeleteObject) included unexpected envelope: %s", deleteData)
	}
	deleted, err := Unmarshal(deleteData, now)
	if err != nil {
		t.Fatalf("Unmarshal(DeleteObject) error = %v", err)
	}
	if deleted.Operation != OperationDeleteObject || deleted.Key != "path/to/object.txt" || deleted.ContentLength != nil {
		t.Fatalf("Unmarshal(DeleteObject) = %#v", deleted)
	}
}

func TestPutObjectTicketRejectsInvalidMetadata(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		edit func(*Ticket)
	}{
		{"missing key", func(t *Ticket) { t.Key = "" }},
		{"range", func(t *Ticket) { t.Range = "bytes=0-1" }},
		{"list", func(t *Ticket) { t.List = &ListRequest{MaxKeys: 1, Rewrite: ListRewrite{Bucket: "public-bucket"}} }},
		{"missing upload source", func(t *Ticket) { t.UploadSourceURL = "" }},
		{"plain upload source", func(t *Ticket) { t.UploadSourceURL = "http://edge/uploads/req-put" }},
		{"upload source credentials", func(t *Ticket) { t.UploadSourceURL = "https://user:pass@edge/uploads/req-put" }},
		{"missing upload token", func(t *Ticket) { t.UploadToken = "" }},
		{"bad upload token", func(t *Ticket) { t.UploadToken = "bad token" }},
		{"missing content length", func(t *Ticket) { t.ContentLength = nil }},
		{"negative content length", func(t *Ticket) { t.ContentLength = int64Ptr(-1) }},
		{"bad content type", func(t *Ticket) { t.ContentType = "text/plain\r\nX-Other: value" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validPutTicket(now)
			tc.edit(&ticket)
			if err := ticket.Validate(now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestDeleteObjectTicketRejectsInvalidMetadata(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		edit func(*Ticket)
	}{
		{"missing key", func(t *Ticket) { t.Key = "" }},
		{"range", func(t *Ticket) { t.Range = "bytes=0-1" }},
		{"list", func(t *Ticket) { t.List = &ListRequest{MaxKeys: 1, Rewrite: ListRewrite{Bucket: "public-bucket"}} }},
		{"upload source", func(t *Ticket) { t.UploadSourceURL = "https://edge/uploads/req-delete" }},
		{"upload token", func(t *Ticket) { t.UploadToken = "upload_tok-123" }},
		{"content length", func(t *Ticket) { t.ContentLength = int64Ptr(0) }},
		{"content type", func(t *Ticket) { t.ContentType = "text/plain" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validDeleteTicket(now)
			tc.edit(&ticket)
			if err := ticket.Validate(now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestUnmarshalRejectsUnknownMutationFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	base, err := json.Marshal(validPutTicket(now))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(base, &m); err != nil {
		t.Fatal(err)
	}
	m["content_md5"] = "abc123"
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Unmarshal(data, now); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("Unmarshal() error = %v, want ErrInvalidTicket", err)
	}
}

func TestListObjectsV2TicketMarshalUnmarshal(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data, err := Marshal(validListTicket(now), now)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !bytes.Contains(data, []byte(`"operation":"ListObjectsV2"`)) || !bytes.Contains(data, []byte(`"list":`)) {
		t.Fatalf("Marshal() omitted list operation metadata: %s", data)
	}

	got, err := Unmarshal(data, now)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.Operation != OperationListObjectsV2 || got.Key != "" || got.Range != "" || got.List == nil {
		t.Fatalf("Unmarshal() = %#v", got)
	}
	if got.List.Prefix != "photos/2026" || got.List.Rewrite.Bucket != "public-bucket" || !got.List.FetchOwner {
		t.Fatalf("Unmarshal().List = %#v", got.List)
	}
}

func TestListObjectsV2TicketRejectsInvalidMetadata(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		edit func(*Ticket)
	}{
		{"non-empty key", func(t *Ticket) { t.Key = "objects/a.txt" }},
		{"range", func(t *Ticket) { t.Range = "bytes=0-1" }},
		{"nil list", func(t *Ticket) { t.List = nil }},
		{"bad max keys", func(t *Ticket) { t.List.MaxKeys = 1001 }},
		{"negative max keys", func(t *Ticket) { t.List.MaxKeys = -1 }},
		{"bad delimiter", func(t *Ticket) { t.List.Delimiter = "," }},
		{"bad encoding", func(t *Ticket) { t.List.EncodingType = "xml" }},
		{"bad prefix", func(t *Ticket) { t.List.Prefix = "/photos" }},
		{"bad start after", func(t *Ticket) { t.List.StartAfter = "photos//last.jpg" }},
		{"bad continuation token", func(t *Ticket) { t.List.ContinuationToken = "opaque\nnext" }},
		{"bad rewrite bucket", func(t *Ticket) { t.List.Rewrite.Bucket = "Bad_Bucket" }},
		{"bad rewrite prefix", func(t *Ticket) { t.List.Rewrite.Prefix = "../private" }},
		{"bad rewrite key prefix", func(t *Ticket) { t.List.Rewrite.KeyPrefix = "cdn\x00photos" }},
		{"upload source", func(t *Ticket) { t.UploadSourceURL = "https://edge/uploads/req-list" }},
		{"upload token", func(t *Ticket) { t.UploadToken = "upload_tok-123" }},
		{"content length", func(t *Ticket) { t.ContentLength = int64Ptr(1) }},
		{"content type", func(t *Ticket) { t.ContentType = "text/plain" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticket := validListTicket(now)
			tc.edit(&ticket)
			if err := ticket.Validate(now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestUnmarshalRejectsUnknownNestedListFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data := []byte(`{"version":1,"request_id":"req-list","bucket":"demo-bucket","key":"","method":"GET","operation":"ListObjectsV2","deadline_unix_ms":9999999999999,"ingest_url":"https://edge/ingest","ingest_token":"tok","list":{"max_keys":10,"rewrite":{"bucket":"public-bucket"},"unexpected":"x"}}`)
	if _, err := Unmarshal(data, now); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("Unmarshal() error = %v, want ErrInvalidTicket", err)
	}
}

func TestUnmarshalRejectsUnknownNestedRewriteFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data := []byte(`{"version":1,"request_id":"req-list","bucket":"demo-bucket","key":"","method":"GET","operation":"ListObjectsV2","deadline_unix_ms":9999999999999,"ingest_url":"https://edge/ingest","ingest_token":"tok","list":{"max_keys":10,"rewrite":{"bucket":"public-bucket","unexpected":"x"}}}`)
	if _, err := Unmarshal(data, now); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("Unmarshal() error = %v, want ErrInvalidTicket", err)
	}
}

func TestUnmarshalRejectsCredentialBytesAndHeaderFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	base, err := json.Marshal(validTicket(now))
	if err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{"secret_access_key", "object_bytes", "public_secret", "raw_headers", "headers", "authorization", "raw_authorization", "authorization_header", "signed_headers", "x_amz_signedheaders", "x_amz_signature", "x_amz_credential", "x_amz_security_token"} {
		t.Run(field, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal(base, &m); err != nil {
				t.Fatal(err)
			}
			m[field] = "forbidden"
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Unmarshal(data, now); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Unmarshal() error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestUnmarshalRejectsUnknownFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data := []byte(`{"version":1,"request_id":"req","bucket":"demo-bucket","key":"a","method":"GET","deadline_unix_ms":9999999999999,"ingest_url":"https://edge/ingest","ingest_token":"tok","surprise":"x"}`)
	if _, err := Unmarshal(data, now); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("Unmarshal() error = %v, want ErrInvalidTicket", err)
	}
}
