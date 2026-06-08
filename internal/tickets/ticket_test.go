package tickets

import (
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

func TestTicketMarshalUnmarshalValidatesClosedSchema(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	data, err := Marshal(validTicket(now), now)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	got, err := Unmarshal(data, now)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.RequestID != "req-123" || got.Bucket != "demo-bucket" || got.Key != "path/to/object.txt" {
		t.Fatalf("Unmarshal() = %#v", got)
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
		{"bad key", func(t *Ticket) { t.Key = "../secret" }},
		{"bad range", func(t *Ticket) { t.Range = "bytes=10-1" }},
		{"expired", func(t *Ticket) { t.DeadlineUnixMS = now.Add(-time.Second).UnixMilli() }},
		{"plain ingest url", func(t *Ticket) { t.IngestURL = "http://edge/ingest" }},
		{"ingest url credentials", func(t *Ticket) { t.IngestURL = "https://user:pass@edge/ingest" }},
		{"bad ingest token", func(t *Ticket) { t.IngestToken = "secret value" }},
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

func TestUnmarshalRejectsCredentialBytesAndHeaderFields(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	base, err := json.Marshal(validTicket(now))
	if err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{"secret_access_key", "object_bytes", "public_secret", "raw_headers", "headers"} {
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
