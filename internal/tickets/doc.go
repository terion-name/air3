// Package tickets defines the closed JSON schema for ephemeral NATS work tickets.
//
// A ticket contains only version, request_id, bucket, key, method, optional
// range, optional server alias, deadline_unix_ms, ingest_url, ingest_token, and
// optional trace_id. It rejects unknown fields and explicit credential,
// object-byte, client-secret, and raw-header fields so NATS remains a control
// plane only.
package tickets
