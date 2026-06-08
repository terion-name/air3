// Package signing creates and validates HMAC-SHA256 public object URLs.
//
// Signatures cover the HTTP method, bucket, key, expiration, and supported
// request modifiers using a deterministic canonical string. Validation compares
// signatures in constant time and can be explicitly disabled for local demos.
package signing
