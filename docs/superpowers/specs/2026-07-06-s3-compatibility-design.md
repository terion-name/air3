# S3 compatibility upgrades: aws-chunked, signed payloads, multipart upload

Date: 2026-07-06. Approved scope: features 1, 2, 4 from the compatibility
review; all defaults confirmed (signed payloads always-on, trailers parsed
and discarded, four core multipart operations).

## Goal

A stock AWS CLI/SDK with no custom configuration can upload through the
edge's S3-compatible API: `aws s3api put-object` works with default
checksum/signing settings, and `aws s3 cp` of files >8 MiB works via
multipart upload.

## Feature 1 — aws-chunked upload decoding (edge)

Modern clients send upload bodies as
`x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER` with
aws-chunked framing and a checksum trailer (e.g. `x-amz-checksum-crc32`).

- The edge accepts this mode for upload operations (PutObject, UploadPart,
  CompleteMultipartUpload body). `X-Amz-Decoded-Content-Length` is
  required and becomes the object/part content length.
- Unsigned aws-chunked framing is identical to HTTP/1.1 chunked framing;
  decoding wraps `httputil.NewChunkedReader` plus exact decoded-length
  enforcement (`internal/s3api`).
- Trailers are parsed for framing validity and discarded. By the time the
  trailer arrives the payload has already streamed to the backend, so a
  checksum mismatch cannot fail the upload; this is documented.
- Signed-chunk streaming modes (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD*`)
  remain rejected: clients use them only on plain-HTTP endpoints and the
  edge is HTTPS-only.
- One body-resolution step in the edge feeds both the routed
  (upload-source) and direct-server paths.

## Feature 2 — signed-payload uploads (edge)

`ValidatePayloadHashForOperation` additionally accepts a 64-hex-digit
payload hash for mutation operations. The hash is covered by the verified
SigV4 signature; the edge does not re-hash the streaming body (impossible
without buffering). Transport integrity remains TLS's job. Always-on, no
config flag. Documented as "accepted but not verified".

## Feature 4 — multipart upload

Operations: CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
AbortMultipartUpload — the set `aws s3 cp` needs. ListParts and
ListMultipartUploads are deferred.

- **Classification** (`internal/s3api`): by query params — POST `?uploads`
  → Create; PUT `?partNumber&uploadId` → UploadPart; POST `?uploadId` →
  Complete; DELETE `?uploadId` → Abort.
- **Gating**: all four are mutations, gated by `MUTATIONS_ENABLED` on both
  edge and connector, same as PutObject/DeleteObject.
- **Tickets** (`internal/tickets`): new operations plus a `Multipart`
  envelope: `UploadID`, `PartNumber`, and `Rewrite{Bucket,Key}` carrying
  the public names for response XML. Part lists never ride NATS tickets,
  keeping message size bounded.
- **Body transport**: UploadPart streams its body through the existing
  upload-source channel exactly like PutObject. CompleteMultipartUpload's
  part-list XML travels the same way, capped at 1 MiB (S3 allows at most
  10,000 parts ≈ <1 MiB of XML).
- **Backend calls** (`internal/s3fetch`): SDK CreateMultipartUpload /
  UploadPart (UNSIGNED-PAYLOAD, same as PutObject) / Complete (parses the
  XML via `internal/s3api`) / Abort. Create and Complete responses are
  rendered deterministically with public bucket/key (same pattern as the
  ListObjectsV2 rewrite).
- **Statuses**: Create 200 + XML, UploadPart 200 + ETag header,
  Complete 200 + XML, Abort 204.
- **Direct-server aliases** get all four operations through the shared
  s3fetch code (edge-only gate, as with other direct mutations).

## Error handling

- Malformed chunked framing or decoded-length mismatch surfaces as a read
  error mid-stream; the backend PUT aborts (short body) and the client
  receives the standard stream-failure error.
- Unknown/oversized Complete bodies → 400 InvalidRequest at the edge.
- Multipart requests while mutations are disabled → 405, `Allow`
  reflecting the gate.

## Testing

Unit tests per package (chunked decoder incl. malformed framing and length
mismatches; classification; ticket validation; s3fetch operations against
stub servers; edge/connector wiring). End-to-end: compose stack driven by
a default-configured `aws` CLI — put-object without custom config, and
`aws s3 cp` of a >8 MiB file (multipart), plus abort and gate-off probes.

## Docs

README limitations updated (multipart/aws-chunked/signed-payload lines),
`docs/configuration.md` operation list, and a short "Using the AWS CLI"
note. `deploy/scripts/smoke.sh` gains default-config and multipart checks.
