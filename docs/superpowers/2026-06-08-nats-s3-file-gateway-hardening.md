# NATS S3 File Gateway Hardening Notes

Date: 2026-06-08  
Status: Project completion hardening notes

These notes tie the final hardening pass back to the approved project documents:

- [Design specification](specs/2026-06-08-nats-s3-file-gateway-design.md)
- [Implementation plan](plans/2026-06-08-nats-s3-file-gateway-implementation-plan.md)

## Final behavior decisions

- **NATS Core only:** tickets remain live HTTP work items and are not persisted, replayed, or queued durably.
- **Single edge instance:** pending responses are process-local and are lost on edge restart.
- **Range support:** the implementation accepts one standard HTTP byte range (`bytes=start-end`, `bytes=start-`, or `bytes=-suffix`). Malformed or multiple ranges are rejected before ticket publication. With signed URLs, the range must be signed and any `Range` header must match the signed claim.
- **Safe exposure:** public errors are generic mapped HTTP statuses. Runtime request logs redact detailed downstream errors so credentials, private hostnames, NATS subjects, and storage topology are not exposed.
- **Ingest authorization:** private ingest requires a valid one-time token and, when configured, an allowed connector client certificate identity.

## Validation boundaries

`make validate` is the local development validation target. It formats Go code, runs Go tests, builds all binaries, and validates Docker Compose configuration. The Docker-backed demo remains intentionally separate as `make e2e` (`make certs`, `make compose-up`, `make seed`, `make smoke`, `make compose-down`) so local validation can run without requiring Docker services to start.

Race-enabled Go tests are expected to pass with:

```sh
go test ./... -race
```
