# NATS S3 File Gateway Implementation Plan

Date: 2026-06-08  
Status: Implementation planning document  
Source design: [`docs/superpowers/specs/2026-06-08-nats-s3-file-gateway-design.md`](../specs/2026-06-08-nats-s3-file-gateway-design.md)

## Planning constraints

This plan implements the approved project design without changing its trust model:

- **NATS Core only / no JetStream.** Tickets are live, ephemeral HTTP work items. They are not durable, replayed, or stored as a work queue.
- **Object bytes never pass through NATS.** NATS carries only small control-plane tickets and optional small status metadata.
- **The edge gateway remains single-instance.** The implementation should not add reverse-proxy routing, instance-ID fanout, shared request registries, or multi-edge ingress routing.
- **The private connector has no public inbound application listener.** It connects outbound to NATS and to the edge ingest endpoint.
- **S3 credentials stay private.** Only the private connector receives S3-compatible storage credentials.
- **Scope is implementation planning only.** This document does not add code, Compose, certificates, scripts, or runtime configuration.

Production notes may mention that multi-edge routing, autoscaling, stronger policy engines, and richer observability are out of scope for this project design, but implementation work should stay aligned with the single-edge, NATS Core architecture.

## Target repository layout

The implementation should create the following concrete files and directories over the planned work packages:

```text
.
├── cmd/
│   ├── edge-gateway/
│   │   └── main.go
│   ├── private-connector/
│   │   └── main.go
│   └── signurl/
│       └── main.go
├── internal/
│   ├── config/
│   │   ├── config.go
│   │   └── config_test.go
│   ├── tickets/
│   │   ├── ticket.go
│   │   └── ticket_test.go
│   ├── signing/
│   │   ├── hmac.go
│   │   └── hmac_test.go
│   ├── mtls/
│   │   ├── tls.go
│   │   └── tls_test.go
│   ├── natsclient/
│   │   ├── client.go
│   │   └── client_test.go
│   ├── pending/
│   │   ├── registry.go
│   │   └── registry_test.go
│   ├── ingest/
│   │   ├── handler.go
│   │   └── handler_test.go
│   └── s3fetch/
│       ├── fetcher.go
│       └── fetcher_test.go
├── deploy/
│   ├── compose.yaml
│   └── nats/
│       └── nats.conf
├── docs/
│   └── superpowers/
│       ├── plans/
│       │   └── 2026-06-08-nats-s3-file-gateway-implementation-plan.md
│       └── specs/
│           └── 2026-06-08-nats-s3-file-gateway-design.md
├── scripts/
│   ├── certs/
│   │   └── generate-dev-certs.sh
│   ├── seed/
│   │   └── seed-versitygw.sh
│   └── smoke/
│       └── smoke.sh
├── Dockerfile
├── Makefile
├── README.md
└── go.mod
```

Additional test files, package-specific helpers, and documentation may be added when they serve the work packages below, but the implementation should avoid adding alternate architecture directories that duplicate these responsibilities.

## Work package dependency map

```text
WP1 repository foundation
  ├─> WP2 shared protocol, config, signing, mTLS
  │     ├─> WP3 pending registry and ingest handoff
  │     │     └─> WP4 edge gateway
  │     └─> WP5 NATS Core client
  │           └─> WP4 edge gateway
  │           └─> WP6 private connector
  ├─> WP7 S3-compatible fetcher
  │     └─> WP6 private connector
  └─> WP8 Docker Compose and NATS config
        ├─> WP9 cert, seed, sign, and smoke scripts
        └─> WP10 docs, hardening, and full validation
```

The recommended order is linear for early packages and then parallelizable once the shared interfaces exist:

1. Build the repository and configuration foundation.
2. Implement shared protocol/security packages.
3. Implement gateway-local pending and ingest plumbing.
4. Implement NATS Core publish/subscribe integration.
5. Implement the public edge gateway and private connector around those shared packages.
6. Add Compose and scripts to validate the complete demo environment.
7. Complete hardening tests, documentation, and operational checks.

## Phase 1 — Repository foundation and toolchain

**Depends on:** approved design only.  
**Unlocks:** all implementation packages.

### Scope

Create the Go module, command directories, initial package directories, build tooling, and repository documentation skeleton.

### Expected files/directories

- `go.mod`
- `cmd/edge-gateway/main.go`
- `cmd/private-connector/main.go`
- `cmd/signurl/main.go`
- `internal/config/`
- `internal/tickets/`
- `internal/signing/`
- `internal/mtls/`
- `internal/natsclient/`
- `internal/pending/`
- `internal/ingest/`
- `internal/s3fetch/`
- `Makefile`
- `Dockerfile`
- `README.md`

### Implementation notes

- Use one root Go module.
- Command packages should remain thin: parse config, construct dependencies, start services, and handle shutdown.
- Internal packages should contain testable behavior.
- The initial `Dockerfile` can use a multi-stage Go build that supports all three binaries by build argument or separate targets.
- The initial `Makefile` should expose common developer commands, such as `fmt`, `test`, `build`, `compose-up`, `compose-down`, and `smoke`.

### Acceptance criteria

- Repository has the agreed package layout.
- All three commands compile, even if service behavior is introduced in later phases.
- `README.md` states the architecture constraints: NATS Core only, no object bytes through NATS, single edge gateway, and no S3 credentials on the edge.
- The `Makefile` commands used by later phases are documented.

### Validation commands

```sh
go test ./...
go build ./cmd/edge-gateway ./cmd/private-connector ./cmd/signurl
go fmt ./...
```

## Phase 2 — Shared configuration, tickets, signing, and mTLS packages

**Depends on:** Phase 1.  
**Unlocks:** edge gateway, private connector, NATS client, ingest handling, and signurl helper.

### Scope

Implement shared building blocks that define the system contract before wiring runtime services.

### Expected files/directories

- `internal/config/config.go`
- `internal/config/config_test.go`
- `internal/tickets/ticket.go`
- `internal/tickets/ticket_test.go`
- `internal/signing/hmac.go`
- `internal/signing/hmac_test.go`
- `internal/mtls/tls.go`
- `internal/mtls/tls_test.go`
- `cmd/signurl/main.go`

### Implementation notes

- `internal/config` should parse environment variables and optional config file paths for:
  - public listener address;
  - private ingest listener address;
  - ingest public-to-connector callback URL;
  - NATS URL, subject, queue group, TLS files, and credentials where needed;
  - S3 endpoint, region, bucket allowlist, access key, secret key, path-style setting, and TLS behavior;
  - signing key, signature TTL, pending request TTL, and stream timeout values.
- `internal/tickets` should define the NATS ticket schema, including request ID, method, bucket, key, optional range, deadline, ingest URL, one-time ingest token, and safe metadata.
- Tickets must not include S3 credentials, object bytes, public client secrets, or raw untrusted request headers.
- `internal/signing` should implement HMAC signed URL creation and validation with constant-time comparison.
- The signature should cover method, bucket, key, expiry, and any supported range/response-control parameters.
- `internal/mtls` should centralize TLS server/client config loading and client identity authorization helpers for NATS and ingest connections.
- `cmd/signurl` should call `internal/signing` and use the same canonicalization path as the gateway.

### Acceptance criteria

- Ticket schema is documented in package comments and tests.
- Signing tests cover valid signatures, tampered bucket/key/method, expired signatures, malformed timestamps, and constant-time comparison path.
- Config tests cover required fields, defaults, invalid durations, invalid allowlist entries, and TLS file validation hooks.
- mTLS helpers can load CA, certificate, and key files and reject missing or mismatched configuration.
- `cmd/signurl` generates URLs accepted by the signing validator.

### Validation commands

```sh
go test ./internal/config ./internal/tickets ./internal/signing ./internal/mtls ./cmd/signurl
go test ./...
```

## Phase 3 — Pending request registry and private ingest handoff

**Depends on:** Phase 2.  
**Unlocks:** edge gateway request lifecycle.

### Scope

Implement the in-memory edge-local pending request registry and the private ingest handler used by the connector to stream data back into the held public response.

### Expected files/directories

- `internal/pending/registry.go`
- `internal/pending/registry_test.go`
- `internal/ingest/handler.go`
- `internal/ingest/handler_test.go`

### Implementation notes

- `internal/pending` should own request IDs, deadlines, cancellation, one-time token state, and response handoff channels.
- Registry entries are local to the single edge gateway process. Do not introduce a shared registry or cross-instance routing.
- Late ingest after expiry must be rejected.
- Token consumption should be one-time and bound to request ID, method, bucket, key, and deadline.
- Client cancellation should remove the pending entry and cause later ingest to fail.
- `internal/ingest` should expose an HTTP handler that:
  - requires an authorized mTLS client identity;
  - validates request ID and one-time ingest token;
  - receives object status and safe metadata from connector headers or a small prelude format;
  - streams the request body into the matching pending response path;
  - maps invalid identity/token/request state to private ingest rejection and a safe public failure.

### Acceptance criteria

- Concurrent pending requests are isolated from each other.
- Expired or cancelled pending entries cannot be completed.
- Token replay is rejected.
- Metadata propagation is allowlisted.
- Ingest body streaming does not buffer entire objects in memory.

### Validation commands

```sh
go test ./internal/pending ./internal/ingest -race
go test ./...
```

## Phase 4 — NATS Core control-plane client

**Depends on:** Phase 2.  
**Unlocks:** edge ticket publishing and connector ticket subscription.

### Scope

Implement NATS Core connection helpers, subject configuration, queue subscription support, and small status/error message helpers if needed.

### Expected files/directories

- `internal/natsclient/client.go`
- `internal/natsclient/client_test.go`
- `deploy/nats/nats.conf` later completed in Phase 8

### Implementation notes

- Use Core NATS publish/subscribe and queue groups only.
- Do not create streams, durable consumers, replay settings, or persistence configuration.
- Connection setup should support TLS or mTLS from `internal/mtls`.
- Subject permissions should be designed so the edge can publish request tickets and the connector can subscribe through a queue group.
- Any status subject should be small metadata only and must not carry object bytes.
- Publish operations should observe request deadlines and return an error promptly when NATS is unavailable.

### Acceptance criteria

- Unit tests verify subject construction and permission-related configuration values.
- Integration tests can start a local NATS server and prove one queue subscriber receives one ticket.
- No JetStream APIs or configuration are present.
- Public request behavior remains live/ephemeral: if no connector handles the ticket before deadline, the gateway times out.

### Validation commands

```sh
go test ./internal/natsclient
go test ./... -run 'NATS|Queue|Ticket'
```

When the Compose environment exists:

```sh
docker compose -f deploy/compose.yaml exec nats nats-server --signal reload || true
```

## Phase 5 — Edge gateway service

**Depends on:** Phases 2, 3, and 4.  
**Unlocks:** end-to-end request flow once the connector exists.

### Scope

Implement `cmd/edge-gateway` as a public HTTPS file listener plus a separate private mTLS ingest listener.

### Expected files/directories

- `cmd/edge-gateway/main.go`
- Supporting files in `internal/config`, `internal/signing`, `internal/tickets`, `internal/pending`, `internal/ingest`, `internal/natsclient`, and `internal/mtls`

### Implementation notes

- The public listener should support `GET` and `HEAD`.
- Validate path shape, method, bucket/key allowlist, signed URL material, and optional range syntax before publishing a ticket.
- Create a request ID, pending registry entry, deadline, and one-time ingest token before ticket publish.
- Publish a NATS Core ticket containing only safe control-plane metadata.
- Hold the public response until ingest completes, client disconnects, publish fails, or pending TTL expires.
- Run the private ingest listener on a separate address/port with mTLS requirements.
- Sanitize public error bodies and response headers.
- Keep the edge process as a single-instance owner of pending requests.

### Acceptance criteria

- Invalid signatures fail before any NATS ticket is published.
- Disallowed bucket/key requests fail before any NATS ticket is published.
- NATS unavailable maps to a safe `503 Service Unavailable` or a short timeout response.
- No connector available maps to `504 Gateway Timeout` after the pending request deadline.
- `GET` streams the body from ingest to the public response.
- `HEAD` returns metadata without a response body.
- Client disconnect cancels the pending record.
- Logs contain request IDs and high-level outcomes, not HMAC secrets, one-time ingest tokens, S3 credentials, or full signed query strings.

### Validation commands

```sh
go test ./cmd/edge-gateway ./internal/pending ./internal/ingest ./internal/signing ./internal/natsclient -race
go test ./...
```

After Compose exists:

```sh
make compose-up
make smoke
make compose-down
```

## Phase 6 — Private connector and S3-compatible fetcher

**Depends on:** Phases 2, 4, and S3-compatible storage config from Phase 8 for full integration.  
**Unlocks:** complete object retrieval path.

### Scope

Implement `cmd/private-connector` and `internal/s3fetch` so the connector receives tickets, fetches objects from VersityGW, and streams them to the edge ingest endpoint.

### Expected files/directories

- `cmd/private-connector/main.go`
- `internal/s3fetch/fetcher.go`
- `internal/s3fetch/fetcher_test.go`
- Supporting use of `internal/config`, `internal/tickets`, `internal/mtls`, and `internal/natsclient`

### Implementation notes

- The connector subscribes to the configured NATS Core ticket subject using a queue group.
- One connector should handle each ticket.
- The connector validates ticket shape, deadline, bucket/key allowlist, and supported method/range before fetching.
- Fetch objects from S3-compatible storage using private credentials and path-style addressing if required by VersityGW.
- Stream response data directly from S3 to the edge ingest endpoint over HTTPS mTLS.
- Include the one-time ingest token with the ingest request.
- Stop reading from S3 if the ingest request fails or the context is cancelled.
- Map S3 missing object and backend errors into safe status metadata for the edge to expose.

### Acceptance criteria

- Connector has no public inbound application listener.
- S3 credentials are read only by the connector process.
- Queue group behavior distributes tickets without duplicating object streams.
- Missing object maps to `404 Not Found` on the public response.
- S3/backend unavailable maps to safe `503` or timeout behavior according to failure point.
- Connector rejects expired tickets and disallowed bucket/key paths.

### Validation commands

```sh
go test ./cmd/private-connector ./internal/s3fetch ./internal/natsclient ./internal/tickets -race
go test ./...
```

After Compose exists:

```sh
make compose-up
scripts/seed/seed-versitygw.sh
scripts/smoke/smoke.sh
make compose-down
```

## Phase 7 — Development signing helper

**Depends on:** Phase 2.  
**Unlocks:** repeatable smoke tests and README examples.

### Scope

Complete `cmd/signurl` as a development/demo helper for producing signed public URLs.

### Expected files/directories

- `cmd/signurl/main.go`
- `internal/signing/hmac.go`
- README usage examples
- Smoke script usage in `scripts/smoke/smoke.sh`

### Implementation notes

- `signurl` should accept method, base URL, bucket, key, expiry/TTL, and optional range or safe response controls when supported.
- It should use exactly the same canonicalization and HMAC implementation as the edge gateway.
- It should avoid printing secrets.
- Output should be easy to consume from shell scripts.

### Acceptance criteria

- Generated `GET` and `HEAD` URLs pass gateway validation.
- Tampered URLs fail validation.
- Expired URLs fail validation.
- README examples can be copied and run in the Compose environment.

### Validation commands

```sh
go test ./cmd/signurl ./internal/signing
go run ./cmd/signurl --help
```

## Phase 8 — Docker Compose, NATS config, and network boundaries

**Depends on:** Phase 1 for build artifacts; full runtime validation depends on Phases 5 and 6.  
**Unlocks:** end-to-end smoke tests.

### Scope

Add the demo deployment environment with separated public, broker, and private networks.

### Expected files/directories

- `deploy/compose.yaml`
- `deploy/nats/nats.conf`
- `Dockerfile`
- `Makefile` targets for Compose lifecycle
- Development certificate paths consumed from `scripts/certs/`

### Implementation notes

- Compose services should include edge gateway, private connector, NATS, and VersityGW.
- Compose networks should be:
  - `public`: edge gateway and public test access;
  - `broker`: edge gateway, NATS, and private connector;
  - `private`: private connector and VersityGW.
- VersityGW must attach only to `private`.
- Edge gateway must not attach to `private`.
- Connector attaches to `broker` and `private`.
- NATS should expose only the broker-facing endpoint needed by edge and connector.
- The public file listener and private ingest listener must use separate ports and TLS settings.
- `deploy/nats/nats.conf` should configure TLS/mTLS and subject permissions for Core NATS only.

### Acceptance criteria

- `docker compose -f deploy/compose.yaml config` succeeds.
- Network inspection confirms the edge gateway cannot reach VersityGW directly.
- Edge can reach NATS on the broker network.
- Connector can reach NATS and VersityGW.
- NATS config contains no stream or persistence setup.
- Compose starts and stops cleanly through `Makefile` targets.

### Validation commands

```sh
docker compose -f deploy/compose.yaml config
make compose-up
docker compose -f deploy/compose.yaml ps
make compose-down
```

Network boundary checks to include in the implementation or smoke script:

```sh
docker compose -f deploy/compose.yaml exec edge-gateway sh -c 'nc -zv versitygw 9000 && exit 1 || exit 0'
docker compose -f deploy/compose.yaml exec private-connector sh -c 'nc -zv versitygw 9000'
```

## Phase 9 — Development scripts and smoke tests

**Depends on:** Phases 5, 6, 7, and 8.  
**Unlocks:** repeatable validation for the complete demo.

### Scope

Add development scripts for local certificates, storage seeding, and end-to-end smoke tests.

### Expected files/directories

- `scripts/certs/generate-dev-certs.sh`
- `scripts/seed/seed-versitygw.sh`
- `scripts/smoke/smoke.sh`
- `Makefile` targets: `certs`, `seed`, `smoke`
- README instructions for running the scripts

### Implementation notes

- Certificate generation should create a development CA and separate server/client certificates for:
  - public edge listener as needed by local smoke tests;
  - private edge ingest listener;
  - connector client identity;
  - NATS server and client identities.
- Generated secrets and certificates should be ignored by Git unless there is a deliberate checked-in sample certificate strategy for non-secret demo fixtures.
- Seeding should create a demo bucket and at least one text object and one binary-ish object.
- Smoke tests should generate signed URLs, test `GET`, test `HEAD`, test missing objects, test bad signatures, and test no-connector timeout behavior.
- Smoke tests should verify expected live-ticket behavior: when the connector is down, the public request fails by timeout or mapped unavailable response rather than being replayed later.

### Acceptance criteria

- A fresh clone can run cert generation, Compose startup, seeding, and smoke tests using documented commands.
- Smoke tests verify successful object retrieval and expected failure modes.
- Smoke tests confirm no object bytes transit NATS by relying only on ticket subjects and by not configuring any byte-transfer subject.
- Smoke tests confirm the edge cannot connect directly to VersityGW.
- Scripts fail fast with actionable messages.

### Validation commands

```sh
make certs
make compose-up
make seed
make smoke
make compose-down
```

## Phase 10 — Hardening, documentation, and release readiness

**Depends on:** Phases 1 through 9.  
**Unlocks:** handoff to normal project maintenance.

### Scope

Close gaps in tests, docs, failure-mode coverage, and safe operational behavior.

### Expected files/directories

- Additional tests under `internal/*`
- README architecture and quickstart sections
- Updates under `docs/superpowers/` linking spec and plan
- Optional `docs/superpowers/runbooks/` page for troubleshooting demo failures
- `Makefile` validation target, for example `make validate`

### Implementation notes

- Expand tests for timeout, cancellation, disconnect, late ingest, invalid mTLS identity, invalid one-time token, header sanitization, and unsafe logging.
- Decide whether Range support is included. If included, test `206 Partial Content` and `Content-Range`; if not included, return clear client-facing rejection for Range requests.
- Add README diagrams or prose showing public, broker, and private network boundaries.
- Document expected constraints from live ephemeral tickets: no replay, no durable queue, held responses lost on edge restart, and timeout behavior when connector/NATS/backend is unavailable.
- Document out-of-scope production considerations separately from implementation work.

### Acceptance criteria

- `make validate` passes and includes formatting, unit tests, race-sensitive package tests where practical, Compose config validation, and smoke tests.
- README quickstart works from a fresh checkout.
- Documentation clearly states NATS Core only, no JetStream, edge single-instance, and S3 credentials only on the connector.
- Failure modes are covered by tests or smoke checks.
- Logs and errors do not reveal secrets or private topology.

### Validation commands

```sh
make validate
go test ./... -race
docker compose -f deploy/compose.yaml config
make smoke
```

## Cross-cutting test strategy

### Unit tests

Unit tests should cover deterministic package behavior without Docker dependencies:

- `internal/config`: defaults, required values, invalid values, duration parsing, allowlist parsing.
- `internal/tickets`: JSON encode/decode, schema validation, deadline handling, disallowed fields.
- `internal/signing`: canonicalization, valid/tampered/expired signatures, constant-time comparison path.
- `internal/mtls`: TLS config construction and client identity authorization with generated test certificates.
- `internal/pending`: concurrent pending records, cancellation, expiry, one-time token consumption.
- `internal/ingest`: mTLS identity checks, token validation, metadata allowlisting, streaming behavior.
- `internal/natsclient`: subject construction, queue subscription callback behavior with a local NATS process or test harness.
- `internal/s3fetch`: S3 result mapping using fake clients and, separately, integration against VersityGW.

### Integration tests

Integration tests should use local processes or Compose services where practical:

- NATS TLS/mTLS connection success and failure.
- Queue group delivery where exactly one connector receives each ticket.
- Connector fetch from VersityGW with valid private credentials.
- Edge ingest with valid and invalid connector certificates.
- Public request cancellation causing connector stream cancellation.

### Smoke tests

Smoke tests should run against `deploy/compose.yaml`:

- Generate development certificates.
- Start NATS, VersityGW, edge gateway, and private connector.
- Seed demo objects.
- Generate a signed `GET` URL and verify status, body, and safe headers.
- Generate a signed `HEAD` URL and verify metadata with no response body.
- Verify bad signature rejection.
- Verify expired signature rejection.
- Verify missing object maps to `404 Not Found`.
- Stop the connector and verify a new public request times out or returns a mapped unavailable response.
- Restart the connector and verify the previous failed request is not replayed.
- Confirm the edge container cannot connect to VersityGW directly.

## Risk register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Public response waits can leak goroutines or pending entries | Memory/resource leak under slow clients or unavailable connector | Enforce pending TTLs, context cancellation, registry cleanup tests, and race tests. |
| Streaming path accidentally buffers whole objects | Large object requests can exhaust memory | Use `io.Copy`/streaming readers, avoid full-body reads, and include large-object smoke checks. |
| Ticket schema grows to include sensitive data | Secrets could cross NATS or logs | Keep ticket package narrow, add tests for prohibited fields, and review logs. |
| NATS outage behavior is ambiguous | Public clients see inconsistent failures | Define short publish timeout and pending timeout mappings in config and tests. |
| mTLS identity validation is too broad | Unauthorized connector can ingest bytes | Pin client CA and authorized subject/SAN values in config and tests. |
| Signed URL canonicalization differs between `signurl` and edge | Demo URLs fail or unsafe URLs pass | Reuse `internal/signing` in both command and gateway; test generated URLs. |
| Compose network boundaries drift | Edge might reach private storage | Add explicit smoke checks and document required networks. |
| HEAD behavior accidentally streams a body | Protocol correctness issue | Share metadata path while suppressing body copy for HEAD; test via smoke and unit tests. |
| Connector continues reading after public client disconnect | Wasted private bandwidth | Propagate cancellation via ingest failure/context close and test disconnect behavior. |
| Range support is partially implemented | Incorrect `206`/`Content-Range` behavior | Either fully test Range semantics or reject Range requests clearly. |

## Proposed GitHub issue breakdown

The following issues can be copied into GitHub. Create them in dependency order and link child issues back to this planning document and the approved design spec.

### Issue 1 — Repository foundation for NATS S3 file gateway

```markdown
## Summary
Create the initial Go module, command directories, internal package directories, Dockerfile, Makefile, and README skeleton for the approved NATS S3 file gateway design.

## Design constraints
- Use one root Go module.
- Keep commands thin and behavior in `internal/*` packages.
- NATS Core only / no JetStream.
- Edge gateway remains single-instance.
- Do not place S3 credentials on the edge gateway.

## Checklist
- [ ] Add `go.mod`.
- [ ] Add `cmd/edge-gateway/main.go`.
- [ ] Add `cmd/private-connector/main.go`.
- [ ] Add `cmd/signurl/main.go`.
- [ ] Add package directories for `internal/config`, `internal/tickets`, `internal/signing`, `internal/mtls`, `internal/natsclient`, `internal/pending`, `internal/ingest`, and `internal/s3fetch`.
- [ ] Add `Dockerfile` with a path for building the binaries.
- [ ] Add `Makefile` targets for formatting, testing, building, Compose lifecycle, and smoke tests.
- [ ] Add README architecture constraints and quickstart placeholders.

## Acceptance criteria
- [ ] `go test ./...` passes.
- [ ] `go build ./cmd/edge-gateway ./cmd/private-connector ./cmd/signurl` passes.
- [ ] README states NATS Core only, no object bytes through NATS, single edge gateway, and connector-only S3 credentials.
```

### Issue 2 — Shared config, ticket schema, signing, and mTLS packages

```markdown
## Summary
Implement shared configuration parsing, ticket schema, HMAC signed URL logic, and TLS/mTLS helpers.

## Dependencies
Depends on Issue 1.

## Checklist
- [ ] Implement `internal/config` for listener, NATS, S3, signing, allowlist, and timeout settings.
- [ ] Implement `internal/tickets` with the control-plane ticket schema and validation.
- [ ] Implement `internal/signing` for HMAC URL signing and validation.
- [ ] Implement `internal/mtls` for TLS/mTLS config loading and identity authorization.
- [ ] Wire `cmd/signurl` to `internal/signing`.
- [ ] Add tests for invalid config, ticket validation, signature tampering, expiration, and mTLS loading.

## Acceptance criteria
- [ ] Tickets cannot include S3 credentials, object bytes, public client secrets, or raw untrusted headers.
- [ ] Signature validation uses constant-time comparison.
- [ ] `cmd/signurl` output is accepted by the validator.
- [ ] `go test ./internal/config ./internal/tickets ./internal/signing ./internal/mtls ./cmd/signurl` passes.
```

### Issue 3 — Pending request registry and private ingest handler

```markdown
## Summary
Implement edge-local pending request lifecycle management and private mTLS ingest handling.

## Dependencies
Depends on Issue 2.

## Checklist
- [ ] Implement `internal/pending` registry with request IDs, deadlines, cancellation, and one-time token state.
- [ ] Implement `internal/ingest` HTTP handler for connector streams.
- [ ] Validate mTLS connector identity before ingest.
- [ ] Validate one-time ingest token and pending request state.
- [ ] Add safe metadata allowlisting.
- [ ] Add streaming handoff without buffering whole objects.
- [ ] Add tests for expiry, cancellation, token replay, late ingest, and concurrent requests.

## Acceptance criteria
- [ ] Registry is in-memory and local to the single edge gateway process.
- [ ] Expired/cancelled requests cannot be completed.
- [ ] Token replay is rejected.
- [ ] `go test ./internal/pending ./internal/ingest -race` passes.
```

### Issue 4 — NATS Core client and queue subscription behavior

```markdown
## Summary
Implement NATS Core connection helpers, ticket publish, and queue group subscription behavior.

## Dependencies
Depends on Issue 2.

## Checklist
- [ ] Implement `internal/natsclient` connection setup with TLS/mTLS support.
- [ ] Implement ticket publishing with context/deadline handling.
- [ ] Implement connector queue group subscription.
- [ ] Add subject configuration and tests.
- [ ] Add a local or containerized integration test proving one queue subscriber receives one ticket.

## Acceptance criteria
- [ ] Uses Core NATS publish/subscribe and queue groups only.
- [ ] No streams, durable consumers, replay, or persistence setup are added.
- [ ] NATS unavailable returns promptly enough for gateway error mapping.
- [ ] `go test ./internal/natsclient` passes.
```

### Issue 5 — Edge gateway public listener and private ingest listener

```markdown
## Summary
Implement `cmd/edge-gateway` with public GET/HEAD handling, signed URL validation, pending request creation, NATS ticket publish, and separate private mTLS ingest listener.

## Dependencies
Depends on Issues 2, 3, and 4.

## Checklist
- [ ] Add public HTTPS handler for `GET` and `HEAD`.
- [ ] Validate method, bucket/key path, allowlist, signature, and optional range syntax before publishing tickets.
- [ ] Create pending request records with deadline and one-time ingest token.
- [ ] Publish NATS Core tickets.
- [ ] Hold public responses until ingest completes, client disconnects, publish fails, or TTL expires.
- [ ] Run private ingest listener on a separate mTLS port.
- [ ] Sanitize public errors, headers, and logs.

## Acceptance criteria
- [ ] Bad signatures and disallowed paths fail before NATS publish.
- [ ] No connector available maps to timeout/unavailable behavior.
- [ ] `GET` streams from ingest to public response.
- [ ] `HEAD` returns metadata without a body.
- [ ] Client disconnect cancels pending state.
- [ ] `go test ./cmd/edge-gateway ./internal/pending ./internal/ingest ./internal/signing ./internal/natsclient -race` passes.
```

### Issue 6 — Private connector and S3-compatible fetcher

```markdown
## Summary
Implement `cmd/private-connector` and `internal/s3fetch` to consume tickets, fetch from VersityGW/S3-compatible storage, and stream objects to edge ingest.

## Dependencies
Depends on Issues 2 and 4. Full validation also depends on Issue 8.

## Checklist
- [ ] Implement connector startup and shutdown.
- [ ] Subscribe to NATS Core ticket subject using a queue group.
- [ ] Validate ticket shape, deadline, method, range, and bucket/key allowlist.
- [ ] Implement S3-compatible object fetch in `internal/s3fetch`.
- [ ] Stream S3 body to edge ingest over HTTPS mTLS with one-time token.
- [ ] Map missing objects and backend errors into safe status metadata.
- [ ] Stop reading from S3 when ingest fails or context is cancelled.

## Acceptance criteria
- [ ] Connector has no public inbound application listener.
- [ ] Only connector receives S3 credentials.
- [ ] One ticket produces at most one connector stream.
- [ ] Missing object maps to public `404 Not Found`.
- [ ] `go test ./cmd/private-connector ./internal/s3fetch ./internal/natsclient ./internal/tickets -race` passes.
```

### Issue 7 — Docker Compose demo environment and NATS configuration

```markdown
## Summary
Add Docker Compose services, separated networks, and NATS TLS/mTLS subject permissions for the demo environment.

## Dependencies
Depends on Issue 1. End-to-end validation depends on Issues 5 and 6.

## Checklist
- [ ] Add `deploy/compose.yaml` with edge gateway, private connector, NATS, and VersityGW services.
- [ ] Add `deploy/nats/nats.conf`.
- [ ] Define `public`, `broker`, and `private` networks.
- [ ] Attach edge gateway to `public` and `broker`, not `private`.
- [ ] Attach connector to `broker` and `private`.
- [ ] Attach VersityGW only to `private`.
- [ ] Configure separate public file and private ingest ports.
- [ ] Add Makefile targets for Compose lifecycle.

## Acceptance criteria
- [ ] `docker compose -f deploy/compose.yaml config` passes.
- [ ] Edge cannot connect directly to VersityGW.
- [ ] Connector can connect to VersityGW and NATS.
- [ ] NATS config is Core NATS only and contains no stream/persistence setup.
```

### Issue 8 — Certificate, seed, signing, and smoke scripts

```markdown
## Summary
Add development scripts to generate certificates, seed VersityGW, and run repeatable end-to-end smoke tests.

## Dependencies
Depends on Issues 5, 6, and 7.

## Checklist
- [ ] Add `scripts/certs/generate-dev-certs.sh`.
- [ ] Add `scripts/seed/seed-versitygw.sh`.
- [ ] Add `scripts/smoke/smoke.sh`.
- [ ] Add `make certs`, `make seed`, and `make smoke` targets.
- [ ] Smoke test signed `GET` success.
- [ ] Smoke test signed `HEAD` success with no body.
- [ ] Smoke test bad signature and expired signature rejection.
- [ ] Smoke test missing object maps to `404`.
- [ ] Smoke test connector-down timeout/unavailable behavior and no replay after connector restart.
- [ ] Smoke test edge cannot reach VersityGW directly.

## Acceptance criteria
- [ ] Fresh checkout can run cert generation, Compose startup, seeding, and smoke tests from README commands.
- [ ] Scripts fail fast with actionable messages.
- [ ] `make certs && make compose-up && make seed && make smoke && make compose-down` passes.
```

### Issue 9 — Hardening tests and project documentation

```markdown
## Summary
Complete failure-mode coverage, safe logging/header checks, README quickstart, and docs links back to the approved design and implementation plan.

## Dependencies
Depends on Issues 1 through 8.

## Checklist
- [ ] Add tests for timeouts, cancellation, client disconnect, late ingest, invalid mTLS identity, invalid token, and unsafe headers.
- [ ] Decide and document Range behavior; either fully implement/test it or reject Range requests clearly.
- [ ] Add README architecture overview and quickstart.
- [ ] Document expected live-ticket constraints: no replay, no durable queue, held responses lost on edge restart, timeout when connector/NATS/backend is unavailable.
- [ ] Add or update docs under `docs/superpowers/` linking the design spec and plan.
- [ ] Add `make validate` covering format, tests, Compose config, and smoke tests.

## Acceptance criteria
- [ ] `make validate` passes.
- [ ] `go test ./... -race` passes or documented packages with acceptable race-test exclusions are explained.
- [ ] README quickstart works from a fresh checkout.
- [ ] Logs and public errors do not expose secrets or private topology.
```

## Suggested milestone order

1. **Foundation:** Issues 1 and 2.
2. **Control path and edge internals:** Issues 3 and 4.
3. **Runtime services:** Issues 5 and 6.
4. **Demo environment:** Issues 7 and 8.
5. **Hardening and handoff:** Issue 9.

Each issue should link to the approved design spec and this implementation plan. Pull requests should remain small enough to validate independently while preserving the dependency order above.
