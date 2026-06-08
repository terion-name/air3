# NATS S3 File Gateway Design

Date: 2026-06-08  
Status: Approved design specification  
Scope: Documentation for the MVP architecture; no implementation is included in this step.

## Summary

This project is a Go monorepo for a demo that securely exposes files from private S3-compatible storage to the public internet without placing S3 credentials, the S3 service, or the private storage network on the public edge.

The MVP uses:

- A public **edge gateway** that accepts file requests and holds the HTTP response open while waiting for a private connector to stream the object back.
- A **private connector** that runs inside the private perimeter, receives control-plane tickets over NATS, fetches objects from S3-compatible storage, and streams object bytes to a private edge ingest endpoint.
- **NATS as the control plane only**. Object bytes never pass through NATS.
- **VersityGW** as the S3-compatible demo storage service.
- Docker Compose networks that keep public, broker, and private responsibilities separated.

The MVP deliberately does not use JetStream and does not attempt multi-edge reverse-proxy fanout. Requests are ephemeral: if the broker, connector, storage backend, or ingest path is unavailable, the public request fails by timeout or mapped error response.

## Goals

- Expose selected objects from private S3-compatible storage through public HTTPS URLs.
- Keep all S3 credentials off the public edge gateway.
- Keep S3-compatible storage reachable only from the private network.
- Use NATS for low-latency request ticket delivery between the public edge and private connector.
- Support a Docker Compose demo with clear network boundaries.
- Provide a monorepo shape that can grow from demo to production without changing the core trust model.

## Non-goals for the MVP

- No JetStream persistence, replay, durable work queues, or object byte transport through NATS.
- No reverse-proxy fanout or request routing across multiple edge gateway instances.
- No public inbound application listener on the connector.
- No S3 credentials on the edge gateway.
- No code, Compose file, certificates, Makefile, seed scripts, smoke scripts, or other implementation files in this documentation step.

## Monorepo shape

The recommended repository layout is a single Go module with focused binaries and shared internal packages:

```text
.
├── cmd/
│   ├── edge-gateway/
│   ├── private-connector/
│   └── signurl/
├── internal/
│   ├── auth/
│   ├── config/
│   ├── gateway/
│   ├── ingest/
│   ├── natscontrol/
│   ├── objectstore/
│   └── protocol/
├── deploy/
│   └── compose.yaml
├── docs/
│   └── superpowers/
│       └── specs/
├── scripts/
│   ├── certs/
│   ├── seed/
│   └── smoke/
└── go.mod
```

For this step, only this specification file is added. The layout above describes the approved target shape and is not scaffolded here.

### Binaries

- `cmd/edge-gateway`: Public HTTPS file listener plus private mTLS ingest listener.
- `cmd/private-connector`: Private worker that consumes NATS tickets, fetches from S3-compatible storage, and streams objects to the edge ingest endpoint.
- `cmd/signurl`: Development/demo helper for producing signed public URLs.

### Shared internal packages

- `internal/auth`: HMAC signed URL validation, ingest token generation/verification, and allowlist checks.
- `internal/config`: Environment and file-based configuration parsing.
- `internal/gateway`: Public request lifecycle and held response coordination.
- `internal/ingest`: Private ingest endpoint handling and stream handoff.
- `internal/natscontrol`: NATS connection setup, subjects, queue group subscription, TLS/mTLS options, and publish/subscribe helpers.
- `internal/objectstore`: S3-compatible object operations using the AWS SDK for Go.
- `internal/protocol`: Ticket and ingest metadata structures shared by the edge and connector.

## Runtime components

### Edge gateway

The edge gateway has two separate listeners:

1. **Public HTTPS file listener**
   - Accepts public file requests at `https://files.example.com/{bucket}/{key...}`.
   - Validates signed URL material when configured.
   - Creates a request ID and pending request record.
   - Publishes a NATS ticket for private connectors.
   - Holds the public HTTP request until the connector streams data back or the request expires.

2. **Private mTLS ingest listener**
   - Accepts object streams from the connector over HTTPS with mutual TLS.
   - Requires a one-time ingest token tied to the pending request ID.
   - Does not serve public file requests.
   - Forwards the connector's stream to the matching held public response.

The edge gateway is a single instance in the MVP. This makes request ownership simple: the process that created a request ID is the only process that can receive its ingest stream. Production deployments can later add instance-ID routing, a shared request registry, or a fronting private load-balancer strategy, but those are outside the MVP.

### Private connector

The private connector:

- Connects outbound to NATS over TLS/mTLS.
- Subscribes to ticket subjects using a NATS queue group so that one connector handles each file request.
- Fetches the requested object from S3-compatible storage using private S3 credentials.
- Streams the object to the edge gateway's private ingest endpoint over HTTPS mTLS.
- Includes the one-time ingest token received in the ticket.
- Has no public inbound application listener.

The connector bridges the broker and private storage networks through outbound connections only. It is the only application component that needs S3-compatible storage credentials.

### NATS broker

NATS is the control plane. It carries small tickets and optional control replies, not object bytes.

The MVP uses core NATS publish/subscribe and queue groups. It does not use JetStream. This choice keeps the request model ephemeral and aligned with held HTTP responses: if no connector is available while the public request is pending, the gateway times out the request rather than relying on replay.

All application connections to NATS use TLS or mTLS. Subject permissions should restrict the edge gateway and connector to only the subjects they require.

### VersityGW S3-compatible storage

VersityGW provides the S3-compatible demo storage service. It is attached only to the private Docker Compose network. The edge gateway is not attached to that network and cannot reach VersityGW directly.

## Public request flow

1. A client requests:

   ```text
   https://files.example.com/{bucket}/{key...}
   ```

   The URL includes an HMAC signature when signed URLs are required. Signature material can be carried in query parameters or headers, as finalized by implementation.

2. The edge gateway validates the request:
   - Method is supported.
   - Bucket and key are syntactically valid.
   - Bucket/key allowlist permits the object path.
   - Signature is valid and not expired when signatures are enabled.
   - Unsafe headers are ignored rather than trusted.

3. The edge gateway creates:
   - A request ID.
   - A pending response record with a deadline.
   - A one-time ingest token tied to the request ID, method, bucket, key, and expiry.

4. The edge gateway publishes a NATS ticket to the connector subject.

5. A private connector receives the ticket through its queue group subscription.

6. The connector fetches the object from S3-compatible storage.

7. The connector opens an HTTPS mTLS connection to the edge gateway's private ingest endpoint and POSTs the object stream with the request ID and one-time ingest token.

8. The edge gateway validates the ingest connection, token, and pending request state.

9. The edge gateway forwards the connector stream to the held public response.

10. The edge gateway completes or cancels the pending request record.

## Control-plane ticket

A ticket is a small control message. It should include only information required to authorize and fetch the object:

- Request ID.
- HTTP method, limited to supported methods.
- Bucket name.
- Object key.
- Requested byte range when accepted by the edge.
- Request deadline or pending TTL.
- One-time ingest token or token reference.
- Edge ingest endpoint URL for the specific MVP edge instance.
- Safe request metadata needed by the connector.

Tickets must not include S3 credentials, object bytes, public client secrets, or unvalidated user-controlled headers.

## HTTP behavior

### Methods

The MVP should support:

- `GET` for object retrieval.
- `HEAD` for object metadata without a response body.

`Range` support is desirable when feasible. If implemented, the edge should validate the Range header, pass the range intent in the ticket, have the connector issue an S3 ranged read, and return the correct partial-content response semantics. If Range support is not included in the first implementation slice, the edge should return a clear client-facing response for unsupported Range requests rather than silently ignoring them.

### Status mapping

Recommended public status behavior:

| Condition | Public response |
| --- | --- |
| Invalid path, unsupported method, malformed range, or bad signature shape | `400 Bad Request` |
| Missing, invalid, or expired HMAC signature | `401 Unauthorized` or `403 Forbidden`, chosen consistently by implementation |
| Bucket/key not allowed | `403 Forbidden` |
| Object missing in S3-compatible storage | `404 Not Found` |
| Backend unavailable before object status is known | `503 Service Unavailable` |
| Pending request deadline exceeded | `504 Gateway Timeout` |
| Connector ingest token invalid or mTLS identity unauthorized | `502 Bad Gateway` for the held public request and private ingest rejection |
| Connector reports unsupported operation | `501 Not Implemented` or `400 Bad Request`, depending on whether the client requested an unsupported optional feature |

Implementation should avoid exposing internal topology, credentials, subject names, private hostnames, or detailed connector errors to public clients.

### Metadata propagation

The public response can propagate safe object metadata, such as:

- `Content-Type`.
- `Content-Length` when known.
- `ETag` when safe for the demo use case.
- `Last-Modified`.
- `Content-Range` for ranged responses.

The edge should sanitize or omit metadata that could leak internal storage details or carry unsafe header names/values. User-provided request headers should not be forwarded to the public response without explicit allowlisting.

## Security model

### Signed public URLs

Public requests are authorized with HMAC signed URLs when configured. The signature should cover:

- HTTP method.
- Bucket.
- Key.
- Expiry timestamp.
- Optional range parameters if ranged requests are authorized.
- Optional content-disposition or other safe response controls if supported.

Signatures should be checked using constant-time comparison. Expired signatures should fail before a NATS ticket is published.

### Bucket/key allowlist

The edge should enforce a bucket/key allowlist before publishing tickets. The connector should also enforce an allowlist defense-in-depth check before fetching from S3-compatible storage. This prevents a compromised edge configuration, malformed ticket, or subject misuse from broadening storage access.

### NATS TLS/mTLS and permissions

All edge and connector control-plane connections to NATS use TLS or mTLS. NATS authorization should restrict:

- The edge gateway to publishing ticket subjects and receiving only explicitly needed control replies.
- The connector to subscribing to ticket subjects through the configured queue group and publishing only expected status or diagnostic subjects.

Subject names should avoid embedding secrets. Request IDs are identifiers, not authorization proof.

### Connector-to-edge ingest security

Connector ingest requires both:

1. HTTPS mTLS with an authorized connector client identity.
2. A one-time ingest token bound to the pending request.

The ingest token should expire with the pending request TTL and should be consumed once. Reuse, wrong request ID, wrong method, wrong bucket/key, or expired tokens should be rejected.

### No S3 credentials on the edge

Only the private connector has S3-compatible storage credentials. The edge gateway does not mount, receive, log, or derive S3 credentials.

### Cancellation and deadlines

Each public request has a pending TTL. If the client disconnects, the edge should cancel the pending record and make later ingest attempts fail. If the connector notices cancellation or an ingest failure, it should stop reading from S3 and close the stream.

Deadlines should apply to:

- Public request hold time.
- Ticket validity.
- Connector S3 fetch startup.
- Connector ingest connection establishment.
- End-to-end stream inactivity where practical.

### Header sanitization and logging

The system should log request IDs and high-level outcomes, not secrets. Logs should avoid:

- HMAC secret material.
- Full signed URLs when query strings contain signatures.
- One-time ingest tokens.
- S3 credentials.
- Raw untrusted headers.

## Docker Compose demo network model

The demo Compose environment should use three networks:

| Network | Attached services | Purpose |
| --- | --- | --- |
| `public` | Edge gateway, public test client or host-published public listener | Public HTTPS file access |
| `broker` | Edge gateway, NATS, private connector | NATS control plane and private ingest reachability as configured |
| `private` | Private connector, VersityGW | S3-compatible storage access |

Required boundaries:

- VersityGW is attached only to `private`.
- The edge gateway is not attached to `private`.
- The connector is attached to `broker` and `private`.
- The connector has no public inbound application listener.
- NATS is reachable only by components that need the control plane.
- The public file listener and private ingest listener are exposed on separate ports with separate TLS configuration.

## MVP operational behavior

Because the MVP uses core NATS without JetStream and keeps the edge gateway single-instance:

- Tickets are not durable.
- Public requests are not replayed after gateway restart.
- If no connector receives a ticket in time, the request times out.
- If the connector starts streaming after the edge pending record expires, ingest is rejected.
- If the edge process restarts, held public responses are lost.
- If NATS is unavailable, new public requests fail fast or after a short control-plane timeout.

These are acceptable demo constraints and should be visible in smoke tests and documentation.

## Future production considerations

The following are intentionally outside the MVP but should remain compatible with the design:

- Multiple edge gateway instances with instance-ID routing for ingest callbacks.
- A private ingest load balancer or per-edge private endpoint discovery.
- JetStream for durable ticketing only if paired with a production model that no longer depends on a single held HTTP response.
- Connector fleet autoscaling and health-aware routing.
- Stronger policy engines for bucket/key authorization.
- Structured audit logs and SIEM integration.
- Rate limiting, abuse protection, and per-principal quotas.
- Object metadata caching where allowed by policy.
- OpenTelemetry traces across edge, NATS publish, connector fetch, and ingest stream.

## Implementation phases

### Phase 1: Repository and configuration foundation

- Initialize the single Go module.
- Add the three command packages and shared internal package directories.
- Define configuration schemas for listener addresses, TLS files, NATS URL, subject names, S3 endpoint, allowlists, timeouts, and signing keys.
- Add development-only certificate generation and configuration examples.

### Phase 2: Edge gateway control path

- Implement public request parsing and signature validation.
- Implement pending request lifecycle with TTL and cancellation.
- Publish NATS tickets.
- Expose the private mTLS ingest endpoint and validate ingest tokens.

### Phase 3: Private connector

- Connect to NATS with TLS/mTLS.
- Subscribe to request tickets through a queue group.
- Fetch objects from VersityGW using the AWS SDK for Go S3 client.
- Stream objects to the edge ingest endpoint with mTLS and one-time ingest tokens.

### Phase 4: Demo environment

- Add Docker Compose services and networks.
- Add VersityGW demo storage.
- Add certificate, seed, signing, and smoke scripts.
- Demonstrate successful GET and HEAD behavior and expected failure modes.

### Phase 5: Hardening pass

- Add subject permissions.
- Expand header sanitization tests.
- Add timeout, cancellation, disconnect, and failed-ingest coverage.
- Add optional Range support if not already included.

## Validation plan

The implementation should be validated with unit, integration, and smoke tests:

- HMAC signed URL acceptance and rejection cases.
- Bucket/key allowlist acceptance and rejection cases.
- NATS TLS/mTLS connection success and failure cases.
- Queue group behavior where one connector handles one ticket.
- Public request timeout when no connector is available.
- Connector fetch and stream from VersityGW for GET.
- HEAD behavior without object body propagation.
- Missing object maps to `404 Not Found`.
- Backend unavailable maps to `503 Service Unavailable` or `504 Gateway Timeout` according to where failure occurs.
- Client disconnect cancels pending request state.
- Late ingest after pending TTL is rejected.
- Invalid mTLS identity or invalid one-time ingest token is rejected.
- Header sanitization prevents unsafe propagated response headers.
- Compose network checks confirm the edge cannot reach VersityGW directly.

## Research references

Primary references for the approved design:

- NATS TLS configuration: <https://docs.nats.io/running-a-nats-service/configuration/securing_nats/tls>
- NATS authorization, including subject permissions: <https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization>
- NATS request-reply pattern: <https://docs.nats.io/nats-concepts/core-nats/reqreply>
- NATS queue groups: <https://docs.nats.io/nats-concepts/core-nats/queue>
- AWS SDK for Go v2 S3 examples, including `GetObject` response body streaming: <https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/go_s3_code_examples.html>
- VersityGW S3-compatible gateway repository: <https://github.com/versity/versitygw>
