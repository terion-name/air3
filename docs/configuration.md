# air3 configuration

air3 is configured with environment variables. The edge gateway and private connector share NATS, timeout, allowlist, and mTLS settings, but only the private connector has S3 credentials.

Durations use Go duration syntax such as `5s`, `30s`, or `15m`. Comma-separated lists trim spaces, reject empty entries, and deduplicate values.

## Edge gateway

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_EDGE_PUBLIC_ADDR` | `:8080` | Address for the public `GET`/`HEAD` listener. |
| `AIR3_EDGE_INGEST_ADDR` | `:8443` | Address for the private connector ingest listener. |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | URL placed in tickets so the connector knows where to `POST /ingest`. In Compose this is `https://edge-gateway:9443/ingest`. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Buckets the edge accepts before publishing tickets. |
| `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` | unset | Optional comma-separated connector certificate identities allowed on the edge ingest listener. |

The edge gateway has no S3 endpoint, access key, or secret key settings. It should not be attached to the private S3 network.

## Private connector

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | Edge ingest endpoint used for outbound mTLS upload streams. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Buckets accepted by the connector for defense-in-depth before S3 access. |

The connector has no public inbound application listener. It connects outbound to NATS, S3-compatible storage, and the edge ingest URL.

## NATS broker

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_NATS_URL` | `nats://localhost:4222` | NATS server URL. Use `tls://...` when TLS is enabled. |
| `AIR3_NATS_SUBJECT` | `air3.tickets` | Subject used for transfer tickets. |
| `AIR3_NATS_QUEUE` | edge: unset; connector: `air3-connectors` | Queue group. The edge normally publishes only; connectors use the queue so one connector handles each ticket. |
| `AIR3_NATS_CREDS_FILE` | unset | Optional NATS credentials file. If set, the file must exist. |
| `AIR3_NATS_NKEY_FILE` | unset | Optional NKey seed file. If set, the file must exist. |
| `AIR3_NATS_USER` | unset | Optional username. Must be paired with `AIR3_NATS_PASSWORD`. |
| `AIR3_NATS_PASSWORD` | unset | Optional password. Must be paired with `AIR3_NATS_USER`. |

NATS carries short-lived fetch tickets and control messages between the edge gateway and private connector. Object bytes stream over HTTPS ingest from the connector to the edge gateway; the broker is not the object data path.

## S3-compatible storage

These variables belong to the private connector only.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_S3_ENDPOINT` | `http://localhost:7070` | S3-compatible endpoint reachable from the connector. |
| `AIR3_S3_REGION` | `us-east-1` | S3 region passed to the SDK. |
| `AIR3_S3_ALLOWED_BUCKETS` | value of `AIR3_ALLOWED_BUCKETS` | Connector-side S3 bucket allowlist. |
| `AIR3_S3_ACCESS_KEY_ID` | unset | S3 access key ID. Required for the connector. |
| `AIR3_S3_SECRET_ACCESS_KEY` | unset | S3 secret access key. Required for the connector. |
| `AIR3_S3_USE_PATH_STYLE` | `true` | Use path-style S3 addressing. Useful for many S3-compatible services and the Compose demo. |
| `AIR3_S3_INSECURE_SKIP_VERIFY` | `false` | Skip S3 TLS verification. Use only for controlled test endpoints. |

## Signing

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_SIGNING_SECRET` | unset | HMAC secret used by the edge gateway and signing helpers. Required unless signing is disabled. |
| `AIR3_SIGNING_TTL` | `15m` | Default lifetime used by signing-related configuration. |
| `AIR3_SIGNING_DISABLED` | `false` | Disable signed URL verification. Intended only for controlled local scenarios. |

Edge signed URLs authorize air3 edge requests. They are not S3 presigned URLs and never grant direct S3 API access.

## TLS and mTLS

The TLS helpers use the same suffixes for edge ingest mTLS, connector ingest-client mTLS, and NATS TLS:

| Prefix | Used by |
| --- | --- |
| `AIR3_EDGE_MTLS_*` | Edge private ingest listener. |
| `AIR3_CONNECTOR_MTLS_*` | Connector client connection to edge ingest. |
| `AIR3_NATS_TLS_*` | Edge and connector NATS connections. |

| Suffix | Purpose |
| --- | --- |
| `_CA_FILE` | CA certificate file. If set, the file must exist. |
| `_CERT_FILE` | Client or server certificate file. Must be paired with `_KEY_FILE`. |
| `_KEY_FILE` | Client or server private key file. Must be paired with `_CERT_FILE`. |
| `_SERVER_NAME` | Optional TLS server name override for outbound clients. |
| `_INSECURE_SKIP_VERIFY` | Skip TLS verification for that connection. Use only in controlled test environments. |

For example, the Compose demo sets `AIR3_EDGE_MTLS_CA_FILE`, `AIR3_EDGE_MTLS_CERT_FILE`, and `AIR3_EDGE_MTLS_KEY_FILE` for the edge ingest listener, and sets `AIR3_CONNECTOR_MTLS_*` for the connector's outbound ingest client certificate.

## Timeouts and allowlists

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_PENDING_TTL` | `30s` | How long the edge holds a public request waiting for connector ingest. |
| `AIR3_STREAM_TIMEOUT` | `5m` | End-to-end stream timeout budget. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Shared high-level bucket allowlist for edge and connector. |
| `AIR3_S3_ALLOWED_BUCKETS` | value of `AIR3_ALLOWED_BUCKETS` | Connector S3-specific bucket allowlist. |
| `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` | unset | Optional mTLS identity allowlist for connector ingest clients. |

Bucket names must be DNS-style names: 3-63 characters with lowercase letters, numbers, dots, or hyphens, without leading/trailing dots or hyphens and without adjacent dots.

## Compose demo configuration

The Compose demo is defined in `deploy/compose.yaml` and uses separated networks:

| Network | Services | Purpose |
| --- | --- | --- |
| `public` | `edge-gateway` | Host-published public HTTPS on `https://localhost:8443`. |
| `broker` | `edge-gateway`, `nats`, `private-connector` | NATS control messages and edge ingest reachability. |
| `private` | `private-connector`, `versitygw`, `aws-cli` tools profile | S3-compatible storage access. Marked `internal: true`. |

Demo highlights:

- `edge-gateway` exposes only public HTTPS to the host and is not attached to the `private` network.
- `private-connector` has S3 credentials and no host-published application port.
- `nats` uses `deploy/nats/nats.conf`, TLS/mTLS, and username/password auth for broker connections.
- `versitygw` is reachable only from the private network.
- Demo certificates are generated by `deploy/scripts/certs.sh` under `deploy/certs/generated/`, which is ignored by git.
- `deploy/scripts/seed-s3.sh` uses the private-network `aws-cli` helper to seed demo objects.
- `deploy/scripts/smoke.sh` uses `cmd/signurl` to generate edge signed URLs and verify the end-to-end path.

Useful demo commands:

```sh
make certs
make compose-up
make seed
make smoke
make compose-down
```

or:

```sh
make e2e
```

## Release artifacts and images

The release workflow builds the `edge-gateway`, `private-connector`, and `signurl` binaries for Linux, macOS, and Windows on amd64 and arm64, publishes packaged artifacts with SHA-256 checksums, and publishes separate multi-architecture Linux images:

- `ghcr.io/terion-name/air3/edge-gateway:<tag>`
- `ghcr.io/terion-name/air3/private-connector:<tag>`
- `ghcr.io/terion-name/air3/signurl:<tag>`

The root `Dockerfile` builds one selected binary into `/usr/local/bin/air3` with the `APP` build argument. Supported values are `edge-gateway`, `private-connector`, and `signurl`.
