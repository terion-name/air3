# Configuration Guide

**air3** uses environment variables for all configuration. The Edge Gateway and Private Connector share some settings (like timeouts, allowed buckets, and NATS details), but **only the Private Connector has your S3 credentials**.

*Note: Durations use standard Go syntax (e.g., `5s`, `30s`, `15m`). Comma-separated lists automatically trim spaces and remove empty or duplicate entries.*

## Edge Gateway

The Edge Gateway is your public-facing entry point. It has **no** S3 settings and should **never** be attached to your private S3 network.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_EDGE_PUBLIC_ADDR` | `:8080` | The host and port for public HTTP `GET`/`HEAD` traffic. |
| `AIR3_EDGE_INGEST_ADDR` | `:8443` | The host and port for the private Connector's ingest stream. |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The ingest URL the Edge embeds in tickets so the Connector knows where to push the file. In the Compose demo, this is `https://edge-gateway:9443/ingest`. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | A strict comma-separated allowlist of bucket names. The Edge drops requests for unlisted buckets before even making a NATS ticket. |
| `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` | unset | (Optional) Comma-separated list of allowed Connector certificate identities for mTLS ingest connections. |

## Private Connector

The Private Connector is your secure worker. It has **no public inbound listeners** and connects outbound to NATS, S3, and the Edge Gateway.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The default fallback Edge ingest endpoint for outbound mTLS uploads. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Defense-in-depth: the Connector also enforces this allowlist before attempting to reach S3. |

## NATS Broker

NATS coordinates transfers between the Edge and the Connector. Object bytes **do not** stream through NATS.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_NATS_URL` | `nats://localhost:4222` | NATS connection string. Use `tls://...` for secure connections. |
| `AIR3_NATS_SUBJECT` | `air3.tickets` | The NATS subject used for transferring fetch tickets. |
| `AIR3_NATS_QUEUE` | edge: unset; connector: `air3-connectors` | Queue group. Connectors use this so that only one Connector picks up a given ticket. |
| `AIR3_NATS_CREDS_FILE` | unset | (Optional) Path to a NATS credentials file. |
| `AIR3_NATS_NKEY_FILE` | unset | (Optional) Path to an NKey seed file. |
| `AIR3_NATS_USER` | unset | (Optional) NATS username. Must be paired with `AIR3_NATS_PASSWORD`. |
| `AIR3_NATS_PASSWORD` | unset | (Optional) NATS password. Must be paired with `AIR3_NATS_USER`. |

## S3-Compatible Storage (Connector Only)

These highly sensitive settings belong **only** to the Private Connector.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_S3_ENDPOINT` | `http://localhost:7070` | Your private S3-compatible service URL. |
| `AIR3_S3_REGION` | `us-east-1` | S3 region used by the internal AWS SDK. |
| `AIR3_S3_ALLOWED_BUCKETS` | value of `AIR3_ALLOWED_BUCKETS` | Connector-specific bucket allowlist (defaults to the shared allowlist). |
| `AIR3_S3_ACCESS_KEY_ID` | unset | **Your private S3 Access Key ID.** |
| `AIR3_S3_SECRET_ACCESS_KEY` | unset | **Your private S3 Secret Access Key.** |
| `AIR3_S3_USE_PATH_STYLE` | `true` | Use path-style addressing (great for self-hosted S3 alternatives). |
| `AIR3_S3_INSECURE_SKIP_VERIFY` | `false` | Skip S3 TLS verification. (Use for local testing only!) |

## URL Signing

Edge signed URLs authorize requests against the air3 gateway itself. They are **not** S3 presigned URLs.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_SIGNING_SECRET` | unset | The HMAC secret used to verify incoming signed URLs. Required in production. |
| `AIR3_SIGNING_TTL` | `15m` | Default lifetime for signing-related tasks. |
| `AIR3_SIGNING_DISABLED` | `false` | Disable URL signature verification completely. **Do not use in production.** |

## TLS and mTLS (Security)

We use standardized variable suffixes for configuring TLS and mutual TLS (mTLS) across the different components:

| Prefix | Used by |
| --- | --- |
| `AIR3_EDGE_MTLS_*` | The Edge Gateway's private ingest listener. |
| `AIR3_CONNECTOR_MTLS_*` | The Connector's outbound client connection to the Edge ingest port. |
| `AIR3_NATS_TLS_*` | Edge and Connector connections to the NATS broker. |

| Suffix | Purpose |
| --- | --- |
| `_CA_FILE` | Path to the CA certificate file. |
| `_CERT_FILE` | Path to the client or server certificate. (Requires `_KEY_FILE`). |
| `_KEY_FILE` | Path to the client or server private key. (Requires `_CERT_FILE`). |
| `_SERVER_NAME` | (Optional) TLS server name override for outbound connections. |
| `_INSECURE_SKIP_VERIFY` | Skip TLS verification. **(Testing only!)** |

*Example:* To secure the ingest port, the Edge uses `AIR3_EDGE_MTLS_CA_FILE`, `AIR3_EDGE_MTLS_CERT_FILE`, and `AIR3_EDGE_MTLS_KEY_FILE`. The Connector then uses `AIR3_CONNECTOR_MTLS_*` to securely connect to it.

## Timeouts and Limits

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_PENDING_TTL` | `30s` | Maximum time the Edge will wait for the Connector to start pushing file data before giving up. |
| `AIR3_STREAM_TIMEOUT` | `5m` | Maximum total time allowed for the entire file stream transfer. |

*Bucket Name Rules:* Bucket names must be 3-63 characters long, containing only lowercase letters, numbers, dots, or hyphens (DNS style). They cannot start or end with a dot or hyphen, and cannot have adjacent dots.

## Docker Compose Demo Network Map

Our included `deploy/compose.yaml` demo clearly illustrates the required network isolation:

| Network | Services | Security Posture |
| --- | --- | --- |
| `public` | `edge-gateway` | The only network exposed to your host (via `https://localhost:8443`). |
| `broker` | `edge-gateway`, `nats`, `private-connector` | The middle ground for NATS control messages and Edge ingest routing. |
| `private` | `private-connector`, `versitygw`, `aws-cli` | Deeply isolated S3 access. Marked `internal: true`. The Edge Gateway cannot reach this. |

## Release artifacts and images

The release workflow builds the `edge-gateway`, `private-connector`, and `signurl` binaries for Linux, macOS, and Windows on amd64 and arm64, publishes packaged artifacts with SHA-256 checksums, and publishes separate multi-architecture Linux images:

- `ghcr.io/terion-name/air3/edge-gateway:<tag>`
- `ghcr.io/terion-name/air3/private-connector:<tag>`
- `ghcr.io/terion-name/air3/signurl:<tag>`

The root `Dockerfile` builds one selected binary into `/usr/local/bin/air3` with the `APP` build argument. Supported values are `edge-gateway`, `private-connector`, and `signurl`.
