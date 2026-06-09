# Configuration Guide

**air3** uses environment variables for all configuration. The Edge Gateway and Private Connector share some settings (like timeouts, allowed buckets, and NATS details), but **only the Private Connector has your S3 credentials**.

*Note: Durations use standard Go syntax (e.g., `5s`, `30s`, `15m`). Comma-separated lists automatically trim spaces and remove empty or duplicate entries.*

## Edge Gateway

The Edge Gateway is your public-facing entry point. It has **no** S3 settings and should **never** be attached to your private S3 network.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_EDGE_PUBLIC_ADDR` | `:8080` | The host and port for public HTTP `GET`/`HEAD` traffic. |
| `AIR3_EDGE_INGEST_ADDR` | `:8443` | The host and port for the private Connector's default HTTP ingest stream. |
| `AIR3_INGEST_TRANSPORT` | `http` | Connector→edge ingest transport. Accepted values are `http`, `http1`, `http2`, `tcp`, and `smux`. Use explicit `http1`, `http2`, `tcp`, or `smux` for new runs; legacy `http` remains the default for compatibility and uses `AIR3_INGEST_DISABLE_HTTP2` to choose the HTTP version. |
| `AIR3_EDGE_INGEST_TCP_ADDR` | unset (Compose: `:9444`) | Edge TCP/smux ingest listener address. Required only when `AIR3_INGEST_TRANSPORT=tcp` or `smux`; Compose keeps this port on the internal network and does not publish it to the host by default. `smux` reuses this TCP listener address. |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The HTTPS ingest URL the Edge embeds in tickets and uses as the HTTP fallback. In `tcp` and `smux` modes this remains the fallback/ticket URL. In the Compose demo, this is `https://edge-gateway:9443/ingest`. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | A strict comma-separated allowlist of bucket names. The Edge drops requests for unlisted buckets before even making a NATS ticket. |
| `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` | unset | (Optional) Comma-separated list of allowed Connector certificate identities for mTLS ingest connections. |

## Private Connector

The Private Connector is your secure worker. It has **no public inbound listeners** and connects outbound to NATS, S3, and the Edge Gateway.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The default/fallback HTTPS Edge ingest endpoint for outbound mTLS uploads. It remains the ticket URL and HTTP fallback even when `tcp` or `smux` ingest is enabled. |
| `AIR3_INGEST_TRANSPORT` | `http` | Connector→edge ingest transport. Accepted values are `http`, `http1`, `http2`, `tcp`, and `smux`. Use explicit `http1`, `http2`, `tcp`, or `smux` for new runs; legacy `http` remains the default for compatibility and uses `AIR3_INGEST_DISABLE_HTTP2` to choose the HTTP version. |
| `AIR3_INGEST_TCP_ADDR` | unset (Compose: `edge-gateway:9444`) | Connector TCP/smux dial address for the Edge TCP/smux ingest listener. Required only when `AIR3_INGEST_TRANSPORT=tcp` or `smux`. `smux` reuses this TCP dial address. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Defense-in-depth: the Connector also enforces this allowlist before attempting to reach S3. |
| `AIR3_INGEST_DISABLE_HTTP2` | `false` | Legacy compatibility knob used only when `AIR3_INGEST_TRANSPORT=http`: `true` forces HTTP/1.1 and `false` enables HTTP/2. Explicit `AIR3_INGEST_TRANSPORT=http1` or `http2` ignores this setting. |

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

*Example:* To secure the ingest port, the Edge uses `AIR3_EDGE_MTLS_CA_FILE`, `AIR3_EDGE_MTLS_CERT_FILE`, and `AIR3_EDGE_MTLS_KEY_FILE`. The Connector then uses `AIR3_CONNECTOR_MTLS_*` to securely connect to it. HTTP ingest (explicit `http1`/`http2` or legacy `http`), experimental TCP ingest, and experimental `smux` ingest all use these same mTLS files, the same optional `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` identity allowlist, and the same one-time ingest token semantics. TCP ingest is an mTLS TCP stream with a MessagePack metadata frame before object bytes. `smux` uses MessagePack-framed ingest streams over persistent mTLS TCP, with one smux stream per object.

## Timeouts and Limits

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_PENDING_TTL` | `30s` | Maximum time the Edge will wait for the Connector to start pushing file data before giving up. |
| `AIR3_STREAM_TIMEOUT` | `5m` | Maximum total time allowed for the entire file stream transfer. |
| `AIR3_STREAM_COPY_BUFFER_BYTES` | `262144` bytes | Size of each per-stream `io.CopyBuffer` buffer on edge streaming paths. Units are bytes; minimum `32768` bytes, maximum `1048576` bytes. This bounds streaming copy buffers without introducing body buffering. |

*Runtime tuning note:* `AIR3_STREAM_COPY_BUFFER_BYTES`, `AIR3_INGEST_DISABLE_HTTP2`, and `AIR3_INGEST_TRANSPORT` only tune streaming/transport behavior. They do not change authentication, authorization, or other security behavior, and they do not introduce request or response body buffering. `AIR3_INGEST_DISABLE_HTTP2` is legacy/compatibility-only for `AIR3_INGEST_TRANSPORT=http`; explicit `http1` and `http2` ignore it.

### Ingest transport variants

Accepted `AIR3_INGEST_TRANSPORT` values are:

- `http1`: force connector HTTP/1.1 ingest.
- `http2`: enable connector HTTP/2 ingest.
- `tcp`: use experimental mTLS TCP stream ingest with a MessagePack metadata frame.
- `smux`: use MessagePack-framed ingest streams over persistent mTLS TCP, one smux stream per object. It reuses `AIR3_EDGE_INGEST_TCP_ADDR` and `AIR3_INGEST_TCP_ADDR`.
- `http`: legacy compatibility mode. This remains the Compose default and uses `AIR3_INGEST_DISABLE_HTTP2` (`true` = HTTP/1.1, `false` = HTTP/2).

For new benchmarking runs, prefer explicit transport values so result filenames and CSV labels show the intended connector ingest behavior:

```sh
AIR3_INGEST_TRANSPORT=http1 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=http2 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=tcp AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=smux AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
```

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
