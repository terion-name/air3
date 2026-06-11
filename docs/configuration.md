# Configuration Guide

**air3** uses environment variables for all configuration. The Edge Gateway and Private Connector share some settings (like timeouts, allowed buckets, and NATS details), but in the default recommended topology **only the Private Connector has your S3 credentials**. The only exception is the explicit direct-server mode documented below, which intentionally moves selected S3 credentials onto the Edge.

*Note: Durations use standard Go syntax (e.g., `5s`, `30s`, `15m`). Most comma-separated lists automatically trim spaces and remove empty or duplicate entries. Direct-server alias lists are stricter: empty aliases are rejected, and exact duplicate aliases are ignored.*

## Edge Gateway

The Edge Gateway is your public-facing entry point. In the default recommended topology it has **no** S3 credentials and should **never** be attached to your private S3 network. Direct-server aliases are an explicit exception and are documented separately below.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_EDGE_PUBLIC_ADDR` | `:8080` | The host and port for public HTTP `GET`/`HEAD` traffic. |
| `AIR3_EDGE_INGEST_ADDR` | `:8443` | The host and port for the private Connector's default HTTP ingest stream. |
| `AIR3_INGEST_TRANSPORT` | `http` | Connector→edge ingest transport. Accepted values are `http`, `http1`, `http2`, `http3`, `tcp`, `smux`, and `quic`. Use explicit `http1`, `http2`, `http3`, `tcp`, `smux`, or `quic` for new runs; legacy `http` remains the default for compatibility and uses `AIR3_INGEST_DISABLE_HTTP2` to choose the HTTP version. |
| `AIR3_EDGE_INGEST_TCP_ADDR` | unset (Compose: `:9444`) | Edge TCP/smux ingest listener address. Required only when `AIR3_INGEST_TRANSPORT=tcp` or `smux`; Compose keeps this port on the internal network and does not publish it to the host by default. `smux` reuses this TCP listener address. |
| `AIR3_EDGE_INGEST_QUIC_ADDR` | unset (Compose: `:9445`) | Edge QUIC ingest listener address. Required only when `AIR3_INGEST_TRANSPORT=quic`; Compose keeps this UDP listener on the internal network and does not publish it to the host by default. If you expose it on a host, use an explicit `/udp` port mapping. |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The HTTPS ingest URL the Edge embeds in tickets and uses as the HTTP ingest endpoint/fallback. The HTTP family (`http`, `http1`, `http2`, `http3`) uses this URL with the existing headers and body semantics. In custom stream modes (`tcp`, `smux`, `quic`) this remains the fallback/ticket URL. In the Compose demo, this is `https://edge-gateway:9443/ingest`. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | A strict comma-separated allowlist of bucket names. The Edge drops requests for unlisted buckets before even making a NATS ticket. |
| `AIR3_S3_BUCKET` | unset | Optional single-server default bucket for short public paths. With `AIR3_S3_BUCKET=demo`, signed URLs generated with `cmd/signurl -default-bucket-path` use `/{key}` while signatures and connector tickets still bind bucket `demo`. This is a routing/signing default bucket name, not an S3 credential. |
| `AIR3_MULTI_SERVER` | `false` | Opt-in public path mode. `false` uses legacy `/{bucket}/{key}` unless `AIR3_S3_BUCKET` enables single-server short paths. `true` normally uses `/{server}/{bucket}/{key}` and routes connector tickets by server alias; aliases with `S3_{SUFFIX}_BUCKET` use short-form `/{server}/{key}`. |
| `AIR3_DIRECT_SERVERS` | unset | Edge-only comma-separated direct-server aliases. Each alias is served directly by the Edge from its `S3_{SUFFIX}_*` settings instead of via NATS/Connector. Requires `AIR3_MULTI_SERVER=true` to be routable. |
| `S3_{SUFFIX}_BUCKET` | unset | Optional per-server default bucket for multi-server public paths. For alias `blue`, `S3_BLUE_BUCKET=demo` lets signed URLs use `/blue/{key}` instead of `/blue/demo/{key}`. |
| `DIRECT_SERVERS` | unset | Bare fallback name for `AIR3_DIRECT_SERVERS`. If both are set, the values must match exactly or startup fails. |
| `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` | unset | (Optional) Comma-separated list of allowed Connector certificate identities for mTLS ingest connections. |

## Private Connector

The Private Connector is your secure worker. It has **no public inbound listeners** and connects outbound to NATS, S3, and the Edge Gateway.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_INGEST_URL` | `https://localhost:8443/ingest` | The default/fallback HTTPS Edge ingest endpoint for outbound mTLS uploads. The HTTP family (`http`, `http1`, `http2`, `http3`) uses this URL with the existing headers and body semantics; custom stream transports (`tcp`, `smux`, `quic`) keep it as the ticket/fallback URL. |
| `AIR3_INGEST_TRANSPORT` | `http` | Connector→edge ingest transport. Accepted values are `http`, `http1`, `http2`, `http3`, `tcp`, `smux`, and `quic`. Use explicit `http1`, `http2`, `http3`, `tcp`, `smux`, or `quic` for new runs; legacy `http` remains the default for compatibility and uses `AIR3_INGEST_DISABLE_HTTP2` to choose the HTTP version. |
| `AIR3_INGEST_TCP_ADDR` | unset (Compose: `edge-gateway:9444`) | Connector TCP/smux dial address for the Edge TCP/smux ingest listener. Required only when `AIR3_INGEST_TRANSPORT=tcp` or `smux`. `smux` reuses this TCP dial address. |
| `AIR3_INGEST_QUIC_ADDR` | unset (Compose: `edge-gateway:9445`) | Connector QUIC dial address for the Edge QUIC ingest listener. Required only when `AIR3_INGEST_TRANSPORT=quic`. |
| `AIR3_INGEST_POOL_SIZE` | `32` (perf: `1024`) | Connector-side reusable ingest pool cap for HTTP-family, smux, QUIC, and HTTP/3 transports. Raw TCP remains one connection per object. Must be `1`-`4096`. |
| `AIR3_CONNECTOR_WORKERS` | `1` (perf: `1024`) | Per-connector concurrent ticket handling worker limit. Must be `1`-`4096`. |
| `AIR3_ALLOWED_BUCKETS` | `demo` | Defense-in-depth: the Connector also enforces this allowlist before attempting to reach S3. |
| `AIR3_SERVER_NAME` | unset | Optional connector server alias for routed multi-server mode. When set, the connector rejects tickets for other aliases and derives its NATS subject from `AIR3_NATS_SUBJECT_TEMPLATE` unless `AIR3_NATS_SUBJECT` is explicitly set. |
| `AIR3_INGEST_DISABLE_HTTP2` | `false` | Legacy compatibility knob used only when `AIR3_INGEST_TRANSPORT=http`: `true` forces HTTP/1.1 and `false` enables HTTP/2. Explicit `AIR3_INGEST_TRANSPORT=http1`, `http2`, or `http3` ignores this setting. |

## NATS Broker

NATS coordinates transfers between the Edge and the Connector. Object bytes **do not** stream through NATS.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_NATS_URL` | `nats://localhost:4222` | NATS connection string. Use `tls://...` for secure connections. |
| `AIR3_NATS_SUBJECT` | `air3.tickets` | The NATS subject used for single-server ticket transfer. On connectors with `AIR3_SERVER_NAME`, an explicit value overrides subject derivation. |
| `AIR3_NATS_SUBJECT_TEMPLATE` | `air3.{server}` | Routed multi-server subject template. Must contain `{server}` and render to a valid literal NATS subject; the Edge uses it per request when `AIR3_MULTI_SERVER=true`, and connectors use it with `AIR3_SERVER_NAME` when `AIR3_NATS_SUBJECT` is not set. |
| `AIR3_NATS_QUEUE` | edge: unset; connector: `air3-connectors` | Queue group. Connectors use this so that only one Connector picks up a given ticket. |
| `AIR3_NATS_CREDS_FILE` | unset | (Optional) Path to a NATS credentials file. |
| `AIR3_NATS_NKEY_FILE` | unset | (Optional) Path to an NKey seed file. |
| `AIR3_NATS_USER` | unset | (Optional) NATS username. Must be paired with `AIR3_NATS_PASSWORD`. |
| `AIR3_NATS_PASSWORD` | unset | (Optional) NATS password. Must be paired with `AIR3_NATS_USER`. |

## S3-Compatible Storage (default Connector path)

These highly sensitive settings belong **only** to the Private Connector in the recommended connector-routed topology. Direct-server aliases use separate Edge-side `S3_{SUFFIX}_*` settings below. Do not confuse them with Edge `AIR3_S3_BUCKET`: despite its name, that variable is only a public-path routing/signing default bucket and is not an S3 access credential.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_S3_ENDPOINT` | `http://localhost:7070` | Your private S3-compatible service URL. |
| `AIR3_S3_REGION` | `us-east-1` | S3 region used by the internal AWS SDK. |
| `AIR3_S3_ALLOWED_BUCKETS` | value of `AIR3_ALLOWED_BUCKETS` | Connector-specific bucket allowlist (defaults to the shared allowlist). |
| `AIR3_S3_ACCESS_KEY_ID` | unset | **Your private S3 Access Key ID.** |
| `AIR3_S3_SECRET_ACCESS_KEY` | unset | **Your private S3 Secret Access Key.** |
| `AIR3_S3_USE_PATH_STYLE` | `true` | Use path-style addressing (great for self-hosted S3 alternatives). |
| `AIR3_S3_INSECURE_SKIP_VERIFY` | `false` | Skip S3 TLS verification. (Use for local testing only!) |

## Single-server default-bucket paths

Single-server mode is the default and remains the recommended isolated topology: the Edge publishes tickets to `AIR3_NATS_SUBJECT` (default `air3.tickets`), and the Private Connector fetches from S3. Without `AIR3_S3_BUCKET`, public URLs keep the legacy `/{bucket}/{key}` shape.

Set `AIR3_S3_BUCKET=demo` on the Edge to make bucket `demo` the single-server default bucket. Signed URLs generated with `cmd/signurl -default-bucket-path` omit the bucket from the public path, such as `/hello.txt`, but signatures, tickets, and connector fetches still use bucket `demo`. Short-form parsing wins when the default is configured: `/demo/file.txt` means key `demo/file.txt` in bucket `demo`, not key `file.txt` in bucket `demo`. Leave `AIR3_S3_BUCKET` unset when callers must continue using explicit `/{bucket}/{key}` paths.

## Multi-server routing

Use `AIR3_MULTI_SERVER=true` only when one Edge must route multiple connector or direct aliases. Public paths become `/{server}/{bucket}/{key}` unless that server alias has a default bucket. The server segment is part of the signed URL claims and must match the request path. For non-direct aliases, the Edge publishes the ticket to `AIR3_NATS_SUBJECT_TEMPLATE` with `{server}` replaced by the alias. The default template `air3.{server}` routes `/blue/demo/file.txt` to subject `air3.blue`; a connector with `AIR3_SERVER_NAME=blue` subscribes to that same derived subject and rejects tickets for other server aliases.

Set `S3_{SUFFIX}_BUCKET` on the Edge to give any multi-server alias a default bucket (`S3_BLUE_BUCKET=demo` for alias `blue`, `S3_BETA_SERVER_BUCKET=archive` for alias `beta-server`). Signed URLs generated with `cmd/signurl -default-bucket-path` then omit the bucket from the public path, such as `/blue/hello.txt`, while the signature and ticket still bind the real bucket `demo`. When a default bucket is configured, short-form parsing wins for that alias: every path segment after `/{server}/` is treated as the key, so `/blue/demo/hello.txt` targets key `demo/hello.txt` in default bucket `demo` rather than an explicit bucket named `demo`. Use aliases without `S3_{SUFFIX}_BUCKET` when you need full-path `/{server}/{bucket}/{key}` behavior.

`AIR3_NATS_SUBJECT` is still the single-server subject. On a connector, setting it explicitly overrides `AIR3_SERVER_NAME`/template derivation; leave it unset for the normal routed multi-server pattern.

For a runnable Compose example, see the `deploy/compose.multiserver.yaml` overlay and `make e2e-multiserver`. The example covers `blue` as a connector-routed alias and `direct` as an Edge direct-S3 alias, both with default bucket `demo`, plus a full-path `green` request without a default bucket.

## Direct-server aliases (Edge S3 exception)

Direct servers are an explicit Edge trust-boundary exception for multi-server deployments. An alias listed in `AIR3_DIRECT_SERVERS` (or the compatible fallback `DIRECT_SERVERS`) is fetched directly by the Edge from S3 and bypasses NATS and the Private Connector for that alias. This means the Edge must hold S3 credentials and be able to reach that S3 endpoint. If `S3_{SUFFIX}_BUCKET` is set for a direct alias, startup verifies that default bucket is included in `S3_{SUFFIX}_ALLOWED_BUCKETS`. Do not use direct servers for storage that must remain private from the public edge.

Alias rules:

- Aliases are 1-63 ASCII characters, must start with a letter or digit, and may then contain letters, digits, `_`, or `-`. They are matched exactly in the public URL.
- `AIR3_DIRECT_SERVERS` and `DIRECT_SERVERS` are comma-separated; spaces are trimmed, empty entries are rejected, and exact duplicate aliases are ignored.
- Per-alias environment suffixes are normalized by uppercasing and replacing `-` with `_`: `beta-server` uses `S3_BETA_SERVER_*`. Aliases that normalize to the same suffix, such as `a-b` and `a_b`, are rejected to avoid ambiguous credentials.

For each direct alias, configure all required `S3_{SUFFIX}_*` settings on the Edge:

| Variable | Default | Purpose |
| --- | --- | --- |
| `S3_{SUFFIX}_ENDPOINT` | required | S3-compatible endpoint for this direct alias. |
| `S3_{SUFFIX}_REGION` | required | S3 region for this direct alias. |
| `S3_{SUFFIX}_BUCKET` | unset | Optional default bucket for short-form URLs on this direct alias. If set, it must also appear in `S3_{SUFFIX}_ALLOWED_BUCKETS`. |
| `S3_{SUFFIX}_ALLOWED_BUCKETS` | required | Per-alias bucket allowlist enforced by the Edge before direct S3 fetches. |
| `S3_{SUFFIX}_ACCESS_KEY_ID` | required | S3 access key held by the Edge for this direct alias. |
| `S3_{SUFFIX}_SECRET_ACCESS_KEY` | required | S3 secret key held by the Edge for this direct alias. |
| `S3_{SUFFIX}_USE_PATH_STYLE` | `true` | Use path-style addressing for this direct alias. |
| `S3_{SUFFIX}_INSECURE_SKIP_VERIFY` | `false` | Skip S3 TLS verification for this direct alias. Use only for local testing. |

Example:

```sh
AIR3_MULTI_SERVER=true
AIR3_DIRECT_SERVERS=alpha,beta-server
S3_ALPHA_ENDPOINT=https://alpha-s3.example
S3_ALPHA_REGION=us-east-1
S3_ALPHA_BUCKET=demo
S3_ALPHA_ALLOWED_BUCKETS=demo,logs
S3_ALPHA_ACCESS_KEY_ID=...
S3_ALPHA_SECRET_ACCESS_KEY=...
S3_BETA_SERVER_ENDPOINT=https://beta-s3.example
S3_BETA_SERVER_REGION=us-west-2
S3_BETA_SERVER_ALLOWED_BUCKETS=archive
S3_BETA_SERVER_ACCESS_KEY_ID=...
S3_BETA_SERVER_SECRET_ACCESS_KEY=...
```

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

*Example:* To secure the ingest port, the Edge uses `AIR3_EDGE_MTLS_CA_FILE`, `AIR3_EDGE_MTLS_CERT_FILE`, and `AIR3_EDGE_MTLS_KEY_FILE`. The Connector then uses `AIR3_CONNECTOR_MTLS_*` to securely connect to it. HTTP ingest (explicit `http1`/`http2`/`http3` or legacy `http`), experimental TCP ingest, experimental `smux` ingest, and experimental custom QUIC ingest all use these same mTLS files, the same optional `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES` identity allowlist, and the same one-time ingest token semantics. HTTP-family transports use the existing ingest URL, headers, and body. Custom stream transports (`tcp`, `smux`, `quic`) use a shared MessagePack metadata frame, raw object body, and ack semantics; `smux` multiplexes these streams over persistent mTLS TCP, one smux stream per object.

## Timeouts and Limits

| Variable | Default | Purpose |
| --- | --- | --- |
| `AIR3_PENDING_TTL` | `30s` | Maximum time the Edge will wait for the Connector to start pushing file data before giving up. |
| `AIR3_STREAM_TIMEOUT` | `5m` | Maximum total time allowed for the entire file stream transfer. |
| `AIR3_STREAM_COPY_BUFFER_BYTES` | `262144` bytes | Size of each per-stream `io.CopyBuffer` buffer on edge streaming paths. Units are bytes; minimum `32768` bytes, maximum `1048576` bytes. This bounds streaming copy buffers without introducing body buffering. |

*Runtime tuning note:* `AIR3_STREAM_COPY_BUFFER_BYTES`, `AIR3_INGEST_DISABLE_HTTP2`, `AIR3_INGEST_TRANSPORT`, `AIR3_INGEST_POOL_SIZE`, and `AIR3_CONNECTOR_WORKERS` only tune streaming/transport/concurrency behavior. They do not change authentication, authorization, or other security behavior, and they do not introduce request or response body buffering. `AIR3_INGEST_DISABLE_HTTP2` is legacy/compatibility-only for `AIR3_INGEST_TRANSPORT=http`; explicit `http1`, `http2`, and `http3` ignore it.

### Ingest transport variants

Accepted `AIR3_INGEST_TRANSPORT` values are:

- `http`: legacy compatibility mode. This remains the Compose default and uses `AIR3_INGEST_DISABLE_HTTP2` (`true` = HTTP/1.1, `false` = HTTP/2).
- `http1`: force connector HTTP/1.1 ingest.
- `http2`: enable connector HTTP/2 ingest.
- `http3`: use HTTP/3 ingest. Like the other HTTP-family transports, it uses `AIR3_INGEST_URL`, existing ingest headers, and the object body.
- `tcp`: use experimental mTLS TCP stream ingest with the shared MessagePack metadata frame, raw object body, and ack semantics.
- `smux`: use the shared MessagePack-framed ingest stream over persistent mTLS TCP, one smux stream per object. It reuses `AIR3_EDGE_INGEST_TCP_ADDR` and `AIR3_INGEST_TCP_ADDR`.
- `quic`: use experimental custom QUIC ingest with the shared MessagePack metadata frame, raw object body, and ack semantics. It uses `AIR3_EDGE_INGEST_QUIC_ADDR` and `AIR3_INGEST_QUIC_ADDR`.

`AIR3_INGEST_POOL_SIZE` caps reusable connector-side connection, session, or client pools for HTTP-family, smux, QUIC, and HTTP/3 ingest transports. Raw TCP remains one connection per object. The normal default is `32`; the perf Compose harness defaults to `1024` for high-concurrency runs.

`AIR3_CONNECTOR_WORKERS` bounds how many tickets each connector handles concurrently. `AIR3_PERF_CONNECTORS` controls how many connector containers the perf harness starts, and `AIR3_INGEST_POOL_SIZE` controls reusable connector→edge ingest resources inside each connector; increase them deliberately depending on whether you want more connector replicas, more per-connector ticket concurrency, or larger ingest connection/session pools.

For new benchmarking runs, prefer explicit transport values so result filenames and CSV labels show the intended connector ingest behavior:

```sh
AIR3_INGEST_TRANSPORT=http1 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=http2 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=tcp AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=smux AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=quic AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=http3 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
```

*Bucket Name Rules:* Bucket names must be 3-63 characters long, containing only lowercase letters, numbers, dots, or hyphens (DNS style). They cannot start or end with a dot or hyphen, and cannot have adjacent dots.

## Docker Compose Demo Network Map

Our included `deploy/compose.yaml` demo clearly illustrates the required network isolation. The base demo sets Edge `AIR3_S3_BUCKET=demo`, so `make smoke` exercises short single-server public paths while connector tickets and S3 fetches still use bucket `demo`:

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
