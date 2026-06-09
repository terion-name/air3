# air3: NATS S3 File Gateway

**air3** is a secure file gateway that lets you serve files from a strictly private S3-compatible storage to the public internet—without ever exposing your S3 credentials to public-facing servers or opening any inbound firewall ports to your private network.

It acts as a bridge between the wild public internet and your highly secure private network, relying on a NATS message broker to coordinate transfers and strict zone separation to keep your data safe.

## The Usecase: Securely Serving Private Files

Imagine you have sensitive S3 storage hosted deep within your private infrastructure. You need to let specific public users download certain files via signed URLs.

Normally, you'd have to:
- Generate S3 pre-signed URLs, which grant direct access to your S3 API.
- Open your S3 network to the public internet or put a proxy in a DMZ that holds your highly-privileged S3 credentials.

**air3 solves this by splitting the responsibility:**
1. A **Public Edge Gateway** sits on the public internet. It validates signed URLs from clients but has **zero** knowledge of your S3 credentials and no direct access to S3.
2. A **Private Connector** sits in your private network. It securely holds the S3 credentials but has **no inbound public access**. It only makes outbound connections.
3. A **NATS Broker** acts as a middleman (control plane) passing messages between them.

When a public user requests a file, the Edge Gateway asks the Private Connector (via NATS) to fetch it. The Connector grabs the file from S3 and securely streams it back out to the Edge, which directly delivers it to the user.

## Documentation

- [Architecture Details](docs/architecture.md)
- [Configuration Guide](docs/configuration.md)

## How It Works: Request and Stream Flow

- **Control Plane (NATS):** NATS is only used to pass short-lived "fetch tickets" (instructions) from the Edge to the Connector. Object bytes never pass through NATS.
- **Data Plane (HTTPS by default):** File bytes stream directly from your S3 storage to the Private Connector, and then securely out to the Edge Gateway over an outbound mTLS connection. Experimental TCP and smux ingest transports are available as opt-in benchmark/runtime tuning modes; HTTP remains the default and fallback.
- **No Direct S3 Access:** Users get URLs signed for the Edge Gateway, not S3. The Edge handles authorization before the private network even knows about the request.

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Edge as Edge gateway
    participant NATS as NATS
    participant Connector as Private connector
    participant S3 as S3 storage

    Client->>Edge: Signed GET or HEAD
    Edge->>Edge: Verify edge URL and create live ticket
    Edge->>NATS: Publish ticket (control plane only)
    Connector->>NATS: Pull ticket from queue
    Connector->>S3: Fetch object bytes or metadata
    S3-->>Connector: Return object stream or status
    Connector->>Edge: Ingest stream (mTLS + token; HTTP default, TCP/smux opt-in)
    Edge-->>Client: Forward stream to held response
    Edge->>Edge: Complete pending request
```

## Security and Conceptual Zones

air3 strictly separates your infrastructure into isolated zones:

```mermaid
flowchart TB
    subgraph Public["Public Internet / public network"]
        Client["Client or app"]
        PublicEdge["Edge public listener\nGET/HEAD HTTPS"]

        subgraph Broker["Broker / control plane"]
            NATS["NATS broker\nshort-lived tickets"]
            Ingest["Edge private ingest listener\nmTLS HTTP /ingest, TCP, or smux"]
        end
    end

    subgraph Private["Private network"]
        Connector["Private connector\nno public inbound listener"]
        S3["S3-compatible storage\nVersityGW in demo"]
    end

    Client -->|"public HTTPS"| PublicEdge
    PublicEdge -->|"mTLS ticket publish"| NATS
    Connector -->|"NATS mTLS control plane connection"| NATS
    Connector -->|"S3 private API"| S3
    Connector -->|"outbound mTLS ingest stream (HTTP, TCP, or smux)"| Ingest
    Ingest -->|"stream handoff inside edge"| PublicEdge
```

## System Components

- `cmd/edge-gateway`: The public-facing server. It listens for client `GET`/`HEAD` requests and provides a private mTLS "ingest" listener to receive the file stream from the connector (HTTP by default, with experimental opt-in TCP and smux).
- `cmd/private-connector`: The private worker. It securely holds S3 credentials, pulls tickets from NATS, fetches the files from S3, and pushes them out to the edge gateway.
- `cmd/signurl`: A CLI utility for generating signed URLs for your Edge Gateway.
- `internal/*`: Core logic for configuration, signing, mTLS, NATS communication, and object fetching.

## Quickstart: Docker Compose Demo

Want to see it in action? The demo spins up a fully separated environment using Docker Compose.

**Prerequisites:**
- Go 1.22+
- `make`
- Docker with Compose plugin
- `openssl` (for demo certificates)
- `curl` (for smoke testing)

The demo creates four isolated services to represent the different network zones:
- `edge-gateway` (Public & Broker networks, exposed on `https://localhost:8443`)
- `private-connector` (Broker & Private networks, completely hidden from the host)
- `nats` (Broker network with mTLS enabled)
- `versitygw` as a mock S3 server (Private network only)

**Run the end-to-end demo:**

```sh
make e2e
```
*(This is a shortcut for `make certs`, `make compose-up`, `make seed`, `make smoke`, and `make compose-down`)*

The smoke tests (`make smoke`) automatically verify that signed `GET`/`HEAD` requests work, expired signatures are rejected, missing objects return `404`, and most importantly, that the Edge container *cannot* bypass the system to connect directly to the private S3 server.

## Compose Performance Test

The Compose benchmark starts the demo stack with a performance override, exposes perf-only public read baselines, seeds three Wikimedia objects into the demo bucket, and compares those baselines against signed air3 gateway-through downloads. The reported paths are:

- `direct_s3`: unsigned public read directly from host-exposed VersityGW.
- `caddy_s3`: unsigned public read through stock `caddy:2-alpine` reverse proxy with `cpus: 1.0`.
- `air3_gateway`: signed Air3 gateway URL through normal edge/private-connector path.

These public reads are perf/demo-only. The normal Compose security topology remains unchanged: outside the perf override, VersityGW stays private and the edge still cannot bypass the connector path.

```sh
make perf                         # one private connector, 5 iterations per object
AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 make perf
make perf-multi                   # AIR3_PERF_MULTI_CONNECTORS private connectors (default: 3)
AIR3_PERF_PARALLELISM=8 make perf # optional parallel gateway-through phase
AIR3_INGEST_TRANSPORT=http1 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=http2 AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=tcp AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
AIR3_INGEST_TRANSPORT=smux AIR3_PERF_ITERATIONS=1 AIR3_PERF_SKIP_BIG=1 AIR3_PERF_CONNECTORS=1 ./deploy/scripts/perf-compose.sh
```

Results are written under `.air3-perf-results/` as per-request CSV plus a summary CSV with average latency, speed, throughput, and penalty percentages for `direct_s3`, `caddy_s3`, and `air3_gateway`. Default result filenames include the transport label, and the CSVs include an `ingest_transport` column so explicit HTTP variants, legacy HTTP, and experimental TCP/smux runs are distinguishable. Downloaded source files are cached under `.air3-perf-cache/` so repeat runs do not re-download them.

Useful knobs:

- `AIR3_PERF_ITERATIONS` (default `5`)
- `AIR3_PERF_CONNECTORS` (default `1`)
- `AIR3_PERF_MULTI_CONNECTORS` (default `3`, used by `make perf-multi`)
- `AIR3_PERF_PARALLELISM` for the optional parallel gateway-through phase
- `AIR3_PERF_SKIP_BIG=1` to skip the video object for a quick run
- `AIR3_PERF_S3_PORT` to change the temporary localhost VersityGW port
- `AIR3_PERF_CADDY_PORT` to change the temporary localhost Caddy baseline port
- `AIR3_PERF_CADDY_BASE_URL` to point the `caddy_s3` baseline at a different proxy URL
- `AIR3_PERF_PUBLIC_READ_MODE` controls how the perf script enables anonymous reads: ACL mode sets a public-read bucket ACL in VersityGW; `auto` falls back to a bucket policy if ACL-only anonymous reads are not sufficient.
- `AIR3_STREAM_COPY_BUFFER_BYTES` (default `262144`) tunes the Edge streaming copy buffer size for Compose perf runs.
- `AIR3_INGEST_TRANSPORT=http|http1|http2|tcp|smux` selects the connector→edge ingest transport. Legacy `http` remains the default for compatibility; `http1` forces connector HTTP/1.1, `http2` enables connector HTTP/2, `tcp` uses experimental mTLS TCP stream ingest with a MessagePack metadata frame, and `smux` uses MessagePack-framed ingest streams over persistent mTLS TCP, one smux stream per object. Compose keeps the TCP/smux ingest port internal by default.
- `AIR3_INGEST_DISABLE_HTTP2` (default `false`) is legacy/compatibility-only for `AIR3_INGEST_TRANSPORT=http` (`true` = HTTP/1.1, `false` = HTTP/2). Explicit `http1` and `http2` ignore it.
- `AIR3_EDGE_INGEST_TCP_ADDR` (Compose default `:9444`) controls the edge TCP/smux ingest listener when `tcp` or `smux` transport is enabled.
- `AIR3_INGEST_TCP_ADDR` (Compose default `edge-gateway:9444`) controls the connector TCP/smux dial address when `tcp` or `smux` transport is enabled. `smux` reuses these TCP address variables. `AIR3_INGEST_URL` remains the HTTPS ingest fallback/ticket URL in all transport modes.
- `AIR3_PERF_CACHE_DIR` and `AIR3_PERF_RESULTS_DIR` to relocate cache/results

The perf override limits each `edge-gateway` and `private-connector` container to one CPU. It also adds the `caddy-s3` service and `deploy/Caddyfile.perf` for the Caddy baseline. Unsigned public baselines use curl against the perf-exposed endpoints; gateway measurements use `cmd/signurl` and `curl --http1.1 --cacert deploy/certs/generated/dev-ca.crt` against `https://localhost:8443` for stable streaming timings. TCP and smux ingest use the same mTLS files, connector identity allowlist, and one-time ingest token semantics as HTTP ingest. TCP sends a MessagePack metadata frame before object bytes; smux uses MessagePack-framed ingest streams over persistent mTLS TCP, one smux stream per object.

## Generating Signed URLs

To allow users to download files, your backend application must generate an **edge signed URL**. These use a standard HMAC signature. Since the gateway verifies the signature before generating a ticket, bogus requests are dropped at the edge—saving your private network from dealing with malicious traffic.

We provide drop-in packages for Go, TypeScript, and Python under the `packages/` directory. The TypeScript package is published to the public npm registry as `@terion/air3-edgesign`:

```sh
npm install @terion/air3-edgesign
```

### Go Example

```go
raw, err := edgesign.SignURL(edgesign.SignInput{
    Method:  "GET",
    BaseURL: "https://files.example.com",
    Bucket:  "demo-bucket",
    Key:     "dir/object.txt",
    Secret:  signingSecret,
    Expires: time.Now().Add(15 * time.Minute),
    Range:   "bytes=0-99", // optional
})
```

### TypeScript Example

```ts
import { signUrl, verifyUrl } from '@terion/air3-edgesign';

const raw = signUrl({
  method: 'GET',
  baseUrl: 'https://files.example.com',
  bucket: 'demo-bucket',
  key: 'dir/object.txt',
  secret: signingSecret,
  expires: Math.floor(Date.now() / 1000) + 900,
  range: 'bytes=0-99', // optional
});
```

### Python Example

```python
from datetime import datetime, timedelta, timezone
from edgesign import sign_url, verify_url

raw = sign_url(
    method="GET",
    base_url="https://files.example.com",
    bucket="demo-bucket",
    key="dir/object.txt",
    secret=signing_secret,
    expires=datetime.now(timezone.utc) + timedelta(minutes=15),
    range="bytes=0-99",  # optional
)
```

## HTTP Range Requests

If you need to serve partial files (like for streaming video), `air3` fully supports standard HTTP `Range` headers.

If you're using signed URLs and expect clients to send `Range` headers, you must include the exact range claim when generating the URL (e.g., `cmd/signurl -range 'bytes=0-99'`). The Connector forwards authorized ranges to S3, seamlessly returning a `206 Partial Content` response to the client.

## Local Development & Validation

```sh
make validate        # format, test, build, and check compose config
make fmt             # format Go code
make test            # run Go tests
make build           # build binaries into ./bin
make ts-test         # install, test, and build the TypeScript package
make python-test     # run Python package tests
go test ./... -race  # run race-enabled Go tests
```

## Releases & Docker Images

We publish multi-architecture Docker images (Linux on `amd64` and `arm64`) to the GitHub Container Registry on every release. Cross-compiled binaries for macOS, Windows, and Linux are also attached to GitHub releases. The same tag workflow publishes the public npm package `@terion/air3-edgesign` to npmjs with the tag as the npm version. Publishing uses npm Trusted Publishing from `.github/workflows/release.yml`, so the GitHub Actions job authenticates to npm through OIDC instead of an npm token.

- `ghcr.io/terion-name/air3/edge-gateway:<tag>`
- `ghcr.io/terion-name/air3/private-connector:<tag>`
- `ghcr.io/terion-name/air3/signurl:<tag>`

You can build local images using the root `Dockerfile` by setting the `APP` argument:
```sh
docker build --build-arg APP=edge-gateway -t air3-edge-gateway:local .
docker build --build-arg APP=private-connector -t air3-private-connector:local .
docker build --build-arg APP=signurl -t air3-signurl:local .
```
