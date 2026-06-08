# air3: NATS S3 File Gateway

air3 is a file gateway that lets an edge service coordinate signed public object downloads with a private connector over NATS while keeping object bytes out of the NATS control plane.

## Project documentation

This implementation follows the approved design and plan:

- [NATS S3 File Gateway Design](docs/superpowers/specs/2026-06-08-nats-s3-file-gateway-design.md)
- [NATS S3 File Gateway Implementation Plan](docs/superpowers/plans/2026-06-08-nats-s3-file-gateway-implementation-plan.md)
- [NATS S3 File Gateway Hardening Notes](docs/superpowers/2026-06-08-nats-s3-file-gateway-hardening.md)

## Architecture constraints

- **NATS Core only:** the gateway uses NATS Core publish/subscribe patterns; JetStream is out of scope.
- **No object bytes through NATS:** NATS carries control messages, tickets, and status only. File content moves over HTTP/S3 paths.
- **Single edge gateway:** the edge gateway is intentionally single-instance for the approved design.
- **Connector-only S3 credentials:** the edge gateway has no S3 credentials and is not attached to the private S3 network. S3 access belongs to the private connector side.

## Live-ticket behavior

The gateway uses live, in-memory tickets rather than a durable queue:

- Tickets are not replayed and are not stored in JetStream or any other persistent work queue.
- Each public request is held by the single edge gateway process that created it; held responses are lost if that process restarts.
- If the connector, NATS broker, edge ingest path, or private S3-compatible backend is unavailable, the public request fails with a mapped error such as `503 Service Unavailable` or `504 Gateway Timeout`.
- One-time ingest tokens and configured connector client certificate identities are validated on the private ingest listener.

## Components

- `cmd/edge-gateway`: public HTTPS entry point for signed `GET`/`HEAD` object requests and private mTLS ingest listener for connector responses.
- `cmd/private-connector`: private-side worker that receives NATS Core tickets, fetches from S3-compatible storage, and posts object bytes to the edge ingest listener.
- `cmd/signurl`: development utility for signing public object URLs.
- `internal/config`: environment configuration loading and validation.
- `internal/tickets`: transfer ticket models.
- `internal/signing`: signed URL HMAC creation and verification.
- `internal/mtls`: TLS and mTLS support.
- `internal/natsclient`: NATS Core client wiring.
- `internal/pending`: in-flight request tracking.
- `internal/ingest`: edge-side ingest coordination.
- `internal/s3fetch`: connector-side S3 object fetching.

## Development

Prerequisites:

- Go 1.22 or newer
- `make`
- Docker with the Compose plugin for the demo environment
- `openssl` for development certificate generation
- `curl` for smoke tests

Common commands:

```sh
make fmt             # format Go code
make test            # run Go tests
make build           # build binaries into ./bin
make compose-config  # validate deploy/compose.yaml
make validate        # format, test, build, and validate Compose config
go test ./... -race  # run race-enabled Go tests (expected to pass)
```

## Releases

Pushing a bare semver-like tag such as `0.0.1` runs the release workflow. The workflow tests the Go packages, validates the Compose file when Docker Compose is available, cross-compiles `edge-gateway`, `private-connector`, and `signurl` for Linux, macOS, and Windows on amd64 and arm64, uploads the packaged artifacts, and publishes them to the GitHub Release for the tag with SHA-256 checksums.

The same release workflow also publishes separate multi-architecture Linux images to GitHub Container Registry. Images are tagged with the release tag:

- `ghcr.io/terion-name/air3/edge-gateway:<tag>`
- `ghcr.io/terion-name/air3/private-connector:<tag>`
- `ghcr.io/terion-name/air3/signurl:<tag>`

Pull a released image with, for example:

```sh
docker pull ghcr.io/terion-name/air3/edge-gateway:0.0.1
docker pull ghcr.io/terion-name/air3/private-connector:0.0.1
docker pull ghcr.io/terion-name/air3/signurl:0.0.1
```

## Docker Compose demo quickstart

The demo starts four runtime services:

- `edge-gateway` on the `public` and `broker` networks, with public HTTPS exposed on <https://localhost:8443>.
- `private-connector` on the `broker` and `private` networks, with no host-published application port.
- `nats` on the `broker` network, using TLS/mTLS and NATS Core only.
- `versitygw` on the `private` network only, with no host-published S3 port.

Generated development certificates live under `deploy/certs/generated/`, which is ignored by git.

Run the end-to-end demo:

```sh
make certs
make compose-up
make seed
make smoke
make compose-down
```

Or run the same sequence as one target:

```sh
make e2e
```

`make smoke` verifies:

- signed `GET` returns the seeded object content,
- signed `HEAD` succeeds without a response body,
- bad and expired signatures are rejected,
- missing objects map to `404`,
- connector downtime maps to a gateway timeout and fresh requests work after restart,
- the edge container cannot connect directly to `versitygw:10000`.

If Docker is unavailable, `make validate` remains the local non-e2e validation path. It runs Go formatting, Go tests, binary builds, and `docker compose -f deploy/compose.yaml config`.

## HTTP and Range behavior

Public object URLs support `GET` and `HEAD`. The gateway supports one HTTP byte range per request using standard `Range` forms such as `bytes=0-99`, `bytes=100-`, or `bytes=-100`. Multiple ranges and malformed ranges are rejected with `400 Bad Request` before a ticket is published.

When URL signing is enabled, a ranged request must include the range claim in the signed URL (`cmd/signurl -range 'bytes=0-99'`). If a client also sends a `Range` header, it must exactly match the signed range claim. The connector forwards accepted ranges to S3 and returns `206 Partial Content` metadata (`Content-Range`, `Accept-Ranges`) when the backend supplies it.

## Demo configuration details

- NATS config is in `deploy/nats/nats.conf`. It enables TLS with client certificate verification and does not configure JetStream, streams, or persistence.
- The edge private ingest listener pins the demo connector certificate identity with `AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES=connector-ingest-client`.
- Dev certificates are generated by `deploy/scripts/certs.sh` for the local CA, NATS server, edge server, edge NATS client, connector NATS client, and connector ingest client.
- S3 seed data is created by `deploy/scripts/seed-s3.sh` using the `amazon/aws-cli` helper service on the private network.
- Smoke tests are implemented in `deploy/scripts/smoke.sh` and use `cmd/signurl` to produce signed URLs.

## Docker image

The root `Dockerfile` builds one selected binary into a small runtime image. Select the command with the `APP` build argument; supported values are `edge-gateway`, `private-connector`, and `signurl`. The selected binary is copied to `/usr/local/bin/air3`, which is the image default command.

```sh
docker build --build-arg APP=edge-gateway -t air3-edge-gateway:dev .
docker build --build-arg APP=private-connector -t air3-private-connector:dev .
docker build --build-arg APP=signurl -t air3-signurl:dev .
```

The Compose demo sets `APP` per service and builds separate local images for `edge-gateway` and `private-connector`.
