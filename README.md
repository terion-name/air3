# air3: NATS S3 File Gateway

air3 is a planned file gateway that lets an edge service coordinate uploads and downloads with a private connector over NATS while keeping object bytes out of the NATS control plane.

## Architecture constraints

- **NATS Core only:** the gateway uses NATS Core request/reply and publish/subscribe patterns; JetStream is out of scope.
- **No object bytes through NATS:** NATS carries control messages, tickets, and status only. File content moves over HTTP/S3 paths.
- **Single edge gateway:** the edge gateway is intentionally single-instance for the approved design.
- **Connector-only S3 credentials:** the edge gateway must not have S3 credentials. S3 access belongs to the private connector side.

## Planned components

- `cmd/edge-gateway`: public edge entry point for client upload/download requests.
- `cmd/private-connector`: private-side connector that can access S3 and respond to edge control-plane requests.
- `cmd/signurl`: development utility planned for signing URL workflows.
- `internal/config`: configuration loading and validation.
- `internal/tickets`: transfer ticket models.
- `internal/signing`: ticket signing and verification.
- `internal/mtls`: mutual TLS support.
- `internal/natsclient`: NATS Core client wiring.
- `internal/pending`: in-flight request tracking.
- `internal/ingest`: edge-side HTTP flow coordination.
- `internal/s3fetch`: connector-side S3 coordination.

Runtime behavior is not implemented yet. This repository currently contains the compiling foundation for those components.

## Development

Prerequisites for the current skeleton:

- Go 1.22 or newer
- `make`

Common commands:

```sh
make fmt       # format Go code
make test      # run Go tests
make build     # build placeholder binaries into ./bin
make validate  # format, test, and build
```

The following quickstart steps are planned but not implemented yet:

1. Generate development certificates with `make certs`.
2. Start NATS, S3-compatible storage, the edge gateway, and the private connector with `make compose-up`.
3. Seed S3 test data with `make seed`.
4. Run smoke tests with `make smoke`.
5. Stop local services with `make compose-down`.

## Docker

The root `Dockerfile` builds all three placeholder binaries into a small runtime image. Runtime image layout and entrypoint conventions may change once the gateway behavior is implemented.
