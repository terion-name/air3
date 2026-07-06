---
name: verify
description: Build, launch, and drive the air3 compose stack to verify edge/connector changes end-to-end.
---

# Verifying air3 changes end-to-end

The runtime surface is the edge gateway's public HTTPS listener (S3 API +
signed URLs), backed by NATS, the private connector, and versitygw.

## Launch

```bash
# certs already generated in deploy/certs/generated (else: make certs)
export MUTATIONS_ENABLED=true AIR3_S3_API_ENABLED=true \
  AIR3_S3_API_ACCESS_KEY_ID=edge-api-key AIR3_S3_API_SECRET_ACCESS_KEY=edge-api-secret
docker compose -f deploy/compose.yaml up -d --build
./deploy/scripts/seed-s3.sh          # seeds s3://demo/hello.txt
```

Rebuilding one service after a code change: `docker compose -f deploy/compose.yaml up -d --build private-connector`.

## Drive the S3 API (no aws CLI on host)

Run `amazon/aws-cli` on the compose public network. Two settings are
mandatory or the edge rejects mutations with InvalidRequest: path-style
addressing and unsigned payloads (plus `when_required` checksums):

```bash
printf '[default]\ns3 =\n    addressing_style = path\n    payload_signing_enabled = false\n' > /tmp/aws-config
docker run --rm --network air3-demo_public \
  -v "$PWD/deploy/certs/generated:/certs:ro" -v /tmp:/work \
  -e AWS_CONFIG_FILE=/work/aws-config \
  -e AWS_ACCESS_KEY_ID=edge-api-key -e AWS_SECRET_ACCESS_KEY=edge-api-secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
  -e AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
  amazon/aws-cli --endpoint-url https://edge-gateway:8443 --ca-bundle /certs/dev-ca.crt \
  s3api get-object --bucket demo --key hello.txt /dev/stdout
```

## Drive signed URLs (public HMAC path)

```bash
URL=$(go run ./cmd/signurl -method GET -base-url https://localhost:8443 \
  -bucket demo -key hello.txt -secret dev-signing-secret-change-me \
  -expiration 2m -default-bucket-path)
curl --cacert deploy/certs/generated/dev-ca.crt \
  --resolve edge-gateway:8443:127.0.0.1 "${URL/localhost/edge-gateway}"
```

Raw SigV4 probes (odd methods/headers) work with
`curl --aws-sigv4 "aws:amz:us-east-1:s3" --user edge-api-key:edge-api-secret`.

## Gotchas

- Logs are intentionally redacted (`safeLogError`) — both services log
  almost nothing. Debug by watching `docker logs air3-demo-versitygw-1`,
  which access-logs every backend request; if an operation never appears
  there, it failed before reaching S3.
- A status-only error (no XML `<Code>`) comes from the connector's
  fetch-error path; XML errors come from the edge or connector mutation
  paths. The aws CLI prints `(503)` for status-only and `(ServiceUnavailable)`
  for XML — useful to localize failures.
- Tear down: `docker compose -f deploy/compose.yaml down --remove-orphans`
  (volumes persist; delete test objects via the `aws-cli` tools profile
  against `http://versitygw:10000`).
