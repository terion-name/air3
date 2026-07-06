#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml"}
CERT_DIR=${AIR3_CERT_DIR:-"$ROOT_DIR/deploy/certs/generated"}
BASE_URL=${AIR3_DEMO_BASE_URL:-https://localhost:8443}
BUCKET=${AIR3_DEMO_BUCKET:-demo}
KEY=${AIR3_DEMO_KEY:-hello.txt}
MISSING_KEY=${AIR3_DEMO_MISSING_KEY:-missing.txt}
SECRET=${AIR3_SIGNING_SECRET:-dev-signing-secret-change-me}
EXPECTED=${AIR3_DEMO_CONTENT:-$'hello from air3 compose demo\n'}
S3_ENDPOINT=${AIR3_DEMO_S3_ENDPOINT:-http://versitygw:10000}
SHORT_FORM_COLLISION_KEY=${AIR3_DEMO_SHORT_FORM_KEY:-$BUCKET/file.txt}
LEGACY_COLLISION_KEY=${AIR3_DEMO_LEGACY_COLLISION_KEY:-file.txt}
SHORT_FORM_EXPECTED=${AIR3_DEMO_SHORT_FORM_CONTENT:-"short-form wins: key $SHORT_FORM_COLLISION_KEY"$'\n'}
LEGACY_COLLISION_CONTENT=${AIR3_DEMO_LEGACY_COLLISION_CONTENT:-"legacy full-path sentinel: key $LEGACY_COLLISION_KEY"$'\n'}

run_compose() {
  # shellcheck disable=SC2086
  $COMPOSE -f "$COMPOSE_FILE" "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}

sign_url() {
  local method=$1
  local key=$2
  local expiration=${3:-2m}
  go run ./cmd/signurl -method "$method" -base-url "$BASE_URL" -bucket "$BUCKET" -key "$key" -secret "$SECRET" -expiration "$expiration" -default-bucket-path
}

put_object() {
  local key=$1
  local content=$2
  printf '%s' "$content" | run_compose run --rm --no-deps -T aws-cli "cat > /tmp/air3-smoke-object && aws --endpoint-url '$S3_ENDPOINT' s3 cp /tmp/air3-smoke-object 's3://$BUCKET/$key' --content-type text/plain >/dev/null"
}

status_for() {
  local url=$1
  curl --silent --show-error --output /dev/null --write-out '%{http_code}' --cacert "$CERT_DIR/dev-ca.crt" "$url"
}

assert_status() {
  local label=$1
  local expected=$2
  local url=$3
  local status
  status=$(status_for "$url")
  if [ "$status" != "$expected" ]; then
    echo "error: $label expected HTTP $expected, got $status" >&2
    exit 1
  fi
  echo "ok: $label returned HTTP $expected"
}

assert_body() {
  local label=$1
  local expected=$2
  local url=$3
  local body
  body=$(curl --silent --show-error --fail --cacert "$CERT_DIR/dev-ca.crt" "$url")
  if [ "$body"$'\n' != "$expected" ] && [ "$body" != "$expected" ]; then
    echo "error: $label body did not match expected content" >&2
    printf 'expected: %q\nactual:   %q\n' "$expected" "$body" >&2
    exit 1
  fi
  echo "ok: $label returned expected content"
}

s3_api_smoke_ready() {
  if [ "${AIR3_S3_API_ENABLED:-false}" != "true" ]; then
    echo "skip: optional S3-compatible API smoke checks are disabled (set AIR3_S3_API_ENABLED=true to enable)"
    return 1
  fi
  if [ -z "${AIR3_S3_API_ACCESS_KEY_ID:-}" ] || [ -z "${AIR3_S3_API_SECRET_ACCESS_KEY:-}" ]; then
    echo "skip: optional S3-compatible API smoke checks need AIR3_S3_API_ACCESS_KEY_ID and AIR3_S3_API_SECRET_ACCESS_KEY"
    return 1
  fi
  if ! command -v aws >/dev/null 2>&1; then
    echo "skip: optional S3-compatible API smoke checks need the aws CLI on PATH"
    return 1
  fi
  return 0
}

aws_s3api() {
  local aws_config rc
  aws_config=$(mktemp)
  printf '[default]\ns3 =\n    addressing_style = path\n    payload_signing_enabled = false\n' >"$aws_config"
  AWS_CONFIG_FILE="$aws_config" \
    AWS_ACCESS_KEY_ID="$AIR3_S3_API_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AIR3_S3_API_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION="${AIR3_S3_API_REGION:-us-east-1}" \
    AWS_PAGER="" \
    AWS_REQUEST_CHECKSUM_CALCULATION="when_required" \
    AWS_RESPONSE_CHECKSUM_VALIDATION="when_required" \
    aws --endpoint-url "$BASE_URL" --ca-bundle "$CERT_DIR/dev-ca.crt" s3api "$@"
  rc=$?
  rm -f "$aws_config"
  return "$rc"
}

# Like aws_s3api but with stock CLI settings (no payload-signing or checksum
# overrides), proving default-configured clients work against the edge.
aws_default() {
  AWS_CONFIG_FILE=/dev/null \
    AWS_ACCESS_KEY_ID="$AIR3_S3_API_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AIR3_S3_API_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION="${AIR3_S3_API_REGION:-us-east-1}" \
    AWS_PAGER="" \
    aws --endpoint-url "$BASE_URL" --ca-bundle "$CERT_DIR/dev-ca.crt" "$@"
}

mutation_gate_enabled() {
  local primary=${MUTATIONS_ENABLED:-}
  local alias=${AIR3_MUTATIONS_ENABLED:-}
  if [ -n "$primary" ] && [ -n "$alias" ] && [ "$primary" != "$alias" ]; then
    return 1
  fi
  if [ -n "$primary" ]; then
    [ "$primary" = "true" ]
    return
  fi
  [ "$alias" = "true" ]
}

mutation_smoke_ready() {
  s3_api_smoke_ready || return 1
  if ! mutation_gate_enabled; then
    echo "skip: optional S3-compatible mutation smoke checks need MUTATIONS_ENABLED=true (or matching AIR3_MUTATIONS_ENABLED=true)"
    return 1
  fi
  return 0
}

run_optional_s3_api_smoke() {
  s3_api_smoke_ready || return 0

  echo "Running optional S3-compatible API smoke checks..."
  local body list_keys
  body=$(mktemp)
  if ! aws_s3api get-object --bucket "$BUCKET" --key "$KEY" "$body" >/dev/null; then
    rm -f "$body"
    echo "error: S3 API GetObject failed" >&2
    exit 1
  fi
  if ! cmp -s "$body" <(printf '%s' "$EXPECTED"); then
    echo "error: S3 API GetObject body did not match expected content" >&2
    rm -f "$body"
    exit 1
  fi
  rm -f "$body"
  echo "ok: S3 API GetObject returned expected content"

  if ! aws_s3api head-object --bucket "$BUCKET" --key "$KEY" >/dev/null; then
    echo "error: S3 API HeadObject failed" >&2
    exit 1
  fi
  echo "ok: S3 API HeadObject succeeded"

  if ! aws_s3api head-bucket --bucket "$BUCKET" >/dev/null; then
    echo "error: S3 API HeadBucket validation failed" >&2
    exit 1
  fi
  echo "ok: S3 API HeadBucket validation succeeded"

  if ! list_keys=$(aws_s3api list-objects-v2 --bucket "$BUCKET" --prefix "$KEY" --query 'Contents[].Key' --output text); then
    echo "error: S3 API ListObjectsV2 failed" >&2
    exit 1
  fi
  if [[ "$list_keys" != *"$KEY"* ]]; then
    echo "error: S3 API ListObjectsV2 did not include $KEY" >&2
    printf 'keys: %s\n' "$list_keys" >&2
    exit 1
  fi
  echo "ok: S3 API ListObjectsV2 included $KEY"
}

run_optional_s3_mutation_smoke() {
  mutation_smoke_ready || return 0

  echo "Running optional S3-compatible mutation smoke checks..."
  local mutation_key mutation_content mutation_body get_url deleted_url
  mutation_key=${AIR3_DEMO_MUTATION_KEY:-"air3-smoke-mutation-$(date +%s)-$$.txt"}
  mutation_content=${AIR3_DEMO_MUTATION_CONTENT:-$'air3 mutation smoke\n'}
  mutation_body=$(mktemp)
  printf '%s' "$mutation_content" >"$mutation_body"

  if ! aws_s3api put-object --bucket "$BUCKET" --key "$mutation_key" --body "$mutation_body" --content-type text/plain >/dev/null; then
    rm -f "$mutation_body"
    echo "error: S3 API PutObject failed" >&2
    exit 1
  fi
  rm -f "$mutation_body"
  echo "ok: S3 API PutObject created temporary object"

  get_url=$(sign_url GET "$mutation_key" 2m)
  assert_body "signed GET after S3 API PutObject" "$mutation_content" "$get_url"

  if ! aws_s3api head-object --bucket "$BUCKET" --key "$mutation_key" >/dev/null; then
    echo "error: S3 API HeadObject after PutObject failed" >&2
    exit 1
  fi
  echo "ok: S3 API HeadObject found temporary object"

  if ! aws_s3api delete-object --bucket "$BUCKET" --key "$mutation_key" >/dev/null; then
    echo "error: S3 API DeleteObject failed" >&2
    exit 1
  fi
  echo "ok: S3 API DeleteObject removed temporary object"

  deleted_url=$(sign_url GET "$mutation_key" 2m)
  assert_status "signed GET after S3 API DeleteObject" "404" "$deleted_url"

  run_default_client_mutation_smoke
  run_multipart_mutation_smoke
}

run_default_client_mutation_smoke() {
  echo "Running default-settings AWS CLI mutation smoke checks..."
  local key body readback
  key="air3-smoke-default-$(date +%s)-$$.txt"
  body=$(mktemp)
  readback=$(mktemp)
  printf 'default client settings\n' >"$body"

  if ! aws_default s3api put-object --bucket "$BUCKET" --key "$key" --body "$body" --content-type text/plain >/dev/null; then
    rm -f "$body" "$readback"
    echo "error: default-settings PutObject (aws-chunked) failed" >&2
    exit 1
  fi
  echo "ok: default-settings PutObject (aws-chunked trailer mode) succeeded"

  if ! aws_default s3api get-object --bucket "$BUCKET" --key "$key" "$readback" >/dev/null; then
    rm -f "$body" "$readback"
    echo "error: default-settings GetObject failed" >&2
    exit 1
  fi
  if ! cmp -s "$body" "$readback"; then
    rm -f "$body" "$readback"
    echo "error: default-settings readback did not match uploaded content" >&2
    exit 1
  fi
  rm -f "$body" "$readback"
  echo "ok: default-settings readback matched uploaded content"

  if ! aws_default s3api delete-object --bucket "$BUCKET" --key "$key" >/dev/null; then
    echo "error: default-settings DeleteObject failed" >&2
    exit 1
  fi
  echo "ok: default-settings DeleteObject removed temporary object"
}

run_multipart_mutation_smoke() {
  echo "Running multipart upload smoke checks (aws s3 cp)..."
  local key big_body big_readback size_mb
  key="air3-smoke-multipart-$(date +%s)-$$.bin"
  size_mb=${AIR3_DEMO_MULTIPART_MB:-16}
  big_body=$(mktemp)
  big_readback=$(mktemp)
  dd if=/dev/urandom of="$big_body" bs=1048576 count="$size_mb" status=none

  # aws s3 cp switches to multipart upload above the 8 MiB threshold.
  if ! aws_default s3 cp "$big_body" "s3://$BUCKET/$key" >/dev/null; then
    rm -f "$big_body" "$big_readback"
    echo "error: multipart upload via aws s3 cp failed" >&2
    exit 1
  fi
  echo "ok: multipart upload via aws s3 cp succeeded (${size_mb} MiB)"

  if ! aws_default s3 cp "s3://$BUCKET/$key" "$big_readback" >/dev/null; then
    rm -f "$big_body" "$big_readback"
    echo "error: multipart download via aws s3 cp failed" >&2
    exit 1
  fi
  if ! cmp -s "$big_body" "$big_readback"; then
    rm -f "$big_body" "$big_readback"
    echo "error: multipart readback did not match uploaded content" >&2
    exit 1
  fi
  rm -f "$big_body" "$big_readback"
  echo "ok: multipart readback matched uploaded content"

  if ! aws_default s3api delete-object --bucket "$BUCKET" --key "$key" >/dev/null; then
    echo "error: multipart cleanup DeleteObject failed" >&2
    exit 1
  fi
  echo "ok: multipart cleanup removed temporary object"
}

wait_for_edge() {
  echo "Waiting for edge gateway at $BASE_URL..."
  local url
  url=$(sign_url GET "$KEY" 2m)
  for i in $(seq 1 60); do
    if [ "$(status_for "$url" || true)" = "200" ]; then
      echo "ok: edge gateway is serving signed requests"
      return 0
    fi
    sleep 1
  done
  echo "error: edge gateway did not become ready at $BASE_URL" >&2
  echo "hint: run 'make certs compose-up seed' first, then retry smoke" >&2
  exit 1
}

require_cmd docker
require_cmd go
require_cmd curl

if [ ! -f "$CERT_DIR/dev-ca.crt" ]; then
  echo "error: missing $CERT_DIR/dev-ca.crt; run make certs first" >&2
  exit 1
fi

wait_for_edge

get_url=$(sign_url GET "$KEY" 2m)
assert_body "signed GET" "$EXPECTED" "$get_url"

head_url=$(sign_url HEAD "$KEY" 2m)
head_headers=$(mktemp)
head_body=$(mktemp)
trap 'rm -f "$head_headers" "$head_body"' EXIT
head_status=$(curl --silent --show-error --head --output "$head_headers" --write-out '%{http_code}' --cacert "$CERT_DIR/dev-ca.crt" "$head_url")
if [ "$head_status" != "200" ]; then
  echo "error: signed HEAD expected HTTP 200, got $head_status" >&2
  exit 1
fi
curl --silent --show-error --request HEAD --ignore-content-length --output "$head_body" --cacert "$CERT_DIR/dev-ca.crt" "$head_url"
if [ -s "$head_body" ]; then
  echo "error: signed HEAD returned a response body" >&2
  exit 1
fi
echo "ok: signed HEAD returned headers and no body"

# With AIR3_S3_BUCKET=demo, signing key demo/file.txt emits /demo/file.txt;
# this must fetch key demo/file.txt, not legacy full-path key file.txt.
echo "Preparing short-form default-bucket collision objects..."
put_object "$LEGACY_COLLISION_KEY" "$LEGACY_COLLISION_CONTENT"
put_object "$SHORT_FORM_COLLISION_KEY" "$SHORT_FORM_EXPECTED"
short_form_url=$(sign_url GET "$SHORT_FORM_COLLISION_KEY" 2m)
assert_body "short-form /$SHORT_FORM_COLLISION_KEY default-bucket routing" "$SHORT_FORM_EXPECTED" "$short_form_url"

run_optional_s3_api_smoke
run_optional_s3_mutation_smoke

bad_url=$(printf '%s' "$get_url" | sed 's/sig=[^&]*/sig=deadbeef/')
assert_status "bad signature rejection" "403" "$bad_url"

expired_url=$(sign_url GET "$KEY" 1s)
sleep 2
assert_status "expired signature rejection" "403" "$expired_url"

missing_url=$(sign_url GET "$MISSING_KEY" 2m)
assert_status "missing object mapping" "404" "$missing_url"

if run_compose exec -T edge-gateway sh -c 'nc -z -w 2 versitygw 10000' >/dev/null 2>&1; then
  echo "error: edge-gateway unexpectedly reached private S3 service versitygw:10000" >&2
  exit 1
fi
echo "ok: edge-gateway cannot connect directly to VersityGW"

echo "Checking connector-down timeout behavior..."
run_compose stop -t 2 private-connector >/dev/null
trap 'run_compose start private-connector >/dev/null 2>&1 || true; rm -f "$head_headers" "$head_body"' EXIT
down_url=$(sign_url GET "$KEY" 30s)
assert_status "connector-down timeout/unavailable behavior" "504" "$down_url"
run_compose start private-connector >/dev/null
for i in $(seq 1 30); do
  fresh_url=$(sign_url GET "$KEY" 30s)
  if [ "$(status_for "$fresh_url" || true)" = "200" ]; then
    echo "ok: connector restarted and fresh requests work (Core NATS did not replay the down-period request)"
    trap 'rm -f "$head_headers" "$head_body"' EXIT
    echo "Smoke tests passed"
    exit 0
  fi
  sleep 1
done

echo "error: private connector did not recover after restart" >&2
exit 1
