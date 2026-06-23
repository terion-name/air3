#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml:$ROOT_DIR/deploy/compose.multiserver.yaml"}
CERT_DIR=${AIR3_CERT_DIR:-"$ROOT_DIR/deploy/certs/generated"}
BASE_URL=${AIR3_DEMO_BASE_URL:-https://localhost:8443}
BUCKET=${AIR3_DEMO_BUCKET:-demo}
KEY=${AIR3_DEMO_KEY:-hello.txt}
SECRET=${AIR3_SIGNING_SECRET:-dev-signing-secret-change-me}
EXPECTED=${AIR3_DEMO_CONTENT:-$'hello from air3 compose demo\n'}
BLUE_SERVER=${AIR3_BLUE_SERVER:-blue}
DIRECT_SERVER=${AIR3_DIRECT_SERVER:-direct}
GREEN_SERVER=${AIR3_GREEN_SERVER:-green}

compose_args=()
IFS=':' read -r -a compose_files <<< "$COMPOSE_FILE"
for compose_file in "${compose_files[@]}"; do
  if [ -n "$compose_file" ]; then
    compose_args+=("-f" "$compose_file")
  fi
done

temp_files=()
connector_stopped=false
cleanup() {
  if [ "$connector_stopped" = true ]; then
    run_compose start private-connector-blue >/dev/null 2>&1 || true
  fi
  if [ "${#temp_files[@]}" -gt 0 ]; then
    rm -f "${temp_files[@]}"
  fi
}
trap cleanup EXIT

run_compose() {
  # shellcheck disable=SC2086
  $COMPOSE "${compose_args[@]}" "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}

sign_url() {
  local method=$1
  local server=$2
  local key=$3
  local expiration=${4:-2m}
  go run ./cmd/signurl -method "$method" -server "$server" -base-url "$BASE_URL" -bucket "$BUCKET" -key "$key" -secret "$SECRET" -expiration "$expiration"
}

sign_default_bucket_url() {
  local method=$1
  local server=$2
  local key=$3
  local expiration=${4:-2m}
  go run ./cmd/signurl -method "$method" -server "$server" -base-url "$BASE_URL" -bucket "$BUCKET" -key "$key" -secret "$SECRET" -expiration "$expiration" -default-bucket-path
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

assert_body_equals() {
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

assert_body() {
  local label=$1
  local url=$2
  assert_body_equals "$label" "$EXPECTED" "$url"
}

assert_head_no_body() {
  local label=$1
  local url=$2
  local headers body status
  headers=$(mktemp)
  body=$(mktemp)
  temp_files+=("$headers" "$body")

  status=$(curl --silent --show-error --head --output "$headers" --write-out '%{http_code}' --cacert "$CERT_DIR/dev-ca.crt" "$url")
  if [ "$status" != "200" ]; then
    echo "error: $label HEAD expected HTTP 200, got $status" >&2
    exit 1
  fi
  curl --silent --show-error --request HEAD --ignore-content-length --output "$body" --cacert "$CERT_DIR/dev-ca.crt" "$url"
  if [ -s "$body" ]; then
    echo "error: $label HEAD returned a response body" >&2
    exit 1
  fi
  echo "ok: $label HEAD returned headers and no body"
}

check_optional_head() {
  local label=$1
  local url=$2
  local headers body status
  headers=$(mktemp)
  body=$(mktemp)
  temp_files+=("$headers" "$body")

  status=$(curl --silent --show-error --head --output "$headers" --write-out '%{http_code}' --cacert "$CERT_DIR/dev-ca.crt" "$url")
  case "$status" in
    200)
      curl --silent --show-error --request HEAD --ignore-content-length --output "$body" --cacert "$CERT_DIR/dev-ca.crt" "$url"
      if [ -s "$body" ]; then
        echo "error: $label HEAD returned a response body" >&2
        exit 1
      fi
      echo "ok: $label HEAD returned headers and no body"
      ;;
    405|501)
      echo "ok: $label HEAD returned HTTP $status; treating as unsupported"
      ;;
    *)
      echo "error: $label HEAD expected HTTP 200 or unsupported 405/501, got $status" >&2
      exit 1
      ;;
  esac
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
  temp_files+=("$aws_config")
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
    echo "skip: optional multi-server S3-compatible mutation smoke checks need MUTATIONS_ENABLED=true (or matching AIR3_MUTATIONS_ENABLED=true)"
    return 1
  fi
  return 0
}

assert_s3_api_body() {
  local label=$1
  local bucket=$2
  local key=$3
  local body
  body=$(mktemp)
  temp_files+=("$body")
  if ! aws_s3api get-object --bucket "$bucket" --key "$key" "$body" >/dev/null; then
    echo "error: $label S3 API GetObject failed" >&2
    exit 1
  fi
  if ! cmp -s "$body" <(printf '%s' "$EXPECTED"); then
    echo "error: $label S3 API GetObject body did not match expected content" >&2
    exit 1
  fi
  echo "ok: $label S3 API GetObject returned expected content"
}

assert_s3_api_list_contains() {
  local label=$1
  local bucket=$2
  local prefix=$3
  local expected_key=$4
  local list_keys
  if ! list_keys=$(aws_s3api list-objects-v2 --bucket "$bucket" --prefix "$prefix" --query 'Contents[].Key' --output text); then
    echo "error: $label S3 API ListObjectsV2 failed" >&2
    exit 1
  fi
  if [[ "$list_keys" != *"$expected_key"* ]]; then
    echo "error: $label S3 API ListObjectsV2 did not include $expected_key" >&2
    printf 'keys: %s\n' "$list_keys" >&2
    exit 1
  fi
  echo "ok: $label S3 API ListObjectsV2 included $expected_key"
}

run_optional_s3_api_smoke() {
  s3_api_smoke_ready || return 0

  echo "Running optional multi-server S3-compatible API smoke checks..."
  assert_s3_api_body "blue default-bucket mapping" "$BLUE_SERVER" "$KEY"
  assert_s3_api_list_contains "blue default-bucket mapping" "$BLUE_SERVER" "$KEY" "$KEY"

  if ! aws_s3api head-bucket --bucket "$BLUE_SERVER" >/dev/null; then
    echo "error: blue S3 API HeadBucket validation failed" >&2
    exit 1
  fi
  echo "ok: blue S3 API HeadBucket validation succeeded"

  assert_s3_api_body "direct default-bucket mapping" "$DIRECT_SERVER" "$KEY"
  assert_s3_api_list_contains "direct default-bucket mapping" "$DIRECT_SERVER" "$KEY" "$KEY"
}

run_optional_s3_mutation_smoke() {
  mutation_smoke_ready || return 0

  echo "Running optional multi-server S3-compatible mutation smoke checks..."
  local mutation_key mutation_content mutation_body get_url head_url deleted_url
  mutation_key=${AIR3_DEMO_MUTATION_KEY:-"air3-smoke-mutation-$(date +%s)-$$.txt"}
  mutation_content=${AIR3_DEMO_MUTATION_CONTENT:-$'air3 routed mutation smoke\n'}
  mutation_body=$(mktemp)
  temp_files+=("$mutation_body")
  printf '%s' "$mutation_content" >"$mutation_body"

  if ! aws_s3api put-object --bucket "$BLUE_SERVER" --key "$mutation_key" --body "$mutation_body" --content-type text/plain >/dev/null; then
    echo "error: blue routed S3 API PutObject failed" >&2
    exit 1
  fi
  echo "ok: blue routed S3 API PutObject created temporary object"

  get_url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$mutation_key" 2m)
  assert_body_equals "blue signed GET after routed S3 API PutObject" "$mutation_content" "$get_url"

  head_url=$(sign_default_bucket_url HEAD "$BLUE_SERVER" "$mutation_key" 2m)
  assert_head_no_body "blue signed HEAD after routed S3 API PutObject" "$head_url"

  if ! aws_s3api delete-object --bucket "$BLUE_SERVER" --key "$mutation_key" >/dev/null; then
    echo "error: blue routed S3 API DeleteObject failed" >&2
    exit 1
  fi
  echo "ok: blue routed S3 API DeleteObject removed temporary object"

  deleted_url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$mutation_key" 2m)
  assert_status "blue signed GET after routed S3 API DeleteObject" "404" "$deleted_url"
}

wait_for_blue() {
  echo "Waiting for edge gateway at $BASE_URL with server '$BLUE_SERVER'..."
  local url
  url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$KEY" 2m)
  for i in $(seq 1 60); do
    if [ "$(status_for "$url" || true)" = "200" ]; then
      echo "ok: edge gateway is serving routed signed requests"
      return 0
    fi
    sleep 1
  done
  echo "error: edge gateway did not become ready for server '$BLUE_SERVER' at $BASE_URL" >&2
  echo "hint: run 'make certs compose-multiserver-up seed-multiserver' first, then retry smoke-multiserver" >&2
  exit 1
}

require_cmd docker
require_cmd go
require_cmd curl

if [ "${#compose_args[@]}" -eq 0 ]; then
  echo "error: COMPOSE_FILE must contain at least one compose file" >&2
  exit 1
fi

if [ ! -f "$CERT_DIR/dev-ca.crt" ]; then
  echo "error: missing $CERT_DIR/dev-ca.crt; run make certs first" >&2
  exit 1
fi

wait_for_blue

blue_get_url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$KEY" 2m)
assert_body "blue signed GET" "$blue_get_url"

blue_head_url=$(sign_default_bucket_url HEAD "$BLUE_SERVER" "$KEY" 2m)
assert_head_no_body "blue signed" "$blue_head_url"

direct_get_url=$(sign_default_bucket_url GET "$DIRECT_SERVER" "$KEY" 2m)
assert_body "direct signed GET" "$direct_get_url"

direct_head_url=$(sign_default_bucket_url HEAD "$DIRECT_SERVER" "$KEY" 2m)
check_optional_head "direct signed" "$direct_head_url"

run_optional_s3_api_smoke
run_optional_s3_mutation_smoke

mutated_url=${blue_get_url/\/$BLUE_SERVER\//\/$GREEN_SERVER\/$BUCKET\/}
if [ "$mutated_url" = "$blue_get_url" ]; then
  echo "error: failed to mutate '$BLUE_SERVER' default-bucket URL to '$GREEN_SERVER' full-path URL" >&2
  exit 1
fi
assert_status "server alias signature isolation" "403" "$mutated_url"

green_url=$(sign_url GET "$GREEN_SERVER" "$KEY" 30s)
assert_status "green no-subscriber behavior" "504" "$green_url"

echo "Checking routed connector-down isolation behavior..."
run_compose stop -t 2 private-connector-blue >/dev/null
connector_stopped=true

blue_down_url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$KEY" 30s)
assert_status "blue connector-down behavior" "504" "$blue_down_url"

direct_while_down_url=$(sign_default_bucket_url GET "$DIRECT_SERVER" "$KEY" 30s)
assert_body "direct signed GET while blue connector is stopped" "$direct_while_down_url"

run_compose start private-connector-blue >/dev/null
connector_stopped=false
for i in $(seq 1 30); do
  fresh_url=$(sign_default_bucket_url GET "$BLUE_SERVER" "$KEY" 30s)
  if [ "$(status_for "$fresh_url" || true)" = "200" ]; then
    echo "ok: blue connector restarted and fresh routed requests work"
    echo "Multi-server smoke tests passed"
    exit 0
  fi
  sleep 1
done

echo "error: private-connector-blue did not recover after restart" >&2
exit 1
