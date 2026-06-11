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

assert_body() {
  local label=$1
  local url=$2
  local body
  body=$(curl --silent --show-error --fail --cacert "$CERT_DIR/dev-ca.crt" "$url")
  if [ "$body"$'\n' != "$EXPECTED" ] && [ "$body" != "$EXPECTED" ]; then
    echo "error: $label body did not match expected content" >&2
    printf 'expected: %q\nactual:   %q\n' "$EXPECTED" "$body" >&2
    exit 1
  fi
  echo "ok: $label returned expected content"
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
