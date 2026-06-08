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
  go run ./cmd/signurl -method "$method" -base-url "$BASE_URL" -bucket "$BUCKET" -key "$key" -secret "$SECRET" -expiration "$expiration"
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
body=$(curl --silent --show-error --fail --cacert "$CERT_DIR/dev-ca.crt" "$get_url")
if [ "$body"$'\n' != "$EXPECTED" ] && [ "$body" != "$EXPECTED" ]; then
  echo "error: GET body did not match expected content" >&2
  printf 'expected: %q\nactual:   %q\n' "$EXPECTED" "$body" >&2
  exit 1
fi
echo "ok: signed GET returned expected content"

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
