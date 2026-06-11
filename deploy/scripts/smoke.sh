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
