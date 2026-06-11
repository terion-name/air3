#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml:$ROOT_DIR/deploy/compose.multiserver.yaml"}
BUCKET=${AIR3_DEMO_BUCKET:-demo}
KEY=${AIR3_DEMO_KEY:-hello.txt}
CONTENT=${AIR3_DEMO_CONTENT:-$'hello from air3 compose demo\n'}
ENDPOINT=${AIR3_DEMO_S3_ENDPOINT:-http://versitygw:10000}

compose_args=()
IFS=':' read -r -a compose_files <<< "$COMPOSE_FILE"
for compose_file in "${compose_files[@]}"; do
  if [ -n "$compose_file" ]; then
    compose_args+=("-f" "$compose_file")
  fi
done

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

require_cmd docker

if [ "${#compose_args[@]}" -eq 0 ]; then
  echo "error: COMPOSE_FILE must contain at least one compose file" >&2
  exit 1
fi

echo "Ensuring VersityGW is running..."
run_compose up -d versitygw >/dev/null

echo "Waiting for S3 endpoint $ENDPOINT..."
for i in $(seq 1 60); do
  if run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$ENDPOINT' s3api list-buckets >/dev/null" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "error: VersityGW did not become ready at $ENDPOINT" >&2
    exit 1
  fi
  sleep 1
done

echo "Creating bucket s3://$BUCKET if needed..."
run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$ENDPOINT' s3api head-bucket --bucket '$BUCKET' >/dev/null 2>&1 || aws --endpoint-url '$ENDPOINT' s3api create-bucket --bucket '$BUCKET' >/dev/null"

echo "Writing demo object s3://$BUCKET/$KEY..."
printf '%s' "$CONTENT" | run_compose run --rm --no-deps -T aws-cli "cat > /tmp/air3-seed-object && aws --endpoint-url '$ENDPOINT' s3 cp /tmp/air3-seed-object 's3://$BUCKET/$KEY' --content-type text/plain >/dev/null"

echo "Seeded s3://$BUCKET/$KEY"
