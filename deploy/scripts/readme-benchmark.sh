#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml"}
PERF_COMPOSE_FILE=${AIR3_PERF_COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.perf.yaml"}
CERT_DIR=${AIR3_CERT_DIR:-"$ROOT_DIR/deploy/certs/generated"}
CACHE_DIR=${AIR3_PERF_CACHE_DIR:-"$ROOT_DIR/.air3-perf-cache"}
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR=${AIR3_README_BENCH_RESULTS_DIR:-"$ROOT_DIR/.air3-perf-results/readme-$TIMESTAMP"}

SCALES_TEXT=${AIR3_README_BENCH_SCALES:-"single scaled"}
TRANSPORTS_TEXT=${AIR3_README_BENCH_TRANSPORTS:-"http1 http2 tcp smux quic http3"}
CONTENT_MODES_TEXT=${AIR3_README_BENCH_CONTENT_MODES:-"small medium big mixed"}
TRAFFIC_MODES_TEXT=${AIR3_README_BENCH_TRAFFIC_MODES:-"sequential concurrent"}
SEQ_REPEATS=${AIR3_README_BENCH_SEQ_REPEATS:-3}
CONCURRENT_REQUESTS=${AIR3_README_BENCH_CONCURRENT_REQUESTS:-16}
CONCURRENCY=${AIR3_README_BENCH_CONCURRENCY:-16}

S3_HOST_PORT=${AIR3_PERF_S3_PORT:-10000}
CADDY_HOST_PORT=${AIR3_PERF_CADDY_PORT:-10080}
CADDY_BASE_URL=${AIR3_PERF_CADDY_BASE_URL:-http://localhost:$CADDY_HOST_PORT}
PUBLIC_READ_MODE=${AIR3_PERF_PUBLIC_READ:-${AIR3_PERF_PUBLIC_READ_MODE:-auto}}
BUCKET=${AIR3_PERF_BUCKET:-demo}
BASE_URL=${AIR3_PERF_BASE_URL:-https://localhost:8443}
SECRET=${AIR3_SIGNING_SECRET:-dev-signing-secret-change-me}
S3_ENDPOINT_IN_COMPOSE=${AIR3_PERF_S3_ENDPOINT_IN_COMPOSE:-http://versitygw:10000}
S3_ENDPOINT_ON_HOST=${AIR3_PERF_S3_ENDPOINT_ON_HOST:-http://localhost:$S3_HOST_PORT}
SKIP_BIG=${AIR3_PERF_SKIP_BIG:-0}

: "${AIR3_CONNECTOR_WORKERS:=1024}"
: "${AIR3_INGEST_POOL_SIZE:=1024}"
export AIR3_CONNECTOR_WORKERS AIR3_INGEST_POOL_SIZE

RAW_CSV="$RESULTS_DIR/raw.csv"
AGGREGATE_CSV="$RESULTS_DIR/aggregate.csv"
SUMMARY_MD="$RESULTS_DIR/summary.md"
LOG_DIR="$RESULTS_DIR/logs"
VALIDATE_COMPOSE=0

SMALL_URL='https://upload.wikimedia.org/wikipedia/commons/8/8d/%22Ontology%22%2C_2011-2012.jpg'
MEDIUM_URL='https://upload.wikimedia.org/wikipedia/commons/b/b1/GLI.TC-H_gallery.1.jpg'
BIG_URL='https://upload.wikimedia.org/wikipedia/commons/c/c0/Cosmic_Dust_Bin_-_Interstellar_%28Trance_Music_Video%29.webm'

OBJECT_NAMES=(small medium big)
OBJECT_FILES=(small.jpg medium.jpg big.webm)
OBJECT_KEYS=(perf/small.jpg perf/medium.jpg perf/big.webm)
OBJECT_URLS=("$SMALL_URL" "$MEDIUM_URL" "$BIG_URL")

usage() {
  cat <<EOFUSAGE
Usage: $(basename "$0") [--help] [--validate-compose]

Runs the README-oriented Caddy-vs-Air3 Docker Compose benchmark matrix and writes
README-ready artifacts into a result directory.

Options:
  --help              show this help
  --validate-compose  render per-scale overrides and run docker compose config only

README benchmark env knobs:
  AIR3_README_BENCH_SCALES              scales to run (default: $SCALES_TEXT)
  AIR3_README_BENCH_TRANSPORTS          Air3 transports (default: $TRANSPORTS_TEXT)
  AIR3_README_BENCH_CONTENT_MODES       content modes (default: $CONTENT_MODES_TEXT)
  AIR3_README_BENCH_TRAFFIC_MODES       traffic modes (default: $TRAFFIC_MODES_TEXT)
  AIR3_README_BENCH_SEQ_REPEATS         sequential repeats per object (default: $SEQ_REPEATS)
  AIR3_README_BENCH_CONCURRENT_REQUESTS concurrent requests per group (default: $CONCURRENT_REQUESTS)
  AIR3_README_BENCH_CONCURRENCY         concurrent job limit (default: $CONCURRENCY)
  AIR3_README_BENCH_RESULTS_DIR         output directory (default: $RESULTS_DIR)

Existing-compatible perf knobs honored where relevant:
  COMPOSE, AIR3_PERF_S3_PORT, AIR3_PERF_CADDY_PORT, AIR3_PERF_BASE_URL,
  AIR3_PERF_BUCKET, AIR3_SIGNING_SECRET, AIR3_PERF_PUBLIC_READ or
  AIR3_PERF_PUBLIC_READ_MODE, AIR3_PERF_CACHE_DIR, AIR3_PERF_SKIP_BIG,
  AIR3_PERF_S3_ENDPOINT_IN_COMPOSE, AIR3_PERF_S3_ENDPOINT_ON_HOST,
  AIR3_PERF_CADDY_BASE_URL, AIR3_CONNECTOR_WORKERS, AIR3_INGEST_POOL_SIZE.

Outputs:
  $RAW_CSV
  $AGGREGATE_CSV
  $SUMMARY_MD
  $RESULTS_DIR/compose-single.yaml
  $RESULTS_DIR/compose-scaled.yaml
EOFUSAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --validate-compose)
      VALIDATE_COMPOSE=1
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  shift
done

read -r -a SCALES <<<"$SCALES_TEXT"
read -r -a TRANSPORTS <<<"$TRANSPORTS_TEXT"
read -r -a CONTENT_MODES <<<"$CONTENT_MODES_TEXT"
read -r -a TRAFFIC_MODES <<<"$TRAFFIC_MODES_TEXT"

run_compose() {
  local override_file=$1
  shift
  # shellcheck disable=SC2086
  $COMPOSE -f "$COMPOSE_FILE" -f "$PERF_COMPOSE_FILE" -f "$override_file" "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}

require_positive_int() {
  local name=$1
  local value=$2
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: $name must be a positive integer, got '$value'" >&2
    exit 1
  fi
}

validate_public_read_mode() {
  case "$PUBLIC_READ_MODE" in
    auto|bucket-acl|bucket-policy) ;;
    *)
      echo "error: AIR3_PERF_PUBLIC_READ/AIR3_PERF_PUBLIC_READ_MODE must be auto, bucket-acl, or bucket-policy; got '$PUBLIC_READ_MODE'" >&2
      exit 1
      ;;
  esac
}

validate_scale() {
  case "$1" in
    single|scaled) ;;
    *)
      echo "error: scale must be single or scaled, got '$1'" >&2
      exit 1
      ;;
  esac
}

validate_transport() {
  case "$1" in
    http|http1|http2|http3|tcp|smux|quic) ;;
    *)
      echo "error: transport must be one of http, http1, http2, http3, tcp, smux, quic; got '$1'" >&2
      exit 1
      ;;
  esac
}

validate_content_mode() {
  case "$1" in
    small|medium|big|mixed) ;;
    *)
      echo "error: content mode must be small, medium, big, or mixed; got '$1'" >&2
      exit 1
      ;;
  esac
}

validate_traffic_mode() {
  case "$1" in
    sequential|concurrent) ;;
    *)
      echo "error: traffic mode must be sequential or concurrent, got '$1'" >&2
      exit 1
      ;;
  esac
}

validate_inputs() {
  require_positive_int AIR3_README_BENCH_SEQ_REPEATS "$SEQ_REPEATS"
  require_positive_int AIR3_README_BENCH_CONCURRENT_REQUESTS "$CONCURRENT_REQUESTS"
  require_positive_int AIR3_README_BENCH_CONCURRENCY "$CONCURRENCY"
  require_positive_int AIR3_CONNECTOR_WORKERS "$AIR3_CONNECTOR_WORKERS"
  require_positive_int AIR3_INGEST_POOL_SIZE "$AIR3_INGEST_POOL_SIZE"
  validate_public_read_mode

  local value
  for value in "${SCALES[@]}"; do validate_scale "$value"; done
  for value in "${TRANSPORTS[@]}"; do validate_transport "$value"; done
  for value in "${CONTENT_MODES[@]}"; do validate_content_mode "$value"; done
  for value in "${TRAFFIC_MODES[@]}"; do validate_traffic_mode "$value"; done
}

scale_connector_count() {
  case "$1" in
    single) printf '1' ;;
    scaled) printf '3' ;;
  esac
}

scale_edge_cpus() {
  case "$1" in
    single) printf '2.0' ;;
    scaled) printf '4.0' ;;
  esac
}

scale_caddy_cpus() {
  scale_edge_cpus "$1"
}

scale_connector_cpus() {
  case "$1" in
    single|scaled) printf '2.0' ;;
  esac
}

compose_override_path() {
  printf '%s/compose-%s.yaml' "$RESULTS_DIR" "$1"
}

render_scale_override() {
  local scale=$1
  local output=$2
  cat >"$output" <<EOFOVERRIDE
services:
  edge-gateway:
    cpus: "$(scale_edge_cpus "$scale")"

  caddy-s3:
    cpus: "$(scale_caddy_cpus "$scale")"

  private-connector:
    cpus: "$(scale_connector_cpus "$scale")"
EOFOVERRIDE
}

validate_compose_for_scale() {
  local scale=$1
  local override_file=$2
  local connectors
  connectors=$(scale_connector_count "$scale")
  echo "Validating Compose config for scale=$scale connectors=$connectors..."
  run_compose "$override_file" config >/dev/null
}

sql_quote() {
  printf "%s" "$1" | sed "s/'/'\\''/g"
}

s3_uri_for_key() {
  local key=$1
  printf 's3://%s/%s' "$BUCKET" "$key"
}

host_s3_url_for_key() {
  local key=$1
  printf '%s/%s/%s' "$S3_ENDPOINT_ON_HOST" "$BUCKET" "$key"
}

host_caddy_url_for_key() {
  local key=$1
  printf '%s/%s/%s' "$CADDY_BASE_URL" "$BUCKET" "$key"
}

sign_gateway_url() {
  local key=$1
  go run ./cmd/signurl \
    -method GET \
    -base-url "$BASE_URL" \
    -bucket "$BUCKET" \
    -key "$key" \
    -secret "$SECRET" \
    -expiration 10m
}

object_index() {
  local name=$1
  local i
  for i in "${!OBJECT_NAMES[@]}"; do
    if [ "${OBJECT_NAMES[$i]}" = "$name" ]; then
      printf '%s' "$i"
      return 0
    fi
  done
  return 1
}

object_key() {
  local index
  index=$(object_index "$1")
  printf '%s' "${OBJECT_KEYS[$index]}"
}

object_file() {
  local index
  index=$(object_index "$1")
  printf '%s' "${OBJECT_FILES[$index]}"
}

should_skip_object() {
  local name=$1
  [ "$name" = "big" ] && [ "$SKIP_BIG" = "1" ]
}

object_names_for_content_mode() {
  local mode=$1
  local name
  case "$mode" in
    small|medium|big)
      if ! should_skip_object "$mode"; then
        printf '%s\n' "$mode"
      fi
      ;;
    mixed)
      for name in "${OBJECT_NAMES[@]}"; do
        if ! should_skip_object "$name"; then
          printf '%s\n' "$name"
        fi
      done
      ;;
  esac
}

download_objects() {
  mkdir -p "$CACHE_DIR"
  local i file url
  for i in "${!OBJECT_NAMES[@]}"; do
    if should_skip_object "${OBJECT_NAMES[$i]}"; then
      continue
    fi
    file="$CACHE_DIR/${OBJECT_FILES[$i]}"
    url="${OBJECT_URLS[$i]}"
    if [ -s "$file" ]; then
      echo "Using cached ${OBJECT_NAMES[$i]} object: $file"
      continue
    fi
    echo "Downloading ${OBJECT_NAMES[$i]} object to $file..."
    curl --location --fail --show-error --output "$file" "$url"
  done
}

seed_objects() {
  local override_file=$1
  echo "Waiting for S3 endpoint $S3_ENDPOINT_IN_COMPOSE..."
  for i in $(seq 1 60); do
    if run_compose "$override_file" run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api list-buckets >/dev/null" >/dev/null 2>&1; then
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: VersityGW did not become ready at $S3_ENDPOINT_IN_COMPOSE" >&2
      exit 1
    fi
    sleep 1
  done

  echo "Creating bucket s3://$BUCKET if needed..."
  run_compose "$override_file" run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api head-bucket --bucket '$(sql_quote "$BUCKET")' >/dev/null 2>&1 || aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api create-bucket --bucket '$(sql_quote "$BUCKET")' >/dev/null"

  local i local_path key s3_uri content_type
  for i in "${!OBJECT_NAMES[@]}"; do
    if should_skip_object "${OBJECT_NAMES[$i]}"; then
      continue
    fi
    local_path="/perf-cache/${OBJECT_FILES[$i]}"
    key="${OBJECT_KEYS[$i]}"
    s3_uri=$(s3_uri_for_key "$key")
    case "${OBJECT_FILES[$i]}" in
      *.jpg) content_type=image/jpeg ;;
      *.webm) content_type=video/webm ;;
      *) content_type=application/octet-stream ;;
    esac
    echo "Seeding $s3_uri..."
    run_compose "$override_file" run --rm --no-deps -v "$CACHE_DIR:/perf-cache:ro" aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3 cp '$(sql_quote "$local_path")' '$(sql_quote "$s3_uri")' --content-type '$(sql_quote "$content_type")' >/dev/null"
  done
}

verify_anonymous_direct_read() {
  local key=$1
  local mechanism=$2
  local url
  url=$(host_s3_url_for_key "$key")
  if curl --silent --show-error --fail --output /dev/null "$url"; then
    return 0
  fi
  echo "public-read verification failed for $mechanism with anonymous direct curl: $url" >&2
  return 1
}

enable_bucket_acl_public_read() {
  local override_file=$1
  local sample_key=$2
  echo "Configuring public reads with bucket ACL public-read..."
  if ! run_compose "$override_file" run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api put-bucket-acl --acl public-read --bucket '$(sql_quote "$BUCKET")' >/dev/null"; then
    echo "bucket ACL public-read setup failed for s3://$BUCKET" >&2
    return 1
  fi
  verify_anonymous_direct_read "$sample_key" bucket-acl
}

enable_bucket_policy_public_read() {
  local override_file=$1
  local sample_key=$2
  local policy
  policy=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}' "$BUCKET")
  echo "Configuring public reads with bucket policy..."
  if ! run_compose "$override_file" run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api put-bucket-policy --bucket '$(sql_quote "$BUCKET")' --policy '$(sql_quote "$policy")' >/dev/null"; then
    echo "bucket policy public-read setup failed for s3://$BUCKET" >&2
    return 1
  fi
  verify_anonymous_direct_read "$sample_key" bucket-policy
}

configure_public_read() {
  local override_file=$1
  local sample_key=$2
  case "$PUBLIC_READ_MODE" in
    bucket-acl)
      if enable_bucket_acl_public_read "$override_file" "$sample_key"; then
        echo "Public-read mechanism: bucket-acl"
      else
        echo "error: AIR3_PERF_PUBLIC_READ=bucket-acl failed" >&2
        exit 1
      fi
      ;;
    bucket-policy)
      if enable_bucket_policy_public_read "$override_file" "$sample_key"; then
        echo "Public-read mechanism: bucket-policy"
      else
        echo "error: AIR3_PERF_PUBLIC_READ=bucket-policy failed" >&2
        exit 1
      fi
      ;;
    auto)
      if enable_bucket_acl_public_read "$override_file" "$sample_key"; then
        echo "Public-read mechanism: bucket-acl"
      else
        echo "bucket ACL public-read setup failed; trying bucket policy..." >&2
        if enable_bucket_policy_public_read "$override_file" "$sample_key"; then
          echo "Public-read mechanism: bucket-policy"
        else
          echo "error: automatic public-read setup failed with both bucket ACL and bucket policy" >&2
          exit 1
        fi
      fi
      ;;
  esac
}

curl_caddy_s3() {
  local key=$1
  curl --silent --show-error --http1.1 \
    --output /dev/null \
    --write-out '%{http_code},%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$(host_caddy_url_for_key "$key")"
}

curl_air3_gateway() {
  local key=$1
  local url
  url=$(sign_gateway_url "$key")
  curl --silent --show-error --http1.1 \
    --output /dev/null \
    --cacert "$CERT_DIR/dev-ca.crt" \
    --write-out '%{http_code},%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$url"
}

curl_direct_s3() {
  local key=$1
  curl --silent --show-error --fail \
    --output /dev/null \
    --write-out '%{http_code},%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$(host_s3_url_for_key "$key")"
}

curl_path() {
  local path=$1
  local key=$2
  case "$path" in
    caddy_s3) curl_caddy_s3 "$key" ;;
    air3_gateway) curl_air3_gateway "$key" ;;
    direct_s3) curl_direct_s3 "$key" ;;
    *)
      echo "error: unknown benchmark path '$path'" >&2
      exit 1
      ;;
  esac
}

wait_for_direct_s3() {
  local key=$1
  echo "Waiting for anonymous direct S3 endpoint at $S3_ENDPOINT_ON_HOST..."
  for i in $(seq 1 60); do
    if curl_direct_s3 "$key" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: anonymous direct S3 endpoint did not become reachable at $S3_ENDPOINT_ON_HOST" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_caddy_s3() {
  local key=$1
  echo "Waiting for anonymous Caddy S3 endpoint at $CADDY_BASE_URL..."
  for i in $(seq 1 60); do
    if curl --silent --show-error --http1.1 --fail --output /dev/null "$(host_caddy_url_for_key "$key")" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: perf Caddy S3 endpoint did not become reachable at $CADDY_BASE_URL" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_gateway() {
  local key=$1
  echo "Waiting for Air3 gateway at $BASE_URL..."
  for i in $(seq 1 60); do
    if curl_air3_gateway "$key" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: Air3 gateway did not become ready at $BASE_URL" >&2
      exit 1
    fi
    sleep 1
  done
}

is_2xx_status() {
  [[ "$1" =~ ^2[0-9][0-9]$ ]]
}

append_request_row() {
  local row_file=$1
  local scale=$2
  local transport=$3
  local content_mode=$4
  local traffic_mode=$5
  local path=$6
  local object=$7
  local key=$8
  local request_index=$9
  local concurrency=${10}
  local metrics=${11}

  local http_status bytes ttfb_seconds total_seconds speed_download ttfb_ms
  IFS=',' read -r http_status bytes ttfb_seconds total_seconds speed_download <<<"$metrics"
  : "${http_status:=000}"
  : "${bytes:=0}"
  : "${ttfb_seconds:=0}"
  : "${total_seconds:=0}"
  : "${speed_download:=0}"
  ttfb_ms=$(awk -v seconds="$ttfb_seconds" 'BEGIN { printf "%.3f", seconds * 1000 }')

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$object" "$key" \
    "$request_index" "$concurrency" "$http_status" "$bytes" "$ttfb_ms" "$total_seconds" "$speed_download" >"$row_file"

  is_2xx_status "$http_status"
}

perform_request() {
  local scale=$1
  local transport=$2
  local content_mode=$3
  local traffic_mode=$4
  local path=$5
  local object=$6
  local request_index=$7
  local concurrency=$8
  local row_file=$9
  local stderr_file=${10}
  local key metrics curl_status

  key=$(object_key "$object")
  set +e
  metrics=$(curl_path "$path" "$key" 2>"$stderr_file")
  curl_status=$?
  set -e
  if [ "$curl_status" -ne 0 ]; then
    metrics="000,0,0,0,0"
  fi
  append_request_row "$row_file" "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$object" "$key" "$request_index" "$concurrency" "$metrics"
}

write_aggregate_row() {
  local rows_file=$1
  local scale=$2
  local transport=$3
  local content_mode=$4
  local traffic_mode=$5
  local path=$6
  local concurrency=$7
  local wall_seconds=$8
  local stats

  stats=$(awk -F, -v wall="$wall_seconds" '
    $10 ~ /^2[0-9][0-9]$/ {
      count++
      total_bytes += $11
      total_ttfb += $12
    }
    END {
      avg_ttfb = count > 0 ? total_ttfb / count : 0
      rps = wall > 0 ? count / wall : 0
      throughput = wall > 0 ? total_bytes / 1048576 / wall : 0
      printf "%d,%.3f,%.6f,%.6f,%.0f", count, avg_ttfb, rps, throughput, total_bytes
    }
  ' "$rows_file")

  local requests avg_ttfb rps throughput total_bytes p50_ttfb
  IFS=',' read -r requests avg_ttfb rps throughput total_bytes <<<"$stats"
  if [ "$requests" -gt 0 ]; then
    p50_ttfb=$(awk -F, '$10 ~ /^2[0-9][0-9]$/ { print $12 }' "$rows_file" | sort -n | awk '{ values[NR] = $1 } END { if (NR == 0) printf "0.000"; else printf "%.3f", values[int((NR + 1) / 2)] }')
  else
    p50_ttfb=0.000
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$requests" "$concurrency" \
    "$avg_ttfb" "$p50_ttfb" "$rps" "$throughput" "$total_bytes" "$wall_seconds" >>"$AGGREGATE_CSV"
}

copy_rows_to_raw() {
  local rows_dir=$1
  local row
  for row in "$rows_dir"/*.csv; do
    [ -e "$row" ] || continue
    cat "$row" >>"$RAW_CSV"
  done
}

build_request_objects() {
  local content_mode=$1
  local traffic_mode=$2
  local objects_file=$3
  local -a names=()
  local name repeat index

  while IFS= read -r name; do
    [ -n "$name" ] && names+=("$name")
  done < <(object_names_for_content_mode "$content_mode")

  : >"$objects_file"
  if [ "${#names[@]}" -eq 0 ]; then
    return 0
  fi

  case "$traffic_mode" in
    sequential)
      if [ "$content_mode" = "mixed" ]; then
        for repeat in $(seq 1 "$SEQ_REPEATS"); do
          for name in "${names[@]}"; do
            printf '%s\n' "$name" >>"$objects_file"
          done
        done
      else
        for repeat in $(seq 1 "$SEQ_REPEATS"); do
          printf '%s\n' "${names[0]}" >>"$objects_file"
        done
      fi
      ;;
    concurrent)
      for index in $(seq 1 "$CONCURRENT_REQUESTS"); do
        printf '%s\n' "${names[$(((index - 1) % ${#names[@]}))]}" >>"$objects_file"
      done
      ;;
  esac
}

run_request_group() {
  local scale=$1
  local transport=$2
  local content_mode=$3
  local traffic_mode=$4
  local path=$5
  local group_concurrency=$6
  local objects_file rows_dir combined_rows group_log_dir start_ns end_ns wall_seconds request_count request_index object running group_failed row_file stderr_file

  objects_file=$(mktemp)
  build_request_objects "$content_mode" "$traffic_mode" "$objects_file"
  request_count=$(wc -l <"$objects_file" | tr -d ' ')
  if [ "$request_count" -eq 0 ]; then
    echo "Skipping scale=$scale transport=$transport path=$path content=$content_mode traffic=$traffic_mode: no objects selected"
    rm -f "$objects_file"
    return 0
  fi

  rows_dir=$(mktemp -d "$RESULTS_DIR/.rows.XXXXXX")
  group_log_dir="$LOG_DIR/$scale-$transport-$content_mode-$traffic_mode-$path"
  mkdir -p "$group_log_dir"

  echo "Running scale=$scale transport=$transport path=$path content=$content_mode traffic=$traffic_mode requests=$request_count concurrency=$group_concurrency..."
  start_ns=$(date +%s%N)
  group_failed=0
  request_index=0
  running=0

  if [ "$traffic_mode" = "sequential" ]; then
    while IFS= read -r object; do
      request_index=$((request_index + 1))
      row_file=$(printf '%s/%06d.csv' "$rows_dir" "$request_index")
      stderr_file=$(printf '%s/%06d.stderr.log' "$group_log_dir" "$request_index")
      if ! perform_request "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$object" "$request_index" "$group_concurrency" "$row_file" "$stderr_file"; then
        group_failed=1
      fi
    done <"$objects_file"
  else
    while IFS= read -r object; do
      request_index=$((request_index + 1))
      row_file=$(printf '%s/%06d.csv' "$rows_dir" "$request_index")
      stderr_file=$(printf '%s/%06d.stderr.log' "$group_log_dir" "$request_index")
      perform_request "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$object" "$request_index" "$group_concurrency" "$row_file" "$stderr_file" &
      running=$((running + 1))
      if [ "$running" -ge "$group_concurrency" ]; then
        if ! wait -n; then
          group_failed=1
        fi
        running=$((running - 1))
      fi
    done <"$objects_file"
    while [ "$running" -gt 0 ]; do
      if ! wait -n; then
        group_failed=1
      fi
      running=$((running - 1))
    done
  fi

  end_ns=$(date +%s%N)
  wall_seconds=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.6f", (end - start) / 1000000000 }')
  combined_rows="$rows_dir/.group.csv"
  cat "$rows_dir"/*.csv >"$combined_rows"
  copy_rows_to_raw "$rows_dir"
  write_aggregate_row "$combined_rows" "$scale" "$transport" "$content_mode" "$traffic_mode" "$path" "$group_concurrency" "$wall_seconds"

  rm -rf "$rows_dir"
  rm -f "$objects_file"

  if [ "$group_failed" -ne 0 ]; then
    echo "error: one or more requests failed for scale=$scale transport=$transport path=$path content=$content_mode traffic=$traffic_mode; see $group_log_dir and $RAW_CSV" >&2
    return 1
  fi
}

run_caddy_baseline_matrix() {
  local scale=$1
  local content_mode traffic_mode concurrency
  for content_mode in "${CONTENT_MODES[@]}"; do
    for traffic_mode in "${TRAFFIC_MODES[@]}"; do
      if [ "$traffic_mode" = "sequential" ]; then
        concurrency=1
      else
        concurrency=$CONCURRENCY
      fi
      run_request_group "$scale" caddy "$content_mode" "$traffic_mode" caddy_s3 "$concurrency"
    done
  done
}

run_air3_transport_matrix() {
  local scale=$1
  local transport=$2
  local content_mode traffic_mode concurrency
  for content_mode in "${CONTENT_MODES[@]}"; do
    for traffic_mode in "${TRAFFIC_MODES[@]}"; do
      if [ "$traffic_mode" = "sequential" ]; then
        concurrency=1
      else
        concurrency=$CONCURRENCY
      fi
      run_request_group "$scale" "$transport" "$content_mode" "$traffic_mode" air3_gateway "$concurrency"
    done
  done
}

write_summary_md() {
  {
    echo "# README benchmark summary"
    echo
    echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "| Scale | Transport | Content | Traffic | Path | Requests | Concurrency | Avg TTFB (ms) | P50 TTFB (ms) | RPS | Throughput (MiB/s) | Total seconds |"
    echo "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|"
    awk -F, '
      NR == 1 { next }
      {
        printf "| %s | %s | %s | %s | %s | %s | %s | %.3f | %.3f | %.3f | %.3f | %.3f |\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $13
      }
    ' "$AGGREGATE_CSV"
  } >"$SUMMARY_MD"
}

start_stack_for_scale() {
  local scale=$1
  local override_file=$2
  local connectors=$3
  local initial_transport=$4
  export AIR3_INGEST_TRANSPORT="$initial_transport"
  echo "Starting Compose services for scale=$scale connectors=$connectors initial_transport=$initial_transport..."
  run_compose "$override_file" up -d --build --scale "private-connector=$connectors" nats edge-gateway versitygw caddy-s3 private-connector >/dev/null
}

recreate_air3_for_transport() {
  local override_file=$1
  local connectors=$2
  local transport=$3
  export AIR3_INGEST_TRANSPORT="$transport"
  echo "Recreating Air3 services for transport=$transport connectors=$connectors..."
  run_compose "$override_file" up -d --build --force-recreate --scale "private-connector=$connectors" edge-gateway private-connector >/dev/null
}

run_scale() {
  local scale=$1
  local override_file=$2
  local connectors sample_key initial_transport transport
  connectors=$(scale_connector_count "$scale")
  sample_key=${OBJECT_KEYS[0]}
  initial_transport=${TRANSPORTS[0]}

  start_stack_for_scale "$scale" "$override_file" "$connectors" "$initial_transport"
  seed_objects "$override_file"
  configure_public_read "$override_file" "$sample_key"
  wait_for_direct_s3 "$sample_key"
  wait_for_caddy_s3 "$sample_key"
  wait_for_gateway "$sample_key"

  run_caddy_baseline_matrix "$scale"

  for transport in "${TRANSPORTS[@]}"; do
    recreate_air3_for_transport "$override_file" "$connectors" "$transport"
    wait_for_gateway "$sample_key"
    run_air3_transport_matrix "$scale" "$transport"
  done
}

validate_inputs
require_cmd docker
require_cmd awk
mkdir -p "$RESULTS_DIR"

for scale in "${SCALES[@]}"; do
  override_file=$(compose_override_path "$scale")
  render_scale_override "$scale" "$override_file"
  validate_compose_for_scale "$scale" "$override_file"
done

if [ "$VALIDATE_COMPOSE" -eq 1 ]; then
  echo "Compose validation succeeded for scales: $SCALES_TEXT"
  echo "Generated overrides in $RESULTS_DIR"
  exit 0
fi

require_cmd go
require_cmd curl

if [ ! -f "$CERT_DIR/dev-ca.crt" ]; then
  echo "Generating development certificates..."
  "$ROOT_DIR/deploy/scripts/certs.sh"
fi

mkdir -p "$LOG_DIR"
echo "scale,transport,content_mode,traffic_mode,path,object,object_key,request_index,concurrency,http_status,bytes,ttfb_ms,total_seconds,speed_download_bps" >"$RAW_CSV"
echo "scale,transport,content_mode,traffic_mode,path,requests,concurrency,avg_ttfb_ms,p50_ttfb_ms,rps,throughput_mib_s,total_bytes,total_seconds" >"$AGGREGATE_CSV"

download_objects

for scale in "${SCALES[@]}"; do
  run_scale "$scale" "$(compose_override_path "$scale")"
done

write_summary_md

echo
echo "README benchmark results: $RESULTS_DIR"
echo "Raw CSV:       $RAW_CSV"
echo "Aggregate CSV: $AGGREGATE_CSV"
echo "Summary:       $SUMMARY_MD"
