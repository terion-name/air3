#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml"}
PERF_COMPOSE_FILE=${AIR3_PERF_COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.perf.yaml"}
CERT_DIR=${AIR3_CERT_DIR:-"$ROOT_DIR/deploy/certs/generated"}
CACHE_DIR=${AIR3_PERF_CACHE_DIR:-"$ROOT_DIR/.air3-perf-cache"}
RESULTS_DIR=${AIR3_PERF_RESULTS_DIR:-"$ROOT_DIR/.air3-perf-results"}
ITERATIONS=${AIR3_PERF_ITERATIONS:-5}
CONNECTORS=${AIR3_PERF_CONNECTORS:-1}
MULTI_CONNECTORS=${AIR3_PERF_MULTI_CONNECTORS:-3}
PARALLELISM=${AIR3_PERF_PARALLELISM:-0}
SKIP_BIG=${AIR3_PERF_SKIP_BIG:-0}
S3_HOST_PORT=${AIR3_PERF_S3_PORT:-10000}
CADDY_HOST_PORT=${AIR3_PERF_CADDY_PORT:-10080}
CADDY_BASE_URL=${AIR3_PERF_CADDY_BASE_URL:-http://localhost:$CADDY_HOST_PORT}
PUBLIC_READ_MODE=${AIR3_PERF_PUBLIC_READ_MODE:-auto}
INGEST_TRANSPORT=${AIR3_INGEST_TRANSPORT:-http}
INGEST_TCP_LISTEN_ADDR=${AIR3_EDGE_INGEST_TCP_ADDR:-:9444}
INGEST_TCP_DIAL_ADDR=${AIR3_INGEST_TCP_ADDR:-edge-gateway:9444}
BUCKET=${AIR3_PERF_BUCKET:-demo}
BASE_URL=${AIR3_PERF_BASE_URL:-https://localhost:8443}
SECRET=${AIR3_SIGNING_SECRET:-dev-signing-secret-change-me}
S3_ACCESS_KEY_ID=${AIR3_PERF_S3_ACCESS_KEY_ID:-testuser}
S3_SECRET_ACCESS_KEY=${AIR3_PERF_S3_SECRET_ACCESS_KEY:-secret}
S3_REGION=${AIR3_PERF_S3_REGION:-us-east-1}
S3_ENDPOINT_IN_COMPOSE=${AIR3_PERF_S3_ENDPOINT_IN_COMPOSE:-http://versitygw:10000}
S3_ENDPOINT_ON_HOST=${AIR3_PERF_S3_ENDPOINT_ON_HOST:-http://localhost:$S3_HOST_PORT}
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_CSV=${AIR3_PERF_RESULTS_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP-$INGEST_TRANSPORT.csv"}
SUMMARY_CSV=${AIR3_PERF_SUMMARY_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP-$INGEST_TRANSPORT-summary.csv"}
PARALLEL_CSV=${AIR3_PERF_PARALLEL_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP-$INGEST_TRANSPORT-parallel.csv"}

SMALL_URL='https://upload.wikimedia.org/wikipedia/commons/8/8d/%22Ontology%22%2C_2011-2012.jpg'
MEDIUM_URL='https://upload.wikimedia.org/wikipedia/commons/b/b1/GLI.TC-H_gallery.1.jpg'
BIG_URL='https://upload.wikimedia.org/wikipedia/commons/c/c0/Cosmic_Dust_Bin_-_Interstellar_%28Trance_Music_Video%29.webm'

OBJECT_NAMES=(small medium big)
OBJECT_FILES=(small.jpg medium.jpg big.webm)
OBJECT_KEYS=(perf/small.jpg perf/medium.jpg perf/big.webm)
OBJECT_URLS=("$SMALL_URL" "$MEDIUM_URL" "$BIG_URL")
PATH_NAMES=(direct_s3 caddy_s3 air3_gateway)

usage() {
  cat <<EOFUSAGE
Usage: $(basename "$0") [--help]

Runs the air3 Docker Compose performance benchmark. Configuration is via env vars:

  AIR3_PERF_ITERATIONS          measured requests per object/path (default: $ITERATIONS)
  AIR3_PERF_CONNECTORS          private connector replicas to run (default: $CONNECTORS)
  AIR3_PERF_MULTI_CONNECTORS    multi-connector count used by make perf-multi (default: $MULTI_CONNECTORS)
  AIR3_PERF_PARALLELISM         optional parallel Air3 gateway requests per object (default: $PARALLELISM, disabled when 0/1)
  AIR3_PERF_SKIP_BIG            set to 1 to skip the big webm object (default: $SKIP_BIG)
  AIR3_PERF_CACHE_DIR           downloaded Wikimedia cache (default: $CACHE_DIR)
  AIR3_PERF_RESULTS_DIR         CSV output directory (default: $RESULTS_DIR)
  AIR3_PERF_S3_PORT             host port for direct VersityGW access (default: $S3_HOST_PORT)
  AIR3_PERF_CADDY_PORT          host port for perf Caddy S3 proxy (default: $CADDY_HOST_PORT)
  AIR3_PERF_CADDY_BASE_URL      base URL for perf Caddy S3 proxy (default: $CADDY_BASE_URL)
  AIR3_PERF_PUBLIC_READ_MODE    public-read setup mode: auto|bucket-acl|bucket-policy (default: $PUBLIC_READ_MODE)
  AIR3_INGEST_TRANSPORT         connector→edge ingest transport: http|tcp (default: $INGEST_TRANSPORT; tcp is experimental)
  AIR3_EDGE_INGEST_TCP_ADDR      edge TCP ingest listener in tcp mode (Compose default: $INGEST_TCP_LISTEN_ADDR)
  AIR3_INGEST_TCP_ADDR           connector TCP ingest dial address in tcp mode (Compose default: $INGEST_TCP_DIAL_ADDR)

Benchmark paths:
  direct_s3       anonymous direct VersityGW S3 read
  caddy_s3        anonymous read through perf Caddy S3 proxy
  air3_gateway    signed read through the Air3 gateway

The per-request, parallel, and summary CSVs include an ingest_transport column so
HTTP and experimental TCP ingest runs can be compared safely. In TCP mode,
AIR3_INGEST_URL remains the HTTPS ticket/fallback ingest URL.

Outputs:
  per-request CSV: $RESULTS_CSV
  summary CSV:     $SUMMARY_CSV
EOFUSAGE
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi
if [ "$#" -gt 0 ]; then
  usage >&2
  exit 2
fi

run_compose() {
  # shellcheck disable=SC2086
  $COMPOSE -f "$COMPOSE_FILE" -f "$PERF_COMPOSE_FILE" "$@"
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

require_non_negative_int() {
  local name=$1
  local value=$2
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "error: $name must be a non-negative integer, got '$value'" >&2
    exit 1
  fi
}

validate_public_read_mode() {
  case "$PUBLIC_READ_MODE" in
    auto|bucket-acl|bucket-policy) ;;
    *)
      echo "error: AIR3_PERF_PUBLIC_READ_MODE must be one of auto, bucket-acl, bucket-policy; got '$PUBLIC_READ_MODE'" >&2
      exit 1
      ;;
  esac
}

validate_ingest_transport() {
  case "$INGEST_TRANSPORT" in
    http|tcp) ;;
    *)
      echo "error: AIR3_INGEST_TRANSPORT must be one of http, tcp; got '$INGEST_TRANSPORT'" >&2
      exit 1
      ;;
  esac
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

should_skip_object() {
  local name=$1
  [ "$name" = "big" ] && [ "$SKIP_BIG" = "1" ]
}

seed_objects() {
  echo "Waiting for S3 endpoint $S3_ENDPOINT_IN_COMPOSE..."
  for i in $(seq 1 60); do
    if run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api list-buckets >/dev/null" >/dev/null 2>&1; then
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: VersityGW did not become ready at $S3_ENDPOINT_IN_COMPOSE" >&2
      exit 1
    fi
    sleep 1
  done

  echo "Creating bucket s3://$BUCKET if needed..."
  run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api head-bucket --bucket '$(sql_quote "$BUCKET")' >/dev/null 2>&1 || aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api create-bucket --bucket '$(sql_quote "$BUCKET")' >/dev/null"

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
    run_compose run --rm --no-deps -v "$CACHE_DIR:/perf-cache:ro" aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3 cp '$(sql_quote "$local_path")' '$(sql_quote "$s3_uri")' --content-type '$(sql_quote "$content_type")' >/dev/null"
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
  local sample_key=$1
  echo "Configuring public reads with bucket ACL public-read..."
  if ! run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api put-bucket-acl --acl public-read --bucket '$(sql_quote "$BUCKET")' >/dev/null"; then
    echo "bucket ACL public-read setup failed for s3://$BUCKET" >&2
    return 1
  fi
  verify_anonymous_direct_read "$sample_key" bucket-acl
}

enable_bucket_policy_public_read() {
  local sample_key=$1
  local policy
  policy=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}' "$BUCKET")
  echo "Configuring public reads with bucket policy..."
  if ! run_compose run --rm --no-deps aws-cli "aws --endpoint-url '$(sql_quote "$S3_ENDPOINT_IN_COMPOSE")' s3api put-bucket-policy --bucket '$(sql_quote "$BUCKET")' --policy '$(sql_quote "$policy")' >/dev/null"; then
    echo "bucket policy public-read setup failed for s3://$BUCKET" >&2
    return 1
  fi
  verify_anonymous_direct_read "$sample_key" bucket-policy
}

configure_public_read() {
  local sample_key=$1
  case "$PUBLIC_READ_MODE" in
    bucket-acl)
      if enable_bucket_acl_public_read "$sample_key"; then
        echo "Public-read mechanism: bucket-acl"
      else
        echo "error: AIR3_PERF_PUBLIC_READ_MODE=bucket-acl failed" >&2
        exit 1
      fi
      ;;
    bucket-policy)
      if enable_bucket_policy_public_read "$sample_key"; then
        echo "Public-read mechanism: bucket-policy"
      else
        echo "error: AIR3_PERF_PUBLIC_READ_MODE=bucket-policy failed" >&2
        exit 1
      fi
      ;;
    auto)
      if enable_bucket_acl_public_read "$sample_key"; then
        echo "Public-read mechanism: bucket-acl"
      else
        echo "bucket ACL public-read setup failed; trying bucket policy..." >&2
        if enable_bucket_policy_public_read "$sample_key"; then
          echo "Public-read mechanism: bucket-policy"
        else
          echo "error: automatic public-read setup failed with both bucket ACL and bucket policy" >&2
          exit 1
        fi
      fi
      ;;
  esac
}

curl_direct_s3() {
  local key=$1
  curl --silent --show-error --fail \
    --output /dev/null \
    --write-out '%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$(host_s3_url_for_key "$key")"
}

curl_caddy_s3() {
  local key=$1
  curl --silent --show-error --fail \
    --http1.1 \
    --output /dev/null \
    --write-out '%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$(host_caddy_url_for_key "$key")"
}

curl_air3_gateway() {
  local key=$1
  local url
  url=$(sign_gateway_url "$key")
  curl --silent --show-error --fail \
    --http1.1 \
    --output /dev/null \
    --cacert "$CERT_DIR/dev-ca.crt" \
    --write-out '%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$url"
}

curl_path() {
  local path=$1
  local key=$2
  case "$path" in
    direct_s3) curl_direct_s3 "$key" ;;
    caddy_s3) curl_caddy_s3 "$key" ;;
    air3_gateway) curl_air3_gateway "$key" ;;
    *)
      echo "error: unknown benchmark path '$path'" >&2
      exit 1
      ;;
  esac
}

throughput_mib_from_speed() {
  local speed=$1
  awk -v speed="$speed" 'BEGIN { printf "%.6f", speed / 1048576 }'
}

append_measurement() {
  local connector_count=$1
  local object_name=$2
  local path=$3
  local iteration=$4
  local metrics=$5
  local bytes ttfb total speed throughput
  IFS=',' read -r bytes ttfb total speed <<<"$metrics"
  throughput=$(throughput_mib_from_speed "$speed")
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$connector_count" "$INGEST_TRANSPORT" "$object_name" "$path" "$iteration" "$bytes" "$ttfb" "$total" "$speed" "$throughput" >>"$RESULTS_CSV"
}

warmup_object() {
  local object_name=$1
  local key=$2
  local path
  for path in "${PATH_NAMES[@]}"; do
    echo "Warming $object_name path=$path..."
    curl_path "$path" "$key" >/dev/null
  done
}

measure_object() {
  local connector_count=$1
  local object_name=$2
  local key=$3
  local iteration path metrics
  for iteration in $(seq 1 "$ITERATIONS"); do
    for path in "${PATH_NAMES[@]}"; do
      echo "Measuring connector_count=$connector_count transport=$INGEST_TRANSPORT object=$object_name path=$path iteration=$iteration/$ITERATIONS"
      metrics=$(curl_path "$path" "$key")
      append_measurement "$connector_count" "$object_name" "$path" "$iteration" "$metrics"
    done
  done
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
      echo "hint: change AIR3_PERF_S3_PORT if the port is busy, or AIR3_PERF_S3_ENDPOINT_ON_HOST if Docker publishes ports somewhere other than localhost" >&2
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
      echo "hint: change AIR3_PERF_CADDY_PORT if the port is busy, or AIR3_PERF_CADDY_BASE_URL if Docker publishes ports somewhere other than localhost" >&2
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

run_parallel_phase() {
  local connector_count=$1
  if [ "$PARALLELISM" -le 1 ]; then
    return 0
  fi

  echo "connector_count,ingest_transport,object,path,parallelism,bytes,total_time,speed_bytes_per_sec,throughput_mib_per_sec" >"$PARALLEL_CSV"
  local i object_name key urls_file total_bytes start_ns end_ns duration speed throughput
  for i in "${!OBJECT_NAMES[@]}"; do
    object_name="${OBJECT_NAMES[$i]}"
    if should_skip_object "$object_name"; then
      continue
    fi
    key="${OBJECT_KEYS[$i]}"
    urls_file=$(mktemp)
    trap 'rm -f "$urls_file"' RETURN
    for _ in $(seq 1 "$PARALLELISM"); do
      sign_gateway_url "$key" >>"$urls_file"
    done

    echo "Running parallel Air3 gateway phase transport=$INGEST_TRANSPORT object=$object_name path=air3_gateway parallelism=$PARALLELISM..."
    start_ns=$(date +%s%N)
    xargs -n 1 -P "$PARALLELISM" curl --silent --show-error --fail --http1.1 --output /dev/null --cacert "$CERT_DIR/dev-ca.crt" <"$urls_file"
    end_ns=$(date +%s%N)
    duration=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.6f", (end - start) / 1000000000 }')
    total_bytes=$(awk -v bytes="$(stat_size "$CACHE_DIR/${OBJECT_FILES[$i]}")" -v n="$PARALLELISM" 'BEGIN { printf "%.0f", bytes * n }')
    speed=$(awk -v bytes="$total_bytes" -v total="$duration" 'BEGIN { if (total > 0) printf "%.6f", bytes / total; else print "0" }')
    throughput=$(throughput_mib_from_speed "$speed")
    printf '%s,%s,%s,air3_gateway,%s,%s,%s,%s,%s\n' "$connector_count" "$INGEST_TRANSPORT" "$object_name" "$PARALLELISM" "$total_bytes" "$duration" "$speed" "$throughput" >>"$PARALLEL_CSV"
    rm -f "$urls_file"
    trap - RETURN
  done
  echo "Parallel results: $PARALLEL_CSV"
}

stat_size() {
  local file=$1
  if stat -c %s "$file" >/dev/null 2>&1; then
    stat -c %s "$file"
  else
    stat -f %z "$file"
  fi
}

write_summary() {
  {
    echo "connector_count,ingest_transport,object,path,samples,avg_bytes,avg_ttfb_seconds,avg_total_time_seconds,avg_speed_bytes_per_sec,avg_throughput_mib_per_sec,air3_over_direct_total_ratio,air3_over_caddy_total_ratio"
    awk -F, '
      NR == 1 { next }
      {
        key = $1 "," $2 "," $3 "," $4
        count[key]++
        bytes[key] += $6
        ttfb[key] += $7
        total[key] += $8
        speed[key] += $9
        throughput[key] += $10
        keys[key] = 1
      }
      function avg(sum, n) { return n > 0 ? sum / n : 0 }
      END {
        for (key in keys) {
          split(key, fields, ",")
          connector = fields[1]
          transport = fields[2]
          object = fields[3]
          path = fields[4]
          avg_bytes = avg(bytes[key], count[key])
          avg_ttfb = avg(ttfb[key], count[key])
          avg_total = avg(total[key], count[key])
          avg_speed = avg(speed[key], count[key])
          avg_throughput = avg(throughput[key], count[key])
          direct_ratio = ""
          caddy_ratio = ""
          if (path == "air3_gateway") {
            direct_key = connector "," transport "," object ",direct_s3"
            caddy_key = connector "," transport "," object ",caddy_s3"
            direct_total = avg(total[direct_key], count[direct_key])
            caddy_total = avg(total[caddy_key], count[caddy_key])
            if (direct_total > 0) {
              direct_ratio = sprintf("%.6f", avg_total / direct_total)
            }
            if (caddy_total > 0) {
              caddy_ratio = sprintf("%.6f", avg_total / caddy_total)
            }
          }
          printf "%s,%s,%s,%s,%d,%.0f,%.6f,%.6f,%.6f,%.6f,%s,%s\n", connector, transport, object, path, count[key], avg_bytes, avg_ttfb, avg_total, avg_speed, avg_throughput, direct_ratio, caddy_ratio
        }
      }
    ' "$RESULTS_CSV" | sort -t, -k1,1n -k2,2 -k3,3 -k4,4
  } >"$SUMMARY_CSV"
}

print_summary_table() {
  echo
  echo "Summary (averages, ingest_transport=$INGEST_TRANSPORT):"
  awk -F, '
    BEGIN { printf "%10s  %-9s  %-7s  %-13s  %7s  %12s  %12s  %12s  %12s\n", "connectors", "transport", "object", "path", "samples", "total_s", "MiB/s", "air3/direct", "air3/caddy" }
    NR == 1 { next }
    {
      direct_ratio = $11 == "" ? "-" : $11
      caddy_ratio = $12 == "" ? "-" : $12
      printf "%10s  %-9s  %-7s  %-13s  %7d  %12.6f  %12.3f  %12s  %12s\n", $1, $2, $3, $4, $5, $8, $10, direct_ratio, caddy_ratio
    }
  ' "$SUMMARY_CSV"
}

require_cmd docker
require_cmd go
require_cmd curl
require_cmd awk
require_cmd xargs
require_positive_int AIR3_PERF_ITERATIONS "$ITERATIONS"
require_positive_int AIR3_PERF_CONNECTORS "$CONNECTORS"
require_non_negative_int AIR3_PERF_PARALLELISM "$PARALLELISM"
validate_public_read_mode
validate_ingest_transport

if [ ! -f "$CERT_DIR/dev-ca.crt" ]; then
  echo "Generating development certificates..."
  "$ROOT_DIR/deploy/scripts/certs.sh"
fi

mkdir -p "$RESULTS_DIR"
echo "connector_count,ingest_transport,object,path,iteration,bytes,ttfb_seconds,total_time_seconds,speed_bytes_per_sec,throughput_mib_per_sec" >"$RESULTS_CSV"

download_objects

echo "Starting Compose services with $CONNECTORS private connector(s) using ingest transport $INGEST_TRANSPORT..."
run_compose up -d --build --scale "private-connector=$CONNECTORS" nats edge-gateway versitygw caddy-s3 private-connector >/dev/null
seed_objects
configure_public_read "${OBJECT_KEYS[0]}"
wait_for_direct_s3 "${OBJECT_KEYS[0]}"
wait_for_caddy_s3 "${OBJECT_KEYS[0]}"
wait_for_gateway "${OBJECT_KEYS[0]}"

for i in "${!OBJECT_NAMES[@]}"; do
  if should_skip_object "${OBJECT_NAMES[$i]}"; then
    continue
  fi
  warmup_object "${OBJECT_NAMES[$i]}" "${OBJECT_KEYS[$i]}"
  measure_object "$CONNECTORS" "${OBJECT_NAMES[$i]}" "${OBJECT_KEYS[$i]}"
done

run_parallel_phase "$CONNECTORS"
write_summary
print_summary_table

echo
echo "Per-request results: $RESULTS_CSV"
echo "Summary results:     $SUMMARY_CSV"
