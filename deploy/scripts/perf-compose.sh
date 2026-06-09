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
BUCKET=${AIR3_PERF_BUCKET:-demo}
BASE_URL=${AIR3_PERF_BASE_URL:-https://localhost:8443}
SECRET=${AIR3_SIGNING_SECRET:-dev-signing-secret-change-me}
S3_ACCESS_KEY_ID=${AIR3_PERF_S3_ACCESS_KEY_ID:-testuser}
S3_SECRET_ACCESS_KEY=${AIR3_PERF_S3_SECRET_ACCESS_KEY:-secret}
S3_REGION=${AIR3_PERF_S3_REGION:-us-east-1}
S3_ENDPOINT_IN_COMPOSE=${AIR3_PERF_S3_ENDPOINT_IN_COMPOSE:-http://versitygw:10000}
S3_ENDPOINT_ON_HOST=${AIR3_PERF_S3_ENDPOINT_ON_HOST:-http://localhost:$S3_HOST_PORT}
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_CSV=${AIR3_PERF_RESULTS_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP.csv"}
SUMMARY_CSV=${AIR3_PERF_SUMMARY_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP-summary.csv"}
PARALLEL_CSV=${AIR3_PERF_PARALLEL_CSV:-"$RESULTS_DIR/perf-$TIMESTAMP-parallel.csv"}

SMALL_URL='https://upload.wikimedia.org/wikipedia/commons/8/8d/%22Ontology%22%2C_2011-2012.jpg'
MEDIUM_URL='https://upload.wikimedia.org/wikipedia/commons/b/b1/GLI.TC-H_gallery.1.jpg'
BIG_URL='https://upload.wikimedia.org/wikipedia/commons/c/c0/Cosmic_Dust_Bin_-_Interstellar_%28Trance_Music_Video%29.webm'

OBJECT_NAMES=(small medium big)
OBJECT_FILES=(small.jpg medium.jpg big.webm)
OBJECT_KEYS=(perf/small.jpg perf/medium.jpg perf/big.webm)
OBJECT_URLS=("$SMALL_URL" "$MEDIUM_URL" "$BIG_URL")

usage() {
  cat <<EOFUSAGE
Usage: $(basename "$0") [--help]

Runs the air3 Docker Compose performance benchmark. Configuration is via env vars:

  AIR3_PERF_ITERATIONS          measured requests per object/path (default: $ITERATIONS)
  AIR3_PERF_CONNECTORS          private connector replicas to run (default: $CONNECTORS)
  AIR3_PERF_MULTI_CONNECTORS    multi-connector count used by make perf-multi (default: $MULTI_CONNECTORS)
  AIR3_PERF_PARALLELISM         optional parallel gateway requests per object (default: $PARALLELISM, disabled when 0/1)
  AIR3_PERF_SKIP_BIG            set to 1 to skip the big webm object (default: $SKIP_BIG)
  AIR3_PERF_CACHE_DIR           downloaded Wikimedia cache (default: $CACHE_DIR)
  AIR3_PERF_RESULTS_DIR         CSV output directory (default: $RESULTS_DIR)
  AIR3_PERF_S3_PORT             host port for direct VersityGW access (default: $S3_HOST_PORT)

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

curl_direct() {
  local key=$1
  curl --silent --show-error --fail \
    --output /dev/null \
    --aws-sigv4 "aws:amz:$S3_REGION:s3" \
    --user "$S3_ACCESS_KEY_ID:$S3_SECRET_ACCESS_KEY" \
    --write-out '%{size_download},%{time_starttransfer},%{time_total},%{speed_download}' \
    "$(host_s3_url_for_key "$key")"
}

curl_gateway() {
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
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$connector_count" "$object_name" "$path" "$iteration" "$bytes" "$ttfb" "$total" "$speed" "$throughput" >>"$RESULTS_CSV"
}

warmup_object() {
  local object_name=$1
  local key=$2
  echo "Warming $object_name direct S3..."
  curl_direct "$key" >/dev/null
  echo "Warming $object_name gateway-through..."
  curl_gateway "$key" >/dev/null
}

measure_object() {
  local connector_count=$1
  local object_name=$2
  local key=$3
  local iteration metrics
  for iteration in $(seq 1 "$ITERATIONS"); do
    echo "Measuring connector_count=$connector_count object=$object_name path=direct iteration=$iteration/$ITERATIONS"
    metrics=$(curl_direct "$key")
    append_measurement "$connector_count" "$object_name" direct "$iteration" "$metrics"

    echo "Measuring connector_count=$connector_count object=$object_name path=gateway iteration=$iteration/$ITERATIONS"
    metrics=$(curl_gateway "$key")
    append_measurement "$connector_count" "$object_name" gateway "$iteration" "$metrics"
  done
}

wait_for_direct_s3() {
  local key=$1
  echo "Waiting for direct S3 endpoint at $S3_ENDPOINT_ON_HOST..."
  for i in $(seq 1 60); do
    if curl_direct "$key" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: direct S3 endpoint did not become reachable at $S3_ENDPOINT_ON_HOST" >&2
      echo "hint: change AIR3_PERF_S3_PORT if the port is busy, or AIR3_PERF_S3_ENDPOINT_ON_HOST if Docker publishes ports somewhere other than localhost" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_gateway() {
  local key=$1
  echo "Waiting for gateway at $BASE_URL..."
  for i in $(seq 1 60); do
    if curl_gateway "$key" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "error: edge gateway did not become ready at $BASE_URL" >&2
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

  echo "connector_count,object,path,parallelism,bytes,total_time,speed_bytes_per_sec,throughput_mib_per_sec" >"$PARALLEL_CSV"
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

    echo "Running parallel gateway phase object=$object_name parallelism=$PARALLELISM..."
    start_ns=$(date +%s%N)
    xargs -n 1 -P "$PARALLELISM" curl --silent --show-error --fail --http1.1 --output /dev/null --cacert "$CERT_DIR/dev-ca.crt" <"$urls_file"
    end_ns=$(date +%s%N)
    duration=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.6f", (end - start) / 1000000000 }')
    total_bytes=$(awk -v bytes="$(stat_size "$CACHE_DIR/${OBJECT_FILES[$i]}")" -v n="$PARALLELISM" 'BEGIN { printf "%.0f", bytes * n }')
    speed=$(awk -v bytes="$total_bytes" -v total="$duration" 'BEGIN { if (total > 0) printf "%.6f", bytes / total; else print "0" }')
    throughput=$(throughput_mib_from_speed "$speed")
    printf '%s,%s,gateway,%s,%s,%s,%s,%s\n' "$connector_count" "$object_name" "$PARALLELISM" "$total_bytes" "$duration" "$speed" "$throughput" >>"$PARALLEL_CSV"
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
  awk -F, '
    NR == 1 { next }
    {
      key = $1 "," $2 "," $3
      count[key]++
      bytes[key] += $5
      ttfb[key] += $6
      total[key] += $7
      speed[key] += $8
      throughput[key] += $9
      pairs[$1 "," $2] = 1
    }
    function avg(sum, n) { return n > 0 ? sum / n : 0 }
    BEGIN {
      print "connector_count,object,direct_avg_bytes,gateway_avg_bytes,direct_avg_ttfb,gateway_avg_ttfb,ttfb_penalty_pct,direct_avg_total_time,gateway_avg_total_time,total_time_penalty_pct,direct_avg_speed_bytes_per_sec,gateway_avg_speed_bytes_per_sec,speed_penalty_pct,direct_avg_throughput_mib_per_sec,gateway_avg_throughput_mib_per_sec,throughput_penalty_pct"
    }
    END {
      for (pair in pairs) {
        direct = pair ",direct"
        gateway = pair ",gateway"
        split(pair, fields, ",")
        direct_bytes = avg(bytes[direct], count[direct])
        gateway_bytes = avg(bytes[gateway], count[gateway])
        direct_ttfb = avg(ttfb[direct], count[direct])
        gateway_ttfb = avg(ttfb[gateway], count[gateway])
        direct_total = avg(total[direct], count[direct])
        gateway_total = avg(total[gateway], count[gateway])
        direct_speed = avg(speed[direct], count[direct])
        gateway_speed = avg(speed[gateway], count[gateway])
        direct_throughput = avg(throughput[direct], count[direct])
        gateway_throughput = avg(throughput[gateway], count[gateway])
        ttfb_penalty = direct_ttfb > 0 ? ((gateway_ttfb - direct_ttfb) / direct_ttfb) * 100 : 0
        total_penalty = direct_total > 0 ? ((gateway_total - direct_total) / direct_total) * 100 : 0
        speed_penalty = direct_speed > 0 ? ((direct_speed - gateway_speed) / direct_speed) * 100 : 0
        throughput_penalty = direct_throughput > 0 ? ((direct_throughput - gateway_throughput) / direct_throughput) * 100 : 0
        printf "%s,%s,%.0f,%.0f,%.6f,%.6f,%.2f,%.6f,%.6f,%.2f,%.6f,%.6f,%.2f,%.6f,%.6f,%.2f\n", fields[1], fields[2], direct_bytes, gateway_bytes, direct_ttfb, gateway_ttfb, ttfb_penalty, direct_total, gateway_total, total_penalty, direct_speed, gateway_speed, speed_penalty, direct_throughput, gateway_throughput, throughput_penalty
      }
    }
  ' "$RESULTS_CSV" | sort -t, -k1,1n -k2,2 >"$SUMMARY_CSV"
}

print_summary_table() {
  echo
  echo "Summary (averages):"
  awk -F, '
    NR == 1 { next }
    BEGIN { printf "%10s  %-7s  %12s  %12s  %12s  %12s  %10s\n", "connectors", "object", "direct_s", "gateway_s", "direct_MiB/s", "gateway_MiB/s", "penalty%" }
    {
      printf "%10s  %-7s  %12.6f  %12.6f  %12.3f  %12.3f  %10.2f\n", $1, $2, $8, $9, $14, $15, $10
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

if [ ! -f "$CERT_DIR/dev-ca.crt" ]; then
  echo "Generating development certificates..."
  "$ROOT_DIR/deploy/scripts/certs.sh"
fi

mkdir -p "$RESULTS_DIR"
echo "connector_count,object,path,iteration,bytes,ttfb_seconds,total_time_seconds,speed_bytes_per_sec,throughput_mib_per_sec" >"$RESULTS_CSV"

download_objects

echo "Starting Compose services with $CONNECTORS private connector(s)..."
run_compose up -d --build --scale "private-connector=$CONNECTORS" nats edge-gateway versitygw private-connector >/dev/null
seed_objects
wait_for_direct_s3 "${OBJECT_KEYS[0]}"
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
