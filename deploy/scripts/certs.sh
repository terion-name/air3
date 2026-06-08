#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${AIR3_CERT_DIR:-"$ROOT_DIR/deploy/certs/generated"}
DAYS=${AIR3_CERT_DAYS:-3650}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required to generate development certificates" >&2
    exit 1
  }
}

make_cert() {
  local name=$1
  local cn=$2
  local san=$3
  local usage=$4
  local key="$OUT_DIR/$name.key"
  local csr="$OUT_DIR/$name.csr"
  local crt="$OUT_DIR/$name.crt"
  local ext="$OUT_DIR/$name.ext"

  openssl genrsa -out "$key" 2048 >/dev/null 2>&1
  chmod 0644 "$key"
  openssl req -new -key "$key" -out "$csr" -subj "/CN=$cn" >/dev/null 2>&1
  cat > "$ext" <<EOCERT
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = $usage
subjectAltName = $san
EOCERT
  openssl x509 -req -in "$csr" -CA "$OUT_DIR/dev-ca.crt" -CAkey "$OUT_DIR/dev-ca.key" -CAcreateserial -out "$crt" -days "$DAYS" -sha256 -extfile "$ext" >/dev/null 2>&1
  chmod 0644 "$crt"
  rm -f "$csr" "$ext"
}

require_cmd openssl
mkdir -p "$OUT_DIR"
chmod 0755 "$ROOT_DIR/deploy" "$ROOT_DIR/deploy/certs" "$OUT_DIR"

if [ -e "$OUT_DIR/dev-ca.key" ] || [ -e "$OUT_DIR/dev-ca.crt" ]; then
  echo "Replacing existing development certificates in $OUT_DIR"
  rm -f "$OUT_DIR"/*.key "$OUT_DIR"/*.crt "$OUT_DIR"/*.csr "$OUT_DIR"/*.srl "$OUT_DIR"/*.ext
fi

openssl genrsa -out "$OUT_DIR/dev-ca.key" 4096 >/dev/null 2>&1
chmod 0644 "$OUT_DIR/dev-ca.key"
openssl req -x509 -new -nodes -key "$OUT_DIR/dev-ca.key" -sha256 -days "$DAYS" -out "$OUT_DIR/dev-ca.crt" -subj "/CN=air3-demo-dev-ca" >/dev/null 2>&1
chmod 0644 "$OUT_DIR/dev-ca.crt"

make_cert "nats-server" "nats" "DNS:nats,DNS:localhost,IP:127.0.0.1" "serverAuth"
make_cert "edge-server" "edge-gateway" "DNS:edge-gateway,DNS:localhost,IP:127.0.0.1" "serverAuth"
make_cert "edge-nats-client" "edge-nats-client" "DNS:edge-nats-client" "clientAuth"
make_cert "connector-nats-client" "connector-nats-client" "DNS:connector-nats-client" "clientAuth"
make_cert "connector-ingest-client" "connector-ingest-client" "DNS:connector-ingest-client" "clientAuth"

cat > "$OUT_DIR/README.txt" <<EOREADME
Generated local development certificates for the air3 Compose demo.
These files are intentionally ignored by git and may be deleted/regenerated.
EOREADME

echo "Generated development certificates in $OUT_DIR"
