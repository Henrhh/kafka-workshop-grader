#!/usr/bin/env bash
# Creates a private CA and one TLS certificate for the VM's host name, shared by all three
# brokers. Writes secrets/ca.pem (give it to the grader as KAFKA_CA_CERTIFICATE) and
# secrets/broker.p12 (the brokers' keystore).
#
# Usage: ./gen-certs.sh            (reads KAFKA_HOST and KEYSTORE_PASSWORD from .env)
set -euo pipefail
cd "$(dirname "$0")"

set -a
# shellcheck disable=SC1091
source .env
set +a
: "${KAFKA_HOST:?set KAFKA_HOST in .env}"
: "${KEYSTORE_PASSWORD:?set KEYSTORE_PASSWORD in .env}"

mkdir -p secrets
cd secrets
if [[ -f broker.p12 ]]; then
  echo "secrets/broker.p12 already exists. Delete secrets/ to start over." >&2
  exit 1
fi

openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
  -keyout ca.key -out ca.pem -subj "/CN=Kafka workshop CA"

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout broker.key -out broker.csr -subj "/CN=${KAFKA_HOST}"

cat > broker.ext <<EXT
subjectAltName = DNS:${KAFKA_HOST}, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT

openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out broker.pem -days 365 -sha256 -extfile broker.ext

openssl pkcs12 -export -in broker.pem -inkey broker.key -certfile ca.pem \
  -name broker -out broker.p12 -passout "pass:${KEYSTORE_PASSWORD}"

rm broker.csr broker.ext ca.srl
# The container runs as uid 1000 and must be able to read the keystore.
chmod 644 broker.p12 ca.pem
chmod 600 ca.key broker.key

echo
echo "Done. Give the grader this CA certificate (KAFKA_CA_CERTIFICATE):"
echo
cat ca.pem
