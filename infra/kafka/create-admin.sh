#!/usr/bin/env bash
# Creates or updates the grader's SCRAM user. Run it once the brokers are up.
# It talks to the in-network plain-text listener, so it needs no credentials.
set -euo pipefail
cd "$(dirname "$0")"

set -a
# shellcheck disable=SC1091
source .env
set +a
: "${ADMIN_USERNAME:?set ADMIN_USERNAME in .env}"
: "${ADMIN_PASSWORD:?set ADMIN_PASSWORD in .env}"

docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:19092 \
  --alter --entity-type users --entity-name "${ADMIN_USERNAME}" \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=${ADMIN_PASSWORD}]"

echo "User ${ADMIN_USERNAME} is ready."
