#!/usr/bin/env bash
set -euo pipefail

# Creates deploy/.env from .env.example and fills in generated local secrets.
# Azure connection strings and application secrets still have to be supplied.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${DEPLOY_DIR}/.env"
EXAMPLE_FILE="${DEPLOY_DIR}/.env.example"

if [[ -f "${ENV_FILE}" && "${1:-}" != "--force" ]]; then
    echo "${ENV_FILE} already exists. Re-run with --force to overwrite it." >&2
    exit 1
fi

cp "${EXAMPLE_FILE}" "${ENV_FILE}"

generate_hex() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
    else
        date +%s%N | sha256sum | cut -c1-48
    fi
}

set_value() {
    local key="$1"
    local value="$2"
    # Escape the replacement delimiter for sed.
    value="${value//|/\\|}"
    sed -i.bak "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}"
    rm -f "${ENV_FILE}.bak"
}

set_value "AdminApiKey" "$(generate_hex)"

chmod 600 "${ENV_FILE}"

cat <<EOF
Created ${ENV_FILE}

Next:
  1. Edit ${ENV_FILE} and replace every REPLACE placeholder.
  2. Set IMAGE_NAMESPACE to the lowercase GitHub owner that publishes the images.
  3. Keep COMPOSE_PROFILES empty until the API has been verified against the
     self-hosted stack. Cosmos DB and Service Bus remain on Azure.
EOF
