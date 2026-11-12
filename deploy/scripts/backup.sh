#!/usr/bin/env bash
set -euo pipefail

# Backs up the self-hosted configuration.
# Cosmos DB and Azure Storage are still the source of truth in this phase and
# have their own platform backups.

TARGET_DIR="${TARGET_DIR:-/opt/stacks/strava}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${TARGET_DIR}/backups/${STAMP}"

if [[ ! -f "${TARGET_DIR}/.env" ]]; then
    echo "${TARGET_DIR}/.env is missing." >&2
    exit 1
fi

mkdir -p "${DEST}"
chmod 700 "${DEST}"

cd "${TARGET_DIR}"

echo "==> Saving configuration"
cp .env "${DEST}/.env"
chmod 600 "${DEST}/.env"
docker compose ps >"${DEST}/compose-ps.txt" 2>&1 || true
docker compose config --services >"${DEST}/compose-services.txt" 2>&1 || true

find "${TARGET_DIR}/backups" -mindepth 1 -maxdepth 1 -type d -mtime "+${RETENTION_DAYS}" -exec rm -rf {} +

echo "Backup written to ${DEST}"
