#!/usr/bin/env bash
set -euo pipefail

# Restores .env from a backup directory produced by backup.sh.

TARGET_DIR="${TARGET_DIR:-/opt/stacks/strava}"
BACKUP_DIR="${1:-}"

if [[ -z "${BACKUP_DIR}" || ! -d "${BACKUP_DIR}" ]]; then
    echo "Usage: $0 <backup-directory>" >&2
    exit 2
fi

cd "${TARGET_DIR}"

if [[ -f "${BACKUP_DIR}/.env" ]]; then
    echo "==> Restoring .env"
    cp "${BACKUP_DIR}/.env" .env
    chmod 600 .env
fi

echo "Restore complete."
