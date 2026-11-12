#!/usr/bin/env bash
set -euo pipefail

# Copies the deployment files (the contents of deploy/) into TARGET on an
# already-provisioned Docker host and creates .env from .env.example.
# Docker, the Compose plugin, firewalling, and package management are assumed
# to be set up already.
#
# Run from a checkout of this repository (conventionally at
# /opt/repos/strava; TARGET is the stack's runtime directory):
#
#   sudo ./deploy/scripts/bootstrap-server.sh
#
# Environment overrides:
#   TARGET=/opt/stacks/strava
#   DEPLOY_USER=<user that will run GitHub deploys; defaults to SUDO_USER>
#   START_STACK=1

if [[ "${EUID}" -ne 0 ]]; then
    echo "Run this script as root (sudo)." >&2
    exit 1
fi

for cmd in docker rsync; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "Missing required command: ${cmd}" >&2
        exit 1
    fi
done
if ! docker compose version >/dev/null 2>&1; then
    echo "The Docker Compose plugin is required (docker compose ...)." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"   # the deploy/ directory
TARGET="${TARGET:-/opt/stacks/strava}"
DEPLOY_USER="${DEPLOY_USER:-${SUDO_USER:-root}}"

if [[ "${SOURCE_DIR}" == "${TARGET}" ]]; then
    echo "TARGET (${TARGET}) must not be the deploy/ directory itself." >&2
    exit 1
fi

echo "==> Installing deployment files into ${TARGET}"
mkdir -p "${TARGET}"
rsync -a --delete \
    --exclude '.env' \
    --exclude 'backups' \
    --exclude 'ansible/inventory.ini' \
    "${SOURCE_DIR}/" "${TARGET}/"

if [[ ! -f "${TARGET}/.env" ]]; then
    echo "==> Creating ${TARGET}/.env from .env.example"
    "${TARGET}/scripts/init-env.sh"
else
    chmod 600 "${TARGET}/.env"
fi

echo "==> Setting ownership to ${DEPLOY_USER}"
chown -R "${DEPLOY_USER}" "${TARGET}"
chmod 600 "${TARGET}/.env"

echo
echo "Bootstrap complete."
echo "  1. Edit ${TARGET}/.env"
echo "  2. Log in to GHCR once if the images are private:"
echo "       docker login ghcr.io"
echo "  3. Start the stack:"
echo "       cd ${TARGET} && docker compose up -d --wait"

if [[ "${START_STACK:-0}" == "1" ]]; then
    cd "${TARGET}"
    docker compose up -d --remove-orphans --wait
fi
