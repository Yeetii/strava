#!/usr/bin/env bash
set -euo pipefail

# Pull and activate an image tag. Called by the GitHub Actions deploy workflow,
# or manually for a rollback:
#
#   /opt/stacks/strava/scripts/deploy.sh <git-sha-or-image-tag>

TARGET_DIR="${TARGET_DIR:-/opt/stacks/strava}"
TAG="${1:-${IMAGE_TAG:-}}"

if [[ -z "${TAG}" ]]; then
    echo "Usage: $0 <image-tag>" >&2
    exit 2
fi

if [[ ! "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "Invalid image tag: ${TAG}" >&2
    exit 2
fi

cd "${TARGET_DIR}"

if [[ ! -f .env ]]; then
    echo "${TARGET_DIR}/.env is missing. Run scripts/bootstrap-server.sh first." >&2
    exit 1
fi

cp .env .env.previous
cleanup() {
    rm -f .env.previous
}
trap cleanup EXIT

previous_tag="$(grep -E '^IMAGE_TAG=' .env.previous | head -n1 | cut -d= -f2- || true)"

echo "==> Pulling image tag ${TAG}"
export IMAGE_TAG="${TAG}"
docker compose pull

if grep -qE '^IMAGE_TAG=' .env; then
    sed -i.bak "s|^IMAGE_TAG=.*|IMAGE_TAG=${TAG}|" .env
    rm -f .env.bak
else
    printf '\nIMAGE_TAG=%s\n' "${TAG}" >>.env
fi
chmod 600 .env

echo "==> Starting stack (profiles from .env: $(grep -E '^COMPOSE_PROFILES=' .env | cut -d= -f2- || true))"
if docker compose up -d --remove-orphans --wait --wait-timeout 300; then
    echo "==> Deployment succeeded"
    docker compose ps
    echo "==> pmtiles status"
    docker compose ps pmtiles
    docker image prune -f >/dev/null || true
    exit 0
fi

echo "Deployment failed; attempting rollback to ${previous_tag:-previous tag}" >&2
if [[ -n "${previous_tag}" ]]; then
    export IMAGE_TAG="${previous_tag}"
    sed -i "s|^IMAGE_TAG=.*|IMAGE_TAG=${previous_tag}|" .env
    docker compose pull || true
    docker compose up -d --remove-orphans --wait --wait-timeout 300 || true
fi

exit 1
