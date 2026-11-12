#!/usr/bin/env bash
set -euo pipefail

# Local smoke test. Run from /opt/stacks/strava to verify the stack is healthy.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${DEPLOY_DIR}"

echo "Checking container health..."
docker compose ps

echo "Checking api container responds on port 80..."
docker compose exec -T api \
    bash -c 'exec 3<>/dev/tcp/127.0.0.1/80' || {
    echo "API health check failed" >&2
    exit 1
}

echo "Checking backend-api container responds on port 80..."
docker compose exec -T backend-api \
    bash -c 'exec 3<>/dev/tcp/127.0.0.1/80' || {
    echo "Backend-API health check failed" >&2
    exit 1
}

echo "Smoke test passed."
