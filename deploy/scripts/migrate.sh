#!/usr/bin/env bash
set -euo pipefail

# Apply idempotent schema migrations to an existing database volume.
# PostgreSQL only runs /docker-entrypoint-initdb.d on a fresh data directory,
# so upgrades need this explicit step before the workers start.

TARGET_DIR="${TARGET_DIR:-/opt/stacks/strava}"
cd "${TARGET_DIR}"

echo "==> Ensuring PostgreSQL is available for migrations"
docker compose up -d postgres

# Use the credentials Compose injects into the container from .env, so custom
# POSTGRES_USER/POSTGRES_DB values work without parsing .env in this shell.
for _ in $(seq 1 30); do
    if docker compose exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

shopt -s nullglob
for file in init-db/0[2-9]-*.sql; do
    echo "==> Applying $(basename "${file}")"
    docker compose exec -T postgres sh -c \
        'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$1"' \
        _ "/docker-entrypoint-initdb.d/$(basename "${file}")"
done
