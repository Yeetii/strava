#!/usr/bin/env bash
set -euo pipefail

# Guards against adding a Backend function and forgetting to assign it to one of
# the self-hosted containers (or leaving a stale name in docker-compose.yml).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/docker-compose.yml"

actual_file="$(mktemp)"
assigned_file="$(mktemp)"
trap 'rm -f "${actual_file}" "${assigned_file}"' EXIT

grep -rhoE '\[Function\("([^"]+)"\)\]|\[Function\(nameof\(([A-Za-z0-9_]+)\)\)\]' \
    "${ROOT_DIR}/Backend" --include='*.cs' \
    | sed -E -e 's/.*nameof\(([^)]+)\).*/\1/' -e 's/.*"([^"]+)".*/\1/' \
    | sort -u >"${actual_file}"

grep -oE 'AzureFunctionsJobHost__functions__[0-9]+: *[A-Za-z0-9_]+' "${COMPOSE_FILE}" \
    | sed -E 's/.*: *//' \
    | sort -u >"${assigned_file}"

duplicate_assignments="$(grep -oE 'AzureFunctionsJobHost__functions__[0-9]+: *[A-Za-z0-9_]+' "${COMPOSE_FILE}" \
    | sed -E 's/.*: *//' | sort | uniq -d || true)"

unassigned="$(comm -23 "${actual_file}" "${assigned_file}")"
unknown="$(comm -13 "${actual_file}" "${assigned_file}")"

status=0

if [[ -n "${duplicate_assignments}" ]]; then
    echo "Functions assigned to more than one container:" >&2
    echo "${duplicate_assignments}" >&2
    status=1
fi

if [[ -n "${unassigned}" ]]; then
    echo "Backend functions not assigned to any self-hosted container:" >&2
    echo "${unassigned}" >&2
    status=1
fi

if [[ -n "${unknown}" ]]; then
    echo "Self-hosted function names that no longer exist in Backend:" >&2
    echo "${unknown}" >&2
    status=1
fi

if [[ "${status}" -eq 0 ]]; then
    echo "All Backend functions are assigned exactly once."
fi

exit "${status}"
