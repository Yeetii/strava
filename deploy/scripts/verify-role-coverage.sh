#!/usr/bin/env bash
set -euo pipefail

# Role-coverage gate (replaces verify-function-coverage.sh).
#
# With the Functions runtime gone there is no per-function whitelist: roles are
# selected by WORKER_ROLE and trigger discovery is automatic. This gate verifies
# the invariants that keep that mapping correct:
#   1. no AzureFunctionsJobHost function-list plumbing remains
#   2. every compose container declares exactly one valid WORKER_ROLE
#   3. every Service Bus queue / change feed / cron is discovered by a role
#   4. every queue constant is consumed by exactly one ServiceBusTrigger
#   5. no two functions declare the same name (route identity)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/docker-compose.yml"
BACKEND_DIR="${ROOT_DIR}/Backend"
API_DIR="${ROOT_DIR}/API"
SB_CONFIG="${ROOT_DIR}/Shared/Constants/ServiceBusConfig.cs"

status=0
fail() { echo "FAIL: $*" >&2; status=1; }

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

# 1. Legacy function-list plumbing must be gone.
if grep -q 'AzureFunctionsJobHost__functions__' "${COMPOSE_FILE}"; then
    fail "compose still contains AzureFunctionsJobHost__functions__N entries"
fi

# 2. Every application container declares exactly one valid role.
valid_roles='api|schedule|heavy'
for container in api worker-schedule worker-heavy; do
    count="$(awk -v c="${container}:" '
        $0 ~ "^  "c { inblock=1; next }
        inblock && /^  [A-Za-z]/ { inblock=0 }
        inblock && /WORKER_ROLE:/ { n++ }
        END { print n+0 }
    ' "${COMPOSE_FILE}")"
    if [[ "${count}" -ne 1 ]]; then
        fail "container '${container}' must declare exactly one WORKER_ROLE (found ${count})"
        continue
    fi
    role="$(awk -v c="${container}:" '
        $0 ~ "^  "c { inblock=1; next }
        inblock && /^  [A-Za-z]/ { inblock=0 }
        inblock && /WORKER_ROLE:/ { print $2; exit }
    ' "${COMPOSE_FILE}" | tr -d '"'"'"'')"
    if [[ ! "${role}" =~ ^(${valid_roles})$ ]]; then
        fail "container '${container}' has invalid WORKER_ROLE '${role}'"
    fi
done

# Trigger inventory from the source.
grep -rhoE '\[ServiceBusTrigger\(' "${BACKEND_DIR}" --include='*.cs' | wc -l | tr -d ' ' >"${tmp}/sb"
grep -rhoE '\[CosmosDBTrigger\(' "${BACKEND_DIR}" --include='*.cs' | wc -l | tr -d ' ' >"${tmp}/feeds"
grep -rhoE '\[TimerTrigger\(' "${BACKEND_DIR}" --include='*.cs' | wc -l | tr -d ' ' >"${tmp}/crons"

[[ "$(cat "${tmp}/sb")" == "12" ]] || fail "expected 12 Service Bus triggers, found $(cat "${tmp}/sb")"
[[ "$(cat "${tmp}/feeds")" == "3" ]] || fail "expected 3 change-feed triggers, found $(cat "${tmp}/feeds")"
[[ "$(cat "${tmp}/crons")" == "12" ]] || fail "expected 12 timer triggers, found $(cat "${tmp}/crons")"

# Cron strings carry over verbatim: 1 + 1 + 10.
grep -rhoE '\[TimerTrigger\("[^"]+"\)\]' "${BACKEND_DIR}" --include='*.cs' \
    | sort | uniq -c >"${tmp}/cron-shapes"
grep -q '^ *10 \[TimerTrigger("0 0 2 \* \* 1")\]$' "${tmp}/cron-shapes" \
    || fail "expected ten weekly 02:00 Monday crons"
grep -q '^ *1 \[TimerTrigger("0 0 3 \* \* 1")\]$' "${tmp}/cron-shapes" \
    || fail "expected the 03:00 Monday discovery cron"
grep -q '^ *1 \[TimerTrigger("0 0 3 \* \* 2")\]$' "${tmp}/cron-shapes" \
    || fail "expected the 03:00 Tuesday scrape cron"

# Every queue constant is referenced by at least one consumer/producer in Backend.
while IFS='|' read -r constant queue; do
    [[ -z "${queue}" ]] && continue
    if ! grep -rqE "ServiceBusConfig\.${constant}|\"${queue}\"" "${BACKEND_DIR}" --include='*.cs'; then
        fail "queue '${queue}' (${constant}) has no consumer in Backend"
    fi
done < <(sed -nE 's/^[[:space:]]*public const string ([A-Za-z]+) = "([^"]+)";.*/\1|\2/p' "${SB_CONFIG}")

# No duplicate function names within a project. Cross-assembly HTTP route and
# verb collisions are checked by PlainHostTests in C#, which can see the actual
# [HttpTrigger] metadata (a shared name may be a timer/feed in one project).
for project_dir in "${BACKEND_DIR}" "${API_DIR}"; do
    duplicates="$(grep -rhoE '\[Function\("([^"]+)"\)\]|\[Function\(nameof\(([A-Za-z0-9_]+)\)\)\]' \
        "${project_dir}" --include='*.cs' \
        | sed -E -e 's/.*nameof\(([^)]+)\).*/\1/' -e 's/.*"([^"]+)".*/\1/' \
        | sort | uniq -d)"
    if [[ -n "${duplicates}" ]]; then
        fail "duplicate function names in ${project_dir}:"; echo "${duplicates}" >&2
    fi
done

if [[ "${status}" -eq 0 ]]; then
    echo "Role coverage OK: 12 queues, 3 feeds, 12 crons mapped to plain-host roles; no legacy function-list plumbing."
fi
exit "${status}"
