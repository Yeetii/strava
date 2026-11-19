#!/usr/bin/env bash
set -euo pipefail

# Native architecture gate. There are no Functions triggers to discover: the
# API host maps minimal APIs explicitly and the Workers host registers typed
# Hangfire jobs. Verify the invariants that keep that split correct:
#   1. no Azure Functions envelope or reflection mapper remains in source
#   2. every worker container declares exactly one valid WORKER_ROLE
#   3. every typed job contract has exactly one implementation in Workers
#   4. every recurring job declares a non-empty cron and job id
#   5. compose image names match the names the deploy workflow publishes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/docker-compose.yml"
CORE_DIR="${ROOT_DIR}/Stigar.Core"
WORKERS_DIR="${ROOT_DIR}/Stigar.Workers"

status=0
fail() { echo "FAIL: $*" >&2; status=1; }

# 1. Legacy Functions envelope must be gone.
if grep -rqE 'Microsoft\.Azure\.Functions\.Worker|Microsoft\.Azure\.WebJobs\.Extensions\.OpenApi|\[HttpTrigger|\[QueueTrigger|\[TimerTrigger|HttpRequestData|HttpResponseData|MapFunctions\(' \
    "${ROOT_DIR}/Stigar.Api" "${WORKERS_DIR}" "${CORE_DIR}" "${ROOT_DIR}/Stigar.Workers.Pmtiles" --include='*.cs'; then
    fail "Azure Functions envelope references remain in source"
fi

# 2. Every worker container declares exactly one valid role.
for container in worker-schedule worker-heavy; do
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
    if [[ ! "${role}" =~ ^(schedule|heavy)$ ]]; then
        fail "container '${container}' has invalid WORKER_ROLE '${role}'"
    fi
done

# 3. Every typed job contract has exactly one implementation in the Workers host.
for iface in $(grep -oE 'public interface I[A-Za-z]+Job\b' "${CORE_DIR}/Jobs/JobHandlers.cs" \
        | awk '{print $3}'); do
    implementations="$(grep -rl ": ${iface}\b" "${WORKERS_DIR}" --include='*.cs' | wc -l | tr -d ' ')"
    if [[ "${implementations}" -ne 1 ]]; then
        fail "job contract ${iface} has ${implementations} implementations in Stigar.Workers (expected 1)"
    fi
done

# 4. Every recurring job declares a job id and a cron expression.
recurring="$(grep -rl ': IRecurringJob\b' "${WORKERS_DIR}" --include='*.cs' | wc -l | tr -d ' ')"
for f in $(grep -rl ': IRecurringJob\b' "${WORKERS_DIR}" --include='*.cs'); do
    grep -q 'public string JobId =>' "${f}" || fail "recurring job ${f} has no JobId"
    grep -q 'public string Cron =>' "${f}" || fail "recurring job ${f} has no Cron"
done

# 5. Every stigar-* image referenced by compose must be built by the workflow.
WORKFLOW_FILE="${ROOT_DIR}/.github/workflows/selfhosted-deploy.yml"
for image in $(grep -oE 'stigar-[a-z]+' "${COMPOSE_FILE}" | sort -u); do
    name="${image#stigar-}"
    grep -qE "name: ${name}$" "${WORKFLOW_FILE}" \
        || fail "compose references image '${image}' but the deploy workflow builds no '${name}' image"
done

if [[ "${status}" -eq 0 ]]; then
    echo "Native architecture OK: ${recurring} recurring jobs, typed job contracts implemented once, no Functions envelope, compose images match the build matrix."
fi
exit "${status}"
