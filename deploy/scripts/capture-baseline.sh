#!/usr/bin/env bash
set -euo pipefail

# Captures the before/after comparison baseline required by P0 of the
# "Azure Functions Runtime Exit" plan: Service Bus queue depths, Cosmos RU/s,
# and API endpoint latencies. Run from a machine with the Azure CLI installed
# and authenticated. Output goes to stdout so the operator can archive it
# outside the repository.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/deploy/.env" 2>/dev/null || true

timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=== baseline ${timestamp} ==="

if command -v az >/dev/null 2>&1; then
    if [[ -n "${SERVICE_BUS_NAMESPACE:-}" ]]; then
        echo "--- Service Bus queue depths ---"
        for queue in calculateSummitsJobs calculateVisitedPathsJobs calculateVisitedAreasJobs \
                     activitiesfetchjobs activityFetchJobs activityprocessed activityDeleteJobs \
                     scrapeRace raceDiscoveryJobs mistralScrape enrichAdminBoundaryJobs accountDeleteJobs; do
            read -r total active deadletter <<<"$(az servicebus queue show \
                --resource-group "${AZURE_RESOURCE_GROUP:-}" \
                --namespace-name "${SERVICE_BUS_NAMESPACE}" \
                --name "${queue}" \
                --query '{total:messageCount, active:countDetails.activeMessageCount, dlq:countDetails.deadLetterMessageCount}' \
                -o tsv 2>/dev/null || printf 'n/a\tn/a\tn/a')"
            printf '%-32s total=%-6s active=%-6s deadletter=%s\n' \
                "${queue}" "${total:-n/a}" "${active:-n/a}" "${deadletter:-n/a}"
        done
    fi

    if [[ -n "${COSMOS_ACCOUNT:-}" ]]; then
        echo "--- Cosmos DB RU/s (last hour, max) ---"
        az monitor metrics list \
            --resource "${COSMOS_ACCOUNT}" \
            --metric NormalizedRUConsumption \
            --interval PT1H --aggregation Maximum -o table 2>/dev/null || echo "n/a"
    fi
else
    echo "az CLI not available; skipping Azure metrics. Queue depths and RU/s must be captured manually."
fi

echo "--- API endpoint latencies ---"
api_base="${BASELINE_API_URL:-https://peakshunters.erikmagnusson.com/api}"
for path in GetCurrentUser GetPeaks GetVisitedPaths GetAdminStatus; do
    if command -v curl >/dev/null 2>&1; then
        latency="$(curl -s -o /dev/null -w '%{time_total}s (HTTP %{http_code})' \
            --max-time 30 "${api_base}/${path}" 2>/dev/null || echo 'n/a')"
        printf '%-28s %s\n' "${path}" "${latency}"
    fi
done

echo "=== end baseline ==="
