#!/usr/bin/env bash
set -euo pipefail

# Build the deploy images' publish output ONCE, so the deploy workflow can COPY
# compiled binaries into each runtime image instead of recompiling the same
# projects inside three separate Docker builds. Local `docker compose ... --build`
# is unaffected: it still uses the SDK build stages in the Dockerfiles.
#
# Usage: ./deploy/scripts/publish-images.sh [output-directory]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${1:-${ROOT_DIR}/image-context}"

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/api" "${OUTPUT_DIR}/workers" "${OUTPUT_DIR}/pmtiles"

publish() {
    local project="$1" name="$2"
    echo "==> Publishing ${project} -> ${OUTPUT_DIR}/${name}"
    dotnet publish "${ROOT_DIR}/${project}/${project}.csproj" \
        --configuration Release \
        --output "${OUTPUT_DIR}/${name}"
}

publish Stigar.Api api
publish Stigar.Workers workers
publish Stigar.Workers.Pmtiles pmtiles

# The workers image has always carried peaks.json (plain-host.Dockerfile copies
# it for every role), but its csproj excludes it from publish output.
cp "${ROOT_DIR}/Stigar.Api/peaks.json" "${OUTPUT_DIR}/workers/peaks.json"

echo "==> Prebuilt image contexts ready in ${OUTPUT_DIR}"
