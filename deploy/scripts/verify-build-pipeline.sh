#!/usr/bin/env bash
set -euo pipefail

# Guards the shared-build contract between the deploy workflow and the image
# Dockerfiles. There is no other automated check that a workflow edit keeps the
# prebuilt publish context in sync with the Dockerfiles' target names.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKFLOW="${ROOT_DIR}/.github/workflows/selfhosted-deploy.yml"
PUBLISH="${ROOT_DIR}/deploy/scripts/publish-images.sh"

status=0
fail() { echo "FAIL: $*" >&2; status=1; }
require() {
    grep -qF -- "$1" "$2" || fail "$3"
}

[[ -x "${PUBLISH}" ]] || fail "publish-images.sh is missing or not executable"

require 'name: stigar-image-context' "${WORKFLOW}" "workflow must use the named prebuilt artifact"
require 'path: image-context/' "${WORKFLOW}" "upload must use path: image-context/"
grep -qE '^[[:space:]]+path: image-context$' "${WORKFLOW}" \
    || fail "download must use path: image-context"
require 'context: image-context/${{ matrix.name }}' "${WORKFLOW}" "image builds must use the per-image prebuilt context"
require 'target: prebuilt' "${WORKFLOW}" "image builds must target the prebuilt stage"

guards="$(grep -cF "github.event_name == 'workflow_dispatch' && inputs.image_tag != ''" "${WORKFLOW}" || true)"
[[ "${guards}" -eq 3 ]] || fail "expected 3 workflow_dispatch guards (publish, upload, build), found ${guards}"

for dockerfile in "${ROOT_DIR}/deploy/docker/plain-host.Dockerfile" \
                  "${ROOT_DIR}/Stigar.Workers.Pmtiles/Dockerfile"; do
    name="${dockerfile##*/}"
    last_from="$(grep -E '^FROM ' "${dockerfile}" | tail -n1)"
    [[ "${last_from}" == *' AS final' ]] \
        || fail "${name}: last stage must be final so local source builds stay the default (got: ${last_from})"
    grep -qE '^FROM runtime-base AS prebuilt$' "${dockerfile}" \
        || fail "${name}: prebuilt stage must derive from runtime-base"
    if grep -qE '^FROM build AS prebuilt$' "${dockerfile}"; then
        fail "${name}: prebuilt must not depend on the SDK build stage"
    fi
done

require 'publish Stigar.Api api' "${PUBLISH}" "api must be published"
require 'publish Stigar.Workers workers' "${PUBLISH}" "workers must be published"
require 'publish Stigar.Workers.Pmtiles pmtiles' "${PUBLISH}" "pmtiles must be published"
require 'OUTPUT_DIR}/workers/peaks.json' "${PUBLISH}" "workers publish context must include peaks.json"

if [[ "${status}" -eq 0 ]]; then
    echo "Build pipeline OK: one publish artifact feeds every prebuilt image target; local source builds remain the default."
fi
exit "${status}"
