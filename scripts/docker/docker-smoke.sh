#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/smoke-log.sh"
source "${SCRIPT_DIR}/lib/smoke-docker.sh"

docker_smoke_init_defaults
cd "${REPO_ROOT}"

KERNEL_ARTIFACT_HOST_DIR="${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}/kernel-artifacts"
KERNEL_ARTIFACT_CONTAINER_DIR="${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}/kernel-artifacts"
KERNEL_PROGRESS_HOST_FILE="${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}/kernel-progress.log"
KERNEL_PROGRESS_CONTAINER_FILE="${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}/kernel-progress.log"
status=0

mkdir -p "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}"

smoke_step "docker smoke"
smoke_state "scenario" "$(docker_smoke_effective_scenario)"
smoke_state "artifacts" "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}"

docker_smoke_build_image \
    "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}/docker-build.log"

mkdir -p "${KERNEL_ARTIFACT_HOST_DIR}"
docker_smoke_set_workspace_args "ro"
docker_smoke_set_kernel_env_args \
    "${KERNEL_ARTIFACT_CONTAINER_DIR}" \
    "${KERNEL_PROGRESS_CONTAINER_FILE}"
docker_smoke_set_artifact_args \
    "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}" \
    "${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}"

smoke_run_with_progress "kernel smoke" \
    "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}/kernel-smoke.log" \
    "${KERNEL_PROGRESS_HOST_FILE}" \
    docker run --rm \
        "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
        "${DOCKER_SMOKE_ENV_ARGS[@]}" \
        "${DOCKER_SMOKE_ARTIFACT_ARGS[@]}" \
        --privileged \
        "${DOCKER_IMAGE}" \
        make kernel-smoke-inner || status=$?

docker_smoke_collect_kernel_artifacts \
    "${KERNEL_ARTIFACT_HOST_DIR}" \
    "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}"

if [ "${status}" -ne 0 ]; then
    exit "${status}"
fi

smoke_ok "docker smoke passed"
smoke_state "artifacts" "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}"
