#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/smoke-log.sh"
source "${SCRIPT_DIR}/lib/smoke-docker.sh"

docker_smoke_init_defaults
cd "${REPO_ROOT}"

NBD_DEVICE_ARTIFACT_HOST_DIR="${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}/nbd-device-artifacts"
NBD_DEVICE_ARTIFACT_CONTAINER_DIR="${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_MOUNT}/nbd-device-artifacts"
NBD_DEVICE_PROGRESS_HOST_FILE="${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}/nbd-device-progress.log"
NBD_DEVICE_PROGRESS_CONTAINER_FILE="${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_MOUNT}/nbd-device-progress.log"
status=0

mkdir -p "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}"

smoke_step "docker smoke"
smoke_state "scenario" "$(docker_smoke_effective_scenario)"
smoke_state "artifacts" "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}"

docker_smoke_build_image \
    "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}/docker-build.log"

mkdir -p "${NBD_DEVICE_ARTIFACT_HOST_DIR}"
docker_smoke_set_workspace_args "ro"
docker_smoke_set_nbd_device_env_args \
    "${NBD_DEVICE_ARTIFACT_CONTAINER_DIR}" \
    "${NBD_DEVICE_PROGRESS_CONTAINER_FILE}"
docker_smoke_set_artifact_args \
    "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}" \
    "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_MOUNT}"

smoke_run_with_progress "NBD device smoke" \
    "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}/nbd-device-smoke.log" \
    "${NBD_DEVICE_PROGRESS_HOST_FILE}" \
    docker run --rm \
        "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
        "${DOCKER_SMOKE_ENV_ARGS[@]}" \
        "${DOCKER_SMOKE_ARTIFACT_ARGS[@]}" \
        --privileged \
        "${DOCKER_IMAGE}" \
        make nbd-device-smoke-inner || status=$?

docker_smoke_collect_nbd_device_artifacts \
    "${NBD_DEVICE_ARTIFACT_HOST_DIR}" \
    "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}"

if [ "${status}" -ne 0 ]; then
    exit "${status}"
fi

smoke_ok "docker smoke passed"
smoke_state "artifacts" "${DOCKER_NBD_DEVICE_SMOKE_ARTIFACT_DIR}"
