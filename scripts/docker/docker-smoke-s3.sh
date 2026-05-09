#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/smoke-log.sh"
source "${SCRIPT_DIR}/lib/smoke-docker.sh"

docker_smoke_init_defaults
cd "${REPO_ROOT}"

MODE="${1:-run}"
RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-alpha.94-glibc}"
DOCKER_SMOKE_S3_NETWORK="${DOCKER_SMOKE_S3_NETWORK:-nbd-smoke-s3-e2e}"
DOCKER_SMOKE_S3_RUSTFS_CONTAINER="${DOCKER_SMOKE_S3_RUSTFS_CONTAINER:-nbd-smoke-s3-e2e-rustfs}"
DOCKER_SMOKE_S3_RUSTFS_ALIAS="${DOCKER_SMOKE_S3_RUSTFS_ALIAS:-rustfs}"
DOCKER_SMOKE_S3_RUSTFS_VOLUME="${DOCKER_SMOKE_S3_RUSTFS_VOLUME:-nbd-smoke-s3-e2e-rustfs-data}"
DOCKER_SMOKE_S3_ARTIFACT_DIR="${DOCKER_SMOKE_S3_ARTIFACT_DIR:-${REPO_ROOT}/.tmp/docker-smoke-s3}"
DOCKER_SMOKE_S3_ACCESS_KEY="${DOCKER_SMOKE_S3_ACCESS_KEY:-rustfsadmin}"
DOCKER_SMOKE_S3_SECRET_KEY="${DOCKER_SMOKE_S3_SECRET_KEY:-rustfsadmin}"
DOCKER_SMOKE_S3_BUCKET="${DOCKER_SMOKE_S3_BUCKET:-everstore}"
DOCKER_SMOKE_S3_KEY_PREFIX="${DOCKER_SMOKE_S3_KEY_PREFIX:-v0.1/blobs/}"
DOCKER_SMOKE_S3_REGION="${DOCKER_SMOKE_S3_REGION:-us-east-1}"
case "${DOCKER_SMOKE_S3_ARTIFACT_DIR}" in
    /*)
        ;;
    *)
        DOCKER_SMOKE_S3_ARTIFACT_DIR="${REPO_ROOT}/${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
        ;;
esac
DOCKER_KERNEL_SMOKE_ARTIFACT_DIR="${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
KERNEL_ARTIFACT_HOST_DIR="${DOCKER_SMOKE_S3_ARTIFACT_DIR}/kernel-artifacts"
KERNEL_ARTIFACT_CONTAINER_DIR="${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}/kernel-artifacts"
S3_RESOURCES_ACTIVE=0

s3_endpoint_url() {
    printf 'http://%s:9000\n' "${DOCKER_SMOKE_S3_RUSTFS_ALIAS}"
}

s3_cleanup_resources() {
    smoke_run_quiet "remove RustFS container" \
        docker rm -f "${DOCKER_SMOKE_S3_RUSTFS_CONTAINER}"
    smoke_run_quiet "remove S3 smoke network" \
        docker network rm "${DOCKER_SMOKE_S3_NETWORK}"
    smoke_run_quiet "remove RustFS volume" \
        docker volume rm "${DOCKER_SMOKE_S3_RUSTFS_VOLUME}"
}

s3_collect_rustfs_log() {
    mkdir -p "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"

    if docker inspect "${DOCKER_SMOKE_S3_RUSTFS_CONTAINER}" \
        >/dev/null 2>&1; then
        docker logs "${DOCKER_SMOKE_S3_RUSTFS_CONTAINER}" \
            >"${DOCKER_SMOKE_S3_ARTIFACT_DIR}/rustfs.log" 2>&1 || true
    fi
}

s3_finish_resources() {
    if [ "${S3_RESOURCES_ACTIVE}" != "1" ]; then
        return 0
    fi

    s3_collect_rustfs_log

    if [ "${KEEP_RUSTFS:-0}" = "1" ]; then
        smoke_ok "kept RustFS sidecar running"
        smoke_state "network" "${DOCKER_SMOKE_S3_NETWORK}"
        smoke_state "container" "${DOCKER_SMOKE_S3_RUSTFS_CONTAINER}"
        smoke_state "cleanup" "make docker-smoke-s3-down"
        return 0
    fi

    s3_cleanup_resources
}

s3_on_exit() {
    local status=$?

    s3_finish_resources
    exit "${status}"
}

s3_print_header() {
    local label="$1"

    smoke_step "${label}"
    smoke_state "rustfs image" "${RUSTFS_IMAGE}"
    smoke_state "network" "${DOCKER_SMOKE_S3_NETWORK}"
    smoke_state "artifacts" "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
}

s3_prepare_step() {
    local label="$1"
    local log_path="$2"
    local status
    shift 2

    mkdir -p "$(dirname "${log_path}")"
    if smoke_verbose; then
        printf '  substep: %s: ' "${label}"
        smoke_redacted_command "$@"
    fi

    if "$@" >"${log_path}" 2>&1; then
        return 0
    else
        status=$?
    fi

    smoke_fail \
        "prepare RustFS sidecar failed at ${label} with exit status ${status}"
    smoke_state "log" "${log_path}" >&2
    smoke_tail_log "${log_path}"
    return "${status}"
}

s3_print_prepare_logs() {
    smoke_state \
        "logs" \
        "create-network.log, create-volume.log, start-rustfs.log, wait-rustfs.log"
}

s3_create_resources() {
    s3_prepare_step "create S3 smoke network" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/create-network.log" \
        docker network create "${DOCKER_SMOKE_S3_NETWORK}"
    S3_RESOURCES_ACTIVE=1
    s3_prepare_step "create RustFS volume" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/create-volume.log" \
        docker volume create "${DOCKER_SMOKE_S3_RUSTFS_VOLUME}"
    s3_prepare_step "start RustFS container" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/start-rustfs.log" \
        docker run -d \
            --name "${DOCKER_SMOKE_S3_RUSTFS_CONTAINER}" \
            --network "${DOCKER_SMOKE_S3_NETWORK}" \
            --network-alias "${DOCKER_SMOKE_S3_RUSTFS_ALIAS}" \
            -v "${DOCKER_SMOKE_S3_RUSTFS_VOLUME}:/data" \
            -e "RUSTFS_ACCESS_KEY=${DOCKER_SMOKE_S3_ACCESS_KEY}" \
            -e "RUSTFS_SECRET_KEY=${DOCKER_SMOKE_S3_SECRET_KEY}" \
            -e "RUSTFS_ADDRESS=:9000" \
            "${RUSTFS_IMAGE}" /data
}

s3_wait_for_rustfs() {
    docker_smoke_set_workspace_args "ro"
    s3_prepare_step "wait for RustFS readiness" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/wait-rustfs.log" \
        docker run --rm \
            "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
            --network "${DOCKER_SMOKE_S3_NETWORK}" \
            -e "RUSTFS_ALIAS=${DOCKER_SMOKE_S3_RUSTFS_ALIAS}" \
            "${DOCKER_IMAGE}" \
            sh -lc \
            'for i in $(seq 1 100); do
                nc -z "${RUSTFS_ALIAS}" 9000 && exit 0
                sleep 0.2
            done
            echo "timed out waiting for RustFS" >&2
            exit 1'
}

s3_set_kernel_env() {
    KERNEL_SMOKE_SCENARIO="wal-durable-s3-basic"
    KERNEL_SMOKE_CARGO_FEATURES="s3"
    docker_smoke_set_kernel_env_args "${KERNEL_ARTIFACT_CONTAINER_DIR}"

    DOCKER_SMOKE_ENV_ARGS+=(
        -e "KERNEL_SMOKE_S3_ENDPOINT_URL=$(s3_endpoint_url)"
        -e "KERNEL_SMOKE_S3_BUCKET=${DOCKER_SMOKE_S3_BUCKET}"
        -e "KERNEL_SMOKE_S3_ACCESS_KEY_ID=${DOCKER_SMOKE_S3_ACCESS_KEY}"
        -e "KERNEL_SMOKE_S3_SECRET_ACCESS_KEY=${DOCKER_SMOKE_S3_SECRET_KEY}"
        -e "KERNEL_SMOKE_S3_REGION=${DOCKER_SMOKE_S3_REGION}"
        -e "KERNEL_SMOKE_S3_KEY_PREFIX=${DOCKER_SMOKE_S3_KEY_PREFIX}"
    )
}

s3_set_test_env() {
    DOCKER_SMOKE_S3_TEST_ENV_ARGS=(
        -e "NBD_TEST_S3_ENDPOINT_URL=$(s3_endpoint_url)"
        -e "NBD_TEST_S3_BUCKET=${DOCKER_SMOKE_S3_BUCKET}"
        -e "NBD_TEST_S3_ACCESS_KEY_ID=${DOCKER_SMOKE_S3_ACCESS_KEY}"
        -e "NBD_TEST_S3_SECRET_ACCESS_KEY=${DOCKER_SMOKE_S3_SECRET_KEY}"
        -e "NBD_TEST_S3_REGION=${DOCKER_SMOKE_S3_REGION}"
        -e "NBD_TEST_S3_KEY_PREFIX=${DOCKER_SMOKE_S3_KEY_PREFIX}"
    )
}

s3_run_kernel_smoke() {
    local status=0

    mkdir -p "${KERNEL_ARTIFACT_HOST_DIR}"
    docker_smoke_set_workspace_args "ro"
    docker_smoke_set_artifact_args \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}" \
        "${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}"
    s3_set_kernel_env

    smoke_run "kernel smoke" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/kernel-smoke.log" \
        docker run --rm \
            "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
            --network "${DOCKER_SMOKE_S3_NETWORK}" \
            "${DOCKER_SMOKE_ENV_ARGS[@]}" \
            "${DOCKER_SMOKE_ARTIFACT_ARGS[@]}" \
            --privileged \
            "${DOCKER_IMAGE}" \
            make kernel-smoke-inner || status=$?

    docker_smoke_collect_kernel_artifacts \
        "${KERNEL_ARTIFACT_HOST_DIR}" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"

    if [ "${status}" -ne 0 ]; then
        exit "${status}"
    fi
}

s3_run_prefix_test() {
    docker_smoke_set_workspace_args "ro"
    s3_set_test_env

    smoke_run "S3 prefix assertion" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/s3-prefix-test.log" \
        docker run --rm \
            "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
            --network "${DOCKER_SMOKE_S3_NETWORK}" \
            "${DOCKER_SMOKE_S3_TEST_ENV_ARGS[@]}" \
            -e "NBD_TEST_S3_REQUIRE_NONEMPTY_PREFIX=1" \
            "${DOCKER_IMAGE}" \
            cargo test -p nbd-server --features s3 \
                --test s3_blob_store \
                s3_configured_prefix_contains_objects_when_required -- --exact
}

s3_run_probe() {
    docker_smoke_set_workspace_args "ro"
    s3_set_test_env

    smoke_run "RustFS S3 probe" \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/rustfs-probe.log" \
        docker run --rm \
            "${DOCKER_SMOKE_WORKSPACE_ARGS[@]}" \
            --network "${DOCKER_SMOKE_S3_NETWORK}" \
            "${DOCKER_SMOKE_S3_TEST_ENV_ARGS[@]}" \
            "${DOCKER_IMAGE}" \
            bash scripts/docker/rustfs-s3-probe.sh
}

s3_prepare() {
    mkdir -p "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
    docker_smoke_build_image \
        "${DOCKER_SMOKE_S3_ARTIFACT_DIR}/docker-build.log"
    s3_cleanup_resources
    trap s3_on_exit EXIT
    smoke_step "prepare RustFS sidecar"
    s3_create_resources
    s3_wait_for_rustfs
    smoke_ok "prepare RustFS sidecar"
    s3_print_prepare_logs
}

case "${MODE}" in
    run)
        s3_print_header "docker S3 smoke"
        smoke_state "scenario" "wal-durable-s3-basic"
        s3_prepare
        s3_run_kernel_smoke
        s3_run_prefix_test
        smoke_ok "docker S3 smoke passed"
        smoke_state "artifacts" "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
        ;;
    probe)
        s3_print_header "docker S3 smoke probe"
        s3_prepare
        s3_run_probe
        smoke_ok "docker S3 smoke probe passed"
        smoke_state "artifacts" "${DOCKER_SMOKE_S3_ARTIFACT_DIR}"
        ;;
    down)
        smoke_step "docker S3 smoke cleanup"
        s3_cleanup_resources
        smoke_ok "docker S3 smoke cleanup complete"
        ;;
    *)
        smoke_fail "unknown docker S3 smoke mode: ${MODE}"
        exit 2
        ;;
esac
