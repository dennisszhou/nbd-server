#!/usr/bin/env bash

docker_smoke_init_defaults() {
    REPO_ROOT="${REPO_ROOT:-$(pwd)}"
    DOCKER_IMAGE="${DOCKER_IMAGE:-nbd-server-dev:local}"
    DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/arm64}"
    DOCKER_WORKDIR="${DOCKER_WORKDIR:-/work}"
    DOCKER_CARGO_TARGET_DIR="${DOCKER_CARGO_TARGET_DIR:-/cargo-target}"
    DOCKER_PATH="${DOCKER_PATH:-${DOCKER_CARGO_TARGET_DIR}/debug:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}"
    KERNEL_SMOKE_OUTPUT="${KERNEL_SMOKE_OUTPUT:-docker-smoke}"
    DOCKER_KERNEL_SMOKE_ARTIFACT_DIR="${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR:-${REPO_ROOT}/.tmp/${KERNEL_SMOKE_OUTPUT}}"
    DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT="${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT:-/tmp/nbd-smoke-artifacts}"

    case "${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}" in
        /*)
            ;;
        *)
            DOCKER_KERNEL_SMOKE_ARTIFACT_DIR="${REPO_ROOT}/${DOCKER_KERNEL_SMOKE_ARTIFACT_DIR}"
            ;;
    esac
}

docker_smoke_effective_scenario() {
    if [ -n "${KERNEL_SMOKE_SCENARIO:-}" ]; then
        printf '%s\n' "${KERNEL_SMOKE_SCENARIO}"
        return 0
    fi

    case "${KERNEL_SMOKE_ENGINE:-wal_durable}" in
        memory)
            printf '%s\n' "memory-basic"
            ;;
        simple_durable | simple-durable)
            printf '%s\n' "simple-durable-basic"
            ;;
        wal_durable | wal-durable)
            printf '%s\n' "wal-durable-basic"
            ;;
        *)
            printf '%s\n' "unknown"
            ;;
    esac
}

docker_smoke_build_image() {
    local log_path="$1"

    smoke_run "build Docker image" "${log_path}" \
        docker buildx build \
            --platform "${DOCKER_PLATFORM}" \
            -t "${DOCKER_IMAGE}" \
            -f docker/Dockerfile \
            --load \
            .
}

docker_smoke_set_workspace_args() {
    local mode="$1"
    local workspace_mount="${REPO_ROOT}:${DOCKER_WORKDIR}"

    if [ "${mode}" = "ro" ]; then
        workspace_mount="${workspace_mount}:ro"
    fi

    DOCKER_SMOKE_WORKSPACE_ARGS=(
        --platform "${DOCKER_PLATFORM}"
        -v "${workspace_mount}"
        -v "nbd-cargo-registry:/usr/local/cargo/registry"
        -v "nbd-cargo-git:/usr/local/cargo/git"
        -v "nbd-cargo-target:${DOCKER_CARGO_TARGET_DIR}"
        -e "CARGO_TARGET_DIR=${DOCKER_CARGO_TARGET_DIR}"
        -e "PATH=${DOCKER_PATH}"
        -w "${DOCKER_WORKDIR}"
    )
}

docker_smoke_set_artifact_args() {
    local host_dir="$1"
    local container_dir="$2"

    DOCKER_SMOKE_ARTIFACT_ARGS=(
        -v "${host_dir}:${container_dir}"
    )
}

docker_smoke_add_env_if_set() {
    local name="$1"
    local value="$2"

    if [ -n "${value}" ]; then
        DOCKER_SMOKE_ENV_ARGS+=(-e "${name}=${value}")
    fi
}

docker_smoke_set_kernel_env_args() {
    local artifact_dir="${1:-${DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT}}"

    DOCKER_SMOKE_ENV_ARGS=()

    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_EXPORT" \
        "${KERNEL_SMOKE_EXPORT:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_SCENARIO" \
        "${KERNEL_SMOKE_SCENARIO:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_SIZE_BYTES" \
        "${KERNEL_SMOKE_SIZE_BYTES:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_ENGINE" \
        "${KERNEL_SMOKE_ENGINE:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_CARGO_FEATURES" \
        "${KERNEL_SMOKE_CARGO_FEATURES:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_PORT" \
        "${KERNEL_SMOKE_PORT:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_DEVICE" \
        "${KERNEL_SMOKE_DEVICE:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_RUST_LOG" \
        "${KERNEL_SMOKE_RUST_LOG:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_ENDPOINT_URL" \
        "${KERNEL_SMOKE_S3_ENDPOINT_URL:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_BUCKET" \
        "${KERNEL_SMOKE_S3_BUCKET:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_ACCESS_KEY_ID" \
        "${KERNEL_SMOKE_S3_ACCESS_KEY_ID:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_SECRET_ACCESS_KEY" \
        "${KERNEL_SMOKE_S3_SECRET_ACCESS_KEY:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_REGION" \
        "${KERNEL_SMOKE_S3_REGION:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_S3_KEY_PREFIX" \
        "${KERNEL_SMOKE_S3_KEY_PREFIX:-}"
    docker_smoke_add_env_if_set \
        "KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS" \
        "${KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS:-}"

    DOCKER_SMOKE_ENV_ARGS+=(
        -e "KERNEL_SMOKE_ARTIFACT_DIR=${artifact_dir}"
    )
}

docker_smoke_collect_kernel_artifacts() {
    local source_dir="$1"
    local destination_dir="$2"

    if [ ! -d "${source_dir}" ]; then
        return 0
    fi

    mkdir -p "${destination_dir}"
    find "${source_dir}" -maxdepth 1 -type f \
        -exec cp -f {} "${destination_dir}/" \;
}
