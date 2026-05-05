#!/usr/bin/env bash
set -Eeuo pipefail

EXPORT_NAME="${KERNEL_SMOKE_EXPORT:-smoke}"
SIZE_BYTES="${KERNEL_SMOKE_SIZE_BYTES:-67108864}"
ENGINE="${KERNEL_SMOKE_ENGINE:-wal_durable}"
REATTACH="${KERNEL_SMOKE_REATTACH:-}"
PORT="${KERNEL_SMOKE_PORT:-10809}"
DEVICE="${KERNEL_SMOKE_DEVICE:-/dev/nbd0}"
ARTIFACT_DIR="${KERNEL_SMOKE_ARTIFACT_DIR:-}"
RUST_LOG_FILTER="${KERNEL_SMOKE_RUST_LOG:-info,nbd_server::storage=info}"
COMPACTION_SETTLE_SECONDS="${KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS:-0.2}"
LISTEN="127.0.0.1:${PORT}"
ROOT="$(mktemp -d /tmp/nbd-smoke.XXXXXX)"
SMOKE_HOME="${ROOT}/home"
CONFIG="${SMOKE_HOME}/.nbd/config.toml"
CATALOG="${SMOKE_HOME}/.nbd/catalog.db"
LOG_FILE="/tmp/nbd/current.log"
PROBE_EXPECTED="${ROOT}/probe.expected"
SECOND_PROBE_EXPECTED="${ROOT}/probe-second.expected"
SERVER_STDOUT="${ROOT}/server.stdout.log"
SERVER_STDERR="${ROOT}/server.stderr.log"
MOUNT_DIR="/mnt/nbd-smoke"
SERVER_PID=""
DEVICE_CONNECTED=0
MOUNT_CREATED=0
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}"
NBDCLI="${CARGO_TARGET_DIR}/debug/nbdcli"
NBD_SERVER="${CARGO_TARGET_DIR}/debug/nbd-server"
NBD_CLIENT="/usr/sbin/nbd-client"
COMPACTION_CHECKPOINT=""
COMPACTION_ROOT=""

cleanup() {
    set +e
    if [ "${MOUNT_CREATED}" = "1" ]; then
        umount "${MOUNT_DIR}"
    fi
    if [ "${DEVICE_CONNECTED}" = "1" ]; then
        "${NBD_CLIENT}" -d "${DEVICE}" >/dev/null 2>&1
    fi
    if [ -n "${SERVER_PID}" ]; then
        kill "${SERVER_PID}" >/dev/null 2>&1
        wait "${SERVER_PID}" >/dev/null 2>&1
    fi
    export_artifacts
    rm -rf "${ROOT}"
}
trap cleanup EXIT

require_kernel_nbd() {
    if [ -r /proc/config.gz ] &&
        ! zgrep -q "CONFIG_BLK_DEV_NBD=[ym]" /proc/config.gz; then
        echo "kernel config does not report CONFIG_BLK_DEV_NBD support" >&2
    fi
    if ! grep -qw nbd /proc/devices; then
        echo "kernel NBD major is not registered in /proc/devices" >&2
        return 1
    fi
    if [ ! -b "${DEVICE}" ]; then
        echo "${DEVICE} is not available in this privileged container" >&2
        return 1
    fi
}

require_executable() {
    if [ ! -x "$1" ]; then
        echo "required executable is not available: $1" >&2
        return 1
    fi
}

wait_for_server() {
    for _ in $(seq 1 100); do
        if nc -z 127.0.0.1 "${PORT}"; then
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for NBD server on ${LISTEN}" >&2
    return 1
}

should_reattach() {
    if [ -n "${REATTACH}" ]; then
        [ "${REATTACH}" = "1" ]
        return
    fi

    [ "${ENGINE}" = "simple_durable" ] || [ "${ENGINE}" = "wal_durable" ]
}

is_wal_durable() {
    [ "${ENGINE}" = "wal_durable" ]
}

start_server() {
    RUST_LOG="${RUST_LOG_FILTER}" HOME="${SMOKE_HOME}" \
        "${NBD_SERVER}" serve --listen "${LISTEN}" \
        >>"${SERVER_STDOUT}" 2>>"${SERVER_STDERR}" &
    SERVER_PID="$!"
    wait_for_server
}

stop_server() {
    if [ -n "${SERVER_PID}" ]; then
        kill "${SERVER_PID}" >/dev/null 2>&1
        wait "${SERVER_PID}" >/dev/null 2>&1 || true
        SERVER_PID=""
    fi
}

connect_device() {
    "${NBD_CLIENT}" 127.0.0.1 "${PORT}" "${DEVICE}" \
        -name "${EXPORT_NAME}" \
        -block-size 4096
    DEVICE_CONNECTED=1
}

disconnect_device() {
    "${NBD_CLIENT}" -d "${DEVICE}"
    DEVICE_CONNECTED=0
}

mount_device() {
    mount -t ext4 "${DEVICE}" "${MOUNT_DIR}"
    MOUNT_CREATED=1
}

unmount_device() {
    umount "${MOUNT_DIR}"
    MOUNT_CREATED=0
}

inspect_export() {
    HOME="${SMOKE_HOME}" "${NBDCLI}" inspect "${EXPORT_NAME}"
}

write_inspect_artifact() {
    local label="$1"
    local path="${ROOT}/inspect-${label}.txt"
    inspect_export >"${path}"
    echo "${path}"
}

inspect_field() {
    local path="$1"
    local field="$2"
    awk -F': ' -v field="${field}" '$1 == field { print $2 }' "${path}"
}

wait_for_wal_compaction() {
    local target_checkpoint="$1"
    local label="$2"
    local path checkpoint root

    for _ in $(seq 1 500); do
        path="$(write_inspect_artifact "${label}")"
        checkpoint="$(inspect_field "${path}" "checkpoint_wal_seq")"
        root="$(inspect_field "${path}" "root_node_id")"
        if [[ "${checkpoint}" =~ ^[0-9]+$ ]] &&
            [ "${checkpoint}" -ge "${target_checkpoint}" ] &&
            [ "${root}" != "<empty>" ]; then
            COMPACTION_CHECKPOINT="${checkpoint}"
            COMPACTION_ROOT="${root}"
            return 0
        fi
        sleep 0.02
    done

    echo "timed out waiting for ${EXPORT_NAME} compaction checkpoint ${target_checkpoint}" >&2
    return 1
}

write_probe_lines() {
    local path="$1"
    local prefix="$2"

    : >"${path}"
    for i in $(seq 1 4096); do
        printf "%s line %04d\n" "${prefix}" "${i}" >>"${path}"
    done
}

settle_compaction() {
    sleep "${COMPACTION_SETTLE_SECONDS}"
}

export_artifacts() {
    if [ -z "${ARTIFACT_DIR}" ]; then
        return 0
    fi

    mkdir -p "${ARTIFACT_DIR}"
    if [ -f "${LOG_FILE}" ]; then
        cp -f "${LOG_FILE}" "${ARTIFACT_DIR}/current.log"
    fi
    if [ -f "${SERVER_STDOUT}" ]; then
        cp -f "${SERVER_STDOUT}" "${ARTIFACT_DIR}/server.stdout.log"
    fi
    if [ -f "${SERVER_STDERR}" ]; then
        cp -f "${SERVER_STDERR}" "${ARTIFACT_DIR}/server.stderr.log"
    fi
    if [ -f "${CONFIG}" ]; then
        cp -f "${CONFIG}" "${ARTIFACT_DIR}/config.toml"
    fi
    find "${ROOT}" -maxdepth 1 -name 'inspect-*.txt' -exec cp -f {} "${ARTIFACT_DIR}/" \;
}

mkdir -p "${MOUNT_DIR}"
mkdir -p "$(dirname "${LOG_FILE}")"
rm -f "${LOG_FILE}"

require_kernel_nbd
require_executable "${NBD_CLIENT}"
if mountpoint -q "${MOUNT_DIR}"; then
    echo "${MOUNT_DIR} is already a mount point" >&2
    exit 1
fi
if "${NBD_CLIENT}" -c "${DEVICE}" >/dev/null 2>&1; then
    echo "${DEVICE} is already connected" >&2
    exit 1
fi

mkdir -p "$(dirname "${CATALOG}")"
DATABASE_URL="file:${CATALOG}" make -C prisma db-migrate
make build-tools
require_executable "${NBDCLI}"
require_executable "${NBD_SERVER}"

HOME="${SMOKE_HOME}" "${NBDCLI}" create "${EXPORT_NAME}" \
    --size "${SIZE_BYTES}" \
    --block-size 4096 \
    --engine "${ENGINE}"
test -f "${CONFIG}"

start_server
connect_device

mkfs.ext4 -F -E nodiscard "${DEVICE}"
mount_device

write_probe_lines "${PROBE_EXPECTED}" "nbd kernel smoke"
cp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"
sync
echo 3 >/proc/sys/vm/drop_caches
cmp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"

if should_reattach; then
    FIRST_ROOT=""
    FIRST_CHECKPOINT=0
    unmount_device
    disconnect_device
    if is_wal_durable; then
        settle_compaction
        wait_for_wal_compaction 1 "first-close"
        FIRST_ROOT="${COMPACTION_ROOT}"
        FIRST_CHECKPOINT="${COMPACTION_CHECKPOINT}"
        write_inspect_artifact "before-second-open" >/dev/null
    fi
    stop_server

    start_server
    connect_device
    mount_device
    echo 3 >/proc/sys/vm/drop_caches
    cmp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"

    if is_wal_durable; then
        write_probe_lines "${SECOND_PROBE_EXPECTED}" "nbd kernel smoke second"
        cp "${SECOND_PROBE_EXPECTED}" "${MOUNT_DIR}/probe-second.txt"
        sync
        echo 3 >/proc/sys/vm/drop_caches
        cmp "${SECOND_PROBE_EXPECTED}" "${MOUNT_DIR}/probe-second.txt"
    fi
fi

unmount_device
disconnect_device
if is_wal_durable; then
    settle_compaction
    if [ -n "${FIRST_ROOT:-}" ]; then
        wait_for_wal_compaction "$((FIRST_CHECKPOINT + 1))" "second-close"
        if [ "${COMPACTION_ROOT}" = "${FIRST_ROOT}" ]; then
            echo "second compaction reused root ${COMPACTION_ROOT}" >&2
            exit 1
        fi
    else
        wait_for_wal_compaction 1 "final-close"
    fi
fi
stop_server
export_artifacts

echo "kernel NBD smoke passed"
