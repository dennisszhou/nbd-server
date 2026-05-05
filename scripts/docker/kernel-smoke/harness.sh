#!/usr/bin/env bash

EXPORT_NAME="${KERNEL_SMOKE_EXPORT:-smoke}"
CLONE_EXPORT_NAME="${KERNEL_SMOKE_CLONE_EXPORT:-${EXPORT_NAME}-clone}"
SIZE_BYTES="${KERNEL_SMOKE_SIZE_BYTES:-67108864}"
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
CLONE_PROBE_EXPECTED="${ROOT}/probe-clone.expected"
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
COMPACTION_WAL_SEQ=""
COMPACTION_ROOT=""
ARTIFACTS_CLEARED=0

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
    local export_name="${1:-${EXPORT_NAME}}"

    "${NBD_CLIENT}" 127.0.0.1 "${PORT}" "${DEVICE}" \
        -name "${export_name}" \
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

format_device() {
    mkfs.ext4 -F -E nodiscard "${DEVICE}"
}

drop_page_cache() {
    echo 3 >/proc/sys/vm/drop_caches
}

inspect_export() {
    local export_name="${1:-${EXPORT_NAME}}"

    HOME="${SMOKE_HOME}" "${NBDCLI}" inspect "${export_name}"
}

write_inspect_artifact() {
    local label="$1"
    local export_name="${2:-${EXPORT_NAME}}"
    local path="${ROOT}/inspect-${label}.txt"
    inspect_export "${export_name}" >"${path}"
    echo "${path}"
}

inspect_field() {
    local path="$1"
    local field="$2"
    awk -F': ' -v field="${field}" '$1 == field { print $2 }' "${path}"
}

wait_for_wal_compaction() {
    local target_wal_seq="$1"
    local label="$2"
    local export_name="${3:-${EXPORT_NAME}}"
    local path wal_seq root

    for _ in $(seq 1 500); do
        path="$(write_inspect_artifact "${label}" "${export_name}")"
        wal_seq="$(inspect_field "${path}" "checkpoint_wal_seq")"
        root="$(inspect_field "${path}" "root_node_id")"
        if [[ "${wal_seq}" =~ ^[0-9]+$ ]] &&
            [ "${wal_seq}" -ge "${target_wal_seq}" ] &&
            [ "${root}" != "<empty>" ]; then
            COMPACTION_WAL_SEQ="${wal_seq}"
            COMPACTION_ROOT="${root}"
            return 0
        fi
        sleep 0.02
    done

    echo "timed out waiting for ${export_name} compaction WAL sequence" \
        "${target_wal_seq}" >&2
    return 1
}

wait_for_wal_reattach_base() {
    local expected_base="$1"
    local export_name="${2:-${EXPORT_NAME}}"

    for _ in $(seq 1 100); do
        if node - "${LOG_FILE}" "${export_name}" "${expected_base}" <<'NODE'
const fs = require("fs");

const [, , logFile, exportName, expectedRaw] = process.argv;
const expected = Number(expectedRaw);
const contents = fs.existsSync(logFile) ? fs.readFileSync(logFile, "utf8") : "";
const lines = contents.split(/\n/).filter(Boolean);
let compactionLine = -1;
let rootLine = -1;

for (let i = 0; i < lines.length; i++) {
    let fields;
    try {
        fields = JSON.parse(lines[i]).fields || {};
    } catch {
        continue;
    }

    if (
        fields.event === "wal.compaction.completed" &&
        Number(fields.target_wal_seq) === expected
    ) {
        compactionLine = i;
    }

    if (
        compactionLine >= 0 &&
        i > compactionLine &&
        fields.event === "wal.root.loaded" &&
        fields.export_name === exportName &&
        Number(fields.base_wal_seq) === expected &&
        fields.root_node_id &&
        fields.root_node_id !== "<empty>"
    ) {
        rootLine = i;
    }

    if (
        rootLine >= 0 &&
        i > rootLine &&
        fields.event === "wal.replay.completed" &&
        fields.export_name === exportName &&
        Number(fields.base_wal_seq) === expected &&
        Number(fields.replayed_through_wal_seq) === expected
    ) {
        process.exit(0);
    }
}

process.exit(1);
NODE
        then
            return 0
        fi
        sleep 0.02
    done

    echo "timed out waiting for ${export_name} WAL reattach base" \
        "${expected_base}" >&2
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

write_and_verify_probe() {
    local expected_path="$1"
    local prefix="$2"
    local target_name="$3"

    write_probe_lines "${expected_path}" "${prefix}"
    cp "${expected_path}" "${MOUNT_DIR}/${target_name}"
    sync
    verify_probe "${expected_path}" "${target_name}"
}

verify_probe() {
    local expected_path="$1"
    local target_name="$2"

    drop_page_cache
    cmp "${expected_path}" "${MOUNT_DIR}/${target_name}"
}

verify_absent() {
    local target_name="$1"

    if [ -e "${MOUNT_DIR}/${target_name}" ]; then
        echo "${target_name} unexpectedly exists on mounted export" >&2
        return 1
    fi
}

settle_compaction() {
    sleep "${COMPACTION_SETTLE_SECONDS}"
}

require_clearable_artifact_dir() {
    case "${ARTIFACT_DIR}" in
        "" | "/" | "/bin" | "/boot" | "/dev" | "/etc" | "/home" | "/lib" | \
            "/lib64" | "/media" | "/mnt" | "/opt" | "/proc" | "/root" | \
            "/run" | "/sbin" | "/srv" | "/sys" | "/tmp" | "/usr" | "/var")
            echo "refusing to clear unsafe artifact dir: ${ARTIFACT_DIR}" >&2
            return 1
            ;;
    esac
}

clear_artifact_dir() {
    if [ -z "${ARTIFACT_DIR}" ]; then
        return 0
    fi

    if [ "${ARTIFACTS_CLEARED}" = "1" ]; then
        return 0
    fi

    require_clearable_artifact_dir
    mkdir -p "${ARTIFACT_DIR}"
    find "${ARTIFACT_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    ARTIFACTS_CLEARED=1
}

export_artifacts() {
    if [ -z "${ARTIFACT_DIR}" ]; then
        return 0
    fi

    clear_artifact_dir
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
    find "${ROOT}" -maxdepth 1 -name 'inspect-*.txt' \
        -exec cp -f {} "${ARTIFACT_DIR}/" \;
}

prepare_kernel_smoke() {
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
}

create_export() {
    local engine="$1"
    local export_name="${2:-${EXPORT_NAME}}"

    HOME="${SMOKE_HOME}" "${NBDCLI}" create "${export_name}" \
        --size "${SIZE_BYTES}" \
        --block-size 4096 \
        --engine "${engine}"
    test -f "${CONFIG}"
}

clone_export() {
    local source_name="${1:-${EXPORT_NAME}}"
    local destination_name="${2:-${CLONE_EXPORT_NAME}}"

    HOME="${SMOKE_HOME}" "${NBDCLI}" clone \
        "${source_name}" \
        "${destination_name}"
}

assert_export_field() {
    local export_name="$1"
    local field="$2"
    local expected="$3"
    local label="$4"
    local path actual

    path="$(write_inspect_artifact "${label}" "${export_name}")"
    actual="$(inspect_field "${path}" "${field}")"
    if [ "${actual}" != "${expected}" ]; then
        echo "${export_name} ${field} expected ${expected}, got ${actual}" >&2
        return 1
    fi
}
