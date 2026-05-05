#!/usr/bin/env bash
set -Eeuo pipefail

EXPORT_NAME="${KERNEL_SMOKE_EXPORT:-smoke}"
SIZE_BYTES="${KERNEL_SMOKE_SIZE_BYTES:-67108864}"
ENGINE="${KERNEL_SMOKE_ENGINE:-simple_durable}"
REATTACH="${KERNEL_SMOKE_REATTACH:-}"
PORT="${KERNEL_SMOKE_PORT:-10809}"
DEVICE="${KERNEL_SMOKE_DEVICE:-/dev/nbd0}"
LISTEN="127.0.0.1:${PORT}"
ROOT="$(mktemp -d /tmp/nbd-smoke.XXXXXX)"
SMOKE_HOME="${ROOT}/home"
CONFIG="${SMOKE_HOME}/.nbd/config.toml"
CATALOG="${SMOKE_HOME}/.nbd/catalog.db"
PROBE_EXPECTED="${ROOT}/probe.expected"
MOUNT_DIR="/mnt/nbd-smoke"
SERVER_PID=""
DEVICE_CONNECTED=0
MOUNT_CREATED=0
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}"
NBDCLI="${CARGO_TARGET_DIR}/debug/nbdcli"
NBD_SERVER="${CARGO_TARGET_DIR}/debug/nbd-server"
NBD_CLIENT="/usr/sbin/nbd-client"

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

    [ "${ENGINE}" = "simple_durable" ]
}

start_server() {
    HOME="${SMOKE_HOME}" "${NBD_SERVER}" serve --listen "${LISTEN}" &
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

mkdir -p "${MOUNT_DIR}"

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

: >"${PROBE_EXPECTED}"
for i in $(seq 1 4096); do
    printf "nbd kernel smoke line %04d\n" "${i}" >>"${PROBE_EXPECTED}"
done
cp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"
sync
echo 3 >/proc/sys/vm/drop_caches
cmp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"

if should_reattach; then
    unmount_device
    disconnect_device
    stop_server

    start_server
    connect_device
    mount_device
    echo 3 >/proc/sys/vm/drop_caches
    cmp "${PROBE_EXPECTED}" "${MOUNT_DIR}/probe.txt"
fi

unmount_device
disconnect_device
stop_server

echo "kernel NBD smoke passed"
