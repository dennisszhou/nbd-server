#!/usr/bin/env bash
set -Eeuo pipefail

EXPORT_NAME="${KERNEL_SMOKE_EXPORT:-smoke}"
SIZE_BYTES="${KERNEL_SMOKE_SIZE_BYTES:-67108864}"
PORT="${KERNEL_SMOKE_PORT:-10809}"
DEVICE="${KERNEL_SMOKE_DEVICE:-/dev/nbd0}"
LISTEN="127.0.0.1:${PORT}"
ROOT="$(mktemp -d /tmp/nbd-smoke.XXXXXX)"
CONFIG="${ROOT}/config.toml"
CATALOG="${ROOT}/catalog.db"
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
    echo "timed out waiting for toy NBD server on ${LISTEN}" >&2
    return 1
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

cat >"${CONFIG}" <<EOF
[catalog]
url = "file:${CATALOG}"

[runtime]
state_dir = "${ROOT}/state"
EOF

mkdir -p "${ROOT}/state"
DATABASE_URL="file:${CATALOG}" make -C prisma db-migrate
make build-tools
require_executable "${NBDCLI}"
require_executable "${NBD_SERVER}"

"${NBDCLI}" --config "${CONFIG}" create "${EXPORT_NAME}" \
    --size "${SIZE_BYTES}" \
    --block-size 4096

"${NBD_SERVER}" serve --config "${CONFIG}" --listen "${LISTEN}" &
SERVER_PID="$!"
wait_for_server

"${NBD_CLIENT}" 127.0.0.1 "${PORT}" "${DEVICE}" \
    -name "${EXPORT_NAME}" \
    -block-size 4096
DEVICE_CONNECTED=1

mkfs.ext4 -F -E nodiscard "${DEVICE}"
mount -t ext4 "${DEVICE}" "${MOUNT_DIR}"
MOUNT_CREATED=1

printf "nbd kernel smoke\n" >"${MOUNT_DIR}/probe.txt"
sync
test "$(cat "${MOUNT_DIR}/probe.txt")" = "nbd kernel smoke"

umount "${MOUNT_DIR}"
MOUNT_CREATED=0
"${NBD_CLIENT}" -d "${DEVICE}"
DEVICE_CONNECTED=0
kill "${SERVER_PID}"
wait "${SERVER_PID}" || true
SERVER_PID=""

echo "kernel NBD smoke passed"
