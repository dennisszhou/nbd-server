#!/usr/bin/env bash

run_smoke_scenario() {
    local first_root
    local first_wal_seq

    create_export "wal_durable"

    start_server
    connect_device
    format_device
    mount_device

    write_and_verify_probe "${PROBE_EXPECTED}" "nbd kernel smoke" "probe.txt"

    unmount_device
    disconnect_device
    settle_compaction
    wait_for_wal_compaction 1 "first-close"
    first_root="${COMPACTION_ROOT}"
    first_wal_seq="${COMPACTION_WAL_SEQ}"
    write_inspect_artifact "before-second-open" >/dev/null
    stop_server

    start_server
    connect_device
    wait_for_wal_reattach_base "${first_wal_seq}"
    mount_device
    verify_probe "${PROBE_EXPECTED}" "probe.txt"
    write_and_verify_probe \
        "${SECOND_PROBE_EXPECTED}" \
        "nbd kernel smoke second" \
        "probe-second.txt"

    unmount_device
    disconnect_device
    settle_compaction
    wait_for_wal_compaction "$((first_wal_seq + 1))" "second-close"
    if [ "${COMPACTION_ROOT}" = "${first_root}" ]; then
        echo "second compaction reused root ${COMPACTION_ROOT}" >&2
        exit 1
    fi
    stop_server
}
