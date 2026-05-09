#!/usr/bin/env bash

run_smoke_scenario() {
    create_export "memory"

    start_server
    connect_device
    format_device
    mount_device

    write_and_verify_probe "${PROBE_EXPECTED}" "nbd device smoke" "probe.txt"

    unmount_device
    disconnect_device
    stop_server
}
