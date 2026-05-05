#!/usr/bin/env bash

run_smoke_scenario() {
    create_export "simple_durable"

    start_server
    connect_device
    format_device
    mount_device

    write_and_verify_probe "${PROBE_EXPECTED}" "nbd kernel smoke" "probe.txt"

    unmount_device
    disconnect_device
    stop_server

    start_server
    connect_device
    mount_device
    verify_probe "${PROBE_EXPECTED}" "probe.txt"

    unmount_device
    disconnect_device
    stop_server
}
