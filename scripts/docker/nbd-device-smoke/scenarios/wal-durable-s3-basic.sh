#!/usr/bin/env bash

run_smoke_scenario() {
    local first_root
    local first_wal_seq
    local clone_root
    local clone_wal_seq

    prepare_clone_nbd_device
    create_export "wal_durable"
    configure_s3_blob_store

    start_server
    connect_device
    format_device
    mount_device

    write_and_verify_probe "${PROBE_EXPECTED}" "nbd device smoke s3" "probe.txt"

    unmount_device
    disconnect_device
    settle_compaction
    wait_for_wal_compaction 1 "first-close"
    first_root="${COMPACTION_ROOT}"
    first_wal_seq="${COMPACTION_WAL_SEQ}"
    write_inspect_artifact "before-second-open" >/dev/null
    stop_server

    clone_export "${EXPORT_NAME}" "${CLONE_EXPORT_NAME}"
    assert_export_field \
        "${CLONE_EXPORT_NAME}" \
        "root_node_id" \
        "${first_root}" \
        "clone-created"
    assert_export_field \
        "${CLONE_EXPORT_NAME}" \
        "base_wal_seq" \
        "0" \
        "clone-created"

    start_server
    connect_device "${CLONE_EXPORT_NAME}" "${CLONE_DEVICE}"
    mount_device "${CLONE_DEVICE}" "${CLONE_MOUNT_DIR}"
    verify_probe "${PROBE_EXPECTED}" "probe.txt" "${CLONE_MOUNT_DIR}"
    write_and_verify_probe \
        "${CLONE_PROBE_EXPECTED}" \
        "nbd device smoke s3 clone" \
        "probe-clone.txt" \
        "${CLONE_MOUNT_DIR}"
    unmount_device "${CLONE_MOUNT_DIR}"
    disconnect_device "${CLONE_DEVICE}"
    settle_compaction
    wait_for_wal_compaction 1 "clone-close" "${CLONE_EXPORT_NAME}"
    clone_root="${COMPACTION_ROOT}"
    clone_wal_seq="${COMPACTION_WAL_SEQ}"
    if [ "${clone_root}" = "${first_root}" ]; then
        echo "clone compaction reused source root ${clone_root}" >&2
        exit 1
    fi
    stop_server

    start_server
    connect_device "${EXPORT_NAME}" "${DEVICE}"
    connect_device "${CLONE_EXPORT_NAME}" "${CLONE_DEVICE}"
    wait_for_wal_reattach_base "${first_wal_seq}"
    wait_for_wal_reattach_base "${clone_wal_seq}" "${CLONE_EXPORT_NAME}"
    mount_device "${DEVICE}" "${MOUNT_DIR}"
    mount_device "${CLONE_DEVICE}" "${CLONE_MOUNT_DIR}" "ro"
    verify_probe "${PROBE_EXPECTED}" "probe.txt" "${MOUNT_DIR}"
    verify_absent "probe-clone.txt" "${MOUNT_DIR}"
    verify_probe "${PROBE_EXPECTED}" "probe.txt" "${CLONE_MOUNT_DIR}"
    verify_probe "${CLONE_PROBE_EXPECTED}" "probe-clone.txt" "${CLONE_MOUNT_DIR}"
    write_and_verify_probe \
        "${SECOND_PROBE_EXPECTED}" \
        "nbd device smoke s3 second" \
        "probe-second.txt" \
        "${MOUNT_DIR}"
    verify_absent "probe-second.txt" "${CLONE_MOUNT_DIR}"

    unmount_device "${CLONE_MOUNT_DIR}"
    unmount_device "${MOUNT_DIR}"
    disconnect_device "${CLONE_DEVICE}"
    disconnect_device "${DEVICE}"
    settle_compaction
    wait_for_wal_compaction "$((first_wal_seq + 1))" "second-close"
    if [ "${COMPACTION_ROOT}" = "${first_root}" ]; then
        echo "second compaction reused root ${COMPACTION_ROOT}" >&2
        exit 1
    fi
    stop_server
}
