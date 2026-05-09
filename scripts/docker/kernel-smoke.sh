#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS="${SCRIPT_DIR}/kernel-smoke/harness.sh"

resolve_scenario() {
    if [ -n "${KERNEL_SMOKE_SCENARIO:-}" ]; then
        printf "%s\n" "${KERNEL_SMOKE_SCENARIO}"
        return 0
    fi

    case "${KERNEL_SMOKE_ENGINE:-wal_durable}" in
        memory)
            printf "%s\n" "memory-basic"
            ;;
        simple_durable | simple-durable)
            printf "%s\n" "simple-durable-basic"
            ;;
        wal_durable | wal-durable)
            printf "%s\n" "wal-durable-basic"
            ;;
        *)
            echo "unknown kernel smoke engine: ${KERNEL_SMOKE_ENGINE}" >&2
            return 1
            ;;
    esac
}

SCENARIO="$(resolve_scenario)"
SCENARIO_FILE="${SCRIPT_DIR}/kernel-smoke/scenarios/${SCENARIO}.sh"

case "${SCENARIO}" in
    memory-basic | simple-durable-basic | wal-durable-basic | \
        wal-durable-s3-basic)
        ;;
    *)
        echo "unknown kernel smoke scenario: ${SCENARIO}" >&2
        echo "available scenarios: memory-basic, simple-durable-basic," \
            "wal-durable-basic, wal-durable-s3-basic" >&2
        exit 1
        ;;
esac

if [ ! -f "${SCENARIO_FILE}" ]; then
    echo "unknown kernel smoke scenario: ${SCENARIO}" >&2
    exit 1
fi

source "${HARNESS}"
source "${SCENARIO_FILE}"

kernel_progress "scenario ${SCENARIO}"
prepare_kernel_smoke
run_smoke_scenario

kernel_progress "export artifacts"
export_artifacts
echo "kernel NBD smoke scenario ${SCENARIO} passed"
