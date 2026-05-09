#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/smoke-log.sh"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nbd-smoke-log-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

fail() {
    echo "test-smoke-log: $*" >&2
    exit 1
}

assert_contains() {
    local needle="$1"
    local path="$2"

    grep -Fq -- "${needle}" "${path}" ||
        fail "expected ${path} to contain ${needle}"
}

assert_not_contains() {
    local needle="$1"
    local path="$2"

    if grep -Fq -- "${needle}" "${path}"; then
        fail "did not expect ${path} to contain ${needle}"
    fi
}

smoke_run "passing step" "${TMP_DIR}/pass.log" \
    bash -c 'printf "pass output\n"' >"${TMP_DIR}/pass.out" 2>&1
assert_contains "ok: passing step" "${TMP_DIR}/pass.out"
assert_contains "pass output" "${TMP_DIR}/pass.log"

SMOKE_PROGRESS_POLL_SECONDS=0.01 \
    smoke_run_with_progress \
        "progress step" \
        "${TMP_DIR}/progress.log" \
        "${TMP_DIR}/progress-events.log" \
        bash -c '
            printf "first milestone\n" >>"$1"
            printf "hidden command output\n"
            sleep 0.05
            printf "second milestone\n" >>"$1"
        ' _ "${TMP_DIR}/progress-events.log" \
        >"${TMP_DIR}/progress.out" 2>&1
assert_contains "progress:" "${TMP_DIR}/progress.out"
assert_contains "first milestone" "${TMP_DIR}/progress.out"
assert_contains "second milestone" "${TMP_DIR}/progress.out"
assert_contains "ok: progress step" "${TMP_DIR}/progress.out"
assert_not_contains "hidden command output" "${TMP_DIR}/progress.out"
assert_contains "hidden command output" "${TMP_DIR}/progress.log"

if smoke_run "failing step" "${TMP_DIR}/fail.log" \
    bash -c 'echo boom >&2; exit 7' >"${TMP_DIR}/fail.out" 2>&1; then
    fail "expected smoke_run to return a failing status"
fi
assert_contains "failing step failed with exit status 7" "${TMP_DIR}/fail.out"
assert_contains "boom" "${TMP_DIR}/fail.out"
assert_contains "boom" "${TMP_DIR}/fail.log"

VERBOSE=1 smoke_redacted_command docker run \
    -e NBD_TEST_S3_SECRET_ACCESS_KEY=rustfsadmin \
    -e KERNEL_SMOKE_S3_KEY_PREFIX=v0.1/blobs/ \
    -e NBD_TEST_S3_BUCKET=everstore \
    >"${TMP_DIR}/redacted.out"
assert_contains "NBD_TEST_S3_SECRET_ACCESS_KEY" "${TMP_DIR}/redacted.out"
assert_contains "KERNEL_SMOKE_S3_KEY_PREFIX" "${TMP_DIR}/redacted.out"
assert_contains "NBD_TEST_S3_BUCKET=everstore" "${TMP_DIR}/redacted.out"
assert_not_contains "rustfsadmin" "${TMP_DIR}/redacted.out"
assert_not_contains "v0.1/blobs/" "${TMP_DIR}/redacted.out"

smoke_run_quiet "quiet failure" bash -c 'echo hidden; exit 9' \
    >"${TMP_DIR}/quiet.out" 2>&1
assert_not_contains "hidden" "${TMP_DIR}/quiet.out"
