#!/usr/bin/env bash

smoke_verbose() {
    [ "${VERBOSE:-0}" = "1" ]
}

smoke_step() {
    printf '==> %s\n' "$1"
}

smoke_state() {
    printf '  %-18s %s\n' "$1:" "$2"
}

smoke_ok() {
    printf 'ok: %s\n' "$1"
}

smoke_warn() {
    printf 'warning: %s\n' "$1" >&2
}

smoke_fail() {
    printf 'error: %s\n' "$1" >&2
}

smoke_is_sensitive_name() {
    local name

    name="$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')"

    [[ "${name}" =~ SECRET|PASSWORD|TOKEN|ACCESS_KEY|(^|_)KEY($|_) ]]
}

smoke_redacted_arg() {
    local arg="$1"
    local name

    if [[ "${arg}" == *=* ]]; then
        name="${arg%%=*}"
        if smoke_is_sensitive_name "${name}"; then
            printf '%s=<redacted>' "${name}"
            return
        fi
    fi

    printf '%s' "${arg}"
}

smoke_redacted_command() {
    local first=1
    local arg redacted

    for arg in "$@"; do
        redacted="$(smoke_redacted_arg "${arg}")"
        if [ "${first}" = "1" ]; then
            first=0
        else
            printf ' '
        fi
        printf '%q' "${redacted}"
    done
    printf '\n'
}

smoke_tail_log() {
    local log_path="$1"
    local lines="${SMOKE_LOG_TAIL_LINES:-80}"

    if [ ! -s "${log_path}" ]; then
        smoke_warn "log is empty: ${log_path}"
        return 0
    fi

    printf '  log tail (%s):\n' "${log_path}" >&2
    tail -n "${lines}" "${log_path}" | sed 's/^/    /' >&2
}

smoke_run() {
    local label="$1"
    local log_path="$2"
    local status
    shift 2

    smoke_step "${label}"
    mkdir -p "$(dirname "${log_path}")"
    if smoke_verbose; then
        printf '  command: '
        smoke_redacted_command "$@"
    fi

    if "$@" >"${log_path}" 2>&1; then
        smoke_ok "${label}"
        smoke_state "log" "${log_path}"
        return 0
    else
        status=$?
    fi

    smoke_fail "${label} failed with exit status ${status}"
    smoke_state "log" "${log_path}" >&2
    smoke_tail_log "${log_path}"
    return "${status}"
}

smoke_follow_progress() {
    local progress_path="$1"
    local stop_path="$2"
    local next_line=1
    local total line

    while :; do
        if [ -f "${progress_path}" ]; then
            total="$(wc -l <"${progress_path}" | tr -d '[:space:]')"
            if [ -z "${total}" ]; then
                total=0
            fi

            if [ "${total}" -ge "${next_line}" ]; then
                sed -n "${next_line},${total}p" "${progress_path}" |
                    while IFS= read -r line; do
                        if [ -n "${line}" ]; then
                            smoke_state "progress" "${line}"
                        fi
                    done
                next_line=$((total + 1))
            fi
        fi

        if [ -e "${stop_path}" ]; then
            break
        fi

        sleep "${SMOKE_PROGRESS_POLL_SECONDS:-0.2}"
    done
}

smoke_run_with_progress() {
    local label="$1"
    local log_path="$2"
    local progress_path="$3"
    local status=0
    local command_pid progress_pid stop_path
    shift 3

    smoke_step "${label}"
    mkdir -p "$(dirname "${log_path}")"
    mkdir -p "$(dirname "${progress_path}")"
    : >"${progress_path}"
    stop_path="${progress_path}.done.$$"
    rm -f "${stop_path}"

    if smoke_verbose; then
        printf '  command: '
        smoke_redacted_command "$@"
    fi

    smoke_follow_progress "${progress_path}" "${stop_path}" &
    progress_pid=$!

    "$@" >"${log_path}" 2>&1 &
    command_pid=$!
    wait "${command_pid}" || status=$?

    : >"${stop_path}"
    wait "${progress_pid}" || true
    rm -f "${stop_path}"

    if [ "${status}" -eq 0 ]; then
        smoke_ok "${label}"
        smoke_state "log" "${log_path}"
        return 0
    fi

    smoke_fail "${label} failed with exit status ${status}"
    smoke_state "log" "${log_path}" >&2
    smoke_tail_log "${log_path}"
    return "${status}"
}

smoke_run_quiet() {
    local label="$1"
    shift

    if smoke_verbose; then
        printf '  quiet: %s: ' "${label}"
        smoke_redacted_command "$@"
    fi

    "$@" >/dev/null 2>&1 || true
}
