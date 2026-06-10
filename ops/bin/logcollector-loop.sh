#!/usr/bin/env bash
# logcollector-loop.sh — runs logcollector.sh forever on an interval.
# pid -> ops/status/logcollector-loop.pid, stdout -> ops/logs/logcollector-loop.out.
# Never dies on a single collection failure.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="logcollector"
INTERVAL="${LOGCOLLECT_INTERVAL:-60}"
PIDFILE="$STATUS_DIR/logcollector-loop.pid"
OUT="$LOGS_DIR/logcollector-loop.out"
SELF_DIR="$(dirname "$0")"
RUN_HEARTBEAT_S="${LOGCOLLECT_RUN_HEARTBEAT_S:-15}"
CHILD_PID=""
HEARTBEAT_PID=""

case "$RUN_HEARTBEAT_S" in (*[!0-9]*|"") RUN_HEARTBEAT_S=15;; esac

echo $$ > "$PIDFILE"
cleanup() {
    local rc="${1:-$?}"
    if [ -n "$HEARTBEAT_PID" ] && kill -0 "$HEARTBEAT_PID" 2>/dev/null; then
        kill "$HEARTBEAT_PID" 2>/dev/null || true
    fi
    if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
        kill "$CHILD_PID" 2>/dev/null || true
    fi
    jlog "info" "$ROLE" "logcollector loop exiting" "{\"exit\":$rc,\"pid\":$$}" >> "$OUT" 2>/dev/null || true
    cleanup_pidfile_if_owner "$PIDFILE" "$$"
}
trap 'cleanup "$?"' EXIT
trap 'cleanup 143; exit 143' INT TERM

heartbeat "$ROLE"
jlog "info" "$ROLE" "logcollector loop started" "{\"interval_s\":$INTERVAL,\"pid\":$$}" | tee -a "$OUT"

while true; do
    heartbeat "$ROLE"
    (
        while true; do
            heartbeat "$ROLE"
            sleep "$RUN_HEARTBEAT_S"
        done
    ) &
    HEARTBEAT_PID=$!

    bash "$SELF_DIR/logcollector.sh" >> "$OUT" 2>&1 &
    CHILD_PID=$!

    wait "$CHILD_PID"
    rc=$?
    CHILD_PID=""
    kill "$HEARTBEAT_PID" 2>/dev/null || true
    wait "$HEARTBEAT_PID" 2>/dev/null || true
    HEARTBEAT_PID=""
    heartbeat "$ROLE"

    if [ "$rc" -eq 0 ]; then
        :
    else
        jlog "error" "$ROLE" "logcollector run failed - continuing" "{\"exit\":$rc}" | tee -a "$OUT"
    fi
    sleep "$INTERVAL"
done
