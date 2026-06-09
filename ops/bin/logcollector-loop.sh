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

echo $$ > "$PIDFILE"
cleanup() {
    local rc="${1:-$?}"
    jlog "info" "$ROLE" "logcollector loop exiting" "{\"exit\":$rc,\"pid\":$$}" >> "$OUT" 2>/dev/null || true
    cleanup_pidfile_if_owner "$PIDFILE" "$$"
}
trap 'cleanup "$?"' EXIT
trap 'cleanup 143; exit 143' INT TERM

jlog "info" "$ROLE" "logcollector loop started" "{\"interval_s\":$INTERVAL,\"pid\":$$}" | tee -a "$OUT"

while true; do
    if bash "$SELF_DIR/logcollector.sh" >> "$OUT" 2>&1; then
        :
    else
        jlog "error" "$ROLE" "logcollector run failed - continuing" "{\"exit\":$?}" | tee -a "$OUT"
    fi
    sleep "$INTERVAL"
done
