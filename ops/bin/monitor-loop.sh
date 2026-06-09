#!/usr/bin/env bash
# ops/bin/monitor-loop.sh — run monitor.sh on a fixed interval.
#
# Writes its pid to ops/status/monitor-loop.pid, appends stdout to
# ops/logs/monitor-loop.out, and sleeps ${MONITOR_INTERVAL:-30}s between runs.
# A single failing iteration never stops the loop.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="monitor"
INTERVAL="${MONITOR_INTERVAL:-30}"
PID_FILE="$STATUS_DIR/monitor-loop.pid"
OUT_FILE="$LOGS_DIR/monitor-loop.out"
MONITOR="$(dirname "$0")/monitor.sh"

echo "$$" > "$PID_FILE"
cleanup() { cleanup_pidfile_if_owner "$PID_FILE" "$$"; }
trap cleanup EXIT
trap 'cleanup; exit 143' TERM INT

jlog "info" "$ROLE" "monitor loop started" "{\"pid\":$$,\"interval_s\":${INTERVAL}}" >> "$OUT_FILE"

while true; do
    if ! bash "$MONITOR" >> "$OUT_FILE" 2>&1; then
        jlog "error" "$ROLE" "monitor iteration failed (continuing)" "{\"exit\":$?}" >> "$OUT_FILE"
    fi
    sleep "$INTERVAL"
done
