#!/usr/bin/env bash
# ops/bin/bq-exporter-loop.sh — run bq-log-exporter.sh on a fixed interval.
#
# Writes its pid to ops/status/bq-exporter-loop.pid; appends stdout to
# ops/logs/bq-exporter-loop.out; sleeps ${OPS_BQ_EXPORT_INTERVAL_SECONDS:-300}s
# between runs. A single failing iteration never stops the loop.
# bq-log-exporter.sh (core/task#1) is responsible for touching status/bq-export.heartbeat.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="bq-export"
INTERVAL="${OPS_BQ_EXPORT_INTERVAL_SECONDS:-300}"
PIDFILE="$STATUS_DIR/bq-exporter-loop.pid"
OUT="$LOGS_DIR/bq-exporter-loop.out"
EXPORTER="$(dirname "$0")/bq-log-exporter.sh"

echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT INT TERM

jlog "info" "$ROLE" "bq-exporter loop started" "{\"interval_s\":$INTERVAL,\"pid\":$$}" | tee -a "$OUT"

while true; do
    if bash "$EXPORTER" >> "$OUT" 2>&1; then
        :
    else
        jlog "error" "$ROLE" "bq-log-exporter run failed - continuing" "{\"exit\":$?}" | tee -a "$OUT"
    fi
    sleep "$INTERVAL"
done
