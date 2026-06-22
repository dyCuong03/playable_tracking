#!/usr/bin/env bash
# ops/bin/pipeline-check-loop.sh — run the read-only pipeline health probe on an interval
# and alert (via jlog severity) when the pipeline is unhealthy/degraded.
#
# Writes pid -> ops/status/pipeline-check-loop.pid, stdout -> ops/logs/pipeline-check-loop.out,
# sleeps ${PIPELINE_CHECK_INTERVAL:-30}s between runs. A single failing iteration never stops
# the loop. Read-only: the probe never mutates Redis or disk.
#
# Mode: defaults to --direct (compute from Redis+disk, for a VPS host with no reachable web
# port). Set PIPELINE_CHECK_MODE=http to probe http://127.0.0.1:${PORT}/debug/pipeline instead.
set -u
. "$(dirname "$0")/../lib/common.sh"
load_ops_env

ROLE="pipeline-check"
INTERVAL="${PIPELINE_CHECK_INTERVAL:-30}"
MODE="${PIPELINE_CHECK_MODE:-direct}"
PID_FILE="$STATUS_DIR/pipeline-check-loop.pid"
OUT_FILE="$LOGS_DIR/pipeline-check-loop.out"
CHECK="$REPO_DIR/scripts/check-pipeline.js"

case "$INTERVAL" in (*[!0-9]*|"") INTERVAL=30;; esac

CHECK_ARGS=""
if [ "$MODE" = "direct" ]; then
    CHECK_ARGS="--direct"
fi

echo "$$" > "$PID_FILE"
cleanup() { cleanup_pidfile_if_owner "$PID_FILE" "$$"; }
trap cleanup EXIT
trap 'cleanup; exit 143' TERM INT

jlog "info" "$ROLE" "pipeline check loop started" "{\"pid\":$$,\"interval_s\":${INTERVAL},\"mode\":\"${MODE}\"}" >> "$OUT_FILE"

while true; do
    # shellcheck disable=SC2086
    OUTPUT="$(cd "$REPO_DIR" && node "$CHECK" $CHECK_ARGS 2>&1)"
    RC=$?

    case "$RC" in
        0) LEVEL="info";  STATUS="healthy"   ;;
        2) LEVEL="warn";  STATUS="degraded"  ;;
        1) LEVEL="error"; STATUS="unhealthy" ;;
        *) LEVEL="error"; STATUS="unknown"   ;;
    esac

    printf '%s\n' "$OUTPUT" >> "$OUT_FILE"
    jlog "$LEVEL" "$ROLE" "pipeline status: ${STATUS}" "{\"exit\":${RC},\"status\":\"${STATUS}\"}" >> "$OUT_FILE"

    sleep "$INTERVAL"
done
