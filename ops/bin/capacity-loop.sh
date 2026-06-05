#!/usr/bin/env bash
# Scheduled daily capacity test. Polls the target's health on a short interval;
# when the target is up AND at least CAPACITY_INTERVAL seconds have passed since
# the last successful run, it kicks off ops/bin/stress.sh and appends the
# resulting verdict to ops/reports/capacity-history.ndjson for trend tracking.
#
# When the target is DOWN it logs a skip and keeps polling - it never hammers a
# dead target. Designed to run unattended; safe to start once and leave running:
#   nohup bash ops/bin/capacity-loop.sh >> ops/logs/capacity-loop.out 2>&1 &
#
# Env knobs: CAPACITY_INTERVAL (min s between runs, default 86400),
#   CAPACITY_POLL (s between health checks, default 300),
#   CAPACITY_MAX_CYCLES (stop after N cycles; 0=forever, used by tests/cron),
#   HEALTH_URL (from common.sh, default http://127.0.0.1:9000/health).
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="loadtester"

CAPACITY_INTERVAL="${CAPACITY_INTERVAL:-86400}"   # min seconds between full runs (default daily)
CAPACITY_POLL="${CAPACITY_POLL:-300}"             # seconds between health checks
CAPACITY_MAX_CYCLES="${CAPACITY_MAX_CYCLES:-0}"   # stop after N cycles (0 = run forever; for tests/cron)
LAST_RUN_FILE="$STATUS_DIR/capacity-last-run"
HISTORY="$REPORTS_DIR/capacity-history.ndjson"
PID_FILE="$STATUS_DIR/capacity-loop.pid"
VERDICT_FILE="$STATUS_DIR/last-stress-verdict.json"

echo $$ > "$PID_FILE"
# Clean the pid file on normal exit AND on signals (e.g. operator `kill`, timeout
# SIGTERM, Ctrl-C). bash does NOT run the EXIT trap for untrapped signals, so the
# signal traps call cleanup then exit (which also fires EXIT - harmless, rm -f).
cleanup() { rm -f "$PID_FILE"; }
trap cleanup EXIT
trap 'cleanup; exit 143' TERM INT

jlog "info" "$ROLE" "capacity-loop started" "{\"interval_s\":$CAPACITY_INTERVAL,\"poll_s\":$CAPACITY_POLL,\"health_url\":$(json_str "$HEALTH_URL")}"

# Epoch seconds of the last successful capacity run (0 if never).
last_run_epoch() {
    if [ -f "$LAST_RUN_FILE" ]; then
        local v; v="$(cat "$LAST_RUN_FILE" 2>/dev/null)"
        case "$v" in (*[!0-9]*|"") echo 0;; (*) echo "$v";; esac
    else
        echo 0
    fi
}

CYCLE=0
while true; do
    CYCLE=$(( CYCLE + 1 ))
    heartbeat "$ROLE"

    # curl's %{http_code} is "000" on connection failure; default if curl emits nothing.
    CODE="$(curl -s -m 3 -o /dev/null -w '%{http_code}' "$HEALTH_URL" 2>/dev/null)"
    CODE="${CODE:-000}"
    if [ "$CODE" != "200" ]; then
        # Target down: log the skip and keep polling - never hammer a dead target.
        jlog "warn" "$ROLE" "target-down, capacity test skipped" "{\"health_url\":$(json_str "$HEALTH_URL"),\"http_code\":$(json_str "$CODE")}"
    else
        NOW="$(date -u +%s)"
        LAST="$(last_run_epoch)"
        AGE=$(( NOW - LAST ))
        if [ "$AGE" -lt "$CAPACITY_INTERVAL" ]; then
            jlog "info" "$ROLE" "capacity test not due yet" "{\"age_s\":$AGE,\"interval_s\":$CAPACITY_INTERVAL}"
        else
            jlog "info" "$ROLE" "capacity test due - running stress" "{\"age_s\":$AGE}"
            if bash "$OPS_DIR/bin/stress.sh"; then
                echo "$NOW" > "$LAST_RUN_FILE"
                if [ -f "$VERDICT_FILE" ]; then
                    cat "$VERDICT_FILE" >> "$HISTORY"
                    jlog "info" "$ROLE" "capacity verdict recorded" "{\"history\":$(json_str "$HISTORY")}"
                else
                    jlog "warn" "$ROLE" "stress finished but no verdict file found" "{\"verdict_file\":$(json_str "$VERDICT_FILE")}"
                fi
            else
                jlog "error" "$ROLE" "stress run failed - not recording verdict" ""
            fi
        fi
    fi

    # Bounded mode for tests/cron: exit cleanly after N cycles (EXIT trap removes pid).
    if [ "$CAPACITY_MAX_CYCLES" -gt 0 ] && [ "$CYCLE" -ge "$CAPACITY_MAX_CYCLES" ]; then
        jlog "info" "$ROLE" "capacity-loop reached max cycles - exiting" "{\"cycles\":$CYCLE}"
        break
    fi

    sleep "$CAPACITY_POLL"
done
