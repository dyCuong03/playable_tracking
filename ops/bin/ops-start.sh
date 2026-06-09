#!/usr/bin/env bash
# ops/bin/ops-start.sh — bring every pixel-ops daemon to a healthy state.
#
# Idempotent reconciliation, NOT a dumb "skip if pidfile/pid exists" launcher.
# For each managed daemon it validates pidfile + pid liveness + /proc cmdline +
# heartbeat freshness, then skips only a genuinely-healthy daemon and otherwise
# clears the stale/ghost pidfile, kills the exact stale pid, and relaunches.
# See reconcile_daemon() in ops/lib/common.sh.
set -u
. "$(dirname "$0")/../lib/common.sh"

START_WAIT_S="${OPS_START_WAIT_S:-20}"
START_SETTLE_S="${OPS_START_SETTLE_S:-8}"
case "$START_WAIT_S" in (*[!0-9]*|"") START_WAIT_S=20;; esac
case "$START_SETTLE_S" in (*[!0-9]*|"") START_SETTLE_S=8;; esac

ROWS_FILE="$(mktemp)"
cleanup_rows() { rm -f "$ROWS_FILE"; }
trap cleanup_rows EXIT
ops_daemon_rows > "$ROWS_FILE"

START_FAIL=0

echo "== pixel-ops reconcile =="
while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
    [ -n "$name" ] || continue
    # start_cmd is a trusted, space-free path ("bash <ops>/bin/<name>.sh") — word-split intentionally.
    # shellcheck disable=SC2086
    reconcile_daemon "$name" "$pidfile" "$expected" "$hbfile" "$max_age" $start_cmd
    if wait_daemon_healthy "$pidfile" "$expected" "$hbfile" "$max_age" "$START_WAIT_S"; then
        echo "[ready] $name healthy"
    else
        echo "[fail]  $name did not become healthy within ${START_WAIT_S}s"
        daemon_status_json "$name" "$pidfile" "$expected" "$hbfile" "$max_age"
        [ "$START_SETTLE_S" -eq 0 ] && START_FAIL=1
    fi
done < "$ROWS_FILE"

if [ "$START_SETTLE_S" -gt 0 ]; then
    START_FAIL=0
    echo
    echo "== pixel-ops settle check (${START_SETTLE_S}s) =="
    sleep "$START_SETTLE_S"
    while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
        [ -n "$name" ] || continue
        if is_daemon_healthy "$pidfile" "$expected" "$hbfile" "$max_age"; then
            echo "[ok]    $name stayed healthy"
            continue
        fi

        echo "[retry] $name unhealthy after settle - reconcile once"
        daemon_status_json "$name" "$pidfile" "$expected" "$hbfile" "$max_age"
        # shellcheck disable=SC2086
        reconcile_daemon "$name" "$pidfile" "$expected" "$hbfile" "$max_age" $start_cmd
        if wait_daemon_healthy "$pidfile" "$expected" "$hbfile" "$max_age" "$START_WAIT_S"; then
            echo "[ready] $name healthy after retry"
        else
            echo "[fail]  $name still unhealthy after retry"
            daemon_status_json "$name" "$pidfile" "$expected" "$hbfile" "$max_age"
            START_FAIL=1
        fi
    done < "$ROWS_FILE"
fi

if [ "$START_FAIL" -ne 0 ]; then
    echo
    echo "ops-start failed: at least one daemon is unhealthy"
    exit 1
fi

echo
echo "Health:   bash $OPS_DIR/bin/ops-status.sh"
echo "Live view: bash $OPS_DIR/bin/dashboard.sh"
echo "Stop:      bash $OPS_DIR/bin/ops-stop.sh"
