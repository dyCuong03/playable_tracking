#!/usr/bin/env bash
# ops/bin/ops-stop.sh — stop all pixel-ops background loops + the dashboard.
# Iterates ops_daemon_rows() so bq-export-loop is included automatically when
# OPS_BQ_EXPORT_ENABLED=1, without any hardcoded daemon names here.
set -u
. "$(dirname "$0")/../lib/common.sh"

wait_for_pid_exit() {
    local pid="$1" i
    [ -n "$pid" ] || return 0
    for i in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null || true
}

# Clean stop: signal each daemon by its exact pid from the pidfile.
while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
    [ -n "$name" ] || continue
    if [ -f "$pidfile" ]; then
        pid="$(cat "$pidfile" 2>/dev/null)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null && echo "[stop] $name (pid $pid)"
            wait_for_pid_exit "$pid"
        else
            echo "[dead] $name (stale pidfile)"
        fi
        rm -f "$pidfile"
    else
        echo "[none] $name not running"
    fi
done < <(ops_daemon_rows)

# Fallback: reap any orphaned loop processes by their expected script name.
while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
    [ -n "$name" ] || continue
    pkill -f "$OPS_DIR/bin/$expected" 2>/dev/null || true
done < <(ops_daemon_rows)

tmux kill-session -t "${TMUX_SESSION:-pixel-ops}" 2>/dev/null && echo "[stop] tmux dashboard" || true
echo "Done."
