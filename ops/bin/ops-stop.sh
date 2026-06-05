#!/usr/bin/env bash
# ops/bin/ops-stop.sh — stop all pixel-ops background loops + the dashboard.
set -u
. "$(dirname "$0")/../lib/common.sh"

stop_loop() {
    local name="$1"
    local pidf="$STATUS_DIR/${name}.pid"
    if [ -f "$pidf" ]; then
        local pid; pid="$(cat "$pidf" 2>/dev/null)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null && echo "[stop] $name (pid $pid)"
        else
            echo "[dead] $name (stale pidfile)"
        fi
        rm -f "$pidf"
    else
        echo "[none] $name not running"
    fi
}

stop_loop monitor-loop
stop_loop logcollector-loop
stop_loop capacity-loop

# Fallback: reap any orphaned loop processes.
pkill -f "$OPS_DIR/bin/monitor-loop.sh"      2>/dev/null || true
pkill -f "$OPS_DIR/bin/logcollector-loop.sh" 2>/dev/null || true
pkill -f "$OPS_DIR/bin/capacity-loop.sh"     2>/dev/null || true

tmux kill-session -t "${TMUX_SESSION:-pixel-ops}" 2>/dev/null && echo "[stop] tmux dashboard" || true
echo "Done."
