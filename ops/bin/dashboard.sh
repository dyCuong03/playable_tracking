#!/usr/bin/env bash
# ops/bin/dashboard.sh — 4-pane tmux live dashboard for the pixel-ops team.
# Read-only viewer over ops/ outputs. Each pane maps to one ops role:
#   pane 0  MONITOR   — latest server-status snapshot + monitor loop tail
#   pane 1  LOGS      — backend error rollup (today's daily dir)
#   pane 2  CAPACITY  — load-capacity trend (sustainable rps over time)
#   pane 3  PLAN+ALERT— load-bearing sizing table + recent alerts
#
# Start the background loops first with ops/bin/ops-start.sh, then run this.
set -u
. "$(dirname "$0")/../lib/common.sh"

SESSION="${TMUX_SESSION:-pixel-ops}"
BIN="$OPS_DIR/bin"
REFRESH="${DASH_REFRESH:-5}"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux not installed — cannot render dashboard." >&2
    exit 1
fi

# A pane that always views today's rollup, even across a date rollover.
LOGS_CMD="while true; do clear; printf '== BACKEND LOGS / ERROR ROLLUP ==\n\n'; \
 f=\$(ls -dt '$LOGS_DIR'/2*/errors-rollup.txt 2>/dev/null | head -1); \
 if [ -n \"\$f\" ]; then echo \"(\$f)\"; echo; tail -n 22 \"\$f\"; else echo 'no rollup yet — logcollector not run'; fi; \
 echo; echo '-- logcollector loop --'; tail -n 4 '$LOGS_DIR/logcollector-loop.out' 2>/dev/null; \
 sleep $REFRESH; done"

MON_CMD="while true; do clear; printf 'PIXEL TARGET: $PIXEL_BASE\n\n'; \
 printf '== DAEMON HEALTH ==\n'; \
 bash '$BIN/ops-status.sh' table 2>/dev/null || echo 'ops-status unavailable'; \
 printf '\n== DOCKER HEALTH ==\n'; \
 bash '$BIN/ops-status.sh' docker 2>/dev/null || echo 'docker block unavailable'; \
 printf '\n== MONITOR (latest snapshot, truncated) ==\n'; \
 if [ -f '$STATUS_DIR/monitor-latest.json' ]; then python3 -m json.tool '$STATUS_DIR/monitor-latest.json' 2>/dev/null | grep -vE '^[[:space:]]*[{}][,]?\$' | head -16; else echo 'no snapshot yet — monitor not run'; fi; \
 sleep $REFRESH; done"

CAP_CMD="while true; do clear; printf '== LOAD CAPACITY (trend) ==\n\n'; \
 hist='$REPORTS_DIR/capacity-history.ndjson'; \
 if [ -s \"\$hist\" ] && [ -x '$BIN/capacity-trend.sh' ]; then bash '$BIN/capacity-trend.sh' 2>/dev/null; \
 else printf 'No capacity history yet.\nStress testing is DISABLED by default (CAPACITY_ENABLED=0, LOADTEST_ENABLED=0).\nManual run (NON-production target):\n  LOADTEST_ENABLED=1 CAPACITY_ENABLED=1 bash ops/bin/stress.sh\nScheduled runs: CAPACITY_ENABLED=1 bash ops/bin/ops-start.sh\n'; fi; \
 sleep $((REFRESH*3)); done"

PLAN_CMD="while true; do clear; printf '== LOAD-BEARING PLAN (sizing) ==\n\n'; \
 if [ -f '$STATUS_DIR/capacity-plan.json' ]; then python3 -m json.tool '$STATUS_DIR/capacity-plan.json' 2>/dev/null | head -26; echo; echo 'Full plan: $REPORTS_DIR/capacity-plan.md'; \
 else printf 'No plan yet. Generate a baseline plan (no loadtest required):\n  bash ops/bin/plan.sh\nWrites: $REPORTS_DIR/capacity-plan.md (+ status/capacity-plan.json)\n'; fi; \
 echo; echo '-- recent alerts --'; tail -n 8 '$STATUS_DIR/alerts.ndjson' 2>/dev/null || echo '(none)'; \
 sleep $REFRESH; done"

tmux kill-session -t "$SESSION" 2>/dev/null || true

P0=$(tmux new-session -d -s "$SESSION" -n dash -P -F '#{pane_id}' "$MON_CMD")
P1=$(tmux split-window -h  -t "$P0" -P -F '#{pane_id}' "$LOGS_CMD")
P2=$(tmux split-window -v  -t "$P0" -P -F '#{pane_id}' "$CAP_CMD")
P3=$(tmux split-window -v  -t "$P1" -P -F '#{pane_id}' "$PLAN_CMD")

tmux select-layout -t "$SESSION":dash tiled
tmux set-option  -t "$SESSION" pane-border-status top 2>/dev/null || true
tmux select-pane -t "$P0" -T "MONITOR"   2>/dev/null || true
tmux select-pane -t "$P1" -T "LOGS"      2>/dev/null || true
tmux select-pane -t "$P2" -T "CAPACITY"  2>/dev/null || true
tmux select-pane -t "$P3" -T "PLAN+ALERTS" 2>/dev/null || true

if [ -n "${TMUX:-}" ]; then
    echo "Already inside tmux. View it with: tmux switch-client -t $SESSION"
else
    tmux attach -t "$SESSION"
fi
