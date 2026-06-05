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

echo "== pixel-ops reconcile =="
while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
    [ -n "$name" ] || continue
    # start_cmd is a trusted, space-free path ("bash <ops>/bin/<name>.sh") — word-split intentionally.
    # shellcheck disable=SC2086
    reconcile_daemon "$name" "$pidfile" "$expected" "$hbfile" "$max_age" $start_cmd
done < <(ops_daemon_rows)

echo
echo "Health:   bash $OPS_DIR/bin/ops-status.sh"
echo "Live view: bash $OPS_DIR/bin/dashboard.sh"
echo "Stop:      bash $OPS_DIR/bin/ops-stop.sh"
