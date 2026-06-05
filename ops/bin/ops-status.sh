#!/usr/bin/env bash
# ops/bin/ops-status.sh — health of every managed pixel-ops daemon.
# Shows alive / cmd_ok / heartbeat_fresh / healthy / reason as SEPARATE fields,
# so a ghost pidfile (alive=true, cmd_ok=false) can never read as healthy.
#
# Usage: ops-status.sh [table|json]   (default: table)
set -u
. "$(dirname "$0")/../lib/common.sh"

fmt="${1:-table}"

collect() {
    while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
        [ -n "$name" ] || continue
        daemon_status_json "$name" "$pidfile" "$expected" "$hbfile" "$max_age"
    done < <(ops_daemon_rows)
}

if [ "$fmt" = "json" ]; then
    collect | python3 -c 'import sys,json; print(json.dumps([json.loads(l) for l in sys.stdin if l.strip()], indent=2))'
else
    printf '%-18s %-6s %-7s %-9s %-8s %-7s %s\n' DAEMON ALIVE CMD_OK HB_FRESH HEALTHY HB_AGE REASON
    printf '%-18s %-6s %-7s %-9s %-8s %-7s %s\n' '------' '-----' '------' '--------' '-------' '------' '------'
    collect | python3 -c '
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    b = lambda x: "yes" if x else "no"
    age = d["heartbeat_age_s"]
    print("%-18s %-6s %-7s %-9s %-8s %-7s %s" % (
        d["name"], b(d["alive"]), b(d["cmd_ok"]), b(d["heartbeat_fresh"]),
        b(d["healthy"]), (str(age)+"s" if age >= 0 else "n/a"), d["reason"]))
'
fi
