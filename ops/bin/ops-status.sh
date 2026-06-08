#!/usr/bin/env bash
# ops/bin/ops-status.sh — health of every managed pixel-ops daemon.
# Shows alive / cmd_ok / heartbeat_fresh / healthy / reason as SEPARATE fields,
# so a ghost pidfile (alive=true, cmd_ok=false) can never read as healthy.
#
# Usage: ops-status.sh [table|json|gate|docker]   (default: table)
#   table  human-readable table (always exit 0)
#   json   JSON array of per-daemon status objects (always exit 0)
#   gate   print the table, then exit 1 if ANY daemon is unhealthy
#          (used by CI / post-deploy to fail the pipeline on bad health)
#   docker render the docker-health block from monitor-latest.json (read-only)
set -u
. "$(dirname "$0")/../lib/common.sh"

fmt="${1:-table}"

collect() {
    while IFS='|' read -r name pidfile expected hbfile max_age start_cmd; do
        [ -n "$name" ] || continue
        daemon_status_json "$name" "$pidfile" "$expected" "$hbfile" "$max_age"
    done < <(ops_daemon_rows)
}

render_table() {
    printf '%-18s %-6s %-7s %-9s %-8s %-7s %s\n' DAEMON ALIVE CMD_OK HB_FRESH HEALTHY HB_AGE REASON
    printf '%-18s %-6s %-7s %-9s %-8s %-7s %s\n' '------' '-----' '------' '--------' '-------' '------' '------'
    printf '%s\n' "$DATA" | python3 -c '
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
}

render_docker() {
    LATEST="$STATUS_DIR/monitor-latest.json" python3 - <<'PY'
import json, os, sys
p = os.environ["LATEST"]
try:
    d = json.load(open(p)).get("docker", {})
except Exception:
    d = {}
if not d:
    print("no docker data yet — monitor not run")
    sys.exit()
if not d.get("available") or not d.get("permission_ok"):
    print("Docker unavailable or permission denied.")
    print("Fix: sudo usermod -aG docker $USER, then logout/login.")
    st = d.get("state") or d.get("status")
    if st:
        print("(%s)" % st)
    sys.exit()
mode = d.get("discovery_mode", "")
proj = d.get("compose_project", "")
if mode:
    hdr = "MODE: %s" % mode
    if proj:
        hdr += " %s" % proj
    print(hdr)
multi = d.get("multiple_projects") or []
if multi:
    print("Multiple compose projects detected: %s" % " ".join(str(m) for m in multi))
    print("Set OPS_DOCKER_COMPOSE_PROJECT=<name>")
if mode == "all_visible" and not d.get("expected_configured", True):
    print("Docker containers visible, but expected set is not configured. Set OPS_DOCKER_COMPOSE_PROJECT for health gating.")
cs = d.get("containers", [])
if not cs:
    print("docker ok — no containers found")
    sys.exit()
fmt = "%-20s %-11s %-9s %-9s %-9s %s"
print(fmt % ("CONTAINER", "STATE", "HEALTH", "RESTARTS", "EXPECTED", "REASON"))
for c in cs:
    r = c.get("restart_count")
    if r is None:
        r = c.get("restarts")
    r = "-" if r is None else r
    exp = "yes" if c.get("expected") else "no"
    print(fmt % (
        str(c.get("name", "?"))[:20], str(c.get("state", "?"))[:11],
        str(c.get("health", "n/a"))[:9], str(r)[:9],
        exp, c.get("reason", "")))
PY
}

DATA="$(collect)"

case "$fmt" in
docker)
    render_docker
    exit 0
    ;;
json)
    printf '%s\n' "$DATA" | python3 -c 'import sys,json; print(json.dumps([json.loads(l) for l in sys.stdin if l.strip()], indent=2))'
    ;;
gate)
    render_table
    printf '%s\n' "$DATA" | python3 -c '
import sys, json
bad = [json.loads(l)["name"] for l in sys.stdin if l.strip() and not json.loads(l)["healthy"]]
if bad:
    print("UNHEALTHY: " + ", ".join(bad))
    sys.exit(1)
print("all daemons healthy")
'
    ;;
*)
    render_table
    ;;
esac
