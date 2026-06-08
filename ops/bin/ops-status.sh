#!/usr/bin/env bash
# ops/bin/ops-status.sh — health of every managed pixel-ops daemon.
# Shows alive / cmd_ok / heartbeat_fresh / healthy / reason as SEPARATE fields,
# so a ghost pidfile (alive=true, cmd_ok=false) can never read as healthy.
#
# Usage: ops-status.sh [table|json|gate|docker|bq]   (default: table)
#   table  human-readable table (always exit 0)
#   json   JSON object {daemons:[...], bq_export:{...}} (always exit 0)
#   gate   print the table, then exit 1 if ANY daemon is unhealthy
#          (used by CI / post-deploy to fail the pipeline on bad health)
#          bq-export-loop is gated ONLY when OPS_BQ_EXPORT_ENABLED=1
#   docker render the docker-health block from monitor-latest.json (read-only)
#   bq     render the BigQuery export status block from bq-export-latest.json
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

# Render BigQuery export status from bq-export-latest.json.
# Schema from core (task#1). Three states: disabled / enabled+missing-config / enabled+running.
# bq-export-latest.json shape:
#   {ts, enabled, nginx:{status,source,container,rows_staged,rows_uploaded_last},
#    redis:{status,rows_uploaded_last}, upload:{status,last_upload_ts,error},
#    tables:{nginx,redis}, staging_backlog_rows}
render_bq() {
    OPS_BQ_EXPORT_ENABLED="${OPS_BQ_EXPORT_ENABLED:-0}" \
    OPS_BQ_PROJECT_ID="${OPS_BQ_PROJECT_ID:-}" \
    GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}" \
    BQ_LATEST="$STATUS_DIR/bq-export-latest.json" python3 - <<'PY'
import json, os, sys

enabled  = os.environ.get("OPS_BQ_EXPORT_ENABLED", "0") == "1"
project  = os.environ.get("OPS_BQ_PROJECT_ID", "").strip()
creds    = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
bq_file  = os.environ.get("BQ_LATEST", "")

if not enabled:
    print("BigQuery log export disabled. Set OPS_BQ_EXPORT_ENABLED=1.")
    sys.exit(0)

missing = []
if not project:
    missing.append("OPS_BQ_PROJECT_ID")
if not creds:
    missing.append("GOOGLE_APPLICATION_CREDENTIALS")
if missing:
    print("BigQuery export enabled but missing %s." % " / ".join(missing))
    sys.exit(0)

try:
    with open(bq_file) as f:
        d = json.load(f)
except FileNotFoundError:
    print("(no bq-export-latest.json yet — exporter not run)")
    sys.exit(0)
except Exception as e:
    print("(error reading bq-export-latest.json: %s)" % e)
    sys.exit(0)

def show(label, val):
    print("%-26s %s" % (label + ":", val if val not in (None, "") else "n/a"))

ng  = d.get("nginx", {})
rd  = d.get("redis", {})
up  = d.get("upload", {})
tbl = d.get("tables", {})

show("last run",             d.get("ts"))
show("upload status",        up.get("status"))
show("last upload",          up.get("last_upload_ts"))
show("nginx status",         ng.get("status"))
show("nginx source",         ng.get("source"))
show("nginx staged",         ng.get("rows_staged"))
show("nginx uploaded (last)", ng.get("rows_uploaded_last"))
show("redis status",         rd.get("status"))
show("redis uploaded (last)", rd.get("rows_uploaded_last"))
show("staging backlog rows", d.get("staging_backlog_rows"))
show("table nginx",          tbl.get("nginx"))
show("table redis",          tbl.get("redis"))
if up.get("error"):
    show("upload error",     up.get("error"))
PY
}

DATA="$(collect)"

case "$fmt" in
docker)
    render_docker
    exit 0
    ;;
bq)
    render_bq
    exit 0
    ;;
json)
    # Output an object with daemons array + optional bq_export from latest json.
    # _DAEMON_DATA passes daemon lines via env var; a pipe+heredoc combination
    # would let the heredoc win stdin and silently discard the piped content.
    OPS_BQ_EXPORT_ENABLED="${OPS_BQ_EXPORT_ENABLED:-0}" \
    BQ_LATEST="$STATUS_DIR/bq-export-latest.json" \
    _DAEMON_DATA="$DATA" \
    python3 <<'PY'
import json, os

enabled = os.environ.get("OPS_BQ_EXPORT_ENABLED", "0") == "1"
bq_file = os.environ.get("BQ_LATEST", "")
raw     = os.environ.get("_DAEMON_DATA", "")

daemons = [json.loads(l) for l in raw.splitlines() if l.strip()]
out = {"daemons": daemons}

if enabled:
    try:
        with open(bq_file) as f:
            out["bq_export"] = json.load(f)
    except FileNotFoundError:
        out["bq_export"] = None
    except Exception as e:
        out["bq_export"] = {"error": str(e)}

print(json.dumps(out, indent=2))
PY
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
