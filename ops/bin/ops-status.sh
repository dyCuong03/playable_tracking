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
# New schema (from core): {enabled, status(ok|disabled|not_configured|partial|error), date,
#   nginx:{source_status, rows_staged, rows_uploaded, staging_file},
#   redis:{source_status, rows_staged, rows_uploaded, staging_file},
#   bigquery:{upload_enabled, project_id, dataset, nginx_table, redis_table, last_error}}
render_bq() {
    OPS_BQ_EXPORT_ENABLED="${OPS_BQ_EXPORT_ENABLED:-0}" \
    BQ_LATEST="$STATUS_DIR/bq-export-latest.json" python3 - <<'PY'
import json, os, sys

enabled = os.environ.get("OPS_BQ_EXPORT_ENABLED", "0") == "1"
bq_file = os.environ.get("BQ_LATEST", "")

if not enabled:
    print("BigQuery log export disabled. Set OPS_BQ_EXPORT_ENABLED=1.")
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
    if val is None or val == "":
        val = "n/a"
    elif val is True:
        val = "yes"
    elif val is False:
        val = "no"
    else:
        val = str(val)
    print("%-28s %s" % (label + ":", val))

status = d.get("status", "unknown")
date   = d.get("date", "")

show("status",      status)
show("date",        date)
show("staging dir", ("ops/logs/%s/bq/" % date) if date else "n/a")

ng = d.get("nginx",    {}) or {}
rd = d.get("redis",    {}) or {}
bq = d.get("bigquery", {}) or {}

print()
print("== nginx ==")
show("  source status",  ng.get("source_status"))
show("  rows staged",    ng.get("rows_staged"))
show("  rows uploaded",  ng.get("rows_uploaded"))
show("  staging file",   ng.get("staging_file"))

print()
print("== redis ==")
show("  source status",  rd.get("source_status"))
show("  rows staged",    rd.get("rows_staged"))
show("  rows uploaded",  rd.get("rows_uploaded"))
show("  staging file",   rd.get("staging_file"))

print()
print("== bigquery ==")
show("  upload enabled", bq.get("upload_enabled"))
show("  project",        bq.get("project_id"))
show("  dataset",        bq.get("dataset"))
show("  nginx table",    bq.get("nginx_table"))
show("  redis table",    bq.get("redis_table"))
last_err = bq.get("last_error")
if last_err:
    show("  last error",  last_err)
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
    GATE_FAIL=0

    # Core 3-daemon health check.
    printf '%s\n' "$DATA" | python3 -c '
import sys, json
bad = [json.loads(l)["name"] for l in sys.stdin if l.strip() and not json.loads(l)["healthy"]]
if bad:
    print("UNHEALTHY: " + ", ".join(bad))
    sys.exit(1)
print("all daemons healthy")
' || GATE_FAIL=1

    # BQ exporter gate — only active when OPS_BQ_EXPORT_ENABLED=1.
    # not_configured -> ok (no creds yet, don't hard-fail).
    # error -> fail.  partial + last_error -> fail.
    if [ "${OPS_BQ_EXPORT_ENABLED:-0}" = "1" ]; then
        BQ_LATEST="$STATUS_DIR/bq-export-latest.json" \
        python3 - <<'BQ_PY' || GATE_FAIL=1
import json, os, sys
bq_file = os.environ.get("BQ_LATEST", "")
try:
    with open(bq_file) as f:
        d = json.load(f)
except FileNotFoundError:
    print("bq-export: ok (not run yet — skipping gate)")
    sys.exit(0)
except Exception as e:
    print("bq-export: warning (cannot read status: %s)" % e)
    sys.exit(0)
status    = d.get("status", "")
last_err  = (d.get("bigquery") or {}).get("last_error")
if status == "error":
    msg = "bq-export UNHEALTHY: status=error"
    if last_err:
        msg += " (%s)" % last_err
    print(msg)
    sys.exit(1)
if status == "partial" and last_err:
    print("bq-export UNHEALTHY: status=partial last_error=%s" % last_err)
    sys.exit(1)
# not_configured / ok / disabled / partial-no-error -> ok
print("bq-export ok: status=%s" % (status or "unknown"))
BQ_PY
    fi

    [ "$GATE_FAIL" -eq 0 ] || exit 1
    ;;
*)
    render_table
    ;;
esac
