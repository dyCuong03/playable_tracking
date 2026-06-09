#!/usr/bin/env bash
# ops/bin/monitor.sh — one-shot server-status snapshot for the pixel-tracking stack.
#
# Probes health/pixel endpoints, Redis stream depth, the on-disk queue, running
# processes, and host system load. Emits ONE JSON line to
# ops/logs/<date>/status.ndjson (also echoed to stdout) and overwrites
# ops/status/monitor-latest.json with the same snapshot.
#
# Alerts are appended to ops/status/alerts.ndjson ONLY on state change (tracked
# in ops/status/monitor-alert-state.json) so a polling loop does not spam.
#
# Designed to degrade gracefully: the target server may be down and redis-cli /
# docker may be missing. Always exits 0 on a clean snapshot.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="monitor"
heartbeat "$ROLE"

# --- tunables (env-overridable) ---
HIGH_LATENCY_MS="${HIGH_LATENCY_MS:-500}"
QUEUE_BACKLOG_FILES="${QUEUE_BACKLOG_FILES:-50}"
STUCK_PROCESSING_S="${STUCK_PROCESSING_S:-300}"
REDIS_STREAM="${REDIS_QUEUE_STREAM:-pixel:events}"
REDIS_GROUP="${REDIS_QUEUE_GROUP:-pixel-workers}"
QUEUE_DIR="${BIGQUERY_QUEUE_DIR:-$REPO_DIR/data/bigquery-queue}"

STATE_FILE="$STATUS_DIR/monitor-alert-state.json"
ALERTS_FILE="$STATUS_DIR/alerts.ndjson"
LATEST_FILE="$STATUS_DIR/monitor-latest.json"
DATE_DIR="$LOGS_DIR/$(ops_log_date)"
mkdir -p "$DATE_DIR"
STATUS_NDJSON="$DATE_DIR/status.ndjson"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# HTTP probe: prints "<http_code> <latency_ms>" (000 0 when unreachable).
probe() {
    local url="$1" out code tt lat
    out="$(curl -s -m3 -o /dev/null -w '%{http_code} %{time_total}' "$url" 2>/dev/null)" || out=""
    [ -z "$out" ] && out="000 0"
    code="${out%% *}"; tt="${out##* }"
    [ -z "$code" ] && code="000"
    lat="$(awk -v t="${tt:-0}" 'BEGIN{printf "%d", t*1000}')"
    printf '%s %s' "$code" "$lat"
}

# Count files + total bytes in a single dir (non-recursive). Prints "<files> <bytes>".
dir_stats() {
    local dir="$1" files=0 bytes=0
    if [ -d "$dir" ]; then
        files="$(find "$dir" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' ')"
        bytes="$(find "$dir" -maxdepth 1 -type f -printf '%s\n' 2>/dev/null \
                 | awk '{s+=$1} END{print s+0}')"
    fi
    printf '%s %s' "${files:-0}" "${bytes:-0}"
}

# Safe process count (pgrep -c exits 1 with no match; capture so it prints once).
pcount() { local n; n="$(pgrep -fc "$1" 2>/dev/null)"; echo "${n:-0}"; }

# Read prior alert state (1/0) for an event from the JSON state file.
read_state() {
    python3 - "$STATE_FILE" "$1" <<'PY' 2>/dev/null || echo 0
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    d = {}
print(int(d.get(sys.argv[2], 0)))
PY
}

# Append one alert line to alerts.ndjson.
append_alert() {
    local severity="$1" event="$2" detail="$3"
    printf '{"ts":"%s","role":"%s","severity":%s,"event":%s,"detail":%s}\n' \
        "$(ts_now)" "$ROLE" "$(json_str "$severity")" "$(json_str "$event")" "$(json_str "$detail")" \
        >> "$ALERTS_FILE"
}

NEW_STATE=""
# check_alert EVENT SEVERITY ACTIVE(1/0) DETAIL
# Emits an alert only on transition into the bad state; logs an info line on recovery.
check_alert() {
    local event="$1" severity="$2" active="$3" detail="$4" prev
    prev="$(read_state "$event")"
    if [ "$active" = "1" ] && [ "$prev" != "1" ]; then
        append_alert "$severity" "$event" "$detail"
    elif [ "$active" = "0" ] && [ "$prev" = "1" ]; then
        append_alert "info" "${event}-recovered" "$detail"
    fi
    NEW_STATE="${NEW_STATE}$(json_str "$event"):${active},"
}

# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------

read HEALTH_CODE HEALTH_MS <<<"$(probe "$HEALTH_URL")"

# --- Pixel endpoint probe (configurable; bare /p.gif returns 400 by design) ---
# A valid pixel request needs e=<start|interaction|store_trigger|end>, sid, and
# per-event params; a bare /p.gif has none, so 400 is EXPECTED. Operators point
# OPS_PIXEL_PROBE_PATH/URL at a valid synthetic event (marked ops_healthcheck,
# env!=production so it lands in the ver_2 table, not real analytics).
PIXEL_EXPECT="${OPS_PIXEL_EXPECT_CODE:-200}"
PIXEL_CONFIGURED=0
if [ -n "${OPS_PIXEL_PROBE_URL:-}" ]; then
    PIXEL_PROBE_URL="$OPS_PIXEL_PROBE_URL"; PIXEL_CONFIGURED=1
elif [ -n "${OPS_PIXEL_PROBE_PATH:-}" ]; then
    PIXEL_PROBE_URL="${PIXEL_BASE}${OPS_PIXEL_PROBE_PATH}"; PIXEL_CONFIGURED=1
else
    PIXEL_PROBE_URL="$PIXEL_URL"
fi
read PIXEL_CODE PIXEL_MS <<<"$(probe "$PIXEL_PROBE_URL")"
if [ "$PIXEL_CODE" = "$PIXEL_EXPECT" ]; then PIXEL_OK=true; else PIXEL_OK=false; fi
PIXEL_NOTE=""
if [ "$PIXEL_OK" != "true" ] && [ "$PIXEL_CONFIGURED" = "0" ]; then
    PIXEL_NOTE='bare /p.gif probe has no query params; http 400 is EXPECTED. Set OPS_PIXEL_PROBE_PATH to a valid synthetic event (e.g. /p.gif?e=interaction&sid=ops_healthcheck&env=ops&event_params=%7B%22name%22%3A%22ops_healthcheck%22%7D) to probe the real ingest path.'
fi
PIXEL_JSON="{\"url\":$(json_str "$PIXEL_PROBE_URL"),\"http_code\":$(json_str "$PIXEL_CODE"),\"expected_code\":$(json_str "$PIXEL_EXPECT"),\"ok\":${PIXEL_OK},\"latency_ms\":${PIXEL_MS},\"configured\":$([ "$PIXEL_CONFIGURED" = 1 ] && echo true || echo false)"
[ -n "$PIXEL_NOTE" ] && PIXEL_JSON="${PIXEL_JSON},\"note\":$(json_str "$PIXEL_NOTE")"
PIXEL_JSON="${PIXEL_JSON}}"

# --- Docker discovery (shared, read-only; DOCKER_STATE feeds the Redis section) ---
# ops_docker_discover echoes one JSON line with: available, permission_ok, state,
# discovery_mode, expected_configured, compose_project, prefixes[], multiple_projects[],
# expected[] (gated container names), all_visible[].
DISC_JSON="$(ops_docker_discover 2>/dev/null || echo '{"available":false,"permission_ok":false,"state":"no_cli","discovery_mode":"unavailable","expected_configured":false,"compose_project":"","prefixes":[],"multiple_projects":[],"expected":[],"all_visible":[]}')"
DOCKER_STATE="$(printf '%s' "$DISC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("state","no_cli"))' 2>/dev/null || echo 'no_cli')"

# --- Redis queue depth ---
# Priority chain:
#   (A) host redis-cli connects and returns depth -> status ok, method host_cli
#   (B) OPS_REDIS_CONTAINER + OPS_REDIS_QUEUE_KEY both set -> docker exec -> status ok/error
#   (C) ops_redis_detect auto-discovers a container:
#         if OPS_REDIS_QUEUE_KEY set -> docker exec -> status ok/error
#         else                       -> status queue_key_not_configured
#   (D) nothing reachable            -> status not_configured
# Configured-but-unreadable paths set REDIS_ALERT=1.
REDIS_CONFIGURED_KEY="${OPS_REDIS_QUEUE_KEY:-}"
REDIS_CONTAINER="${OPS_REDIS_CONTAINER:-}"
REDIS_METHOD=""
REDIS_DEPTH=""
REDIS_STATUS="not_configured"
REDIS_ALERT=0
REDIS_DETAIL=""
REDIS_HOST_CLI=0
DETECTED_REDIS_CONTAINER=""

# redis_query <redis-cli invocation...> — echo depth of REDIS_KEY (stream XLEN
# first, list LLEN fallback) or empty on failure. Read-only commands only.
redis_query() {
    local d
    d="$("$@" XLEN "$REDIS_KEY" 2>/dev/null)"
    case "$d" in (''|*[!0-9]*) d="";; esac
    if [ -z "$d" ]; then
        d="$("$@" LLEN "$REDIS_KEY" 2>/dev/null)"
        case "$d" in (''|*[!0-9]*) d="";; esac
    fi
    printf '%s' "$d"
}

# Initial REDIS_KEY: operator-configured key or fall back to the stream name.
REDIS_KEY="${REDIS_CONFIGURED_KEY:-${REDIS_STREAM}}"

# (A) host redis-cli
if command -v redis-cli >/dev/null 2>&1; then
    REDIS_HOST_CLI=1
    REDIS_ARGS=(redis-cli)
    [ -n "${REDIS_URL:-}" ] && REDIS_ARGS=(redis-cli -u "$REDIS_URL")
    REDIS_DEPTH="$(redis_query "${REDIS_ARGS[@]}")"
    if [ -n "$REDIS_DEPTH" ]; then
        REDIS_STATUS="ok"
        REDIS_METHOD="host_cli"
    else
        REDIS_STATUS="error"
        REDIS_ALERT=1
        REDIS_DETAIL="redis-cli on PATH but queue depth unreadable (key ${REDIS_KEY})"
    fi
fi

# (B) OPS_REDIS_CONTAINER + OPS_REDIS_QUEUE_KEY both explicitly configured
if [ "$REDIS_STATUS" = "not_configured" ] && \
   [ -n "$REDIS_CONTAINER" ] && [ -n "$REDIS_CONFIGURED_KEY" ] && \
   [ "$DOCKER_STATE" = "ok" ]; then
    REDIS_KEY="$REDIS_CONFIGURED_KEY"
    REDIS_DEPTH="$(redis_query docker exec "$REDIS_CONTAINER" redis-cli)"
    if [ -n "$REDIS_DEPTH" ]; then
        REDIS_STATUS="ok"
        REDIS_METHOD="docker_exec"
    else
        REDIS_STATUS="error"
        REDIS_ALERT=1
        REDIS_DETAIL="OPS_REDIS_CONTAINER+OPS_REDIS_QUEUE_KEY set but docker exec redis-cli failed (container=${REDIS_CONTAINER}, key=${REDIS_KEY})"
    fi
fi

# (C) ops_redis_detect auto-discovery fallback
if [ "$REDIS_STATUS" = "not_configured" ]; then
    DETECTED_REDIS_CONTAINER="$(ops_redis_detect 2>/dev/null || true)"
    if [ -n "$DETECTED_REDIS_CONTAINER" ]; then
        if [ -n "$REDIS_CONFIGURED_KEY" ] && [ "$DOCKER_STATE" = "ok" ]; then
            REDIS_KEY="$REDIS_CONFIGURED_KEY"
            REDIS_DEPTH="$(redis_query docker exec "$DETECTED_REDIS_CONTAINER" redis-cli)"
            if [ -n "$REDIS_DEPTH" ]; then
                REDIS_STATUS="ok"
                REDIS_METHOD="docker_exec"
                REDIS_CONTAINER="$DETECTED_REDIS_CONTAINER"
            else
                REDIS_STATUS="error"
                REDIS_ALERT=1
                REDIS_DETAIL="auto-detected redis container ${DETECTED_REDIS_CONTAINER} but docker exec redis-cli failed (key=${REDIS_KEY})"
            fi
        else
            # Container found but OPS_REDIS_QUEUE_KEY not set (or docker unavailable)
            REDIS_STATUS="queue_key_not_configured"
        fi
    fi
fi
# (D) REDIS_STATUS remains "not_configured" when none of the above succeeded.

USED_REDIS_CONTAINER="${REDIS_CONTAINER:-${DETECTED_REDIS_CONTAINER:-}}"
if [ "$REDIS_STATUS" = "ok" ]; then
    REDIS_DETAIL="redis depth ${REDIS_DEPTH} (key ${REDIS_KEY}) via ${REDIS_METHOD}"
    if [ "$REDIS_METHOD" = "docker_exec" ]; then
        REDIS_JSON="{\"status\":\"ok\",\"method\":\"docker_exec\",\"container\":$(json_str "$USED_REDIS_CONTAINER"),\"queue_key\":$(json_str "$REDIS_KEY"),\"depth\":${REDIS_DEPTH}}"
    else
        REDIS_JSON="{\"status\":\"ok\",\"method\":\"host_cli\",\"queue_key\":$(json_str "$REDIS_KEY"),\"depth\":${REDIS_DEPTH}}"
    fi
elif [ "$REDIS_STATUS" = "error" ]; then
    [ -z "$REDIS_DETAIL" ] && REDIS_DETAIL="redis configured but queue depth unreadable"
    REDIS_JSON="{\"status\":\"error\",\"method\":$(json_str "${REDIS_METHOD:-none}"),\"container\":$(json_str "$USED_REDIS_CONTAINER"),\"queue_key\":$(json_str "$REDIS_KEY"),\"message\":$(json_str "$REDIS_DETAIL")}"
elif [ "$REDIS_STATUS" = "queue_key_not_configured" ]; then
    REDIS_JSON="{\"status\":\"queue_key_not_configured\",\"container\":$(json_str "${DETECTED_REDIS_CONTAINER}"),\"message\":\"Set OPS_REDIS_QUEUE_KEY to enable Redis depth checks\"}"
    REDIS_DETAIL="redis container auto-detected (${DETECTED_REDIS_CONTAINER}) but OPS_REDIS_QUEUE_KEY not set"
else
    # (D) not_configured
    REDIS_JSON="{\"status\":\"not_configured\",\"message\":\"Install redis-cli or set OPS_REDIS_CONTAINER and OPS_REDIS_QUEUE_KEY\"}"
    REDIS_DETAIL="redis not configured"
fi

# --- Disk queue ---
read PEND_F  PEND_B  <<<"$(dir_stats "$QUEUE_DIR/pending")"
read READY_F READY_B <<<"$(dir_stats "$QUEUE_DIR/ready")"
read PROC_F  PROC_B  <<<"$(dir_stats "$QUEUE_DIR/processing")"
read REJ_F   REJ_B   <<<"$(dir_stats "$QUEUE_DIR/rejected")"
TOTAL_F=$(( PEND_F + READY_F + PROC_F + REJ_F ))
TOTAL_B=$(( PEND_B + READY_B + PROC_B + REJ_B ))
BACKLOG_F=$(( READY_F + PEND_F ))

# Oldest processing/*.ndjson age (stuck / data-loss signal).
NOW_S="$(date +%s)"
STUCK_COUNT=0; OLDEST_PROC=0
if [ -d "$QUEUE_DIR/processing" ]; then
    for f in "$QUEUE_DIR"/processing/*.ndjson; do
        [ -e "$f" ] || continue
        m="$(stat -c %Y "$f" 2>/dev/null || echo "$NOW_S")"
        age=$(( NOW_S - m ))
        [ "$age" -gt "$OLDEST_PROC" ] && OLDEST_PROC="$age"
        [ "$age" -gt "$STUCK_PROCESSING_S" ] && STUCK_COUNT=$(( STUCK_COUNT + 1 ))
    done
fi

# --- Processes ---
P_SERVER="$(pcount 'src/server.js')"
P_WORKER="$(pcount 'src/worker.js')"
P_DISPATCH="$(pcount 'src/dispatcher.js')"

# --- Docker container health (read-only inspect via ops_docker_discover results) ---
# Rebuilds the "docker" section from discovery JSON: calls docker inspect per
# container for state/health/restart_count/started; uses docker ps -a for
# image/status/ports. Top-level keys: available, permission_ok, discovery_mode,
# expected_configured, compose_project, prefixes, multiple_projects, containers[].
# Each container: name, image, state, status, health, restart_count, ports,
# expected(bool), reason.  Alerts fire ONLY for expected containers that are
# missing/exited/restarting/healthcheck-unhealthy.  Non-expected all_visible
# containers are reported but never trigger alerts.
# DOCKER_HELPER_OUT carries "DOCKER\t<json>" and zero or more "ALERT\t..." lines.
DOCKER_HELPER_OUT=""
if [ "$DOCKER_STATE" = "ok" ]; then
    DOCKER_HELPER_OUT="$(DISC_JSON="$DISC_JSON" python3 - <<'PY' 2>/dev/null || true
import json, os, subprocess

disc = json.loads(os.environ["DISC_JSON"])
expected_set = set(disc.get("expected", []))
all_visible  = disc.get("all_visible", [])

# Inspect order: expected first, then non-expected all_visible
inspect_names = list(disc.get("expected", []))
for n in all_visible:
    if n not in expected_set:
        inspect_names.append(n)

def d(*args):
    try:
        return subprocess.run(["docker"] + list(args), capture_output=True, text=True, timeout=8)
    except Exception:
        class R:
            returncode = 1; stdout = ""; stderr = ""
        return R()

# Single docker ps -a pass for image / status / ports
psmap = {}
ps = d("ps", "-a", "--format", "{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}")
if ps.returncode == 0:
    for line in ps.stdout.splitlines():
        p = line.split("\t")
        if len(p) >= 1:
            psmap[p[0]] = {
                "image":  p[1] if len(p) > 1 else "",
                "status": p[2] if len(p) > 2 else "",
                "ports":  p[3] if len(p) > 3 else "-",
            }

containers = []
alerts = []

for n in inspect_names:
    is_expected = n in expected_set
    ps_info = psmap.get(n)

    if ps_info is None:
        # Not in docker ps -a — container missing
        containers.append({
            "name": n, "image": "", "state": "missing", "status": "missing",
            "health": "n/a", "restart_count": None, "ports": "-",
            "expected": is_expected, "reason": "container not found",
        })
        if is_expected:
            alerts.append((n, 1, "error", "expected container %s not found" % n))
        continue

    # docker inspect for state / health / restart_count / started
    state = "n/a"; health = "n/a"; restart_count = None
    insp = d("inspect", "--format",
             "{{.State.Status}}\t{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}\t{{.RestartCount}}\t{{.State.StartedAt}}", n)
    if insp.returncode == 0:
        f = insp.stdout.strip().split("\t")
        if len(f) > 0 and f[0]: state = f[0]
        if len(f) > 1 and f[1]: health = f[1]
        try: restart_count = int(f[2]) if len(f) > 2 else None
        except Exception: pass

    reason = "ok"; bad = False
    if state in ("exited", "dead"):
        bad = True; reason = "container %s" % state
    elif state == "restarting":
        bad = True; reason = "restarting"
    elif health == "unhealthy":
        bad = True; reason = "healthcheck unhealthy"
    elif state not in ("running", "n/a"):
        reason = state

    containers.append({
        "name":          n,
        "image":         ps_info["image"],
        "state":         state,
        "status":        ps_info["status"],
        "health":        health,
        "restart_count": restart_count,
        "ports":         ps_info["ports"] or "-",
        "expected":      is_expected,
        "reason":        reason,
    })
    # Alerts only for expected containers; non-expected (all_visible) never fail health
    if is_expected:
        alerts.append((n, 1 if bad else 0, "error", reason))

obj = {
    "available":           disc.get("available", False),
    "permission_ok":       disc.get("permission_ok", False),
    "discovery_mode":      disc.get("discovery_mode", "unavailable"),
    "expected_configured": disc.get("expected_configured", False),
    "compose_project":     disc.get("compose_project", ""),
    "prefixes":            disc.get("prefixes", []),
    "multiple_projects":   disc.get("multiple_projects", []),
    "containers":          containers,
}
print("DOCKER\t" + json.dumps(obj))
for (n, active, sev, detail) in alerts:
    print("ALERT\t%s\t%d\t%s\t%s" % (n, active, sev, detail))
PY
)"
    DOCKER_JSON="$(printf '%s\n' "$DOCKER_HELPER_OUT" | sed -n 's/^DOCKER\t//p')"
    if [ -z "$DOCKER_JSON" ]; then
        # Python block failed — surface discovery info with an error flag
        DOCKER_JSON="$(printf '%s' "$DISC_JSON" | python3 -c '
import json,sys
d=json.load(sys.stdin)
out={k:d.get(k) for k in ["available","permission_ok","discovery_mode","expected_configured","compose_project","prefixes","multiple_projects"]}
out["containers"]=[]; out["status"]="docker_inspect_failed"
print(json.dumps(out))
' 2>/dev/null || echo '{"available":true,"permission_ok":true,"containers":[],"status":"docker_inspect_failed"}')"
    fi
else
    # Docker unavailable or permission denied — pass discovery info through directly
    DOCKER_JSON="$(printf '%s' "$DISC_JSON" | python3 -c '
import json,sys
d=json.load(sys.stdin)
out={k:d.get(k) for k in ["available","permission_ok","state","discovery_mode","expected_configured","compose_project","prefixes","multiple_projects"]}
out["containers"]=[]
print(json.dumps(out))
' 2>/dev/null || echo '{"available":false,"permission_ok":false,"containers":[]}')"
fi

# --- System ---
read LA1 LA5 LA15 _ < /proc/loadavg
NPROC="$(nproc 2>/dev/null || echo 1)"
if command -v free >/dev/null 2>&1; then
    read MEM_TOTAL MEM_USED MEM_FREE <<<"$(free -m | awk '/^Mem:/{print $2, $3, $4}')"
else
    MEM_TOTAL="$(awk '/MemTotal:/{print int($2/1024)}' /proc/meminfo 2>/dev/null)"
    MEM_FREE="$(awk '/MemAvailable:/{print int($2/1024)}' /proc/meminfo 2>/dev/null)"
    MEM_USED=$(( ${MEM_TOTAL:-0} - ${MEM_FREE:-0} ))
fi
MEM_TOTAL="${MEM_TOTAL:-0}"; MEM_USED="${MEM_USED:-0}"; MEM_FREE="${MEM_FREE:-0}"
read DISK_TOTAL DISK_USED DISK_AVAIL DISK_PCT <<<"$(df -Pk "$REPO_DIR" | awk 'NR==2{gsub(/%/,"",$5); print $2, $3, $4, $5}')"

# ---------------------------------------------------------------------------
# Snapshot JSON
# ---------------------------------------------------------------------------
SNAP="$(cat <<JSON
{"ts":"$(ts_now)","role":"$ROLE","health":{"url":$(json_str "$HEALTH_URL"),"http_code":$(json_str "$HEALTH_CODE"),"latency_ms":${HEALTH_MS}},"pixel":${PIXEL_JSON},"redis":${REDIS_JSON},"docker":${DOCKER_JSON},"disk_queue":{"pending":{"files":${PEND_F},"bytes":${PEND_B}},"ready":{"files":${READY_F},"bytes":${READY_B}},"processing":{"files":${PROC_F},"bytes":${PROC_B}},"rejected":{"files":${REJ_F},"bytes":${REJ_B}},"total_files":${TOTAL_F},"total_bytes":${TOTAL_B},"backlog_files":${BACKLOG_F},"stuck_processing":${STUCK_COUNT},"oldest_processing_s":${OLDEST_PROC}},"processes":{"server":${P_SERVER},"worker":${P_WORKER},"dispatcher":${P_DISPATCH}},"system":{"loadavg_1m":${LA1},"loadavg_5m":${LA5},"loadavg_15m":${LA15},"nproc":${NPROC},"mem_total_mb":${MEM_TOTAL},"mem_used_mb":${MEM_USED},"mem_free_mb":${MEM_FREE},"disk":{"total_kb":${DISK_TOTAL:-0},"used_kb":${DISK_USED:-0},"avail_kb":${DISK_AVAIL:-0},"use_pct":${DISK_PCT:-0}}}}
JSON
)"

printf '%s\n' "$SNAP" >> "$STATUS_NDJSON"
printf '%s\n' "$SNAP" > "$LATEST_FILE"
printf '%s\n' "$SNAP"

# ---------------------------------------------------------------------------
# Alerts (state-change only)
# ---------------------------------------------------------------------------
# health down
if [ "$HEALTH_CODE" != "200" ]; then
    check_alert "target-down" "error" 1 "health probe returned http_code=$HEALTH_CODE at $HEALTH_URL"
else
    check_alert "target-down" "error" 0 "health probe OK ($HEALTH_URL)"
fi
# high latency (only meaningful when up)
if [ "$HEALTH_CODE" = "200" ] && [ "$HEALTH_MS" -gt "$HIGH_LATENCY_MS" ]; then
    check_alert "high-latency" "warn" 1 "health latency ${HEALTH_MS}ms > ${HIGH_LATENCY_MS}ms"
else
    check_alert "high-latency" "warn" 0 "health latency ${HEALTH_MS}ms within ${HIGH_LATENCY_MS}ms"
fi
# queue backlog
if [ "$BACKLOG_F" -gt "$QUEUE_BACKLOG_FILES" ]; then
    check_alert "queue-backlog" "warn" 1 "ready+pending queue files=${BACKLOG_F} > ${QUEUE_BACKLOG_FILES}"
else
    check_alert "queue-backlog" "warn" 0 "ready+pending queue files=${BACKLOG_F} within ${QUEUE_BACKLOG_FILES}"
fi
# stuck processing
if [ "$STUCK_COUNT" -gt 0 ]; then
    check_alert "stuck-processing" "warn" 1 "${STUCK_COUNT} processing file(s) older than ${STUCK_PROCESSING_S}s (oldest ${OLDEST_PROC}s) — data-loss risk"
else
    check_alert "stuck-processing" "warn" 0 "no processing files older than ${STUCK_PROCESSING_S}s"
fi
# high loadavg
LOAD_HIGH="$(awk -v l="$LA1" -v n="$NPROC" 'BEGIN{print (l>n)?1:0}')"
if [ "$LOAD_HIGH" = "1" ]; then
    check_alert "high-loadavg" "warn" 1 "loadavg_1m=${LA1} > nproc=${NPROC}"
else
    check_alert "high-loadavg" "warn" 0 "loadavg_1m=${LA1} within nproc=${NPROC}"
fi
# redis configured-but-unreadable
check_alert "redis-unreadable" "warn" "$REDIS_ALERT" "$REDIS_DETAIL"
# pixel probe bad — only when an operator configured a real probe (bare 400 default does not alert)
if [ "$PIXEL_CONFIGURED" = "1" ] && [ "$PIXEL_OK" != "true" ]; then
    check_alert "pixel-probe-bad" "warn" 1 "pixel probe ${PIXEL_PROBE_URL} returned ${PIXEL_CODE} != expected ${PIXEL_EXPECT}"
else
    check_alert "pixel-probe-bad" "warn" 0 "pixel probe ok or default bare probe"
fi
# docker expected-container health (one dedup key per container; recovery fires when active=0)
while IFS=$'\t' read -r tag dname dactive dsev ddetail; do
    [ "$tag" = "ALERT" ] || continue
    [ -n "$dname" ] || continue
    check_alert "docker-container-${dname}" "$dsev" "$dactive" "container ${dname}: ${ddetail}"
done < <(printf '%s\n' "$DOCKER_HELPER_OUT")

# Persist new alert state as valid JSON.
printf '{%s}\n' "${NEW_STATE%,}" > "$STATE_FILE"

exit 0
