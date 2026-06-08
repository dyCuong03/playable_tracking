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
DATE_DIR="$LOGS_DIR/$(date -u +%Y-%m-%d)"
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

# --- Docker access classification (shared, read-only) ---
read DOCKER_AVAIL DOCKER_PERM DOCKER_STATE <<<"$(docker_access)"

# --- Redis queue depth (host redis-cli first, docker exec fallback) ---
REDIS_KEY="${OPS_REDIS_QUEUE_KEY:-$REDIS_STREAM}"
REDIS_CONTAINER="${OPS_REDIS_CONTAINER:-}"
REDIS_METHOD=""
REDIS_DEPTH=""
REDIS_OK=0

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

REDIS_HOST_CLI=0
if command -v redis-cli >/dev/null 2>&1; then
    REDIS_HOST_CLI=1
    REDIS_ARGS=(redis-cli)
    [ -n "${REDIS_URL:-}" ] && REDIS_ARGS=(redis-cli -u "$REDIS_URL")
    REDIS_DEPTH="$(redis_query "${REDIS_ARGS[@]}")"
    [ -n "$REDIS_DEPTH" ] && { REDIS_METHOD="host_cli"; REDIS_OK=1; }
fi
if [ "$REDIS_OK" = "0" ] && [ -n "$REDIS_CONTAINER" ] && [ "$DOCKER_STATE" = "ok" ]; then
    REDIS_DEPTH="$(redis_query docker exec "$REDIS_CONTAINER" redis-cli)"
    if [ -n "$REDIS_DEPTH" ]; then REDIS_METHOD="docker_exec"; REDIS_OK=1; fi
fi

REDIS_ALERT=0; REDIS_DETAIL=""
if [ "$REDIS_OK" = "1" ]; then
    if [ "$REDIS_METHOD" = "docker_exec" ]; then
        REDIS_JSON="{\"status\":\"ok\",\"method\":\"docker_exec\",\"container\":$(json_str "$REDIS_CONTAINER"),\"queue_key\":$(json_str "$REDIS_KEY"),\"depth\":${REDIS_DEPTH}}"
    else
        REDIS_JSON="{\"status\":\"ok\",\"method\":\"host_cli\",\"queue_key\":$(json_str "$REDIS_KEY"),\"depth\":${REDIS_DEPTH}}"
    fi
    REDIS_DETAIL="redis depth ${REDIS_DEPTH} (key ${REDIS_KEY}) via ${REDIS_METHOD}"
elif [ -n "$REDIS_CONTAINER" ] || [ "$REDIS_HOST_CLI" = "1" ]; then
    # Configured (a container named or host cli present) but unreadable -> alert.
    REDIS_JSON="{\"status\":\"error\",\"method\":$(json_str "${REDIS_METHOD:-none}"),\"container\":$(json_str "$REDIS_CONTAINER"),\"queue_key\":$(json_str "$REDIS_KEY"),\"message\":\"redis configured but queue depth unreadable (daemon down, wrong key, or docker exec denied)\"}"
    REDIS_ALERT=1
    REDIS_DETAIL="redis configured (key ${REDIS_KEY}, container '${REDIS_CONTAINER:-none}') but depth unreadable"
else
    REDIS_JSON="{\"status\":\"not_configured\",\"message\":\"Set OPS_REDIS_CONTAINER and OPS_REDIS_QUEUE_KEY to enable Redis depth checks\"}"
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

# --- Docker container health (read-only inspect) ---
# Configurable via OPS_DOCKER_CONTAINERS; the detailed `docker` section below is
# the single source of truth (replaces the old hardcoded `grep -c '^pixel-'`).
# DOCKER_HELPER_OUT carries a "DOCKER\t<json>" line plus "ALERT\t..." lines.
DOCKER_HELPER_OUT=""
if [ "$DOCKER_STATE" = "ok" ]; then
    DOCKER_HELPER_OUT="$(OPS_DOCKER_CONTAINERS="${OPS_DOCKER_CONTAINERS:-}" python3 - <<'PY' 2>/dev/null || true
import json, os, subprocess

expected = os.environ.get("OPS_DOCKER_CONTAINERS", "").split()

def d(*args):
    try:
        return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=8)
    except Exception:
        class R:  # mimic a failed run
            returncode = 1; stdout = ""; stderr = ""
        return R()

psmap = {}
ps = d("ps", "-a", "--format", "{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}")
if ps.returncode == 0:
    for line in ps.stdout.splitlines():
        p = line.split("\t")
        if len(p) >= 4:
            psmap[p[0]] = {"image": p[1], "status": p[2], "ports": p[3] or "-"}

names = expected if expected else sorted(psmap.keys())
containers = []
alerts = []
for n in names:
    info = psmap.get(n)
    if info is None:
        containers.append({"name": n, "present": False, "state": "missing",
                           "health": "n/a", "restarts": None, "ports": "-", "reason": "container not found"})
        if expected:
            alerts.append((n, 1, "error", "expected container %s not found" % n))
        continue
    state = health = "n/a"; restarts = None; started = ""
    insp = d("inspect", "--format",
             "{{.State.Status}}\t{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}\t{{.RestartCount}}\t{{.State.StartedAt}}", n)
    if insp.returncode == 0:
        f = insp.stdout.strip().split("\t")
        state = f[0] if len(f) > 0 and f[0] else "n/a"
        health = f[1] if len(f) > 1 and f[1] else "n/a"
        try: restarts = int(f[2])
        except Exception: restarts = None
        started = f[3] if len(f) > 3 else ""
    reason = "ok"; bad = False
    if state in ("exited", "dead"):
        bad = True; reason = "container %s" % state
    elif state == "restarting":
        bad = True; reason = "restarting"
    elif health == "unhealthy":
        bad = True; reason = "healthcheck unhealthy"
    elif state != "running":
        reason = state
    containers.append({"name": n, "present": True, "image": info["image"], "state": state,
                       "health": health, "restarts": restarts, "ports": info["ports"],
                       "status": info["status"], "started": started, "reason": reason})
    if expected:
        alerts.append((n, 1 if bad else 0, "error", reason))

obj = {"available": True, "permission_ok": True,
       "expected_configured": bool(expected), "containers": containers}
print("DOCKER\t" + json.dumps(obj))
for (n, active, sev, detail) in alerts:
    print("ALERT\t%s\t%d\t%s\t%s" % (n, active, sev, detail))
PY
)"
    DOCKER_JSON="$(printf '%s\n' "$DOCKER_HELPER_OUT" | sed -n 's/^DOCKER\t//p')"
    [ -z "$DOCKER_JSON" ] && DOCKER_JSON="{\"available\":true,\"permission_ok\":true,\"containers\":[],\"status\":\"docker_inspect_failed\"}"
else
    DOCKER_JSON="{\"available\":${DOCKER_AVAIL},\"permission_ok\":false,\"status\":\"docker_missing_or_permission_denied\",\"state\":$(json_str "$DOCKER_STATE")}"
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
