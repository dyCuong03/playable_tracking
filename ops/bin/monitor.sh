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
read PIXEL_CODE  PIXEL_MS  <<<"$(probe "$PIXEL_URL")"

# --- Redis stream depth ---
if command -v redis-cli >/dev/null 2>&1; then
    REDIS_ARGS=()
    [ -n "${REDIS_URL:-}" ] && REDIS_ARGS=(-u "$REDIS_URL")
    XLEN="$(redis-cli "${REDIS_ARGS[@]}" XLEN "$REDIS_STREAM" 2>/dev/null)"
    [ -z "$XLEN" ] && XLEN=0
    # XPENDING summary: first token is the pending count.
    XPEND="$(redis-cli "${REDIS_ARGS[@]}" XPENDING "$REDIS_STREAM" "$REDIS_GROUP" 2>/dev/null | head -n1 | tr -d ' \r')"
    case "$XPEND" in (*[!0-9]*|"") XPEND=0;; esac
    REDIS_JSON="{\"stream\":$(json_str "$REDIS_STREAM"),\"xlen\":${XLEN},\"pending\":${XPEND}}"
else
    REDIS_JSON="{\"status\":\"cli_missing\"}"
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
DOCKER_PIXEL="null"
if command -v docker >/dev/null 2>&1; then
    DC="$(timeout 5 docker ps --format '{{.Names}}' 2>/dev/null | grep -c '^pixel-')" || DC=""
    [ -n "$DC" ] && DOCKER_PIXEL="$DC"
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
{"ts":"$(ts_now)","role":"$ROLE","health":{"url":$(json_str "$HEALTH_URL"),"http_code":$(json_str "$HEALTH_CODE"),"latency_ms":${HEALTH_MS}},"pixel":{"url":$(json_str "$PIXEL_URL"),"http_code":$(json_str "$PIXEL_CODE"),"latency_ms":${PIXEL_MS}},"redis":${REDIS_JSON},"disk_queue":{"pending":{"files":${PEND_F},"bytes":${PEND_B}},"ready":{"files":${READY_F},"bytes":${READY_B}},"processing":{"files":${PROC_F},"bytes":${PROC_B}},"rejected":{"files":${REJ_F},"bytes":${REJ_B}},"total_files":${TOTAL_F},"total_bytes":${TOTAL_B},"backlog_files":${BACKLOG_F},"stuck_processing":${STUCK_COUNT},"oldest_processing_s":${OLDEST_PROC}},"processes":{"server":${P_SERVER},"worker":${P_WORKER},"dispatcher":${P_DISPATCH},"docker_pixel":${DOCKER_PIXEL}},"system":{"loadavg_1m":${LA1},"loadavg_5m":${LA5},"loadavg_15m":${LA15},"nproc":${NPROC},"mem_total_mb":${MEM_TOTAL},"mem_used_mb":${MEM_USED},"mem_free_mb":${MEM_FREE},"disk":{"total_kb":${DISK_TOTAL:-0},"used_kb":${DISK_USED:-0},"avail_kb":${DISK_AVAIL:-0},"use_pct":${DISK_PCT:-0}}}}
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

# Persist new alert state as valid JSON.
printf '{%s}\n' "${NEW_STATE%,}" > "$STATE_FILE"

exit 0
