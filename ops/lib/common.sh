# ops/lib/common.sh — shared helpers for the playable-tracking ops team.
# Sourced by ops/bin/*.sh. Recreated by loadtester after a workspace wipe.

OPS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "$OPS_DIR/.." && pwd)"
STATUS_DIR="$OPS_DIR/status"
REPORTS_DIR="$OPS_DIR/reports"
LOGS_DIR="$OPS_DIR/logs"
mkdir -p "$STATUS_DIR" "$REPORTS_DIR" "$LOGS_DIR"

# ---------------------------------------------------------------------------
# Environment loader (idempotent). Sources $OPS_DIR/.env then $REPO_DIR/.env.ops
# if present, each via `set -a; . "$f"; set +a` so every variable in the file is
# exported. Called here — BEFORE any :- defaults — so file values win and the
# defaults only fill remaining gaps. Subsequent calls are no-ops.
# ---------------------------------------------------------------------------
_OPS_ENV_LOADED=0
load_ops_env() {
    [ "${_OPS_ENV_LOADED:-0}" = "1" ] && return 0
    local f
    for f in "$OPS_DIR/.env" "$REPO_DIR/.env.ops"; do
        if [ -f "$f" ]; then
            set -a
            # shellcheck disable=SC1090
            . "$f"
            set +a
        fi
    done 2>/dev/null || true
    _OPS_ENV_LOADED=1
}
load_ops_env

PIXEL_BASE="${PIXEL_BASE:-http://127.0.0.1:9000}"
HEALTH_URL="${HEALTH_URL:-${PIXEL_BASE}/health}"
PIXEL_URL="${PIXEL_URL:-${PIXEL_BASE}/p.gif}"

# UTC ISO-8601 timestamp.
ts_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
# Filename-safe UTC stamp.
ts_file() { date -u +%Y%m%dT%H%M%SZ; }

# Local ops day used for daily archive directories. Operators read these folders
# by server-local calendar day: 00:00 inclusive to next-day 00:00 exclusive.
ops_log_date() {
    if [ -n "${OPS_LOG_DATE:-}" ]; then
        printf '%s\n' "$OPS_LOG_DATE"
    else
        date +%Y-%m-%d
    fi
}

ops_day_start() {
    local day="${1:-$(ops_log_date)}"
    date -d "$day 00:00:00" +%Y-%m-%dT%H:%M:%S%:z
}

ops_day_end() {
    local day="${1:-$(ops_log_date)}" start_epoch
    start_epoch="$(date -d "$day 00:00:00" +%s)"
    date -d "@$((start_epoch + 86400))" +%Y-%m-%dT%H:%M:%S%:z
}

# JSON-escape a string and wrap in quotes.
json_str() {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

# Structured JSON log line: jlog LEVEL ROLE MESSAGE [EXTRA_JSON_OBJECT_FRAGMENT]
# EXTRA is a raw JSON object body without braces, e.g. '"k":1,"x":"y"'
jlog() {
    local level="$1" role="$2" msg="$3" extra="$4"
    local base
    base="{\"ts\":\"$(ts_now)\",\"level\":$(json_str "$level"),\"role\":$(json_str "$role"),\"message\":$(json_str "$msg")"
    if [ -n "$extra" ]; then
        printf '%s,"data":%s}\n' "$base" "$extra"
    else
        printf '%s}\n' "$base"
    fi
}

# Touch this role's heartbeat file.
heartbeat() {
    local role="$1"
    ts_now > "$STATUS_DIR/${role}.heartbeat" 2>/dev/null || true
}

# ===========================================================================
# Docker visibility (READ-ONLY). ops observes containers; it NEVER restarts,
# recreates, or otherwise mutates application containers. Only `docker ps`,
# `docker inspect`, `docker logs`, and `docker exec ... redis-cli` (read cmds)
# are ever used against app containers.
# ===========================================================================

# Classify docker access without mutating anything.
# Echoes three space-separated tokens: <available> <permission_ok> <state>
#   available     true|false  — docker CLI on PATH
#   permission_ok true|false  — current user can talk to the daemon
#   state         ok | no_cli | permission_denied | no_daemon
docker_access() {
    command -v docker >/dev/null 2>&1 || { echo "false false no_cli"; return; }
    local out rc
    out="$(docker ps 2>&1)"; rc=$?
    if [ "$rc" -eq 0 ]; then echo "true true ok"; return; fi
    case "$out" in
        *"permission denied"*|*"dial unix"*|*"Got permission denied"*)
            echo "true false permission_denied" ;;
        *) echo "true false no_daemon" ;;
    esac
}

# Operator-declared expected app containers (space-separated). Empty => the
# monitor runs in discovery-only mode (lists what is present, never fails
# health on a missing name).
ops_docker_expected() { printf '%s' "${OPS_DOCKER_CONTAINERS:-}"; }

# ---------------------------------------------------------------------------
# ops_docker_discover() — READ-ONLY container discovery.
# Echoes ONE JSON object line with keys:
#   available(bool)  permission_ok(bool)  state(string)
#   discovery_mode("explicit|compose|compose_auto|prefix|all_visible|unavailable")
#   expected_configured(bool)  compose_project(string)  prefixes(array)
#   multiple_projects(array)   expected(array)           all_visible(array)
#
# Mode priority: explicit > compose > compose_auto > prefix > all_visible.
#   explicit     OPS_DOCKER_CONTAINERS is set
#   compose      OPS_DOCKER_COMPOSE_PROJECT is set
#   compose_auto exactly ONE distinct com.docker.compose.project label visible
#   prefix       OPS_DOCKER_CONTAINER_PREFIXES is set (names starting with any prefix)
#   all_visible  fallback; multiple_projects populated when >1 compose labels found
#   unavailable  docker unusable (no_cli / permission_denied / no_daemon)
# ---------------------------------------------------------------------------
ops_docker_discover() {
    local avail perm state
    read avail perm state <<<"$(docker_access)"

    local explicit_str="${OPS_DOCKER_CONTAINERS:-}"
    local compose_proj="${OPS_DOCKER_COMPOSE_PROJECT:-}"
    local prefix_str="${OPS_DOCKER_CONTAINER_PREFIXES:-}"
    local ps_raw=""

    if [ "$state" = "ok" ]; then
        ps_raw="$(docker ps -a --format '{{.Names}}\t{{.Label "com.docker.compose.project"}}\t{{.Image}}' 2>/dev/null || true)"
    fi

    OPS_DOCKER_AVAIL="$avail" \
    OPS_DOCKER_PERM="$perm" \
    OPS_DOCKER_STATE="$state" \
    OPS_DOCKER_EXPLICIT="$explicit_str" \
    OPS_DOCKER_COMPOSE_PROJ="$compose_proj" \
    OPS_DOCKER_PREFIX="$prefix_str" \
    OPS_DOCKER_PS_RAW="$ps_raw" \
    python3 - <<'PY'
import json, os

avail        = os.environ.get("OPS_DOCKER_AVAIL", "false") == "true"
perm         = os.environ.get("OPS_DOCKER_PERM", "false") == "true"
state        = os.environ.get("OPS_DOCKER_STATE", "no_cli")
explicit_str = os.environ.get("OPS_DOCKER_EXPLICIT", "").strip()
compose_proj = os.environ.get("OPS_DOCKER_COMPOSE_PROJ", "").strip()
prefix_str   = os.environ.get("OPS_DOCKER_PREFIX", "").strip()
ps_raw       = os.environ.get("OPS_DOCKER_PS_RAW", "").strip()

explicit_list = explicit_str.split() if explicit_str else []
prefix_list   = prefix_str.split() if prefix_str else []

# Parse docker ps output: name, compose_label, image per tab-delimited line.
all_names = []
label_map = {}
image_map = {}
if ps_raw:
    for line in ps_raw.splitlines():
        parts = line.split("\t")
        if parts and parts[0]:
            name = parts[0]
            all_names.append(name)
            label_map[name] = parts[1] if len(parts) > 1 else ""
            image_map[name] = parts[2] if len(parts) > 2 else ""

# Docker unusable → unavailable.
if state != "ok":
    print(json.dumps({
        "available": avail, "permission_ok": perm, "state": state,
        "discovery_mode": "unavailable", "expected_configured": False,
        "compose_project": "", "prefixes": [], "multiple_projects": [],
        "expected": [], "all_visible": [],
    }))
    raise SystemExit(0)

# Mode priority: explicit > compose > compose_auto > prefix > all_visible.
discovery_mode    = "all_visible"
expected          = []
compose_out       = ""
multiple_projects = []

if explicit_list:
    discovery_mode = "explicit"
    expected       = explicit_list
    compose_out    = compose_proj  # echo back if also set
elif compose_proj:
    discovery_mode = "compose"
    compose_out    = compose_proj
    expected       = [n for n in all_names if label_map.get(n, "") == compose_proj]
else:
    distinct = sorted({label_map[n] for n in all_names if label_map.get(n, "")})
    if len(distinct) == 1:
        discovery_mode = "compose_auto"
        compose_out    = distinct[0]
        expected       = [n for n in all_names if label_map.get(n, "") == compose_out]
    elif prefix_list:
        # prefix wins over all_visible (compose_auto inapplicable: 0 or >1 labels)
        discovery_mode = "prefix"
        expected       = [n for n in all_names if any(n.startswith(p) for p in prefix_list)]
    else:
        # all_visible: flag multiple_projects when >1 compose labels found
        discovery_mode    = "all_visible"
        multiple_projects = list(distinct)  # empty if no labels; >1 when ambiguous

print(json.dumps({
    "available": avail,
    "permission_ok": perm,
    "state": state,
    "discovery_mode": discovery_mode,
    "expected_configured": bool(explicit_list or compose_proj or prefix_list),
    "compose_project": compose_out,
    "prefixes": prefix_list,
    "multiple_projects": multiple_projects,
    "expected": expected,
    "all_visible": all_names,
}))
PY
}

# ---------------------------------------------------------------------------
# ops_redis_detect() — READ-ONLY Redis container finder.
# Echoes the single container name whose NAME or IMAGE contains "redis" from
# docker ps -a. Echoes empty string when zero or more than one match (ambiguous).
# ---------------------------------------------------------------------------
ops_redis_detect() {
    local avail perm state
    read avail perm state <<<"$(docker_access)"
    [ "$state" = "ok" ] || { echo ""; return; }
    local name image
    local matches=()
    while IFS=$'\t' read -r name image; do
        case "${name}${image}" in
            *redis*) matches+=("$name") ;;
        esac
    done < <(docker ps -a --format '{{.Names}}\t{{.Image}}' 2>/dev/null || true)
    [ "${#matches[@]}" -eq 1 ] && echo "${matches[0]}" || echo ""
}

# ===========================================================================
# Daemon reconciliation helpers
# ---------------------------------------------------------------------------
# A daemon is HEALTHY only when ALL hold:
#   1. pidfile exists
#   2. that PID is alive
#   3. /proc/<pid>/cmdline contains the expected loop script
#   4. it is therefore not a ghost/old/deleted script
#   5. the daemon's heartbeat is fresh within max_age_s
# "PID alive" alone is NOT health — that false positive is exactly what let
# pre-wipe ghost loops (health-snapshot.sh / collect-logs.sh) freeze monitoring.
# ===========================================================================

# Space-joined /proc/<pid>/cmdline (NUL-delimited in proc). Empty + non-zero on miss.
read_cmdline() {
    local pid="$1"
    [ -n "$pid" ] && [ -r "/proc/$pid/cmdline" ] || return 1
    tr '\0' ' ' < "/proc/$pid/cmdline"
}

# True if pid is a signalable live process.
is_pid_alive() {
    local pid="$1"
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# True if pid's cmdline references the expected script (basename substring match).
is_expected_daemon() {
    local pid="$1" expected="$2" cmd
    cmd="$(read_cmdline "$pid")" || return 1
    case "$cmd" in *"$expected"*) return 0 ;; *) return 1 ;; esac
}

# Heartbeat age in seconds; echoes -1 if file missing/unparseable.
heartbeat_age() {
    local file="$1" hb_epoch now
    [ -f "$file" ] || { echo -1; return; }
    hb_epoch="$(date -u -d "$(cat "$file" 2>/dev/null)" +%s 2>/dev/null)" || hb_epoch=""
    [ -n "$hb_epoch" ] || hb_epoch="$(stat -c %Y "$file" 2>/dev/null)" || { echo -1; return; }
    now="$(date -u +%s)"
    echo $(( now - hb_epoch ))
}

# True if heartbeat within max_age seconds.
is_heartbeat_fresh() {
    local file="$1" max_age="$2" age
    age="$(heartbeat_age "$file")"
    [ "$age" -ge 0 ] 2>/dev/null && [ "$age" -le "$max_age" ]
}

# Emit one daemon's health as a JSON object with the fields separated out:
#   name pid alive cmd_ok heartbeat_fresh healthy heartbeat_age_s cmd reason
# Usage: daemon_status_json NAME PIDFILE EXPECTED_SCRIPT HEARTBEAT_FILE MAX_AGE_S
daemon_status_json() {
    local name="$1" pidfile="$2" expected="$3" hbfile="$4" max_age="$5"
    local pid="" alive=false cmd_ok=false hb_fresh=false healthy=false reason="" cmd="" age
    [ -f "$pidfile" ] && pid="$(cat "$pidfile" 2>/dev/null)"
    age="$(heartbeat_age "$hbfile")"
    if [ -z "$pid" ]; then
        reason="no pidfile"
    elif ! is_pid_alive "$pid"; then
        reason="pid $pid dead (stale pidfile)"
    else
        alive=true
        cmd="$(read_cmdline "$pid" 2>/dev/null)"
        if is_expected_daemon "$pid" "$expected"; then
            cmd_ok=true
            if is_heartbeat_fresh "$hbfile" "$max_age"; then
                hb_fresh=true; healthy=true; reason="ok"
            else
                reason="heartbeat stale (${age}s > ${max_age}s)"
            fi
        else
            reason="ghost: pid $pid cmd does not match $expected"
        fi
    fi
    printf '{"name":%s,"pid":%s,"alive":%s,"cmd_ok":%s,"heartbeat_fresh":%s,"healthy":%s,"heartbeat_age_s":%s,"cmd":%s,"reason":%s}\n' \
        "$(json_str "$name")" "${pid:-null}" "$alive" "$cmd_ok" "$hb_fresh" "$healthy" "${age:--1}" \
        "$(json_str "$cmd")" "$(json_str "$reason")"
}

# Reconcile one daemon to a healthy state. Idempotent and never skips on
# "pidfile exists" or "pid alive" alone:
#   healthy (cmd ok + hb fresh) -> skip, no new process
#   no pidfile                  -> start
#   pid dead (stale pidfile)    -> clear, start
#   ghost (alive, wrong cmd)    -> clear pidfile, kill EXACT pid, start
#   cmd ok but heartbeat stale  -> kill EXACT pid, clear, restart
# Only the exact PID from the pidfile is ever killed — never a broad pkill -f.
# Usage: reconcile_daemon NAME PIDFILE EXPECTED HEARTBEAT MAX_AGE START_CMD...
reconcile_daemon() {
    local name="$1" pidfile="$2" expected="$3" hbfile="$4" max_age="$5"; shift 5
    local start_cmd=("$@")
    local pid="" cmd="" action="" age
    [ -f "$pidfile" ] && pid="$(cat "$pidfile" 2>/dev/null)"
    age="$(heartbeat_age "$hbfile")"

    if [ -n "$pid" ] && is_pid_alive "$pid"; then
        cmd="$(read_cmdline "$pid")"
        if is_expected_daemon "$pid" "$expected"; then
            if is_heartbeat_fresh "$hbfile" "$max_age"; then
                jlog "info" "reconcile" "$name healthy - skip" "{\"pid\":$pid,\"heartbeat_age_s\":$age}"
                echo "[ok]    $name healthy (pid $pid, hb ${age}s) — skip"
                return 0
            fi
            jlog "warn" "reconcile" "$name heartbeat stale - restarting" "{\"pid\":$pid,\"heartbeat_age_s\":$age,\"max_age_s\":$max_age}"
            echo "[stale] $name cmd ok but hb ${age}s > ${max_age}s — kill exact pid $pid + restart"
            kill "$pid" 2>/dev/null
            action="restart-stale-heartbeat"
        else
            jlog "warn" "reconcile" "$name GHOST pidfile - alive pid runs wrong cmd" \
                "{\"pid\":$pid,\"expected\":$(json_str "$expected"),\"cmd\":$(json_str "$cmd")}"
            echo "[ghost] $name pid $pid runs '${cmd% }' (expected $expected) — kill exact pid + restart"
            kill "$pid" 2>/dev/null
            action="restart-ghost"
        fi
    elif [ -n "$pid" ]; then
        jlog "warn" "reconcile" "$name stale pidfile - pid dead" "{\"pid\":$pid}"
        echo "[dead]  $name pid $pid dead — clear stale pidfile + start"
        action="restart-dead-pid"
    else
        echo "[none]  $name no pidfile — start"
        action="start"
    fi

    # Reap the exact killed pid (escalate to KILL only on that pid), then clear.
    if [ -n "$pid" ] && is_pid_alive "$pid"; then
        local i
        for i in 1 2 3 4 5; do is_pid_alive "$pid" || break; sleep 0.3; done
        is_pid_alive "$pid" && kill -9 "$pid" 2>/dev/null
    fi
    rm -f "$pidfile"

    # Launch; the loop script writes its own fresh pidfile via echo $$.
    nohup "${start_cmd[@]}" >>"$LOGS_DIR/${name}.nohup.out" 2>&1 &
    jlog "info" "reconcile" "$name launched" "{\"action\":$(json_str "$action"),\"launcher_pid\":$!}"
    echo "[start] $name launched (action=$action, launcher pid $!)"
    return 0
}

# The managed ops daemons. One row per daemon, pipe-delimited:
#   name | pidfile | expected_script | heartbeat_file | max_age_s | start_cmd
# max_age_s is generous (>= a few loop intervals) to avoid restart thrash:
#   monitor ~30s, logcollector ~60s, capacity heartbeats each ~300s poll.
#   bq-export: interval 300s * 2 + buffer = 660s (matches capacity convention).
# The bq-export-loop row is emitted ONLY when OPS_BQ_EXPORT_ENABLED=1 so that
# ops-start/stop/status auto-include it without any hardcoded conditionals there.
# load_ops_env (called above at module load) ensures OPS_BQ_EXPORT_ENABLED is set.
ops_daemon_rows() {
    printf '%s\n' \
"monitor-loop|$STATUS_DIR/monitor-loop.pid|monitor-loop.sh|$STATUS_DIR/monitor.heartbeat|${MONITOR_MAX_AGE:-120}|bash $OPS_DIR/bin/monitor-loop.sh" \
"logcollector-loop|$STATUS_DIR/logcollector-loop.pid|logcollector-loop.sh|$STATUS_DIR/logcollector.heartbeat|${LOGCOLLECT_MAX_AGE:-180}|bash $OPS_DIR/bin/logcollector-loop.sh" \
"capacity-loop|$STATUS_DIR/capacity-loop.pid|capacity-loop.sh|$STATUS_DIR/loadtester.heartbeat|${CAPACITY_MAX_AGE:-660}|bash $OPS_DIR/bin/capacity-loop.sh"
    if [ "${OPS_BQ_EXPORT_ENABLED:-0}" = "1" ]; then
        printf '%s\n' "bq-exporter-loop|$STATUS_DIR/bq-exporter-loop.pid|bq-exporter-loop.sh|$STATUS_DIR/bq-export.heartbeat|${BQ_EXPORT_MAX_AGE:-660}|bash $OPS_DIR/bin/bq-exporter-loop.sh"
    fi
}
