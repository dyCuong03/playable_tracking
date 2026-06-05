# ops/lib/common.sh — shared helpers for the playable-tracking ops team.
# Sourced by ops/bin/*.sh. Recreated by loadtester after a workspace wipe.

OPS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "$OPS_DIR/.." && pwd)"
STATUS_DIR="$OPS_DIR/status"
REPORTS_DIR="$OPS_DIR/reports"
LOGS_DIR="$OPS_DIR/logs"
mkdir -p "$STATUS_DIR" "$REPORTS_DIR" "$LOGS_DIR"

PIXEL_BASE="${PIXEL_BASE:-http://127.0.0.1:9000}"
HEALTH_URL="${HEALTH_URL:-${PIXEL_BASE}/health}"
PIXEL_URL="${PIXEL_URL:-${PIXEL_BASE}/p.gif}"

# UTC ISO-8601 timestamp.
ts_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
# Filename-safe UTC stamp.
ts_file() { date -u +%Y%m%dT%H%M%SZ; }

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
ops_daemon_rows() {
    printf '%s\n' \
"monitor-loop|$STATUS_DIR/monitor-loop.pid|monitor-loop.sh|$STATUS_DIR/monitor.heartbeat|${MONITOR_MAX_AGE:-120}|bash $OPS_DIR/bin/monitor-loop.sh" \
"logcollector-loop|$STATUS_DIR/logcollector-loop.pid|logcollector-loop.sh|$STATUS_DIR/logcollector.heartbeat|${LOGCOLLECT_MAX_AGE:-180}|bash $OPS_DIR/bin/logcollector-loop.sh" \
"capacity-loop|$STATUS_DIR/capacity-loop.pid|capacity-loop.sh|$STATUS_DIR/loadtester.heartbeat|${CAPACITY_MAX_AGE:-660}|bash $OPS_DIR/bin/capacity-loop.sh"
}
