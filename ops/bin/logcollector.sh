#!/usr/bin/env bash
# logcollector.sh — one-shot backend log collector for the pixel-tracking server.
#
# Collects every backend log source into the daily archive ops/logs/<local-date>/:
#   - app.log               : incremental tail of repo logs/pixel-tracking.txt
#   - local-server.*.log    : incremental tail of repo logs/local-server.{out,err}.log
#   - docker/<container>.log : `docker logs` for the local 00:00..next 00:00 day window
#   - pm2-status.json        : `pm2 jlist` snapshot (if pm2 present)
# Then builds errors-rollup.txt (error/warn lines per source) and a summary JSON,
# and raises an "error-spike" alert when this window's errors exceed ERROR_SPIKE.
#
# Designed for a NOT-yet-deployed server: every source is optional. Missing
# sources are skipped cleanly. Docker status is written to a reason-specific file
# so the name never lies: _docker-unavailable.log (no CLI / denied / no daemon),
# _docker-skipped.log (reachable but all_visible mode, opt-in off),
# _docker-no-matching-containers.log, or _docker-logs-failed.log. Never aborts
# on a single source failure.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="logcollector"
heartbeat "$ROLE"

DATE="$(ops_log_date)"
# Per-spec path: ops/logs/<date>/docker/<container>.log (was containers/ pre-audit).
DAYDIR="$LOGS_DIR/$DATE"
CONTDIR="$DAYDIR/docker"
mkdir -p "$CONTDIR"

OFFSETS_FILE="$STATUS_DIR/logcollector-offsets.json"
ALERT_STATE="$STATUS_DIR/logcollector-alert-state.json"
SUMMARY_FILE="$STATUS_DIR/logcollector-latest.json"
ALERTS_FILE="$STATUS_DIR/alerts.ndjson"
ERROR_SPIKE="${ERROR_SPIKE:-20}"

# Docker log collection tuning — containers determined at runtime via ops_docker_discover.
# Defaults cover the whole local ops day, so ops/logs/YYYY-MM-DD/docker/*.log
# represents 00:00 inclusive through next-day 00:00 exclusive.
DOCKER_SINCE="${LOGCOLLECT_DOCKER_SINCE:-$(ops_day_start "$DATE")}"
DOCKER_UNTIL="${LOGCOLLECT_DOCKER_UNTIL:-$(ops_day_end "$DATE")}"
DOCKER_TAIL="${LOGCOLLECT_DOCKER_TAIL:-all}"

# error/warn matchers for JSON-line app logs (tolerate a space after the colon).
ERR_RE='"level":[[:space:]]*"error"'
WARN_RE='"level":[[:space:]]*"warn"'
# Broader matchers for raw container stdout/stderr (not necessarily JSON).
GENERIC_ERR_RE='([Ee]rror|ERROR|[Ff]atal|FATAL|[Ee]xception|EXCEPTION|"level":[[:space:]]*"error")'
GENERIC_WARN_RE='([Ww]arn|WARN|"level":[[:space:]]*"warn")'

# ---- offset store -----------------------------------------------------------
# Loaded once into an associative array, written once at the end.
declare -A OFFSETS
if [ -f "$OFFSETS_FILE" ]; then
    while IFS=$'\t' read -r k v; do
        [ -n "$k" ] && OFFSETS["$k"]="$v"
    done < <(python3 - "$OFFSETS_FILE" <<'PY' 2>/dev/null || true
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    if isinstance(d, dict):
        for k, v in d.items():
            print("%s\t%s" % (k, int(v)))
except Exception:
    pass
PY
    )
fi

write_offsets() {
    python3 - "$OFFSETS_FILE" "${OFFSET_KV:-}" <<'PY' 2>/dev/null || true
import json, sys
path, blob = sys.argv[1], sys.argv[2]
d = {}
for line in blob.splitlines():
    if not line.strip():
        continue
    k, _, v = line.partition("\t")
    try:
        d[k] = int(v)
    except ValueError:
        pass
with open(path, "w") as f:
    json.dump(d, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# ---- per-run accounting -----------------------------------------------------
# Parallel arrays describing each source touched this run.
SRC_NAMES=()
SRC_LINES=()
SRC_ERRS=()
SRC_WARNS=()
TOTAL_ERR=0
TOTAL_WARN=0

# count_re FILE REGEX  -> echoes match count (0 if file empty/missing)
# grep -c always prints the count (even "0") and just exits 1 on no match,
# so capture stdout and never chain `|| echo 0` (that would double-print).
count_re() {
    local n
    [ -s "$1" ] || { echo 0; return; }
    n=$(grep -Ec "$2" "$1" 2>/dev/null)
    echo "${n:-0}"
}

filter_internal_probe_logs() {
    local src="$1" dst="$2"
    python3 - "$src" "$dst" <<'PY' 2>/dev/null || cp "$src" "$dst" 2>/dev/null || true
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])

def is_internal_probe(line):
    try:
        item = json.loads(line)
    except Exception:
        return False

    if not isinstance(item, dict):
        return False

    server = item.get("server") if isinstance(item.get("server"), dict) else item
    data = item.get("data") if isinstance(item.get("data"), dict) else {}
    uri = str(server.get("uri") or "")
    sid = str(data.get("sid") or data.get("session_id") or "")
    query = str(data.get("query") or "")
    request_uri = str(data.get("request_uri") or "")

    return (
        uri == "/health"
        or sid == "ops_healthcheck"
        or "ops_healthcheck" in query
        or "ops_healthcheck" in request_uri
    )

with src.open("r", encoding="utf-8", errors="replace") as fin, \
        dst.open("w", encoding="utf-8") as fout:
    for line in fin:
        if is_internal_probe(line):
            continue
        fout.write(line)
PY
}

# record_source NAME NEW_LINES_FILE [ERR_RE] [WARN_RE]
# App logs use the JSON-level regexes (default); container logs pass the broader
# generic matchers so plain error/fatal/exception lines are counted too.
record_source() {
    local name="$1" tmp="$2" err_re="${3:-$ERR_RE}" warn_re="${4:-$WARN_RE}"
    local lines errs warns
    lines=$(wc -l < "$tmp" 2>/dev/null | tr -d ' ')
    lines="${lines:-0}"
    errs=$(count_re "$tmp" "$err_re")
    warns=$(count_re "$tmp" "$warn_re")
    SRC_NAMES+=("$name")
    SRC_LINES+=("$lines")
    SRC_ERRS+=("$errs")
    SRC_WARNS+=("$warns")
    TOTAL_ERR=$((TOTAL_ERR + errs))
    TOTAL_WARN=$((TOTAL_WARN + warns))
}

# collect_incremental NAME SRC DEST
# Appends only the bytes added since the last run (tracked by byte offset).
collect_incremental() {
    local name="$1" src="$2" dest="$3"
    [ -f "$src" ] || return 0
    local size off tmp
    size=$(stat -c %s "$src" 2>/dev/null || echo 0)
    off="${OFFSETS[$name]:-0}"
    # Source rotated/truncated -> start over from the top.
    if [ "$off" -gt "$size" ]; then off=0; fi
    if [ "$off" -ge "$size" ]; then
        OFFSETS["$name"]="$size"
        return 0
    fi
    tmp="$(mktemp)"
    tail -c +$((off + 1)) "$src" > "$tmp" 2>/dev/null || true
    cat "$tmp" >> "$dest" 2>/dev/null || true
    OFFSETS["$name"]="$size"
    record_source "$name" "$tmp"
    rm -f "$tmp"
}

# ---- 1. app + local-server logs (incremental) -------------------------------
collect_incremental "app"               "$REPO_DIR/logs/pixel-tracking.txt"   "$DAYDIR/app.log"
collect_incremental "local-server.out"  "$REPO_DIR/logs/local-server.out.log" "$DAYDIR/local-server.out.log"
collect_incremental "local-server.err"  "$REPO_DIR/logs/local-server.err.log" "$DAYDIR/local-server.err.log"

# ---- 2. container logs (via ops_docker_discover) ----------------------------
DOCKER_UNAVAIL="$CONTDIR/_docker-unavailable.log"
DOCKER_REASONS="$(mktemp)"
DOCKER_WARNING=""
DOCKER_WARN_CODE=""
DOCKER_ZERO_REASON=""
COLLECT_CONTAINERS=""

# ops_docker_discover provides consistent docker visibility across all ops tools.
DISCOVER_JSON="$(ops_docker_discover 2>/dev/null || true)"
_parse_result="$(python3 - "$DISCOVER_JSON" <<'PY' 2>/dev/null || true
import json, sys
try:
    d = json.loads(sys.argv[1])
except Exception:
    d = {}
b = lambda x: "true" if x else "false"
print(b(d.get("available")))
print(b(d.get("permission_ok")))
print(d.get("state", "unavailable"))
print(d.get("discovery_mode", "unavailable"))
print(b(d.get("expected_configured")))
print(d.get("compose_project", ""))
print(" ".join(d.get("expected", [])))
print(" ".join(d.get("all_visible", [])))
print(" ".join(d.get("multiple_projects", [])))
PY
)"
DOCKER_AVAIL=$(   printf '%s\n' "$_parse_result" | sed -n '1p')
DOCKER_PERM=$(    printf '%s\n' "$_parse_result" | sed -n '2p')
DOCKER_STATE=$(   printf '%s\n' "$_parse_result" | sed -n '3p')
DISCOVERY_MODE=$( printf '%s\n' "$_parse_result" | sed -n '4p')
EXPECTED_CONFIGURED=$(printf '%s\n' "$_parse_result" | sed -n '5p')
COMPOSE_PROJECT=$(printf '%s\n' "$_parse_result" | sed -n '6p')
CONTAINERS=$(     printf '%s\n' "$_parse_result" | sed -n '7p')
ALL_VISIBLE=$(    printf '%s\n' "$_parse_result" | sed -n '8p')
# Safety defaults if ops_docker_discover or python3 parse failed.
DOCKER_AVAIL="${DOCKER_AVAIL:-false}"
DOCKER_PERM="${DOCKER_PERM:-false}"
DOCKER_STATE="${DOCKER_STATE:-unavailable}"
DISCOVERY_MODE="${DISCOVERY_MODE:-unavailable}"
EXPECTED_CONFIGURED="${EXPECTED_CONFIGURED:-false}"
# COMPOSE_PROJECT, CONTAINERS, ALL_VISIBLE default empty — fine.

# DOCKER_STATUS is surfaced in the summary + rollup so a bare sources=0 is never
# mistaken for "all quiet" when docker is actually blocked by permission.
DOCKER_STATUS="$DOCKER_STATE"
# Stable machine token surfaced in rollup + status whenever docker logs could not
# be collected (no CLI / no daemon / permission denied).
case "$DOCKER_STATE" in ok) ;; *) DOCKER_WARN_CODE="docker_unavailable_or_permission_denied";; esac

if [ "$DOCKER_STATE" = "ok" ]; then
    if [ "$DISCOVERY_MODE" = "all_visible" ] && [ "${OPS_DOCKER_LOG_ALL_VISIBLE:-0}" != "1" ]; then
        # Docker IS reachable. Collection is intentionally skipped because no
        # expected container set is configured and all-visible opt-in is off.
        # This is NOT a docker-unavailable condition (see _docker-skipped.log).
        DOCKER_ZERO_REASON="all_visible_mode_skip"
        jlog "info" "$ROLE" "docker log collection skipped: all_visible mode" \
            "{\"reason\":\"all_visible_mode_skip\",\"discovery_mode\":$(json_str "$DISCOVERY_MODE"),\"ops_docker_log_all_visible\":$(json_str "${OPS_DOCKER_LOG_ALL_VISIBLE:-0}")}" >> "$DOCKER_REASONS"
        {
            printf 'reason=all_visible_mode_skip\n'
            printf 'discovery_mode=%s\n' "$DISCOVERY_MODE"
            printf 'OPS_DOCKER_LOG_ALL_VISIBLE=%s\n' "${OPS_DOCKER_LOG_ALL_VISIBLE:-0}"
            printf 'docker is reachable; container logs are NOT collected until you do ONE of:\n'
            printf '  - set OPS_DOCKER_COMPOSE_PROJECT=<compose_project> in ops/.env (recommended)\n'
            printf '  - set OPS_DOCKER_CONTAINERS="name1 name2 ..." in ops/.env\n'
            printf '  - set OPS_DOCKER_LOG_ALL_VISIBLE=1 in ops/.env to collect all visible containers\n'
            [ -n "$ALL_VISIBLE" ] && printf 'visible containers: %s\n' "$ALL_VISIBLE"
        } >> "$DOCKER_REASONS"
    else
        [ "$DISCOVERY_MODE" = "all_visible" ] && COLLECT_CONTAINERS="$ALL_VISIBLE" || COLLECT_CONTAINERS="$CONTAINERS"
        if [ -z "$COLLECT_CONTAINERS" ]; then
            DOCKER_ZERO_REASON="no-expected-containers-configured"
            jlog "warn" "$ROLE" "no containers in expected set - docker logs skipped" \
                "{\"discovery_mode\":$(json_str "$DISCOVERY_MODE"),\"expected_configured\":$EXPECTED_CONFIGURED}" >> "$DOCKER_REASONS"
        else
            present="$(docker ps -a --format '{{.Names}}' 2>/dev/null || true)"
            _docker_attempted=0
            _docker_collected=0
            _docker_failed=0
            for c in $COLLECT_CONTAINERS; do
                _docker_attempted=$((_docker_attempted+1))
                if printf '%s\n' "$present" | grep -qx "$c"; then
                    tmp="$(mktemp)"
                    filtered="$(mktemp)"
                    docker_log_args=(logs --since "$DOCKER_SINCE" --until "$DOCKER_UNTIL")
                    [ "$DOCKER_TAIL" != "all" ] && docker_log_args+=(--tail "$DOCKER_TAIL")
                    docker_log_args+=("$c")
                    if docker "${docker_log_args[@]}" > "$tmp" 2>&1; then
                        filter_internal_probe_logs "$tmp" "$filtered"
                        cat "$filtered" > "$CONTDIR/$c.log"
                        record_source "container:$c" "$filtered" "$GENERIC_ERR_RE" "$GENERIC_WARN_RE"
                        _docker_collected=$((_docker_collected+1))
                    else
                        jlog "warn" "$ROLE" "docker logs failed for container" "{\"container\":$(json_str "$c")}" >> "$DOCKER_REASONS"
                        _docker_failed=$((_docker_failed+1))
                    fi
                    rm -f "$tmp" "$filtered"
                else
                    jlog "warn" "$ROLE" "container absent - skipped" "{\"container\":$(json_str "$c")}" >> "$DOCKER_REASONS"
                fi
            done
            if [ "$_docker_collected" -eq 0 ] && [ "$_docker_attempted" -gt 0 ]; then
                if [ "$_docker_failed" -gt 0 ]; then
                    DOCKER_ZERO_REASON="docker-logs-failed"
                else
                    DOCKER_ZERO_REASON="no-matching-containers"
                fi
            fi
        fi
    fi
elif [ "$DOCKER_STATE" = "permission_denied" ]; then
    DOCKER_ZERO_REASON="permission_denied"
    DOCKER_WARNING="docker permission denied — add the deploy user to the docker group (sudo usermod -aG docker \$USER, then re-login) so container logs can be collected."
    jlog "warn" "$ROLE" "docker permission denied - container logs skipped" "{\"reason\":\"permission_denied\"}" >> "$DOCKER_REASONS"
elif [ "$DOCKER_STATE" = "no_cli" ]; then
    DOCKER_WARNING="docker CLI not installed — container logs not collected."
    jlog "warn" "$ROLE" "docker CLI missing - container logs skipped" "{\"reason\":\"no_cli\"}" >> "$DOCKER_REASONS"
else
    DOCKER_WARNING="docker daemon unreachable — container logs not collected."
    jlog "warn" "$ROLE" "docker daemon unreachable - container logs skipped" "{\"reason\":\"no_daemon\"}" >> "$DOCKER_REASONS"
fi

# Route the status file by the ACTUAL reason so a name never lies. Docker being
# reachable but skipped must NOT land in _docker-unavailable.log.
if [ -s "$DOCKER_REASONS" ]; then
    if [ "$DOCKER_STATE" = "ok" ]; then
        case "$DOCKER_ZERO_REASON" in
            no-matching-containers) DOCKER_STATUS_FILE="$CONTDIR/_docker-no-matching-containers.log" ;;
            docker-logs-failed)     DOCKER_STATUS_FILE="$CONTDIR/_docker-logs-failed.log" ;;
            *)                      DOCKER_STATUS_FILE="$CONTDIR/_docker-skipped.log" ;;
        esac
    else
        # no_cli / permission_denied / no_daemon / unavailable — genuinely unavailable.
        DOCKER_STATUS_FILE="$DOCKER_UNAVAIL"
    fi
    cat "$DOCKER_REASONS" > "$DOCKER_STATUS_FILE"
fi
rm -f "$DOCKER_REASONS"
# Clear a stale _docker-unavailable.log from an earlier run when docker is now ok.
[ "$DOCKER_STATE" = "ok" ] && rm -f "$DOCKER_UNAVAIL" 2>/dev/null || true

# Mirror the app-side Redis enqueue audit into the daily Redis docker archive
# operators already inspect: ops/logs/<date>/docker/pixel-redis.log.
# Redis itself does not log XADD payloads, so this appends a replaceable block
# sourced from logs/redis-queue/<date>.ndjson after the raw Redis server log.
append_redis_queue_audit_to_redis_log() {
    local audit="$REPO_DIR/logs/redis-queue/$DATE.ndjson"
    [ -s "$audit" ] || return 0

    local target="$CONTDIR/${OPS_REDIS_CONTAINER:-pixel-redis}.log"
    if [ ! -f "$target" ]; then
        local found=""
        found="$(find "$CONTDIR" -maxdepth 1 -type f -iname '*redis*.log' 2>/dev/null | head -n 1 || true)"
        [ -n "$found" ] && target="$found"
    fi

    python3 - "$target" "$audit" "$DATE" <<'PY' 2>/dev/null || true
import sys
from pathlib import Path

target = Path(sys.argv[1])
audit = Path(sys.argv[2])
date = sys.argv[3]
begin = "# redis-queue-audit-begin"
end = "# redis-queue-audit-end"

base = ""
if target.exists():
    base = target.read_text(encoding="utf-8", errors="replace")
    start = base.find(begin)
    if start >= 0:
        base = base[:start].rstrip() + "\n"

audit_text = audit.read_text(encoding="utf-8", errors="replace").strip()
if not audit_text:
    raise SystemExit(0)

target.parent.mkdir(parents=True, exist_ok=True)
with target.open("w", encoding="utf-8") as f:
    if base.strip():
        f.write(base.rstrip() + "\n")
    f.write(f"{begin} date={date} source=logs/redis-queue/{date}.ndjson\n")
    f.write(audit_text)
    f.write(f"\n{end}\n")
PY
}
append_redis_queue_audit_to_redis_log

# ---- 3. pm2 snapshot --------------------------------------------------------
if command -v pm2 >/dev/null 2>&1; then
    pm2 jlist > "$DAYDIR/pm2-status.json" 2>/dev/null || true
    pm2 flush >/dev/null 2>&1 || true
fi

# ---- persist offsets --------------------------------------------------------
OFFSET_KV=""
for k in "${!OFFSETS[@]}"; do
    OFFSET_KV+="$k	${OFFSETS[$k]}"$'\n'
done
write_offsets

# ---- 4. errors rollup (cumulative day view, capped per source) --------------
ROLLUP="$DAYDIR/errors-rollup.txt"
{
    printf '# errors-rollup %s (generated %s)\n' "$DATE" "$(ts_now)"
    printf '# error/warn lines per collected source (last 200 each)\n'
    printf '# sources_collected=%s  docker_status=%s  discovery_mode=%s\n' "${#SRC_NAMES[@]}" "$DOCKER_STATUS" "$DISCOVERY_MODE"
    [ -n "$DOCKER_WARN_CODE" ] && printf '# %s: %s\n' "$DOCKER_WARN_CODE" "$DOCKER_WARNING"
    [ -n "$DOCKER_ZERO_REASON" ] && printf '# docker_zero_reason: %s\n' "$DOCKER_ZERO_REASON"
    [ "$DOCKER_ZERO_REASON" = "all_visible_mode_skip" ] && printf '# docker log collection skipped: all_visible mode (set OPS_DOCKER_COMPOSE_PROJECT or OPS_DOCKER_LOG_ALL_VISIBLE=1)\n'
    [ "$EXPECTED_CONFIGURED" = "false" ] && [ "$DISCOVERY_MODE" != "all_visible" ] && printf '# expected-containers-not-configured: set OPS_DOCKER_COMPOSE_PROJECT or OPS_DOCKER_CONTAINERS\n'
    printf '\n'
    shopt -s nullglob
    for f in "$DAYDIR/app.log" "$DAYDIR/local-server.out.log" "$DAYDIR/local-server.err.log" "$CONTDIR"/*.log; do
        [ -f "$f" ] || continue
        case "$(basename "$f")" in _docker-*.log) continue;; esac
        # Container logs are raw stdout/stderr -> use the broader matchers.
        case "$f" in "$CONTDIR"/*) e_re="$GENERIC_ERR_RE"; w_re="$GENERIC_WARN_RE";; *) e_re="$ERR_RE"; w_re="$WARN_RE";; esac
        ec=$(count_re "$f" "$e_re")
        wc_=$(count_re "$f" "$w_re")
        rel="${f#$DAYDIR/}"
        printf '=== %s : errors=%s warns=%s ===\n' "$rel" "$ec" "$wc_"
        if [ "$ec" -gt 0 ] || [ "$wc_" -gt 0 ]; then
            grep -E "$e_re|$w_re" "$f" 2>/dev/null | tail -n 200
        fi
        printf '\n'
    done
    shopt -u nullglob
} > "$ROLLUP" 2>/dev/null || true

# ---- 5. summary JSON --------------------------------------------------------
SOURCES_JSON="["
for i in "${!SRC_NAMES[@]}"; do
    [ "$i" -gt 0 ] && SOURCES_JSON+=","
    SOURCES_JSON+="{\"name\":$(json_str "${SRC_NAMES[$i]}"),\"lines_collected\":${SRC_LINES[$i]},\"errors\":${SRC_ERRS[$i]},\"warns\":${SRC_WARNS[$i]}}"
done
SOURCES_JSON+="]"

DOCKER_JSON_SUMMARY="{\"status\":$(json_str "$DOCKER_STATUS"),\"available\":${DOCKER_AVAIL},\"permission_ok\":${DOCKER_PERM},\"discovery_mode\":$(json_str "$DISCOVERY_MODE"),\"expected_configured\":${EXPECTED_CONFIGURED},\"compose_project\":$(json_str "$COMPOSE_PROJECT"),\"since\":$(json_str "$DOCKER_SINCE"),\"until\":$(json_str "$DOCKER_UNTIL"),\"tail\":$(json_str "$DOCKER_TAIL")"
[ -n "$DOCKER_WARNING" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"warning\":$(json_str "$DOCKER_WARNING")"
[ -n "$DOCKER_WARN_CODE" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"warning_code\":$(json_str "$DOCKER_WARN_CODE")"
[ -n "$DOCKER_ZERO_REASON" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"zero_reason\":$(json_str "$DOCKER_ZERO_REASON")"
[ -n "${DOCKER_STATUS_FILE:-}" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"status_file\":$(json_str "${DOCKER_STATUS_FILE#$REPO_DIR/}")"
DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"containers_configured\":${EXPECTED_CONFIGURED},\"containers_watched\":$(json_str "$CONTAINERS")}"

SUMMARY="{\"ts\":\"$(ts_now)\",\"role\":\"$ROLE\",\"date\":\"$DATE\",\"sources\":$SOURCES_JSON,\"source_count\":${#SRC_NAMES[@]},\"total_errors\":$TOTAL_ERR,\"total_warns\":$TOTAL_WARN,\"docker\":$DOCKER_JSON_SUMMARY}"
printf '%s\n' "$SUMMARY" > "$SUMMARY_FILE"

# Raise a deduped alert when docker is blocked so sources=0 is explained, not silent.
DOCKER_ALERT_ACTIVE=0
case "$DOCKER_STATUS" in permission_denied|no_daemon) DOCKER_ALERT_ACTIVE=1;; esac
prev_docker=0
DOCKER_ALERT_STATE="$STATUS_DIR/logcollector-docker-alert-state.json"
if [ -f "$DOCKER_ALERT_STATE" ]; then
    prev_docker=$(python3 - "$DOCKER_ALERT_STATE" <<'PY' 2>/dev/null || echo 0
import json, sys
try:
    print(1 if json.load(open(sys.argv[1])).get("blocked") else 0)
except Exception:
    print(0)
PY
)
fi
if [ "$DOCKER_ALERT_ACTIVE" = "1" ] && [ "$prev_docker" != "1" ]; then
    printf '{"ts":"%s","role":"%s","severity":"warn","event":"docker-logs-unavailable","detail":%s}\n' \
        "$(ts_now)" "$ROLE" "$(json_str "$DOCKER_WARNING")" >> "$ALERTS_FILE"
elif [ "$DOCKER_ALERT_ACTIVE" = "0" ] && [ "$prev_docker" = "1" ]; then
    printf '{"ts":"%s","role":"%s","severity":"info","event":"docker-logs-unavailable-recovered","detail":"docker log collection restored"}\n' \
        "$(ts_now)" "$ROLE" >> "$ALERTS_FILE"
fi
printf '{"blocked":%s,"status":%s}\n' "$([ "$DOCKER_ALERT_ACTIVE" = 1 ] && echo true || echo false)" "$(json_str "$DOCKER_STATUS")" > "$DOCKER_ALERT_STATE"

# ---- 6. error-spike alert (deduped via state file) --------------------------
prev_spike=0
if [ -f "$ALERT_STATE" ]; then
    prev_spike=$(python3 - "$ALERT_STATE" <<'PY' 2>/dev/null || echo 0
import json, sys
try:
    with open(sys.argv[1]) as f:
        print(1 if json.load(f).get("in_spike") else 0)
except Exception:
    print(0)
PY
)
fi

cur_spike=0
if [ "$TOTAL_ERR" -gt "$ERROR_SPIKE" ]; then
    cur_spike=1
fi

if [ "$cur_spike" = "1" ] && [ "$prev_spike" != "1" ]; then
    ALERT="{\"ts\":\"$(ts_now)\",\"role\":\"$ROLE\",\"severity\":\"warn\",\"event\":\"error-spike\",\"detail\":\"collected $TOTAL_ERR errors in one window (threshold $ERROR_SPIKE)\",\"total_errors\":$TOTAL_ERR}"
    printf '%s\n' "$ALERT" >> "$ALERTS_FILE"
    jlog "warn" "$ROLE" "error-spike alert raised" "{\"total_errors\":$TOTAL_ERR,\"threshold\":$ERROR_SPIKE}"
fi
printf '{"ts":"%s","in_spike":%s,"total_errors":%s,"threshold":%s}\n' "$(ts_now)" "$([ "$cur_spike" = 1 ] && echo true || echo false)" "$TOTAL_ERR" "$ERROR_SPIKE" > "$ALERT_STATE"

heartbeat "$ROLE"
jlog "info" "$ROLE" "log collection complete" "{\"date\":$(json_str "$DATE"),\"total_errors\":$TOTAL_ERR,\"total_warns\":$TOTAL_WARN,\"sources\":${#SRC_NAMES[@]}}"
exit 0
