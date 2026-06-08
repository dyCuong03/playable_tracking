#!/usr/bin/env bash
# logcollector.sh — one-shot backend log collector for the pixel-tracking server.
#
# Collects every backend log source into the daily archive ops/logs/<UTC-date>/:
#   - app.log               : incremental tail of repo logs/pixel-tracking.txt
#   - local-server.*.log    : incremental tail of repo logs/local-server.{out,err}.log
#   - docker/<container>.log : `docker logs --tail 500` per known container (if docker usable)
#   - pm2-status.json        : `pm2 jlist` snapshot (if pm2 present)
# Then builds errors-rollup.txt (error/warn lines per source) and a summary JSON,
# and raises an "error-spike" alert when this window's errors exceed ERROR_SPIKE.
#
# Designed for a NOT-yet-deployed server: every source is optional. Missing
# sources are skipped cleanly; missing docker/containers produce a timestamped
# _docker-unavailable.log. Never aborts on a single source failure.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="logcollector"
heartbeat "$ROLE"

DATE="$(date -u +%Y-%m-%d)"
# Per-spec path: ops/logs/<date>/docker/<container>.log (was containers/ pre-audit).
DAYDIR="$LOGS_DIR/$DATE"
CONTDIR="$DAYDIR/docker"
mkdir -p "$CONTDIR"

OFFSETS_FILE="$STATUS_DIR/logcollector-offsets.json"
ALERT_STATE="$STATUS_DIR/logcollector-alert-state.json"
SUMMARY_FILE="$STATUS_DIR/logcollector-latest.json"
ALERTS_FILE="$STATUS_DIR/alerts.ndjson"
ERROR_SPIKE="${ERROR_SPIKE:-20}"

# Operator-declared containers win; otherwise fall back to the known names.
# CONTAINERS_CONFIGURED records whether the operator explicitly declared them, so
# the rollup/dashboard can say "expected-containers-not-configured" vs guessing.
if [ -n "${OPS_DOCKER_CONTAINERS:-}" ]; then CONTAINERS_CONFIGURED=true; else CONTAINERS_CONFIGURED=false; fi
CONTAINERS="${OPS_DOCKER_CONTAINERS:-pixel-server pixel-worker pixel-dispatcher redis nginx}"
DOCKER_SINCE="${LOGCOLLECT_DOCKER_SINCE:-10m}"
DOCKER_TAIL="${LOGCOLLECT_DOCKER_TAIL:-500}"

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

# ---- 2. container logs ------------------------------------------------------
DOCKER_UNAVAIL="$CONTDIR/_docker-unavailable.log"
DOCKER_REASONS="$(mktemp)"
# DOCKER_STATUS is surfaced in the summary + rollup so a bare sources=0 is never
# mistaken for "all quiet" when docker is actually blocked by permission.
read DOCKER_AVAIL DOCKER_PERM DOCKER_STATE <<<"$(docker_access)"
DOCKER_STATUS="$DOCKER_STATE"
DOCKER_WARNING=""
# Stable machine token surfaced in rollup + status whenever docker logs could not
# be collected (no CLI / no daemon / permission denied) — see spec task 4.
DOCKER_WARN_CODE=""
case "$DOCKER_STATE" in ok) ;; *) DOCKER_WARN_CODE="docker_unavailable_or_permission_denied";; esac

if [ "$DOCKER_STATE" = "ok" ]; then
    present="$(docker ps -a --format '{{.Names}}' 2>/dev/null || true)"
    for c in $CONTAINERS; do
        if printf '%s\n' "$present" | grep -qx "$c"; then
            tmp="$(mktemp)"
            if docker logs --since "$DOCKER_SINCE" --tail "$DOCKER_TAIL" "$c" > "$tmp" 2>&1; then
                cat "$tmp" > "$CONTDIR/$c.log"
                record_source "container:$c" "$tmp" "$GENERIC_ERR_RE" "$GENERIC_WARN_RE"
            else
                jlog "warn" "$ROLE" "docker logs failed for container" "{\"container\":$(json_str "$c")}" >> "$DOCKER_REASONS"
            fi
            rm -f "$tmp"
        else
            jlog "warn" "$ROLE" "container absent - skipped" "{\"container\":$(json_str "$c")}" >> "$DOCKER_REASONS"
        fi
    done
elif [ "$DOCKER_STATE" = "permission_denied" ]; then
    DOCKER_WARNING="docker permission denied — add the deploy user to the docker group (sudo usermod -aG docker \$USER, then re-login) so container logs can be collected."
    jlog "warn" "$ROLE" "docker permission denied - container logs skipped" "{\"reason\":\"permission_denied\"}" >> "$DOCKER_REASONS"
elif [ "$DOCKER_STATE" = "no_cli" ]; then
    DOCKER_WARNING="docker CLI not installed — container logs not collected."
    jlog "warn" "$ROLE" "docker CLI missing - container logs skipped" "{\"reason\":\"no_cli\"}" >> "$DOCKER_REASONS"
else
    DOCKER_WARNING="docker daemon unreachable — container logs not collected."
    jlog "warn" "$ROLE" "docker daemon unreachable - container logs skipped" "{\"reason\":\"no_daemon\"}" >> "$DOCKER_REASONS"
fi

if [ -s "$DOCKER_REASONS" ]; then
    cat "$DOCKER_REASONS" > "$DOCKER_UNAVAIL"
fi
rm -f "$DOCKER_REASONS"

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
    printf '# sources_collected=%s  docker_status=%s\n' "${#SRC_NAMES[@]}" "$DOCKER_STATUS"
    [ -n "$DOCKER_WARN_CODE" ] && printf '# %s: %s\n' "$DOCKER_WARN_CODE" "$DOCKER_WARNING"
    [ "$CONTAINERS_CONFIGURED" = false ] && printf '# expected-containers-not-configured: set OPS_DOCKER_CONTAINERS to collect container logs\n'
    printf '\n'
    shopt -s nullglob
    for f in "$DAYDIR/app.log" "$DAYDIR/local-server.out.log" "$DAYDIR/local-server.err.log" "$CONTDIR"/*.log; do
        [ -f "$f" ] || continue
        case "$(basename "$f")" in _docker-unavailable.log) continue;; esac
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

DOCKER_JSON_SUMMARY="{\"status\":$(json_str "$DOCKER_STATUS"),\"available\":${DOCKER_AVAIL},\"permission_ok\":${DOCKER_PERM}"
[ -n "$DOCKER_WARNING" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"warning\":$(json_str "$DOCKER_WARNING")"
[ -n "$DOCKER_WARN_CODE" ] && DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"warning_code\":$(json_str "$DOCKER_WARN_CODE")"
DOCKER_JSON_SUMMARY="${DOCKER_JSON_SUMMARY},\"containers_configured\":${CONTAINERS_CONFIGURED},\"containers_watched\":$(json_str "$CONTAINERS")}"

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
