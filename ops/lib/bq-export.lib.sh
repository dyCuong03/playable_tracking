#!/usr/bin/env bash
# ops/lib/bq-export.lib.sh — BigQuery export helpers for the pixel-tracking ops team.
# Sourced by ops/bin/bq-log-exporter.sh AFTER ops/lib/common.sh.
#
# Requires (from common.sh): OPS_DIR STATUS_DIR LOGS_DIR REPO_DIR
#   json_str  jlog  ts_now  heartbeat
#   ops_docker_discover  ops_redis_detect  docker_access
#
# Environment variables consumed (all optional; defaults shown):
#   OPS_NGINX_ACCESS_LOG_PATH    — (A) direct nginx access log file path
#   OPS_NGINX_CONTAINER          — (B) named nginx container for docker logs
#   OPS_LOG_HASH_IP=1            — sha256 remote_addr when "1" (default)
#   OPS_LOG_QUERY_ALLOWLIST      — space-separated query param keys to keep
#                                   default: e pid sid playableId platform camp campaign_raw env
#   OPS_REDIS_CONTAINER          — redis container name (shared with monitor.sh)
#   OPS_REDIS_QUEUE_KEY          — redis queue key for depth check
#   OPS_BQ_PROJECT               — GCP project id
#   OPS_BQ_DATASET               — BigQuery dataset
#   OPS_BQ_NGINX_TABLE           — nginx table name (default: nginx_requests)
#   OPS_BQ_REDIS_TABLE           — redis table name (default: redis_metrics)
#   OPS_BQ_DRY_RUN=0             — pass --dry-run to uploader when "1"
#   OPS_BQ_STAGING_BACKLOG_WARN  — alert threshold in rows (default: 50000)

# ---------------------------------------------------------------------------
# Constants — resolved once after common.sh has set STATUS_DIR / LOGS_DIR.
# ---------------------------------------------------------------------------
BQ_STATE_FILE="${BQ_STATE_FILE:-$STATUS_DIR/bq-export-state.json}"
BQ_LATEST_FILE="${BQ_LATEST_FILE:-$STATUS_DIR/bq-export-latest.json}"
BQ_ALERT_STATE_FILE="${BQ_ALERT_STATE_FILE:-$STATUS_DIR/bq-export-alert-state.json}"
BQ_ALERTS_FILE="${BQ_ALERTS_FILE:-$STATUS_DIR/alerts.ndjson}"

# Default query param allowlist — pixel server reserved params.
_BQ_DEFAULT_ALLOWLIST="e pid sid playableId platform camp campaign_raw env"

# ---------------------------------------------------------------------------
# Collect-output globals (written by nginx_collect / redis_collect).
# Callers read these after the collect call; they are never exported to avoid
# polluting child processes.
# ---------------------------------------------------------------------------
NGINX_STATUS="not_configured"
NGINX_SOURCE="not_configured"
NGINX_CONTAINER=""
NGINX_ROWS_STAGED=0
NGINX_ROWS_UPLOADED_LAST=0

REDIS_STATUS="not_configured"
REDIS_ROWS_UPLOADED_LAST=0
REDIS_CONTAINER_USED=""

BQ_UPLOAD_STATUS="disabled"
BQ_UPLOAD_LAST_TS=""
BQ_UPLOAD_ERROR=""

# Uploader result globals (written by bq_invoke_uploader).
BQ_UPL_STATUS="failed"
BQ_UPL_UPLOADED=0
BQ_UPL_ERRORS=0
BQ_UPL_MESSAGE=""

# Cursor state (written by bq_read_cursor, read by bq_write_cursor).
BQ_CURSOR_NGINX_FILE_PATH=""
BQ_CURSOR_NGINX_FILE_OFFSET=0
BQ_CURSOR_NGINX_DOCKER_SINCE=""
BQ_CURSOR_NGINX_CONTAINER=""

# ---------------------------------------------------------------------------
# Alert helpers — scoped to bq-export, do not conflict with monitor.sh.
# ---------------------------------------------------------------------------

bq_append_alert() {
    local severity="$1" event="$2" detail="$3"
    printf '{"ts":"%s","role":"bq-export","severity":%s,"event":%s,"detail":%s}\n' \
        "$(ts_now)" "$(json_str "$severity")" "$(json_str "$event")" "$(json_str "$detail")" \
        >> "$BQ_ALERTS_FILE" 2>/dev/null || true
}

_bq_read_alert_state() {
    python3 - "$BQ_ALERT_STATE_FILE" "$1" <<'PY' 2>/dev/null || echo 0
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    d = {}
print(int(d.get(sys.argv[2], 0)))
PY
}

# State accumulator — flushed to file by bq_flush_alert_state().
NEW_BQ_ALERT_STATE=""

# bq_check_alert EVENT SEVERITY ACTIVE(1/0) DETAIL
# Fires on edge only (0→1 raises alert, 1→0 raises recovery info).
bq_check_alert() {
    local event="$1" severity="$2" active="$3" detail="$4" prev
    prev="$(_bq_read_alert_state "$event")"
    if [ "$active" = "1" ] && [ "$prev" != "1" ]; then
        bq_append_alert "$severity" "$event" "$detail"
    elif [ "$active" = "0" ] && [ "$prev" = "1" ]; then
        bq_append_alert "info" "${event}-recovered" "$detail"
    fi
    NEW_BQ_ALERT_STATE="${NEW_BQ_ALERT_STATE}$(json_str "$event"):${active},"
}

bq_flush_alert_state() {
    printf '{%s}\n' "${NEW_BQ_ALERT_STATE%,}" > "$BQ_ALERT_STATE_FILE" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Cursor management
# ---------------------------------------------------------------------------

# Populate BQ_CURSOR_* globals from ops/status/bq-export-state.json.
bq_read_cursor() {
    BQ_CURSOR_NGINX_FILE_PATH=""
    BQ_CURSOR_NGINX_FILE_OFFSET=0
    BQ_CURSOR_NGINX_DOCKER_SINCE=""
    BQ_CURSOR_NGINX_CONTAINER=""
    [ -f "$BQ_STATE_FILE" ] || return 0

    local _out
    _out="$(python3 - "$BQ_STATE_FILE" <<'PY' 2>/dev/null || true
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    d = {}
print(d.get("nginx_file_path", ""))
print(str(d.get("nginx_file_offset", 0)))
print(d.get("nginx_docker_since", ""))
print(d.get("nginx_container", ""))
PY
)"
    BQ_CURSOR_NGINX_FILE_PATH="$(  printf '%s\n' "$_out" | sed -n '1p')"
    BQ_CURSOR_NGINX_FILE_OFFSET="$(printf '%s\n' "$_out" | sed -n '2p')"
    BQ_CURSOR_NGINX_DOCKER_SINCE="$(printf '%s\n' "$_out" | sed -n '3p')"
    BQ_CURSOR_NGINX_CONTAINER="$(  printf '%s\n' "$_out" | sed -n '4p')"

    # Validate offset is a non-negative integer.
    case "${BQ_CURSOR_NGINX_FILE_OFFSET:-0}" in
        ''|*[!0-9]*) BQ_CURSOR_NGINX_FILE_OFFSET=0 ;;
    esac
}

# Persist BQ_CURSOR_* globals back to the state file.
# Caller passes the current timestamp so Python doesn't need subprocess.
bq_write_cursor() {
    local now="$1"
    python3 - \
        "$BQ_STATE_FILE" \
        "${BQ_CURSOR_NGINX_FILE_PATH:-}" \
        "${BQ_CURSOR_NGINX_FILE_OFFSET:-0}" \
        "${BQ_CURSOR_NGINX_DOCKER_SINCE:-}" \
        "${BQ_CURSOR_NGINX_CONTAINER:-}" \
        "$now" <<'PY' 2>/dev/null || true
import json, sys
path        = sys.argv[1]
file_path   = sys.argv[2]
offset_s    = sys.argv[3]
docker_since = sys.argv[4]
container   = sys.argv[5]
updated_at  = sys.argv[6]
try:
    offset = int(offset_s)
except ValueError:
    offset = 0
d = {
    "nginx_file_path":     file_path,
    "nginx_file_offset":   offset,
    "nginx_docker_since":  docker_since,
    "nginx_container":     container,
    "updated_at":          updated_at,
}
with open(path, "w") as f:
    json.dump(d, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# ---------------------------------------------------------------------------
# Latest status writer
# ---------------------------------------------------------------------------

# bq_write_latest — overwrite ops/status/bq-export-latest.json atomically.
# All values passed via positional args so callers don't need to export env vars.
# Args: ENABLED NGINX_STATUS NGINX_SOURCE NGINX_CONTAINER
#       NGINX_STAGED NGINX_UPLOADED
#       REDIS_STATUS REDIS_UPLOADED
#       UPLOAD_STATUS UPLOAD_TS UPLOAD_ERR
#       TBL_NGINX TBL_REDIS BACKLOG
bq_write_latest() {
    local _enabled="$1"
    local _ng_status="$2"  _ng_source="$3"  _ng_container="$4"
    local _ng_staged="$5"  _ng_uploaded="$6"
    local _rd_status="$7"  _rd_uploaded="$8"
    local _up_status="$9"  _up_ts="${10}"   _up_err="${11}"
    local _tbl_ng="${12}"  _tbl_rd="${13}"  _backlog="${14}"
    local _now
    _now="$(ts_now)"

    python3 - \
        "$BQ_LATEST_FILE" "$_now" "$_enabled" \
        "$_ng_status"    "$_ng_source"  "$_ng_container" \
        "$_ng_staged"    "$_ng_uploaded" \
        "$_rd_status"    "$_rd_uploaded" \
        "$_up_status"    "$_up_ts"       "$_up_err" \
        "$_tbl_ng"       "$_tbl_rd"      "$_backlog" <<'PY' 2>/dev/null || true
import json, sys

def i(s):
    try:    return int(s)
    except: return 0

(_, path, ts, enabled,
 ng_status, ng_source, ng_container,
 ng_staged, ng_uploaded,
 rd_status, rd_uploaded,
 up_status, up_ts, up_err,
 tbl_ng, tbl_rd, backlog) = sys.argv

doc = {
    "ts":      ts,
    "enabled": enabled == "1",
    "nginx": {
        "status":             ng_status,
        "source":             ng_source,
        "container":          ng_container or None,
        "rows_staged":        i(ng_staged),
        "rows_uploaded_last": i(ng_uploaded),
    },
    "redis": {
        "status":             rd_status,
        "rows_uploaded_last": i(rd_uploaded),
    },
    "upload": {
        "status":        up_status,
        "last_upload_ts": up_ts or None,
        "error":          up_err or None,
    },
    "tables": {
        "nginx": tbl_ng or None,
        "redis": tbl_rd or None,
    },
    "staging_backlog_rows": i(backlog),
}
with open(path, "w") as f:
    json.dump(doc, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# ---------------------------------------------------------------------------
# Uploader invocation
# ---------------------------------------------------------------------------

# bq_invoke_uploader TABLE FILE [--dry-run]
# Writes result into globals: BQ_UPL_STATUS BQ_UPL_UPLOADED BQ_UPL_ERRORS BQ_UPL_MESSAGE
# Always returns 0 (uploader contract: graceful degrade on all errors).
bq_invoke_uploader() {
    local table="$1" file="$2" dry="${3:-}"
    BQ_UPL_STATUS="failed"; BQ_UPL_UPLOADED=0; BQ_UPL_ERRORS=0; BQ_UPL_MESSAGE=""

    local node_bin
    node_bin="$(command -v node 2>/dev/null || true)"
    if [ -z "$node_bin" ]; then
        BQ_UPL_STATUS="config_missing"
        BQ_UPL_MESSAGE="node not found on PATH"
        return 0
    fi

    local uploader="$OPS_DIR/bin/bq-upload.js"
    if [ ! -f "$uploader" ]; then
        BQ_UPL_STATUS="config_missing"
        BQ_UPL_MESSAGE="bq-upload.js not found at $uploader"
        return 0
    fi

    local args=("$node_bin" "$uploader" "--table" "$table" "--file" "$file")
    [ "$dry" = "--dry-run" ] && args+=("--dry-run")

    local out
    out="$("${args[@]}" 2>/dev/null)" || true

    # Parse the single JSON line from stdout.
    local _parsed
    _parsed="$(printf '%s\n' "${out:-}" | python3 - <<'PY' 2>/dev/null || true
import json, sys
try:
    raw = sys.stdin.read().strip()
    # Handle multi-line output: use last non-empty line that parses as JSON.
    d = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except Exception:
            pass
    print(d.get("status", "failed"))
    print(str(d.get("uploaded", 0)))
    print(str(d.get("errors", 0)))
    print(d.get("message", ""))
except Exception as e:
    print("failed"); print("0"); print("0"); print(str(e))
PY
)"
    BQ_UPL_STATUS="$(  printf '%s\n' "$_parsed" | sed -n '1p')"
    BQ_UPL_UPLOADED="$(printf '%s\n' "$_parsed" | sed -n '2p')"
    BQ_UPL_ERRORS="$(  printf '%s\n' "$_parsed" | sed -n '3p')"
    BQ_UPL_MESSAGE="$( printf '%s\n' "$_parsed" | sed -n '4p')"

    # Sanitize numerics.
    case "${BQ_UPL_UPLOADED:-0}" in ''|*[!0-9]*) BQ_UPL_UPLOADED=0 ;; esac
    case "${BQ_UPL_ERRORS:-0}"   in ''|*[!0-9]*) BQ_UPL_ERRORS=0   ;; esac
    [ -z "$BQ_UPL_STATUS" ] && BQ_UPL_STATUS="failed"
    return 0
}

# ---------------------------------------------------------------------------
# Nginx log line parser (Python)
# ---------------------------------------------------------------------------

# _bq_nginx_parse_lines SOURCE CONTAINER HASH_IP ALLOWLIST
# Reads raw nginx log lines from stdin; writes valid parsed NDJSON rows to stdout.
# Unparseable lines are silently skipped (count logged to stderr).
_bq_nginx_parse_lines() {
    local source="$1" container="$2" hash_ip="${3:-1}" allowlist="${4:-$_BQ_DEFAULT_ALLOWLIST}"

    SOURCE="$source" \
    CONTAINER="$container" \
    HASH_IP="$hash_ip" \
    ALLOWLIST="$allowlist" \
    python3 - <<'PY'
import sys, os, re, json, hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

source    = os.environ.get("SOURCE", "file")
container = os.environ.get("CONTAINER", "") or None
hash_ip   = os.environ.get("HASH_IP", "1") == "1"
allow_raw = os.environ.get("ALLOWLIST", "e pid sid playableId platform camp campaign_raw env")
allowlist = set(allow_raw.split()) if allow_raw.strip() else set()

# Nginx combined log regex.
# Standard combined:  … "UA"
# Extended with req time: … "UA" 0.012
COMBINED = re.compile(
    r'^(?P<remote_addr>\S+)'
    r' - (?P<remote_user>\S+)'
    r' \[(?P<time_local>[^\]]+)\]'
    r' "(?P<request>[^"]*)"'
    r' (?P<status>\d+)'
    r' (?P<body_bytes>\d+|-)'
    r' "(?P<referer>[^"]*)"'
    r' "(?P<user_agent>[^"]*)"'
    r'(?:\s+(?P<request_time>[\d.]+))?'
    r'\s*$'
)

def sha256(s):
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()

def parse_time(time_local):
    """Return (event_date str, ts_iso str) or (today, now) on failure."""
    try:
        dt = datetime.strptime(time_local, "%d/%b/%Y:%H:%M:%S %z")
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%d"), dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_path_query(raw):
    """Return (path str, filtered_query dict)."""
    try:
        if not raw.startswith("/"):
            raw = "/" + raw
        p = urlparse(raw)
        path = p.path
        qs = parse_qs(p.query, keep_blank_values=True)
        q_out = {k: v[0] for k, v in qs.items() if k in allowlist} if allowlist else {}
        return path, q_out
    except Exception:
        return raw, {}

skipped = 0
for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue
    m = COMBINED.match(line)
    if not m:
        skipped += 1
        continue

    remote_addr  = m.group("remote_addr")
    time_local   = m.group("time_local")
    request_str  = m.group("request")
    status_str   = m.group("status")
    body_str     = m.group("body_bytes")
    referer      = m.group("referer")
    user_agent   = m.group("user_agent")
    req_time_str = m.group("request_time")

    # "METHOD /path PROTO"
    req_parts = request_str.split(" ", 2)
    method = req_parts[0] if len(req_parts) >= 1 else ""
    path_q = req_parts[1] if len(req_parts) >= 2 else ""
    path, query = parse_path_query(path_q)

    event_date, ts = parse_time(time_local)

    # insert_id: deterministic sha256(raw_line + "\n" + container_name_or_empty)
    insert_id = sha256(line + "\n" + (container or ""))

    row = {
        "insert_id":       insert_id,
        "event_date":      event_date,
        "ts":              ts,
        "source":          source,
        "container":       container,
        "remote_ip_hash":  sha256(remote_addr) if hash_ip else remote_addr,
        "method":          method,
        "path":            path,
        "query":           query,
        "status":          int(status_str) if status_str.isdigit() else None,
        "request_time_ms": int(float(req_time_str) * 1000) if req_time_str else None,
        "body_bytes_sent": int(body_str) if (body_str and body_str.isdigit()) else None,
        "referer":         referer if referer != "-" else None,
        "user_agent_hash": sha256(user_agent),
        "request_id":      None,
        "raw_sample":      None,
    }
    print(json.dumps(row, separators=(",", ":")))

if skipped:
    print(json.dumps({"_skipped_lines": skipped, "source": source}), file=sys.stderr)
PY
}

# ---------------------------------------------------------------------------
# nginx_collect STAGING_DIR
# ---------------------------------------------------------------------------
# Populates globals: NGINX_STATUS NGINX_SOURCE NGINX_CONTAINER NGINX_ROWS_STAGED
# Appends parsed NDJSON rows to STAGING_DIR/nginx_requests.ndjson.
#
# Priority chain:
#   (A) OPS_NGINX_ACCESS_LOG_PATH file exists → incremental byte-cursor read
#   (B) OPS_NGINX_CONTAINER set + docker ok → docker logs --since cursor
#   (C) ops_docker_discover auto-detects a "nginx" container → same as (B)
#   (D) nothing reachable → not_configured (no alert here; caller alerts)
nginx_collect() {
    local staging_dir="$1"
    local ndjson="$staging_dir/nginx_requests.ndjson"
    NGINX_STATUS="not_configured"
    NGINX_SOURCE="not_configured"
    NGINX_CONTAINER=""
    NGINX_ROWS_STAGED=0

    local log_file="${OPS_NGINX_ACCESS_LOG_PATH:-}"
    local nginx_cont="${OPS_NGINX_CONTAINER:-}"
    local hash_ip="${OPS_LOG_HASH_IP:-1}"
    local allowlist="${OPS_LOG_QUERY_ALLOWLIST:-$_BQ_DEFAULT_ALLOWLIST}"

    # --- (A) direct file ---
    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
        local size off
        size="$(stat -c %s "$log_file" 2>/dev/null || echo 0)"
        off="${BQ_CURSOR_NGINX_FILE_OFFSET:-0}"

        # Detect path change (renamed/rotated to a different file).
        if [ -n "${BQ_CURSOR_NGINX_FILE_PATH:-}" ] && \
           [ "$BQ_CURSOR_NGINX_FILE_PATH" != "$log_file" ]; then
            off=0
        fi
        # Detect truncation (file rotated in-place).
        if [ "${off:-0}" -gt "$size" ]; then off=0; fi
        # Nothing new.
        if [ "$off" -ge "$size" ]; then
            NGINX_STATUS="ok"; NGINX_SOURCE="file"
            BQ_CURSOR_NGINX_FILE_PATH="$log_file"
            BQ_CURSOR_NGINX_FILE_OFFSET="$size"
            return 0
        fi

        local _raw _parsed
        _raw="$(mktemp)"
        _parsed="$(mktemp)"
        tail -c +"$(( off + 1 ))" "$log_file" > "$_raw" 2>/dev/null || true
        _bq_nginx_parse_lines "file" "" "$hash_ip" "$allowlist" \
            < "$_raw" > "$_parsed" 2>/dev/null || true
        NGINX_ROWS_STAGED="$(wc -l < "$_parsed" | tr -d ' ')"
        cat "$_parsed" >> "$ndjson" 2>/dev/null || true
        rm -f "$_raw" "$_parsed"

        NGINX_STATUS="ok"; NGINX_SOURCE="file"
        BQ_CURSOR_NGINX_FILE_PATH="$log_file"
        BQ_CURSOR_NGINX_FILE_OFFSET="$size"
        return 0
    fi

    # Docker required for (B) and (C).
    local docker_state
    docker_state="$(docker_access | awk '{print $3}')"

    # --- (B) explicit container ---
    if [ -n "$nginx_cont" ] && [ "$docker_state" = "ok" ]; then
        local since="${BQ_CURSOR_NGINX_DOCKER_SINCE:-1h}"
        local _raw _parsed
        _raw="$(mktemp)"
        _parsed="$(mktemp)"
        if docker logs --since "$since" "$nginx_cont" > "$_raw" 2>&1; then
            _bq_nginx_parse_lines "docker_exec" "$nginx_cont" "$hash_ip" "$allowlist" \
                < "$_raw" > "$_parsed" 2>/dev/null || true
            NGINX_ROWS_STAGED="$(wc -l < "$_parsed" | tr -d ' ')"
            cat "$_parsed" >> "$ndjson" 2>/dev/null || true
            NGINX_STATUS="ok"; NGINX_SOURCE="docker_exec"
            NGINX_CONTAINER="$nginx_cont"
            BQ_CURSOR_NGINX_DOCKER_SINCE="$(ts_now)"
            BQ_CURSOR_NGINX_CONTAINER="$nginx_cont"
        else
            NGINX_STATUS="error"
            jlog "warn" "bq-export" "docker logs failed for nginx container" \
                "{\"container\":$(json_str "$nginx_cont")}"
        fi
        rm -f "$_raw" "$_parsed"
        return 0
    fi

    # --- (C) auto-discover nginx container ---
    if [ "$docker_state" = "ok" ]; then
        local disc_json detected_nginx=""
        disc_json="$(ops_docker_discover 2>/dev/null || true)"
        detected_nginx="$(printf '%s\n' "$disc_json" | python3 -c '
import json, sys
try:
    d = json.loads(sys.stdin.read())
    for n in d.get("all_visible", []):
        if "nginx" in n.lower():
            print(n); raise SystemExit(0)
except Exception:
    pass
' 2>/dev/null || true)"

        if [ -n "$detected_nginx" ]; then
            local since="${BQ_CURSOR_NGINX_DOCKER_SINCE:-1h}"
            local _raw _parsed
            _raw="$(mktemp)"
            _parsed="$(mktemp)"
            if docker logs --since "$since" "$detected_nginx" > "$_raw" 2>&1; then
                _bq_nginx_parse_lines "docker_exec" "$detected_nginx" "$hash_ip" "$allowlist" \
                    < "$_raw" > "$_parsed" 2>/dev/null || true
                NGINX_ROWS_STAGED="$(wc -l < "$_parsed" | tr -d ' ')"
                cat "$_parsed" >> "$ndjson" 2>/dev/null || true
                NGINX_STATUS="ok"; NGINX_SOURCE="docker_exec"
                NGINX_CONTAINER="$detected_nginx"
                BQ_CURSOR_NGINX_DOCKER_SINCE="$(ts_now)"
                BQ_CURSOR_NGINX_CONTAINER="$detected_nginx"
            else
                NGINX_STATUS="error"
                jlog "warn" "bq-export" "docker logs failed for auto-detected nginx" \
                    "{\"container\":$(json_str "$detected_nginx")}"
            fi
            rm -f "$_raw" "$_parsed"
            return 0
        fi
    fi

    # --- (D) not configured ---
    jlog "info" "bq-export" \
        "nginx log source not configured; set OPS_NGINX_ACCESS_LOG_PATH or OPS_NGINX_CONTAINER" \
        "{\"docker_state\":$(json_str "$docker_state")}"
}

# ---------------------------------------------------------------------------
# redis_collect STAGING_DIR
# ---------------------------------------------------------------------------
# Populates globals: REDIS_STATUS REDIS_CONTAINER_USED
# Appends one NDJSON row to STAGING_DIR/redis_metrics.ndjson.
#
# Priority chain mirrors monitor.sh:
#   (A) host redis-cli on PATH
#   (B) OPS_REDIS_CONTAINER + docker ok
#   (C) ops_redis_detect auto-discovery + docker ok
#   (D) not_configured
#
# queue_depth is null when OPS_REDIS_QUEUE_KEY is unset
# (REDIS_STATUS becomes "queue_key_not_configured" in that case, not "error").
redis_collect() {
    local staging_dir="$1"
    local ndjson="$staging_dir/redis_metrics.ndjson"
    REDIS_STATUS="not_configured"
    REDIS_CONTAINER_USED=""

    local queue_key="${OPS_REDIS_QUEUE_KEY:-${REDIS_QUEUE_STREAM:-pixel:events}}"
    local key_configured=1
    [ -z "${OPS_REDIS_QUEUE_KEY:-}" ] && key_configured=0

    local info_out="" depth="" method="not_configured" error_msg=""

    # Helper: read XLEN then LLEN for the queue key.
    _bq_redis_depth() {
        local _d
        _d="$("$@" XLEN "$queue_key" 2>/dev/null)"
        case "${_d:-}" in (''|*[!0-9]*) _d="" ;; esac
        if [ -z "$_d" ]; then
            _d="$("$@" LLEN "$queue_key" 2>/dev/null)"
            case "${_d:-}" in (''|*[!0-9]*) _d="" ;; esac
        fi
        printf '%s' "${_d:-}"
    }

    local docker_state
    docker_state="$(docker_access | awk '{print $3}')"

    # (A) host redis-cli
    if command -v redis-cli >/dev/null 2>&1; then
        local _r_args=(redis-cli)
        [ -n "${REDIS_URL:-}" ] && _r_args=(redis-cli -u "$REDIS_URL")
        info_out="$("${_r_args[@]}" INFO 2>/dev/null || true)"
        if [ -n "$info_out" ]; then
            method="host_cli"
            if [ "$key_configured" = "1" ]; then
                depth="$(_bq_redis_depth "${_r_args[@]}")"
                REDIS_STATUS="ok"
            else
                REDIS_STATUS="queue_key_not_configured"
            fi
        else
            REDIS_STATUS="error"
            error_msg="redis-cli on PATH but INFO returned empty"
        fi
    fi

    # (B) OPS_REDIS_CONTAINER
    if [ "$REDIS_STATUS" = "not_configured" ] && \
       [ -n "${OPS_REDIS_CONTAINER:-}" ] && [ "$docker_state" = "ok" ]; then
        local _rc="${OPS_REDIS_CONTAINER}"
        info_out="$(docker exec "$_rc" redis-cli INFO 2>/dev/null || true)"
        if [ -n "$info_out" ]; then
            method="docker_exec"; REDIS_CONTAINER_USED="$_rc"
            if [ "$key_configured" = "1" ]; then
                depth="$(_bq_redis_depth docker exec "$_rc" redis-cli)"
                REDIS_STATUS="ok"
            else
                REDIS_STATUS="queue_key_not_configured"
            fi
        else
            REDIS_STATUS="error"
            error_msg="docker exec redis-cli INFO failed (container=${_rc})"
        fi
    fi

    # (C) auto-detect
    if [ "$REDIS_STATUS" = "not_configured" ] && [ "$docker_state" = "ok" ]; then
        local _auto
        _auto="$(ops_redis_detect 2>/dev/null || true)"
        if [ -n "$_auto" ]; then
            info_out="$(docker exec "$_auto" redis-cli INFO 2>/dev/null || true)"
            if [ -n "$info_out" ]; then
                method="docker_exec"; REDIS_CONTAINER_USED="$_auto"
                if [ "$key_configured" = "1" ]; then
                    depth="$(_bq_redis_depth docker exec "$_auto" redis-cli)"
                    REDIS_STATUS="ok"
                else
                    REDIS_STATUS="queue_key_not_configured"
                fi
            else
                REDIS_STATUS="error"
                error_msg="docker exec redis-cli INFO failed (auto-detected ${_auto})"
            fi
        fi
    fi

    # Build and append one row regardless of status (errors still produce a row).
    local ts event_date
    ts="$(ts_now)"; event_date="${ts%T*}"
    local insert_id
    insert_id="$(printf '%s' "${ts}${REDIS_CONTAINER_USED:-}" \
        | python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.stdin.read().encode()).hexdigest())' \
        2>/dev/null || echo "")"

    if [ "$REDIS_STATUS" != "ok" ] && [ "$REDIS_STATUS" != "queue_key_not_configured" ]; then
        # Error or not_configured row — minimal fields.
        printf '{"insert_id":%s,"event_date":%s,"ts":%s,"source":%s,"container":%s,"status":%s,"redis_version":null,"uptime_seconds":null,"connected_clients":null,"used_memory_bytes":null,"total_commands_processed":null,"keyspace_hits":null,"keyspace_misses":null,"queue_key":%s,"queue_depth":null,"error":%s}\n' \
            "$(json_str "$insert_id")" "$(json_str "$event_date")" "$(json_str "$ts")" \
            "$(json_str "$method")" "$(json_str "${REDIS_CONTAINER_USED:-}")" \
            "$(json_str "$REDIS_STATUS")" \
            "$(json_str "$queue_key")" "$(json_str "${error_msg:-}")" \
            >> "$ndjson" 2>/dev/null || true
        return 0
    fi

    # Full row with parsed INFO fields.
    INFO_RAW="$info_out" \
    DEPTH="${depth:-}" \
    QUEUE_KEY="$queue_key" \
    KEY_CONFIGURED="$key_configured" \
    TS="$ts" \
    EVENT_DATE="$event_date" \
    INSERT_ID="$insert_id" \
    METHOD="$method" \
    CONTAINER="${REDIS_CONTAINER_USED:-}" \
    R_STATUS="$REDIS_STATUS" \
    python3 - >> "$ndjson" 2>/dev/null <<'PY' || true
import os, json, re

def get_info(info, key):
    m = re.search(r'^' + re.escape(key) + r':(.+)$', info, re.MULTILINE)
    if not m:
        return None
    v = m.group(1).strip()
    # strip trailing unit suffixes that aren't part of pure-numeric fields
    v_clean = re.sub(r'[^0-9].*', '', v)
    if v_clean.isdigit():
        return int(v_clean)
    return None

def get_str(info, key):
    m = re.search(r'^' + re.escape(key) + r':(.+)$', info, re.MULTILINE)
    return m.group(1).strip() if m else None

info        = os.environ.get("INFO_RAW", "")
depth_s     = os.environ.get("DEPTH", "")
key_conf    = os.environ.get("KEY_CONFIGURED", "0") == "1"
r_status    = os.environ.get("R_STATUS", "ok")

depth = int(depth_s) if depth_s.isdigit() else None

row = {
    "insert_id":                os.environ.get("INSERT_ID", ""),
    "event_date":               os.environ.get("EVENT_DATE", ""),
    "ts":                       os.environ.get("TS", ""),
    "source":                   os.environ.get("METHOD", "docker_exec"),
    "container":                os.environ.get("CONTAINER", "") or None,
    "status":                   r_status,
    "redis_version":            get_str(info, "redis_version"),
    "uptime_seconds":           get_info(info, "uptime_in_seconds"),
    "connected_clients":        get_info(info, "connected_clients"),
    "used_memory_bytes":        get_info(info, "used_memory"),
    "total_commands_processed": get_info(info, "total_commands_processed"),
    "keyspace_hits":            get_info(info, "keyspace_hits"),
    "keyspace_misses":          get_info(info, "keyspace_misses"),
    "queue_key":                os.environ.get("QUEUE_KEY", ""),
    "queue_depth":              depth if key_conf else None,
    "error":                    None,
}
print(json.dumps(row, separators=(",", ":")))
PY
}
