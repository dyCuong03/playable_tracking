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
NGINX_SOURCE_STATUS="no_source"
NGINX_SOURCE="not_configured"
NGINX_CONTAINER=""
NGINX_DOCKER_STATE="unknown"
NGINX_ROWS_STAGED=0
NGINX_ROWS_UPLOADED_LAST=0

REDIS_STATUS="not_configured"
REDIS_SOURCE_STATUS="not_configured"
REDIS_ROWS_STAGED=0
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
# Latest status writer (req 6)
# ---------------------------------------------------------------------------

# bq_write_latest — overwrite ops/status/bq-export-latest.json atomically.
# New schema: {enabled,status,date,nginx:{...},redis:{...},bigquery:{...}}
#
# Args: ENABLED TOP_STATUS DATE
#       NG_SOURCE_STATUS NG_ROWS_STAGED NG_ROWS_UPLOADED NG_STAGING_FILE
#       RD_SOURCE_STATUS RD_ROWS_STAGED RD_ROWS_UPLOADED RD_STAGING_FILE
#       UPLOAD_ENABLED PROJECT DATASET NGINX_TABLE REDIS_TABLE LAST_ERROR
bq_write_latest() {
    local _enabled="$1"   _top_status="$2"  _date="$3"
    local _ng_src="$4"    _ng_staged="$5"   _ng_uploaded="$6"  _ng_file="$7"
    local _rd_src="$8"    _rd_staged="$9"   _rd_uploaded="${10}" _rd_file="${11}"
    local _upl_en="${12}" _proj="${13}"      _ds="${14}"
    local _ng_tbl="${15}" _rd_tbl="${16}"    _last_err="${17}"

    python3 - \
        "$BQ_LATEST_FILE" \
        "$_enabled"    "$_top_status" "$_date" \
        "$_ng_src"     "$_ng_staged"  "$_ng_uploaded" "$_ng_file" \
        "$_rd_src"     "$_rd_staged"  "$_rd_uploaded" "$_rd_file" \
        "$_upl_en"     "$_proj"       "$_ds" \
        "$_ng_tbl"     "$_rd_tbl"     "$_last_err" <<'PY' 2>/dev/null || true
import json, sys

def i(s):
    try:    return int(s)
    except: return 0

(_, path,
 enabled, top_status, date,
 ng_src, ng_staged, ng_uploaded, ng_file,
 rd_src, rd_staged, rd_uploaded, rd_file,
 upl_en, proj, ds,
 ng_tbl, rd_tbl, last_err) = sys.argv

doc = {
    "enabled":  enabled == "1",
    "status":   top_status,
    "date":     date,
    "nginx": {
        "source_status":  ng_src,
        "rows_staged":    i(ng_staged),
        "rows_uploaded":  i(ng_uploaded),
        "staging_file":   ng_file or None,
    },
    "redis": {
        "source_status":  rd_src,
        "rows_staged":    i(rd_staged),
        "rows_uploaded":  i(rd_uploaded),
        "staging_file":   rd_file or None,
    },
    "bigquery": {
        "upload_enabled": upl_en == "1",
        "project_id":     proj or None,
        "dataset":        ds or None,
        "nginx_table":    ng_tbl or None,
        "redis_table":    rd_tbl or None,
        "last_error":     last_err or None,
    },
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
# Nginx log line parser (Python) — req 3
# ---------------------------------------------------------------------------

# _bq_nginx_parse_lines SOURCE CONTAINER HASH_IP ALLOWLIST
# Reads raw nginx log lines from stdin; writes valid parsed NDJSON rows to stdout.
# Detects JSON lines (start with '{') → pixel_json format (ts,remote_addr,request_id,
#   method,uri,args,status,body_bytes_sent,request_time,upstream_response_time,
#   upstream_status,http_referer,http_user_agent,host,data).
# Falls back to nginx combined format for non-JSON lines.
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

# Nginx combined log regex (with optional trailing request_time).
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

def parse_iso_ts(ts_str):
    """Parse ISO-8601 timestamp from pixel_json; return (event_date, ts_iso)."""
    if not ts_str:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        s = ts_str.strip()
        # Normalise offset variants to Z.
        s = re.sub(r'\+00:?00$', 'Z', s)
        if s.endswith("Z"):
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(s)
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_combined_ts(time_local):
    """Return (event_date, ts_iso) from nginx combined log time."""
    try:
        dt = datetime.strptime(time_local, "%d/%b/%Y:%H:%M:%S %z")
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%d"), dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%dT%H:%M:%SZ")

def filter_query(qs_str):
    """Parse a query string; return dict of allowlisted keys only."""
    try:
        parsed = parse_qs(qs_str or "", keep_blank_values=True)
        return {k: v[0] for k, v in parsed.items() if k in allowlist} if allowlist else {}
    except Exception:
        return {}

def parse_path_query(raw):
    """Return (path str, filtered_query dict) from a combined-log request target."""
    try:
        if not raw.startswith("/"):
            raw = "/" + raw
        p = urlparse(raw)
        qs = parse_qs(p.query, keep_blank_values=True)
        q_out = {k: v[0] for k, v in qs.items() if k in allowlist} if allowlist else {}
        return p.path, q_out
    except Exception:
        return raw, {}

def ms_or_null(val):
    """Convert seconds (str or numeric) to int ms; None when val is '-', '', or absent."""
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(float(val) * 1000)
    except Exception:
        return None

def safe_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        return None

skipped = 0
for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    row = None

    # ---- JSON path (pixel_json log_format) ----
    if line.startswith("{"):
        try:
            d = json.loads(line)
            event_date, ts = parse_iso_ts(d.get("ts", ""))
            remote_addr = d.get("remote_addr", "") or ""
            request_id  = d.get("request_id") or None
            method      = d.get("method") or None
            uri         = d.get("uri") or ""
            data_obj    = d.get("data") if isinstance(d.get("data"), dict) else {}
            args_str    = d.get("args") or data_obj.get("query") or ""
            status_raw  = d.get("status")
            body_bytes  = d.get("body_bytes_sent")
            req_time    = d.get("request_time")
            ups_time    = d.get("upstream_response_time")
            referer     = d.get("http_referer") or None
            user_agent  = d.get("http_user_agent") or ""

            # Build path; merge uri + args for query filtering.
            path = uri
            query = filter_query(args_str if args_str != "-" else "")
            try:
                p = urlparse(uri)
                if p.path:
                    path = p.path
                # args field holds raw query string from nginx $args
            except Exception:
                pass

            insert_id = sha256((ts or "") + (container or "") + (request_id or ""))

            row = {
                "event_date":                event_date,
                "ts":                        ts,
                "source":                    source,
                "container":                 container,
                "remote_ip_hash":            sha256(remote_addr) if (hash_ip and remote_addr) else (remote_addr or None),
                "method":                    method,
                "path":                      path or None,
                "query":                     query,
                "status":                    safe_int(status_raw),
                "request_time_ms":           ms_or_null(req_time),
                "upstream_response_time_ms": ms_or_null(ups_time),
                "body_bytes_sent":           safe_int(body_bytes),
                "referer":                   referer if referer and referer != "-" else None,
                "user_agent_hash":           sha256(user_agent) if user_agent else None,
                "request_id":                request_id,
                "raw_format":                "json",
                "raw_sample":                line[:200],
                "insert_id":                 insert_id,
            }
        except Exception:
            row = None

    # ---- Combined fallback ----
    if row is None:
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

        req_parts = request_str.split(" ", 2)
        method = req_parts[0] if len(req_parts) >= 1 else ""
        path_q = req_parts[1] if len(req_parts) >= 2 else ""
        path, query = parse_path_query(path_q)

        event_date, ts = parse_combined_ts(time_local)
        insert_id = sha256(ts + (container or "") + "")

        row = {
            "event_date":                event_date,
            "ts":                        ts,
            "source":                    source,
            "container":                 container,
            "remote_ip_hash":            sha256(remote_addr) if hash_ip else remote_addr,
            "method":                    method or None,
            "path":                      path or None,
            "query":                     query,
            "status":                    int(status_str) if status_str.isdigit() else None,
            "request_time_ms":           ms_or_null(req_time_str),
            "upstream_response_time_ms": None,
            "body_bytes_sent":           int(body_str) if (body_str and body_str.isdigit()) else None,
            "referer":                   referer if referer != "-" else None,
            "user_agent_hash":           sha256(user_agent),
            "request_id":                None,
            "raw_format":                "combined",
            "raw_sample":                None,
            "insert_id":                 insert_id,
        }

    if row is not None:
        print(json.dumps(row, separators=(",", ":")))

if skipped:
    print(json.dumps({"_skipped_lines": skipped, "source": source}), file=sys.stderr)
PY
}

# ---------------------------------------------------------------------------
# _bq_nginx_write_status  STAGING_DIR  SOURCE_STATUS  REASON  TS
# Writes nginx_requests.status.json — called when no rows can/will be staged.
# ---------------------------------------------------------------------------
_bq_nginx_write_status() {
    local dir="$1" ss="$2" reason="$3" ts="$4"
    python3 - "$dir/nginx_requests.status.json" "$ss" "$reason" "$ts" <<'PY' 2>/dev/null || true
import json, sys
path, ss, reason, ts = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
with open(path, "w") as f:
    json.dump({"source_status": ss, "reason": reason, "ts": ts}, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# ---------------------------------------------------------------------------
# nginx_collect STAGING_DIR  (req 4)
# ---------------------------------------------------------------------------
# Populates globals: NGINX_STATUS NGINX_SOURCE_STATUS NGINX_SOURCE
#                    NGINX_CONTAINER NGINX_DOCKER_STATE NGINX_ROWS_STAGED
# Appends parsed NDJSON rows to STAGING_DIR/nginx_requests.ndjson ONLY when
# rows > 0. Writes STAGING_DIR/nginx_requests.status.json when source is
# unavailable or zero rows result — never writes a misleading placeholder NDJSON.
#
# Priority chain:
#   (A) OPS_NGINX_ACCESS_LOG_PATH file exists → incremental byte-cursor read
#   (B) OPS_NGINX_CONTAINER set + docker ok → docker logs --since cursor
#   (C) ops_docker_discover auto-detects a "nginx" container → same as (B)
#   (D) nothing reachable → no_source
nginx_collect() {
    local staging_dir="$1"
    local ndjson="$staging_dir/nginx_requests.ndjson"
    NGINX_STATUS="not_configured"
    NGINX_SOURCE_STATUS="no_source"
    NGINX_SOURCE="not_configured"
    NGINX_CONTAINER=""
    NGINX_DOCKER_STATE="unknown"
    NGINX_ROWS_STAGED=0

    local log_file="${OPS_NGINX_ACCESS_LOG_PATH:-}"
    local nginx_cont="${OPS_NGINX_CONTAINER:-}"
    local hash_ip="${OPS_LOG_HASH_IP:-1}"
    local allowlist="${OPS_LOG_QUERY_ALLOWLIST:-$_BQ_DEFAULT_ALLOWLIST}"
    local _now; _now="$(ts_now)"

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

        NGINX_STATUS="ok"; NGINX_SOURCE="file"
        BQ_CURSOR_NGINX_FILE_PATH="$log_file"
        BQ_CURSOR_NGINX_FILE_OFFSET="$size"

        # Nothing new.
        if [ "$off" -ge "$size" ]; then
            NGINX_SOURCE_STATUS="zero_rows"
            _bq_nginx_write_status "$staging_dir" "zero_rows" "zero_rows" "$_now"
            return 0
        fi

        local _raw _parsed
        _raw="$(mktemp)"
        _parsed="$(mktemp)"
        tail -c +"$(( off + 1 ))" "$log_file" > "$_raw" 2>/dev/null || true
        _bq_nginx_parse_lines "file" "" "$hash_ip" "$allowlist" \
            < "$_raw" > "$_parsed" 2>/dev/null || true
        NGINX_ROWS_STAGED="$(wc -l < "$_parsed" | tr -d ' ')"
        if [ "${NGINX_ROWS_STAGED:-0}" -gt 0 ]; then
            cat "$_parsed" >> "$ndjson" 2>/dev/null || true
            NGINX_SOURCE_STATUS="ok"
        else
            NGINX_SOURCE_STATUS="zero_rows"
            _bq_nginx_write_status "$staging_dir" "zero_rows" "zero_rows" "$_now"
        fi
        rm -f "$_raw" "$_parsed"
        return 0
    fi

    # Docker required for (B) and (C).
    local docker_state
    docker_state="$(docker_access | awk '{print $3}')"
    NGINX_DOCKER_STATE="$docker_state"

    if [ "$docker_state" != "ok" ]; then
        jlog "info" "bq-export" "docker unavailable for nginx collect" \
            "{\"docker_state\":$(json_str "$docker_state")}"
        NGINX_SOURCE_STATUS="docker_unavailable"
        _bq_nginx_write_status "$staging_dir" "docker_unavailable" "docker_unavailable" "$_now"
        return 0
    fi

    # --- (B) explicit container ---
    if [ -n "$nginx_cont" ]; then
        local since="${BQ_CURSOR_NGINX_DOCKER_SINCE:-1h}"
        local _raw _parsed
        _raw="$(mktemp)"
        _parsed="$(mktemp)"
        if docker logs --since "$since" "$nginx_cont" > "$_raw" 2>&1; then
            _bq_nginx_parse_lines "docker_exec" "$nginx_cont" "$hash_ip" "$allowlist" \
                < "$_raw" > "$_parsed" 2>/dev/null || true
            NGINX_ROWS_STAGED="$(wc -l < "$_parsed" | tr -d ' ')"
            if [ "${NGINX_ROWS_STAGED:-0}" -gt 0 ]; then
                cat "$_parsed" >> "$ndjson" 2>/dev/null || true
                NGINX_SOURCE_STATUS="ok"
            else
                NGINX_SOURCE_STATUS="zero_rows"
                _bq_nginx_write_status "$staging_dir" "zero_rows" "zero_rows" "$_now"
            fi
            NGINX_STATUS="ok"; NGINX_SOURCE="docker_exec"
            NGINX_CONTAINER="$nginx_cont"
            BQ_CURSOR_NGINX_DOCKER_SINCE="$(ts_now)"
            BQ_CURSOR_NGINX_CONTAINER="$nginx_cont"
        else
            NGINX_STATUS="error"
            NGINX_SOURCE_STATUS="permission_denied"
            _bq_nginx_write_status "$staging_dir" "permission_denied" "permission_denied" "$_now"
            jlog "warn" "bq-export" "docker logs failed for nginx container" \
                "{\"container\":$(json_str "$nginx_cont")}"
        fi
        rm -f "$_raw" "$_parsed"
        return 0
    fi

    # --- (C) auto-discover nginx container ---
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
            if [ "${NGINX_ROWS_STAGED:-0}" -gt 0 ]; then
                cat "$_parsed" >> "$ndjson" 2>/dev/null || true
                NGINX_SOURCE_STATUS="ok"
            else
                NGINX_SOURCE_STATUS="zero_rows"
                _bq_nginx_write_status "$staging_dir" "zero_rows" "zero_rows" "$_now"
            fi
            NGINX_STATUS="ok"; NGINX_SOURCE="docker_exec"
            NGINX_CONTAINER="$detected_nginx"
            BQ_CURSOR_NGINX_DOCKER_SINCE="$(ts_now)"
            BQ_CURSOR_NGINX_CONTAINER="$detected_nginx"
        else
            NGINX_STATUS="error"
            NGINX_SOURCE_STATUS="permission_denied"
            _bq_nginx_write_status "$staging_dir" "permission_denied" "permission_denied" "$_now"
            jlog "warn" "bq-export" "docker logs failed for auto-detected nginx" \
                "{\"container\":$(json_str "$detected_nginx")}"
        fi
        rm -f "$_raw" "$_parsed"
        return 0
    fi

    # --- (D) not configured / no nginx container found ---
    NGINX_SOURCE_STATUS="no_source"
    _bq_nginx_write_status "$staging_dir" "no_source" "no_nginx_source" "$_now"
    jlog "info" "bq-export" \
        "nginx log source not configured; set OPS_NGINX_ACCESS_LOG_PATH or OPS_NGINX_CONTAINER" \
        "{\"docker_state\":$(json_str "$docker_state")}"
}

# ---------------------------------------------------------------------------
# redis_collect STAGING_DIR  (req 5)
# ---------------------------------------------------------------------------
# Populates globals: REDIS_STATUS REDIS_SOURCE_STATUS REDIS_ROWS_STAGED
#                    REDIS_CONTAINER_USED
# Appends one NDJSON row to STAGING_DIR/redis_metrics.ndjson ONLY when a real
# Redis connection is established (not_configured → no row; rows_staged stays 0).
#
# Priority chain mirrors monitor.sh:
#   (A) host redis-cli on PATH
#   (B) OPS_REDIS_CONTAINER + docker ok
#   (C) ops_redis_detect auto-discovery + docker ok
#   (D) not_configured → no row written
#
# queue_depth is null when OPS_REDIS_QUEUE_KEY is unset
# (REDIS_STATUS becomes "queue_key_not_configured" in that case, not "error").
redis_collect() {
    local staging_dir="$1"
    local ndjson="$staging_dir/redis_metrics.ndjson"
    REDIS_STATUS="not_configured"
    REDIS_SOURCE_STATUS="not_configured"
    REDIS_ROWS_STAGED=0
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
                REDIS_SOURCE_STATUS="ok"
            else
                REDIS_STATUS="queue_key_not_configured"
                REDIS_SOURCE_STATUS="queue_key_not_configured"
            fi
        else
            REDIS_STATUS="error"
            REDIS_SOURCE_STATUS="docker_unavailable"
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
                REDIS_SOURCE_STATUS="ok"
            else
                REDIS_STATUS="queue_key_not_configured"
                REDIS_SOURCE_STATUS="queue_key_not_configured"
            fi
        else
            REDIS_STATUS="error"
            REDIS_SOURCE_STATUS="permission_denied"
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
                    REDIS_SOURCE_STATUS="ok"
                else
                    REDIS_STATUS="queue_key_not_configured"
                    REDIS_SOURCE_STATUS="queue_key_not_configured"
                fi
            else
                REDIS_STATUS="error"
                REDIS_SOURCE_STATUS="docker_unavailable"
                error_msg="docker exec redis-cli INFO failed (auto-detected ${_auto})"
            fi
        fi
    fi

    # (D) not_configured — no row written, rows_staged stays 0.
    if [ "$REDIS_STATUS" = "not_configured" ]; then
        jlog "info" "bq-export" "redis not configured; set OPS_REDIS_CONTAINER or ensure redis-cli on PATH" \
            "{\"docker_state\":$(json_str "$docker_state")}"
        return 0
    fi

    # Build and append one row for ok / queue_key_not_configured / error.
    local ts event_date
    ts="$(ts_now)"; event_date="${ts%T*}"
    local insert_id
    insert_id="$(printf '%s' "${ts}${REDIS_CONTAINER_USED:-}" \
        | python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.stdin.read().encode()).hexdigest())' \
        2>/dev/null || echo "")"

    # Error row — minimal fields, no INFO data.
    if [ "$REDIS_STATUS" = "error" ]; then
        printf '{"insert_id":%s,"event_date":%s,"ts":%s,"source":%s,"container":%s,"status":%s,"queue_key":%s,"queue_depth":null,"used_memory":null,"used_memory_human":null,"connected_clients":null,"blocked_clients":null,"instantaneous_ops_per_sec":null,"total_commands_processed":null,"keyspace_hits":null,"keyspace_misses":null,"role":null,"uptime_in_seconds":null,"redis_version":null,"error":%s}\n' \
            "$(json_str "$insert_id")" "$(json_str "$event_date")" "$(json_str "$ts")" \
            "$(json_str "$method")" "$(json_str "${REDIS_CONTAINER_USED:-}")" \
            "$(json_str "$REDIS_STATUS")" \
            "$(json_str "$queue_key")" "$(json_str "${error_msg:-}")" \
            >> "$ndjson" 2>/dev/null || true
        REDIS_ROWS_STAGED=1
        return 0
    fi

    # Full row with parsed INFO fields (ok or queue_key_not_configured).
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
    "insert_id":                    os.environ.get("INSERT_ID", ""),
    "event_date":                   os.environ.get("EVENT_DATE", ""),
    "ts":                           os.environ.get("TS", ""),
    "source":                       os.environ.get("METHOD", "docker_exec"),
    "container":                    os.environ.get("CONTAINER", "") or None,
    "status":                       r_status,
    "queue_key":                    os.environ.get("QUEUE_KEY", ""),
    "queue_depth":                  depth if key_conf else None,
    "used_memory":                  get_info(info, "used_memory"),
    "used_memory_human":            get_str(info, "used_memory_human"),
    "connected_clients":            get_info(info, "connected_clients"),
    "blocked_clients":              get_info(info, "blocked_clients"),
    "instantaneous_ops_per_sec":    get_info(info, "instantaneous_ops_per_sec"),
    "total_commands_processed":     get_info(info, "total_commands_processed"),
    "keyspace_hits":                get_info(info, "keyspace_hits"),
    "keyspace_misses":              get_info(info, "keyspace_misses"),
    "role":                         get_str(info, "role"),
    "uptime_in_seconds":            get_info(info, "uptime_in_seconds"),
    "redis_version":                get_str(info, "redis_version"),
    "error":                        None,
}
print(json.dumps(row, separators=(",", ":")))
PY
    REDIS_ROWS_STAGED=1
}
