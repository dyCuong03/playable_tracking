#!/usr/bin/env bash
# ops/bin/bq-log-exporter.sh — one-shot BigQuery log export for the pixel-tracking stack.
#
# Collects nginx access log rows and Redis metrics, stages them to daily NDJSON
# files under ops/logs/<UTC-date>/bq/, invokes the BigQuery uploader, maintains
# a byte-offset / docker-since cursor, and writes ops/status/bq-export-latest.json.
#
# Usage:
#   bash ops/bin/bq-log-exporter.sh [--dry-run]
#
# Always exits 0 — designed to degrade gracefully on any missing dependency.
# Runs safely from a loop (bq-exporter-loop.sh) or a cron job.
# NEVER touches the pixel ingest path; all work is in this separate process.
#
# Required env (via ops/.env or .env.ops):
#   OPS_BQ_EXPORT_ENABLED=1          — master switch; exits 0 silently when absent/0
#   OPS_BQ_PROJECT                   — GCP project id
#   OPS_BQ_DATASET                   — BigQuery dataset name
#
# Optional env (see ops/lib/bq-export.lib.sh for full list):
#   OPS_BQ_NGINX_TABLE=nginx_requests
#   OPS_BQ_REDIS_TABLE=redis_metrics
#   OPS_BQ_DRY_RUN=0
#   OPS_NGINX_ACCESS_LOG_PATH        — (A) direct nginx log file
#   OPS_NGINX_CONTAINER              — (B) named docker container
#   OPS_LOG_HASH_IP=1
#   OPS_LOG_QUERY_ALLOWLIST
#   OPS_REDIS_CONTAINER
#   OPS_REDIS_QUEUE_KEY
#   OPS_BQ_STAGING_BACKLOG_WARN=50000

set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/../lib/common.sh"
. "$SCRIPT_DIR/../lib/bq-export.lib.sh"

ROLE="bq-export"
heartbeat "$ROLE"

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
DRY_RUN=0
for _arg in "$@"; do
    case "$_arg" in --dry-run) DRY_RUN=1 ;; esac
done
DRY_FLAG=""
{ [ "$DRY_RUN" = "1" ] || [ "${OPS_BQ_DRY_RUN:-0}" = "1" ]; } && DRY_FLAG="--dry-run"

# ---------------------------------------------------------------------------
# Master switch — write disabled status and exit 0 when not enabled.
# ---------------------------------------------------------------------------
EXPORT_ENABLED="${OPS_BQ_EXPORT_ENABLED:-0}"
if [ "$EXPORT_ENABLED" != "1" ]; then
    bq_write_latest \
        "0" \
        "disabled" "disabled" "" "0" "0" \
        "disabled" "0" \
        "disabled" "" "" \
        "" "" "0"
    jlog "info" "$ROLE" "bq-export disabled (OPS_BQ_EXPORT_ENABLED!=1); set it in ops/.env to enable" "{}"
    exit 0
fi

# ---------------------------------------------------------------------------
# BQ table config
# ---------------------------------------------------------------------------
BQ_PROJECT="${OPS_BQ_PROJECT:-}"
BQ_DATASET="${OPS_BQ_DATASET:-}"
NGINX_TABLE_NAME="${OPS_BQ_NGINX_TABLE:-nginx_requests}"
REDIS_TABLE_NAME="${OPS_BQ_REDIS_TABLE:-redis_metrics}"

NGINX_FQTN=""
REDIS_FQTN=""
CONFIG_MISSING=0
if [ -z "$BQ_PROJECT" ] || [ -z "$BQ_DATASET" ]; then
    CONFIG_MISSING=1
    jlog "warn" "$ROLE" "OPS_BQ_PROJECT or OPS_BQ_DATASET not set; collection continues but upload will be skipped" \
        "{\"project\":$(json_str "${BQ_PROJECT:-}"),\"dataset\":$(json_str "${BQ_DATASET:-}")}"
else
    NGINX_FQTN="${BQ_PROJECT}.${BQ_DATASET}.${NGINX_TABLE_NAME}"
    REDIS_FQTN="${BQ_PROJECT}.${BQ_DATASET}.${REDIS_TABLE_NAME}"
fi

# ---------------------------------------------------------------------------
# Staging directories
# ---------------------------------------------------------------------------
DATE="$(date -u +%Y-%m-%d)"
STAGING_DIR="$LOGS_DIR/$DATE/bq"
mkdir -p "$STAGING_DIR"

NGINX_NDJSON="$STAGING_DIR/nginx_requests.ndjson"
REDIS_NDJSON="$STAGING_DIR/redis_metrics.ndjson"

# ---------------------------------------------------------------------------
# Load cursor (byte offset / docker-since timestamp from prior run)
# ---------------------------------------------------------------------------
bq_read_cursor

# ---------------------------------------------------------------------------
# Collect nginx access log rows
# ---------------------------------------------------------------------------
nginx_collect "$STAGING_DIR"
NGINX_ROWS_THIS_RUN="$NGINX_ROWS_STAGED"

jlog "info" "$ROLE" "nginx collect" \
    "{\"status\":$(json_str "$NGINX_STATUS"),\"source\":$(json_str "$NGINX_SOURCE"),\"container\":$(json_str "${NGINX_CONTAINER:-}"),\"rows\":${NGINX_ROWS_THIS_RUN}}"

# ---------------------------------------------------------------------------
# Collect Redis metrics (one row per run)
# ---------------------------------------------------------------------------
redis_collect "$STAGING_DIR"

jlog "info" "$ROLE" "redis collect" \
    "{\"status\":$(json_str "$REDIS_STATUS"),\"container\":$(json_str "${REDIS_CONTAINER_USED:-}")}"

# ---------------------------------------------------------------------------
# Staging backlog: total rows in today's NDJSON files.
# ---------------------------------------------------------------------------
BACKLOG_ROWS=0
for _sf in "$NGINX_NDJSON" "$REDIS_NDJSON"; do
    if [ -f "$_sf" ]; then
        _n="$(wc -l < "$_sf" | tr -d ' ')"
        BACKLOG_ROWS=$(( BACKLOG_ROWS + ${_n:-0} ))
    fi
done

# ---------------------------------------------------------------------------
# Upload — nginx
# ---------------------------------------------------------------------------
NGINX_UPLOADED=0
UPLOAD_STATUS="ok"
UPLOAD_TS=""
UPLOAD_ERR=""

if [ "$CONFIG_MISSING" = "1" ]; then
    UPLOAD_STATUS="config_missing"
    jlog "warn" "$ROLE" "skipping upload — OPS_BQ_PROJECT/OPS_BQ_DATASET not set" "{}"
elif [ ! -s "$NGINX_NDJSON" ]; then
    jlog "info" "$ROLE" "nginx: no rows staged, skipping upload" "{}"
else
    bq_invoke_uploader "$NGINX_FQTN" "$NGINX_NDJSON" "${DRY_FLAG:-}"
    UPLOAD_TS="$(ts_now)"
    NGINX_UPLOADED="${BQ_UPL_UPLOADED:-0}"
    case "$BQ_UPL_STATUS" in
        ok|dry_run)
            jlog "info" "$ROLE" "nginx upload ${BQ_UPL_STATUS}" \
                "{\"uploaded\":${BQ_UPL_UPLOADED},\"errors\":${BQ_UPL_ERRORS},\"table\":$(json_str "$NGINX_FQTN")}"
            ;;
        auth_missing)
            UPLOAD_STATUS="auth_missing"
            UPLOAD_ERR="${BQ_UPL_MESSAGE:-}"
            jlog "warn" "$ROLE" "nginx upload: auth_missing" \
                "{\"message\":$(json_str "${BQ_UPL_MESSAGE:-}")}"
            ;;
        *)
            UPLOAD_STATUS="${BQ_UPL_STATUS:-failed}"
            UPLOAD_ERR="${BQ_UPL_MESSAGE:-upload failed}"
            jlog "warn" "$ROLE" "nginx upload failed" \
                "{\"status\":$(json_str "$BQ_UPL_STATUS"),\"errors\":${BQ_UPL_ERRORS},\"message\":$(json_str "${BQ_UPL_MESSAGE:-}")}"
            ;;
    esac
fi

# ---------------------------------------------------------------------------
# Upload — redis
# ---------------------------------------------------------------------------
REDIS_UPLOADED=0

if [ "$CONFIG_MISSING" != "1" ] && [ -s "$REDIS_NDJSON" ]; then
    bq_invoke_uploader "$REDIS_FQTN" "$REDIS_NDJSON" "${DRY_FLAG:-}"
    [ -z "$UPLOAD_TS" ] && UPLOAD_TS="$(ts_now)"
    REDIS_UPLOADED="${BQ_UPL_UPLOADED:-0}"
    case "$BQ_UPL_STATUS" in
        ok|dry_run)
            jlog "info" "$ROLE" "redis upload ${BQ_UPL_STATUS}" \
                "{\"uploaded\":${BQ_UPL_UPLOADED},\"errors\":${BQ_UPL_ERRORS},\"table\":$(json_str "$REDIS_FQTN")}"
            ;;
        auth_missing)
            # Only escalate if no prior failure already recorded.
            [ "$UPLOAD_STATUS" = "ok" ] && { UPLOAD_STATUS="auth_missing"; UPLOAD_ERR="${BQ_UPL_MESSAGE:-}"; }
            jlog "warn" "$ROLE" "redis upload: auth_missing" \
                "{\"message\":$(json_str "${BQ_UPL_MESSAGE:-}")}"
            ;;
        *)
            [ "$UPLOAD_STATUS" = "ok" ] && { UPLOAD_STATUS="${BQ_UPL_STATUS:-failed}"; UPLOAD_ERR="${BQ_UPL_MESSAGE:-upload failed}"; }
            jlog "warn" "$ROLE" "redis upload failed" \
                "{\"status\":$(json_str "$BQ_UPL_STATUS"),\"errors\":${BQ_UPL_ERRORS},\"message\":$(json_str "${BQ_UPL_MESSAGE:-}")}"
            ;;
    esac
fi

# Apply dry_run label when relevant.
[ -n "$DRY_FLAG" ] && [ "$UPLOAD_STATUS" = "ok" ] && UPLOAD_STATUS="dry_run"

# ---------------------------------------------------------------------------
# Truncate staging files after a clean upload so backlog resets each run.
# Leave intact on failure so rows survive for manual inspection / retry.
# ---------------------------------------------------------------------------
if [ "$UPLOAD_STATUS" = "ok" ] || [ "$UPLOAD_STATUS" = "dry_run" ]; then
    : > "$NGINX_NDJSON" 2>/dev/null || true
    : > "$REDIS_NDJSON" 2>/dev/null || true
    BACKLOG_ROWS=0
fi

# ---------------------------------------------------------------------------
# Persist cursor (updated by nginx_collect)
# ---------------------------------------------------------------------------
NOW="$(ts_now)"
bq_write_cursor "$NOW"

# ---------------------------------------------------------------------------
# Write shared status JSON
# ---------------------------------------------------------------------------
bq_write_latest \
    "1" \
    "$NGINX_STATUS" "$NGINX_SOURCE" "${NGINX_CONTAINER:-}" \
    "$NGINX_ROWS_THIS_RUN" "$NGINX_UPLOADED" \
    "$REDIS_STATUS" "$REDIS_UPLOADED" \
    "$UPLOAD_STATUS" "${UPLOAD_TS:-}" "${UPLOAD_ERR:-}" \
    "${NGINX_FQTN:-}" "${REDIS_FQTN:-}" \
    "$BACKLOG_ROWS"

# ---------------------------------------------------------------------------
# Alerts (state-change dedup; fires only when export is enabled)
# ---------------------------------------------------------------------------
BACKLOG_WARN="${OPS_BQ_STAGING_BACKLOG_WARN:-50000}"

# nginx source missing or errored
_nginx_alert=0
case "$NGINX_STATUS" in error|not_configured) _nginx_alert=1 ;; esac
bq_check_alert "bq-nginx-source-missing" "warn" "$_nginx_alert" \
    "nginx status=${NGINX_STATUS} source=${NGINX_SOURCE} container=${NGINX_CONTAINER:-none}"

# redis unreadable (error only; not_configured and queue_key_not_configured are soft)
_redis_alert=0
[ "$REDIS_STATUS" = "error" ] && _redis_alert=1
bq_check_alert "bq-redis-unreadable" "warn" "$_redis_alert" \
    "redis status=${REDIS_STATUS} container=${REDIS_CONTAINER_USED:-none}"

# BQ upload failure
_upload_alert=0
case "$UPLOAD_STATUS" in failed|auth_missing) _upload_alert=1 ;; esac
bq_check_alert "bq-upload-failed" "error" "$_upload_alert" \
    "upload status=${UPLOAD_STATUS} error=${UPLOAD_ERR:-}"

# BQ project/dataset/auth missing — config_missing is always alertable
_config_alert=0
{ [ "$CONFIG_MISSING" = "1" ] || [ "$UPLOAD_STATUS" = "config_missing" ] || \
  [ "$UPLOAD_STATUS" = "auth_missing" ]; } && _config_alert=1
bq_check_alert "bq-config-missing" "warn" "$_config_alert" \
    "project=${BQ_PROJECT:-unset} dataset=${BQ_DATASET:-unset} upload_status=${UPLOAD_STATUS}"

# Staging backlog too large
_backlog_alert=0
[ "$BACKLOG_ROWS" -gt "$BACKLOG_WARN" ] && _backlog_alert=1
bq_check_alert "bq-staging-backlog" "warn" "$_backlog_alert" \
    "staging backlog ${BACKLOG_ROWS} rows > ${BACKLOG_WARN} threshold"

bq_flush_alert_state

# ---------------------------------------------------------------------------
# Final heartbeat + summary log
# ---------------------------------------------------------------------------
heartbeat "$ROLE"
jlog "info" "$ROLE" "bq-export run complete" \
    "{\"nginx_status\":$(json_str "$NGINX_STATUS"),\"nginx_rows\":${NGINX_ROWS_THIS_RUN},\"nginx_uploaded\":${NGINX_UPLOADED},\"redis_status\":$(json_str "$REDIS_STATUS"),\"redis_uploaded\":${REDIS_UPLOADED},\"upload_status\":$(json_str "$UPLOAD_STATUS"),\"backlog_rows\":${BACKLOG_ROWS},\"dry_run\":$([ -n "$DRY_FLAG" ] && echo true || echo false)}"

exit 0
