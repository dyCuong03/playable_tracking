#!/usr/bin/env bash
# ops/bin/bq-create-tables.sh
#
# Print the BigQuery DDL for the ops metrics tables.
# Optionally create the tables when OPS_BQ_CREATE_TABLES=1.
#
# Required env vars (for --create):
#   GOOGLE_APPLICATION_CREDENTIALS  - path to GCP service account JSON
#   OPS_BQ_PROJECT                  - GCP project ID
#   OPS_BQ_DATASET                  - BigQuery dataset name
#   OPS_BQ_NGINX_TABLE              - table name for nginx metrics   (default: nginx_requests)
#   OPS_BQ_REDIS_TABLE              - table name for redis metrics   (default: redis_metrics)
#
# Usage:
#   # Just print the DDL:
#   bash ops/bin/bq-create-tables.sh
#
#   # Create the tables in BigQuery (requires OPS_BQ_PROJECT + credentials):
#   OPS_BQ_CREATE_TABLES=1 bash ops/bin/bq-create-tables.sh
#
#   # Dry-run (validate SQL substitution, no BQ call):
#   OPS_BQ_CREATE_TABLES=1 DRY_RUN=1 bash ops/bin/bq-create-tables.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_DIR="$(cd "${SCRIPT_DIR}/../bigquery/schema" && pwd)"
BQ_UPLOAD="${SCRIPT_DIR}/bq-upload.js"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OPS_BQ_PROJECT="${OPS_BQ_PROJECT:-}"
OPS_BQ_DATASET="${OPS_BQ_DATASET:-}"
OPS_BQ_NGINX_TABLE="${OPS_BQ_NGINX_TABLE:-nginx_requests}"
OPS_BQ_REDIS_TABLE="${OPS_BQ_REDIS_TABLE:-redis_metrics}"
OPS_BQ_CREATE_TABLES="${OPS_BQ_CREATE_TABLES:-0}"
DRY_RUN="${DRY_RUN:-0}"

# ---------------------------------------------------------------------------
# Print DDL (always)
# ---------------------------------------------------------------------------
echo "=== nginx_requests DDL ==="
cat "${SCHEMA_DIR}/nginx_requests.sql"
echo ""
echo "=== redis_metrics DDL ==="
cat "${SCHEMA_DIR}/redis_metrics.sql"
echo ""

# ---------------------------------------------------------------------------
# Create tables (only when OPS_BQ_CREATE_TABLES=1)
# ---------------------------------------------------------------------------
if [[ "${OPS_BQ_CREATE_TABLES}" != "1" ]]; then
    echo "[bq-create-tables] DRY PRINT ONLY — set OPS_BQ_CREATE_TABLES=1 to create tables."
    exit 0
fi

# Validate required vars
if [[ -z "${OPS_BQ_PROJECT}" ]]; then
    echo "[bq-create-tables] ERROR: OPS_BQ_PROJECT is not set." >&2
    exit 1
fi
if [[ -z "${OPS_BQ_DATASET}" ]]; then
    echo "[bq-create-tables] ERROR: OPS_BQ_DATASET is not set." >&2
    exit 1
fi

echo "[bq-create-tables] Creating tables in ${OPS_BQ_PROJECT}.${OPS_BQ_DATASET} ..."

create_table() {
    local table_name="$1"
    local full_table="${OPS_BQ_PROJECT}.${OPS_BQ_DATASET}.${table_name}"

    echo "[bq-create-tables] → ${full_table}"

    if [[ "${DRY_RUN}" == "1" ]]; then
        echo "[bq-create-tables]   (DRY_RUN=1 — skipping node call)"
        return 0
    fi

    local result
    result=$(node "${BQ_UPLOAD}" --create --table "${full_table}" 2>&1 | tail -1)
    local status
    status=$(echo "${result}" | node -e "try { const r=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); process.stdout.write(r.status||'unknown'); } catch(e){ process.stdout.write('parse_error'); }" 2>/dev/null || echo "parse_error")

    echo "[bq-create-tables]   result: ${result}"

    case "${status}" in
        ok)
            echo "[bq-create-tables]   ✓ ${table_name} ready."
            ;;
        auth_missing|config_missing)
            echo "[bq-create-tables]   ⚠ ${table_name}: ${status} (check credentials + env vars)" >&2
            return 1
            ;;
        *)
            echo "[bq-create-tables]   ✗ ${table_name}: ${status}" >&2
            return 1
            ;;
    esac
}

ERRORS=0

create_table "${OPS_BQ_NGINX_TABLE}" || ERRORS=$((ERRORS + 1))
create_table "${OPS_BQ_REDIS_TABLE}" || ERRORS=$((ERRORS + 1))

if [[ "${ERRORS}" -gt 0 ]]; then
    echo "[bq-create-tables] ${ERRORS} table(s) failed. Check output above." >&2
    exit 1
fi

echo "[bq-create-tables] All tables created successfully."
