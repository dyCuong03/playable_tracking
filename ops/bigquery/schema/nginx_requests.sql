-- ops/bigquery/schema/nginx_requests.sql
--
-- BigQuery DDL for the nginx access-log metrics table.
-- Partitioned by event_date (daily), clustered by path, status, source.
--
-- Placeholders replaced at runtime by bq-upload.js --create:
--   %%PROJECT%%  →  GCP project ID
--   %%DATASET%%  →  BigQuery dataset
--   %%TABLE%%    →  table name (default: nginx_requests)
--
-- Manual execution (requires bq CLI):
--   bq query --use_legacy_sql=false < ops/bigquery/schema/nginx_requests.sql
--
-- Automated execution via bq-create-tables.sh:
--   OPS_BQ_CREATE_TABLES=1 bash ops/bin/bq-create-tables.sh

CREATE TABLE IF NOT EXISTS `%%PROJECT%%.%%DATASET%%.%%TABLE%%`
(
    -- Partition key (DATE) — always populated from the nginx log timestamp
    event_date               DATE        NOT NULL,

    -- Event timestamp (full precision)
    ts                       TIMESTAMP   NOT NULL,

    -- Collector / exporter identity
    source                   STRING      NOT NULL,   -- e.g. "nginx-exporter", hostname

    -- Docker container that emitted the log line (null when source="file")
    container                STRING,

    -- Hashed client IP (SHA-256 prefix, never raw IP)
    remote_ip_hash           STRING,

    -- HTTP method: GET, POST, etc.
    method                   STRING,

    -- Request path without query string
    path                     STRING,

    -- URL query string, stored as JSON object (key→value)
    query                    JSON,

    -- HTTP response status code (200, 404, 503 …)
    status                   INTEGER,

    -- Total request processing time in milliseconds (nginx $request_time × 1000)
    request_time_ms          FLOAT64,

    -- Time spent waiting for the upstream in milliseconds (nginx $upstream_response_time × 1000)
    upstream_response_time_ms FLOAT64,

    -- Bytes sent in the response body
    body_bytes_sent          INTEGER,

    -- Referer header (raw, may be empty)
    referer                  STRING,

    -- Hashed User-Agent (SHA-256 prefix for privacy)
    user_agent_hash          STRING,

    -- Nginx $request_id or equivalent correlation id
    request_id               STRING,

    -- Log format identifier: "json" (pixel_json) or "combined" (fallback)
    raw_format               STRING,

    -- Truncated raw log line for debugging
    raw_sample               STRING,

    -- Dedup key — SHA-256 of (ts, container, request_id); used as BigQuery insertId
    insert_id                STRING      NOT NULL
)
PARTITION BY event_date
CLUSTER BY path, status, source
OPTIONS (
    require_partition_filter = FALSE,
    description = "Nginx access-log metrics exported by the ops monitoring stack"
);
