-- ops/bigquery/schema/redis_metrics.sql
--
-- BigQuery DDL for the Redis INFO metrics table.
-- Partitioned by event_date (daily), clustered by container, queue_key.
--
-- Placeholders replaced at runtime by bq-upload.js --create:
--   %%PROJECT%%  →  GCP project ID
--   %%DATASET%%  →  BigQuery dataset
--   %%TABLE%%    →  table name (default: redis_metrics)
--
-- Manual execution (requires bq CLI):
--   bq query --use_legacy_sql=false < ops/bigquery/schema/redis_metrics.sql
--
-- Automated execution via bq-create-tables.sh:
--   OPS_BQ_CREATE_TABLES=1 bash ops/bin/bq-create-tables.sh
--
-- Schema aligned with bq-export.lib.sh NDJSON output (core task #1).

CREATE TABLE IF NOT EXISTS `%%PROJECT%%.%%DATASET%%.%%TABLE%%`
(
    -- Partition key (DATE) — populated from sample timestamp
    event_date                    DATE        NOT NULL,

    -- Sample timestamp (full precision ISO-8601)
    ts                            TIMESTAMP   NOT NULL,

    -- Collector / exporter identity: "host_cli" | "docker_exec"
    source                        STRING      NOT NULL,

    -- Docker container running Redis (null when using host CLI)
    container                     STRING,

    -- Exporter operational status: "ok" | "error" | "queue_key_not_configured" | "not_configured"
    status                        STRING,

    -- Redis server version (null when unreachable)
    redis_version                 STRING,

    -- Seconds since Redis process started (null when unreachable)
    uptime_seconds                INTEGER,

    -- Number of connected clients (null when unreachable)
    connected_clients             INTEGER,

    -- RSS memory used by Redis in bytes (null when unreachable)
    used_memory_bytes             INTEGER,

    -- Cumulative total commands processed since start (null when unreachable)
    total_commands_processed      INTEGER,

    -- Cumulative keyspace cache hits (null when unreachable)
    keyspace_hits                 INTEGER,

    -- Cumulative keyspace cache misses (null when unreachable)
    keyspace_misses               INTEGER,

    -- Redis stream / list key being monitored (e.g. "pixel:events")
    queue_key                     STRING      NOT NULL,

    -- Pending entries in the monitored stream/list (null when OPS_REDIS_QUEUE_KEY not set)
    queue_depth                   INTEGER,

    -- Error message when status != "ok", null otherwise
    error                         STRING,

    -- Dedup key — SHA-256 of (ts + container_name_or_empty); used as BigQuery insertId
    insert_id                     STRING      NOT NULL
)
PARTITION BY event_date
CLUSTER BY container, queue_key
OPTIONS (
    require_partition_filter = FALSE,
    description = "Redis INFO metrics exported by the ops monitoring stack"
);
