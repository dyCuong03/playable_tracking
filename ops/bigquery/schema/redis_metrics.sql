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

    -- Redis stream / list key being monitored (e.g. "pixel:events")
    queue_key                     STRING      NOT NULL,

    -- Pending entries in the monitored stream/list (null when OPS_REDIS_QUEUE_KEY not set)
    queue_depth                   INT64,

    -- RSS memory used by Redis in bytes (from used_memory in INFO output)
    used_memory                   INT64,

    -- Human-readable memory usage (e.g. "42.50M"), from used_memory_human in INFO
    used_memory_human             STRING,

    -- Number of connected clients (from connected_clients in INFO)
    connected_clients             INT64,

    -- Number of clients pending on a blocking call (from blocked_clients in INFO)
    blocked_clients               INT64,

    -- Number of commands processed per second (from instantaneous_ops_per_sec in INFO)
    instantaneous_ops_per_sec     INT64,

    -- Cumulative total commands processed since start (from total_commands_processed in INFO)
    total_commands_processed      INT64,

    -- Cumulative keyspace cache hits (from keyspace_hits in INFO)
    keyspace_hits                 INT64,

    -- Cumulative keyspace cache misses (from keyspace_misses in INFO)
    keyspace_misses               INT64,

    -- Redis replication role: "master" | "slave" | "sentinel" (from role in INFO)
    role                          STRING,

    -- Seconds since Redis process started (from uptime_in_seconds in INFO)
    uptime_in_seconds             INT64,

    -- Redis server version string (from redis_version in INFO)
    redis_version                 STRING,

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
