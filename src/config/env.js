require("dotenv").config();

const parseBoolean = (value, fallback = false) => {
    if (value === undefined) {
        return fallback;
    }

    if (typeof value === "boolean") {
        return value;
    }

    return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
};

const parseNumber = (value, fallback) => {
    if (value === undefined || value === null || value === "") {
        return fallback;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
};

module.exports = {
    PORT: process.env.PORT || 8080,
    NODE_ENV: process.env.NODE_ENV || "development",
    webConcurrency: parseNumber(process.env.WEB_CONCURRENCY, 1),
    trustProxy: true,
    bigQueryEnabled: parseBoolean(process.env.BIGQUERY_ENABLED, false),
    bigQueryDataset: process.env.BIGQUERY_DATASET || "",
    bigQueryTable: process.env.BIGQUERY_TABLE || "",
    bigQueryBatchSize: parseNumber(process.env.BIGQUERY_BATCH_SIZE, 100),
    bigQueryQueueReadBatch: parseNumber(process.env.BIGQUERY_QUEUE_READ_BATCH, 1000),
    bigQueryMaxRetries: parseNumber(process.env.BIGQUERY_MAX_RETRIES, 5),
    bigQueryRetryDelayMs: parseNumber(process.env.BIGQUERY_RETRY_DELAY_MS, 30_000),
    bigQueryErrorLogIntervalMs: parseNumber(process.env.BIGQUERY_ERROR_LOG_INTERVAL_MS, 10_000),
    bigQueryQueueDir: process.env.BIGQUERY_QUEUE_DIR || "data/bigquery-queue",
    bigQueryQueueShards: parseNumber(process.env.BIGQUERY_QUEUE_SHARDS, 4),
    bigQueryWorkerPollMs: parseNumber(process.env.BIGQUERY_WORKER_POLL_MS, 1000),
    bigQueryWorkerLeaseMs: parseNumber(process.env.BIGQUERY_WORKER_LEASE_MS, 120_000),
    bigQueryWorkerName: process.env.BIGQUERY_WORKER_NAME || "",
    redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
    redisQueueStream: process.env.REDIS_QUEUE_STREAM || "pixel:events",
    redisQueueGroup: process.env.REDIS_QUEUE_GROUP || "pixel-workers",
    redisRejectedStream: process.env.REDIS_REJECTED_STREAM || "pixel:rejected",
    redisQueueMaxLen: parseNumber(process.env.REDIS_QUEUE_MAXLEN, 1_000_000),
    redisConnectTimeoutMs: parseNumber(process.env.REDIS_CONNECT_TIMEOUT_MS, 1_000),
    redisCommandTimeoutMs: parseNumber(process.env.REDIS_COMMAND_TIMEOUT_MS, 2_000),
    redisUnavailableCooldownMs: parseNumber(process.env.REDIS_UNAVAILABLE_COOLDOWN_MS, 5_000),
    redisErrorLogIntervalMs: parseNumber(process.env.REDIS_ERROR_LOG_INTERVAL_MS, 10_000),
    redisDedupeTtlSeconds: parseNumber(process.env.REDIS_DEDUPE_TTL_SECONDS, 86_400),
    requestQueueMaxSize: parseNumber(process.env.REQUEST_QUEUE_MAX_SIZE, 50_000),
    requestQueueFlushBatchSize: parseNumber(process.env.REQUEST_QUEUE_FLUSH_BATCH_SIZE, 200),
    requestQueueFlushIntervalMs: parseNumber(process.env.REQUEST_QUEUE_FLUSH_INTERVAL_MS, 25),
    requestQueueRetryDelayMs: parseNumber(process.env.REQUEST_QUEUE_RETRY_DELAY_MS, 1_000),
    requestQueueDropLogIntervalMs: parseNumber(process.env.REQUEST_QUEUE_DROP_LOG_INTERVAL_MS, 10_000),
    requestQueueBridgePollMs: parseNumber(process.env.REQUEST_QUEUE_BRIDGE_POLL_MS, 250),
    requestQueueBridgeBatchSize: parseNumber(process.env.REQUEST_QUEUE_BRIDGE_BATCH_SIZE, 500),
    requestQueueBridgeMaxFilesPerRun: parseNumber(process.env.REQUEST_QUEUE_BRIDGE_MAX_FILES_PER_RUN, 4),
    requestQueueRotateMinBytes: parseNumber(process.env.REQUEST_QUEUE_ROTATE_MIN_BYTES, 262_144),
    requestQueueRotateMaxAgeMs: parseNumber(process.env.REQUEST_QUEUE_ROTATE_MAX_AGE_MS, 1_000),
    requestQueueBridgeEnabled: parseBoolean(process.env.REQUEST_QUEUE_BRIDGE_ENABLED, false),
    rateLimitWindowMs: parseNumber(process.env.RATE_LIMIT_WINDOW_MS, 10_000),
    rateLimitMax: parseNumber(process.env.RATE_LIMIT_MAX, 200),
    rateLimitPrefix: process.env.RATE_LIMIT_PREFIX || "rate_limit:p.gif",
    // ── Pipeline health / silent-failure detection (phase 2) ──────────────────
    // Heartbeats are written by each tier (web/dispatcher/worker) and expire after
    // the TTL — a missing key means that tier is dead/stuck. TTL should be ~3x the
    // writer interval so a single skipped beat does not flap the status.
    pipelineHeartbeatTtlSeconds: parseNumber(process.env.PIPELINE_HEARTBEAT_TTL_SECONDS, 30),
    pipelineHeartbeatIntervalMs: parseNumber(process.env.PIPELINE_HEARTBEAT_INTERVAL_MS, 5_000),
    pipelineDispatcherStaleMs: parseNumber(process.env.PIPELINE_DISPATCHER_STALE_MS, 30_000),
    pipelineWorkerStaleMs: parseNumber(process.env.PIPELINE_WORKER_STALE_MS, 30_000),
    pipelineBqStaleMs: parseNumber(process.env.PIPELINE_BQ_STALE_MS, 30_000),
    pipelineWebAcceptRecentMs: parseNumber(process.env.PIPELINE_WEB_ACCEPT_RECENT_MS, 30_000),
    pipelineDiskBacklogWarn: parseNumber(process.env.PIPELINE_DISK_BACKLOG_WARN, 5_000),
    pipelineStreamWarn: parseNumber(process.env.PIPELINE_STREAM_WARN, 10_000),
    pipelineHealthKeyPrefix: process.env.PIPELINE_HEALTH_KEY_PREFIX || "pixel:health:",
};
