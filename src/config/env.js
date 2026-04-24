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
    trustProxy: true,
    bigQueryEnabled: parseBoolean(process.env.BIGQUERY_ENABLED, false),
    bigQueryDataset: process.env.BIGQUERY_DATASET || "",
    bigQueryTable: process.env.BIGQUERY_TABLE || "",
    bigQueryBatchSize: parseNumber(process.env.BIGQUERY_BATCH_SIZE, 100),
    bigQueryRetryDelayMs: parseNumber(process.env.BIGQUERY_RETRY_DELAY_MS, 30_000),
    bigQueryErrorLogIntervalMs: parseNumber(process.env.BIGQUERY_ERROR_LOG_INTERVAL_MS, 10_000),
    bigQueryQueueDir: process.env.BIGQUERY_QUEUE_DIR || "data/bigquery-queue",
    bigQueryQueueShards: parseNumber(process.env.BIGQUERY_QUEUE_SHARDS, 4),
    bigQueryWorkerPollMs: parseNumber(process.env.BIGQUERY_WORKER_POLL_MS, 1000),
    bigQueryWorkerLeaseMs: parseNumber(process.env.BIGQUERY_WORKER_LEASE_MS, 120_000),
    bigQueryWorkerName: process.env.BIGQUERY_WORKER_NAME || "",
    rateLimitWindowMs: parseNumber(process.env.RATE_LIMIT_WINDOW_MS, 10_000),
    rateLimitMax: parseNumber(process.env.RATE_LIMIT_MAX, 200),
    rateLimitPrefix: process.env.RATE_LIMIT_PREFIX || "rate_limit:p.gif",
};
