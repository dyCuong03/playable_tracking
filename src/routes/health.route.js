// src/routes/health.route.js

const express = require("express");
const { getQueueStats } = require("../services/redis-queue.service");
const { getBigQueryStatus } = require("../services/bigquery.service");
const { getDispatcherSummary } = require("../services/request-dispatcher.service");

const router = express.Router();
const QUEUE_STATS_TIMEOUT_MS = 500;

const withTimeout = (promise, timeoutMs) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
        reject(new Error(`Timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    promise
        .then((value) => {
            clearTimeout(timer);
            resolve(value);
        })
        .catch((error) => {
            clearTimeout(timer);
            reject(error);
        });
});

router.get("/health", async (req, res) => {
    const includeQueueStats = ["1", "true", "yes"].includes(String(req.query.queue || "").toLowerCase());
    const dispatcher = await getDispatcherSummary().catch((error) => ({
        ok: false,
        error: error.message,
    }));

    const queue = includeQueueStats
        ? await withTimeout(getQueueStats(), QUEUE_STATS_TIMEOUT_MS)
            .catch((error) => ({
                ok: false,
                error: error.message,
            }))
        : {
            ok: true,
            skipped: true,
        };

    res.status(200).json({
        ok: true,
        bigQuery: getBigQueryStatus(),
        dispatcher,
        queue,
    });
});

// Read-only pipeline observability endpoint. Deliberately exposes NO secrets:
// getQueueStats() already redacts the Redis URL credentials, and we never surface
// REDIS_URL or the service-account JSON here.
router.get("/debug/pipeline", async (req, res) => {
    const dispatcher = await getDispatcherSummary().catch((error) => ({
        ok: false,
        error: error.message,
    }));

    const redisStats = await withTimeout(getQueueStats(), QUEUE_STATS_TIMEOUT_MS)
        .catch((error) => ({
            ok: false,
            error: error.message,
        }));

    const redisReachable = !(redisStats && redisStats.ok === false);
    const bigQueryStatus = getBigQueryStatus();

    res.status(200).json({
        ok: true,
        queue_backend: "redis-stream",
        queue_key: redisStats && redisStats.stream ? redisStats.stream : null,
        group: redisStats && redisStats.group ? redisStats.group : null,
        stream_length: redisReachable ? Number(redisStats.length || 0) : null,
        pending: redisReachable ? redisStats.pending : null,
        rejected_length: redisReachable ? Number(redisStats.rejectedLength || 0) : null,
        redis_reachable: redisReachable,
        dispatcher: {
            running: Boolean(dispatcher && dispatcher.bridge && dispatcher.bridge.running),
            lastSuccessAt: dispatcher && dispatcher.bridge ? dispatcher.bridge.lastSuccessAt : null,
            approxDiskItems: dispatcher && typeof dispatcher.approxItems === "number"
                ? dispatcher.approxItems
                : null,
        },
        bigquery_configured: Boolean(bigQueryStatus && bigQueryStatus.configured),
    });
});

module.exports = router;
