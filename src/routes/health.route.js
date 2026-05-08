// src/routes/health.route.js

const express = require("express");
const { getQueueStats } = require("../services/redis-queue.service");
const { getDispatcherStats } = require("../services/request-dispatcher.service");

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
    const dispatcher = await getDispatcherStats().catch((error) => ({
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
        dispatcher,
        queue,
    });
});

module.exports = router;
