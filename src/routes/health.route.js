// src/routes/health.route.js

const express = require("express");
const { getQueueStats } = require("../services/redis-queue.service");
const { getBigQueryStatus } = require("../services/bigquery.service");
const { getDispatcherSummary } = require("../services/request-dispatcher.service");
const { buildPipelineReport } = require("../services/pipeline-health.service");

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

    const pipeline = includeQueueStats
        ? await buildFullReport().catch((error) => ({
            pipeline_status: "unknown",
            error: error.message,
        }))
        : { skipped: true };

    res.status(200).json({
        ok: true,
        bigQuery: getBigQueryStatus(),
        dispatcher,
        queue,
        pipeline,
    });
});

// Build the full cross-process pipeline report (Redis heartbeats + disk backlog + redis
// depth + computed pipeline_status). NO secrets: redis URL is redacted upstream and the
// service-account JSON is never touched. Local dispatcher bridgeState (in-process) is
// merged in as a fallback so single-process / dev deployments still report dispatcher
// liveness even before the first cross-process heartbeat lands.
const buildFullReport = async () => {
    const report = await buildPipelineReport();
    const bigQueryStatus = getBigQueryStatus();

    report.bigquery = {
        configured: Boolean(bigQueryStatus && bigQueryStatus.configured),
        lastInsertAt: report.bigquery ? report.bigquery.lastInsertAt : null,
        failureCount: report.bigquery ? report.bigquery.failureCount : 0,
    };

    // Fallback: if no dispatcher heartbeat is present (e.g. in-process bridge in dev), surface
    // the local bridgeState so the endpoint is not blind in single-process mode.
    if (!report.dispatcher || report.dispatcher.dispatcher_id === null) {
        const local = await getDispatcherSummary().catch(() => null);
        if (local && local.bridge && local.bridge.running) {
            report.dispatcher = {
                ...report.dispatcher,
                running: true,
                lastSuccessAt: local.bridge.lastSuccessAt,
                dispatcher_id: local.bridge.worker || "in-process",
                source: "in-process-bridge",
            };
        }
    }

    return report;
};

router.get("/debug/pipeline", async (req, res) => {
    const report = await buildFullReport().catch((error) => ({
        ok: false,
        pipeline_status: "unknown",
        error: error.message,
    }));

    res.status(200).json({ ok: true, ...report });
});

module.exports = router;
