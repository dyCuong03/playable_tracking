"use strict";

// CONTRACT scenarios 2 and 3 — durability + failure visibility at the web edge.
//
// 2. statusCode 200 is returned ONLY when the event is durably persisted to disk. If the
//    durable write throws, the controller must return 503 and must NOT log the
//    "persisted to durable queue" success line.
// 3. The web path is disk-only, so a Redis outage must NOT affect HTTP acceptance
//    (still 200). Separately, the dispatcher enqueue path must SURFACE the failure
//    (throw + emit redis-enqueue-failed) rather than silently dropping events.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
} = require("./helpers/pipeline-harness");
const { startEvent } = require("./helpers/events");

// ─── 2. durable persist failure -> 503, no "persisted" success log ──────────────

test("durable persist failure returns 503 and never logs a persisted-success line", async () => {
    // Point the disk queue at a path under a regular FILE so mkdir() fails with ENOTDIR,
    // forcing persistRequest -> enqueueDiskEvent to throw.
    const blockerRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-blocker-"));
    const blockerFile = path.join(blockerRoot, "not-a-dir");
    fs.writeFileSync(blockerFile, "x");

    const pipeline = createPipeline({
        env: { BIGQUERY_QUEUE_DIR: path.join(blockerFile, "queue") },
    });
    const capture = createConsoleCapture();
    try {
        const status = await runController(pipeline.services.controller, startEvent("sess-persist-fail"));
        capture.restore();

        assert.equal(status, 503, "must return 503 when durable persist fails");

        const persistedLogs = capture.entries.filter(
            (entry) => entry.type === "pixel-server-request" && entry.statusCode === 200
        );
        assert.equal(persistedLogs.length, 0, "must NOT emit a 200 persisted-success log");

        const failLogs = capture.entries.filter(
            (entry) => entry.type === "request-persist" || (entry.type === "pixel-server-request" && entry.statusCode === 503)
        );
        assert.ok(failLogs.length >= 1, "must emit a persist-failure log");

        // Nothing reached the durable queue.
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), 0);
    } finally {
        capture.restore();
        await pipeline.cleanup();
        fs.rmSync(blockerRoot, { recursive: true, force: true });
    }
});

// ─── 3. Redis down: HTTP still 200 (disk), enqueue surfaces redis-enqueue-failed ──

test("Redis down does not block HTTP 200, but enqueue path fails loudly (redis-enqueue-failed)", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        // Web path is disk-only -> 200 regardless of Redis health.
        const status = await runController(pipeline.services.controller, startEvent("sess-redis-down"));
        assert.equal(status, 200, "disk persist keeps HTTP at 200 even if Redis is unhealthy");

        const stats = await pipeline.services.diskQueue.getQueueStats();
        assert.ok(stats.pending.totalBytes > 0, "event is buffered on disk");

        // Warm the client with one healthy command path, then fail ONLY XADD so the
        // dedupe SET succeeds and the pipeline reaches (and surfaces) the XADD failure.
        await pipeline.services.redisQueue.ensureQueueReady();
        pipeline.redis.state.setFault(true, ["XADD"]);

        await assert.rejects(
            () => drainDiskToRedis(pipeline.services),
            /FAKE_REDIS_DOWN|unavailable/,
            "enqueue must throw — never silently drop"
        );

        capture.restore();
        const failed = capture.byType("redis-enqueue-failed");
        assert.ok(failed.length >= 1, "redis-enqueue-failed contract event must be emitted");
        assert.equal(failed[0].queue_backend, "redis-stream");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});
