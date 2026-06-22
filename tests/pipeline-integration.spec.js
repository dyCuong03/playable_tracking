"use strict";

// End-to-end pipeline integration tests driving the REAL services
// (pixel.controller -> disk queue -> dispatcher bridge -> redis-queue -> bigquery worker)
// against an in-memory fake Redis stream and a mock BigQuery sink.
//
// Covers CONTRACT scenarios: 1 (single event -> one disk -> one XADD), 4 (worker
// consumes), 5 (worker inserts to BQ mock), 6 (partial insert failure logged +
// retried/rejected, not swallowed), 10 (Redis outage -> disk buffers -> drains on
// recovery), 11 (worker restart -> XAUTOCLAIM reclaim, no loss), 12 (BigQuery transient
// failure -> retried -> eventual insert, zero loss).
//
// These tests assert on actual DATA MOVEMENT (XADD/consume/insert counts) so they FAIL if
// events are lost — not merely on log strings. Contract log events are additionally
// asserted where relevant.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

const {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
    sleep,
} = require("./helpers/pipeline-harness");
const { buildPartialFailureError } = require("./helpers/fake-bigquery");
const { startEvent, interactionEvent } = require("./helpers/events");

// ─── 1. single /p.gif -> exactly one disk item -> exactly one Redis XADD ─────────

test("single event: one HTTP 200 -> one disk item -> exactly one Redis XADD", async () => {
    const pipeline = createPipeline();
    try {
        const status = await runController(pipeline.services.controller, startEvent("sess-single"));
        assert.equal(status, 200);

        const stats = await pipeline.services.diskQueue.getQueueStats();
        const diskItems = stats.pending.files.reduce((sum) => sum + 1, 0);
        assert.ok(stats.pending.totalBytes > 0, "event must be persisted to disk");
        assert.ok(diskItems >= 1);

        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, 1, "exactly one item bridged to Redis");
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), 1, "exactly one XADD");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── 4 + 5. worker consumes queued events and inserts them into the BigQuery mock ──

test("worker consumes all queued events and inserts them into the BigQuery mock sink", async () => {
    const pipeline = createPipeline();
    const N = 25;
    try {
        for (let i = 0; i < N; i += 1) {
            const status = await runController(
                pipeline.services.controller,
                interactionEvent(`sess-consume-${i}`, `tap-${i}`)
            );
            assert.equal(status, 200);
        }

        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, N);
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), N);

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);

        assert.equal(pipeline.redis.state.counters.consumedByRead, N, "worker must consume all N");
        assert.equal(pipeline.bigquery.state.inserted.length, N, "worker must insert all N into BQ");

        // Inserted rows carry the expected shape (string-serialized event_params JSON).
        const sample = pipeline.bigquery.state.inserted[0];
        assert.ok(sample.event_hash, "inserted row has event_hash insertId source");
        assert.equal(typeof sample.event_params, "string");

        assert.equal(pipeline.redis.state.streamLength("pixel:events"), 0, "stream fully drained");
        assert.equal(pipeline.redis.state.pendingCount("pixel:events", "pixel-workers"), 0, "no pending left");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── 6. BigQuery partial insert failure is logged + retried/rejected, not swallowed ──

test("partial BigQuery insert failure: retryable row re-queued, bad row rejected, both logged", async () => {
    // Fail once: one retryable row (backendError) and one non-retryable row (invalid).
    // Retry of the requeued row (later call) succeeds.
    const failPlan = (rows, callIndex) => {
        if (callIndex > 0) {
            return null;
        }
        const outcomes = [];
        for (const row of rows) {
            if (String(row.session_id).endsWith("retry")) {
                outcomes.push({ insertId: row.event_hash, reason: "backendError" });
            } else if (String(row.session_id).endsWith("reject")) {
                outcomes.push({ insertId: row.event_hash, reason: "invalid" });
            }
        }
        return outcomes.length ? buildPartialFailureError(outcomes) : null;
    };

    const pipeline = createPipeline({ bigquery: { failPlan } });
    const capture = createConsoleCapture();
    try {
        const queries = [
            interactionEvent("sess-ok", "tap-ok"),
            interactionEvent("sess-retry", "tap-retry"),
            interactionEvent("sess-reject", "tap-reject"),
        ];
        for (const query of queries) {
            assert.equal(await runController(pipeline.services.controller, query), 200);
        }

        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, 3);

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state, { timeoutMs: 15_000 });

        // ok row + retried row both land in BigQuery (2 of 3); reject row goes to rejected stream.
        const insertedSessions = pipeline.bigquery.state.inserted.map((row) => row.session_id);
        assert.ok(insertedSessions.includes("sess-ok"));
        assert.ok(insertedSessions.includes("sess-retry"), "retryable row must eventually insert");
        assert.ok(!insertedSessions.includes("sess-reject"), "non-retryable row must NOT insert");

        assert.equal(pipeline.redis.state.xaddCount("pixel:rejected"), 1, "rejected row routed to pixel:rejected");

        // Failure must be VISIBLE, not swallowed: contract log + insert-error log on disk.
        capture.restore();
        assert.ok(capture.byType("bigquery-insert-failed").length >= 1, "bigquery-insert-failed emitted");

        const trackingLog = path.join(pipeline.paths.logDir, "pixel-tracking.txt");
        const logBody = fs.readFileSync(trackingLog, "utf8");
        assert.ok(logBody.includes("\"bigquery_status\":\"failed\""), "rejected row logged as failed");

        // Zero loss: every accepted event is accounted for as inserted + rejected.
        const inserted = pipeline.bigquery.state.inserted.length;
        const rejected = pipeline.redis.state.xaddCount("pixel:rejected");
        assert.equal(inserted + rejected, 3, "no accepted event silently lost");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});

// ─── 10. Redis outage during load: disk buffers, drains after recovery (no loss) ────

test("Redis outage: HTTP keeps accepting (disk), enqueue fails loudly, drains after recovery", async () => {
    const pipeline = createPipeline();
    const N = 40;
    try {
        // Redis is DOWN for the whole accept window.
        pipeline.redis.state.setFault(true);

        for (let i = 0; i < N; i += 1) {
            // Web path is disk-only -> must still return 200 even with Redis down.
            assert.equal(
                await runController(pipeline.services.controller, startEvent(`sess-outage-${i}`)),
                200
            );
        }

        const stats = await pipeline.services.diskQueue.getQueueStats();
        assert.ok(stats.pending.totalBytes > 0, "events buffered on disk while Redis down");

        // The enqueue path must SURFACE the failure (not silently succeed).
        await assert.rejects(
            () => pipeline.services.redisQueue.enqueueEventBatch([
                { tableName: "pixel_events_ver_2", row: { event_hash: "probe", session_id: "probe" } },
            ]),
            /FAKE_REDIS_DOWN|unavailable/,
            "enqueue must throw when Redis is down"
        );

        // Recovery.
        pipeline.redis.state.setFault(false);
        await sleep(250); // wait out the unavailable cooldown

        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, N, "all buffered events drain to Redis after recovery");

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        assert.equal(pipeline.bigquery.state.inserted.length, N, "no accepted event lost across the outage");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── 11. Worker restart during load: in-flight pending reclaimed via XAUTOCLAIM ─────

test("worker restart: pending entries delivered to a dead consumer are reclaimed, no loss", async () => {
    const pipeline = createPipeline();
    const N = 30;
    try {
        for (let i = 0; i < N; i += 1) {
            assert.equal(
                await runController(pipeline.services.controller, interactionEvent(`sess-reclaim-${i}`, `t-${i}`)),
                200
            );
        }
        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, N);

        // Worker A consumes (delivered into its PEL) but "crashes" before acking.
        await pipeline.services.redisQueue.ensureQueueReady();
        const deliveredToA = await pipeline.services.redisQueue.readQueueBatch("worker-A-dead", N, 0);
        assert.equal(deliveredToA.length, N, "worker A took delivery of all N");
        assert.equal(pipeline.redis.state.pendingCount("pixel:events", "pixel-workers"), N, "N entries pending (unacked)");
        assert.equal(pipeline.bigquery.state.inserted.length, 0, "nothing inserted yet (A crashed)");

        // Wait until the pending entries exceed the worker lease so they become reclaimable.
        await sleep(1200);

        // Worker B starts and must reclaim A's pending entries via XAUTOCLAIM.
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state, { timeoutMs: 15_000 });

        assert.ok(pipeline.redis.state.counters.consumedByReclaim >= N, "reclaimed all pending via XAUTOCLAIM");
        assert.equal(pipeline.bigquery.state.inserted.length, N, "every in-flight event eventually inserted");
        assert.equal(pipeline.redis.state.streamLength("pixel:events"), 0, "stream fully drained after reclaim");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── 5 (traceability). A single event_hash is traceable server -> dispatcher -> worker ──

test("single event_hash is traceable across server, dispatcher (enqueue) and worker logs", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        await runController(pipeline.services.controller, startEvent("sess-trace"));
        await drainDiskToRedis(pipeline.services);
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        capture.restore();

        // Server stage: the pixel-server-request success log carries the event_hash.
        const serverLog = capture.entries.find(
            (entry) => entry.type === "pixel-server-request" && entry.statusCode === 200
        );
        assert.ok(serverLog && serverLog.event_hash, "server logged event_hash");
        const hash = serverLog.event_hash;

        // Dispatcher stage (redis-queue runs in the dispatcher process): enqueue-success.
        const enqueue = capture.byType("redis-enqueue-success").find((entry) => entry.event_hash === hash);
        assert.ok(enqueue, "dispatcher enqueue-success log carries the same event_hash");

        // Worker stage: bigquery-insert-success lists the same event_hash.
        const insert = capture.byType("bigquery-insert-success").find(
            (entry) => Array.isArray(entry.event_hashes) && entry.event_hashes.includes(hash)
        );
        assert.ok(insert, "worker insert-success log carries the same event_hash");

        // Also persisted to the on-disk daily logs the architect reads.
        const day = new Date().toISOString().slice(0, 10);
        const serverFile = path.join(pipeline.paths.logDir, "server", `${day}.ndjson`);
        const workerFile = path.join(pipeline.paths.logDir, "worker", `${day}.ndjson`);
        assert.ok(fs.readFileSync(serverFile, "utf8").includes(hash), "event_hash in server daily log file");
        assert.ok(fs.readFileSync(workerFile, "utf8").includes(hash), "event_hash in worker daily log file");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});

// ─── 12. BigQuery transient failure during load: retried, eventual insert, zero loss ──

test("transient BigQuery failure: whole batch retried then inserted, zero loss", async () => {
    // Whole-batch transient error on the first insert attempt; succeeds thereafter.
    const failPlan = (rows, callIndex) => {
        if (callIndex === 0) {
            return new Error("ECONNRESET transient BigQuery outage");
        }
        return null;
    };

    const pipeline = createPipeline({ bigquery: { failPlan } });
    const N = 8;
    try {
        for (let i = 0; i < N; i += 1) {
            assert.equal(
                await runController(pipeline.services.controller, interactionEvent(`sess-transient-${i}`, `t-${i}`)),
                200
            );
        }
        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, N);

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state, { timeoutMs: 15_000 });

        assert.ok(pipeline.bigquery.state.insertCalls >= 2, "insert was retried after the transient failure");
        assert.equal(pipeline.bigquery.state.inserted.length, N, "all events inserted after retry (zero loss)");
        assert.equal(pipeline.redis.state.xaddCount("pixel:rejected"), 0, "nothing rejected for a transient error");
        assert.equal(pipeline.redis.state.streamLength("pixel:events"), 0, "stream fully drained");
    } finally {
        await pipeline.cleanup();
    }
});
