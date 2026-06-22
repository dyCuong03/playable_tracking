"use strict";

// Phase 2 — full event_hash traceability + the real dispatcher loop's heartbeat/logs.
//
// 1. A single event is traceable by its event_hash through EVERY stage:
//    HTTP (disk-persist-success) -> disk NDJSON row -> Redis (redis-enqueue-success)
//    -> worker (bigquery-insert-attempt + bigquery-insert-success) -> mock BigQuery sink.
// 2. The real request-dispatcher runBridgeLoop writes a pixel:health:dispatcher heartbeat
//    and emits dispatcher-status / dispatcher-backlog-summary, so cross-process health has
//    real data to read.

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
const { interactionEvent } = require("./helpers/events");

const readDiskHashes = async (diskQueue) => {
    const dir = diskQueue.PENDING_DIR;
    const entries = await fs.promises.readdir(dir).catch(() => []);
    const hashes = [];
    for (const entry of entries) {
        if (!entry.endsWith(".ndjson")) {
            continue;
        }
        const items = await diskQueue.parseQueueFile(path.join(dir, entry));
        for (const item of items) {
            if (item && item.row && item.row.event_hash) {
                hashes.push(item.row.event_hash);
            }
        }
    }
    return hashes;
};

test("one event_hash is traceable HTTP -> disk -> redis -> worker -> mock BigQuery", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        await runController(pipeline.services.controller, interactionEvent("sess-trace2", "tap"));

        // HTTP/disk stage: disk-persist-success log carries the hash AND the NDJSON row has it.
        const diskLog = capture.byType("disk-persist-success")[0];
        assert.ok(diskLog && diskLog.event_hash, "disk-persist-success logged with event_hash");
        const hash = diskLog.event_hash;

        const diskHashes = await readDiskHashes(pipeline.services.diskQueue);
        assert.ok(diskHashes.includes(hash), "event_hash present in the on-disk NDJSON row");

        await drainDiskToRedis(pipeline.services);
        const enqueue = capture.byType("redis-enqueue-success").find((e) => e.event_hash === hash);
        assert.ok(enqueue, "redis-enqueue-success carries the same event_hash");

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        capture.restore();

        const attempt = capture.byType("bigquery-insert-attempt").find(
            (e) => (Array.isArray(e.event_hashes) && e.event_hashes.includes(hash)) || e.event_hash === hash
        );
        assert.ok(attempt, "bigquery-insert-attempt carries the same event_hash");

        const success = capture.byType("bigquery-insert-success").find(
            (e) => Array.isArray(e.event_hashes) && e.event_hashes.includes(hash)
        );
        assert.ok(success, "bigquery-insert-success carries the same event_hash");

        // Final sink: the row really landed in the mock BigQuery, same insertId source.
        assert.ok(pipeline.bigquery.state.inserted.some((row) => row.event_hash === hash), "row inserted into mock BigQuery");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});

test("real dispatcher loop writes a dispatcher heartbeat and backlog-summary logs", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    const N = 15;
    let bridgePromise = null;
    try {
        for (let i = 0; i < N; i += 1) {
            assert.equal(await runController(pipeline.services.controller, interactionEvent(`sess-disp-hb-${i}`, `t-${i}`)), 200);
        }

        // Run the REAL dispatcher bridge loop (not the inline drain helper). Capture the
        // loop promise so it is awaited on stop — never left to outlive the test.
        bridgePromise = pipeline.services.dispatcher.startDispatcher();
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline && pipeline.redis.state.xaddCount("pixel:events") < N) {
            await sleep(50);
        }
        pipeline.services.dispatcher.stopDispatcher();
        await bridgePromise; // ensure the loop has fully exited before any teardown
        capture.restore();

        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), N, "dispatcher drained all events");

        // Cross-process heartbeat is written to the shared Redis store.
        const beat = pipeline.redis.state.getKey("pixel:health:dispatcher");
        assert.ok(beat, "dispatcher wrote pixel:health:dispatcher heartbeat");
        const parsed = JSON.parse(beat);
        assert.equal(parsed.running, true);

        // Observability logs emitted by the loop.
        assert.ok(capture.byType("dispatcher-backlog-summary").length >= 1, "dispatcher-backlog-summary emitted");

        // Drain the stream so nothing leaks to the next test.
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        assert.equal(pipeline.bigquery.state.inserted.length, N, "all dispatched events inserted, zero loss");
    } finally {
        capture.restore();
        pipeline.services.dispatcher.stopDispatcher();
        // Always await the loop's exit before teardown, even if the body threw.
        if (bridgePromise) {
            await bridgePromise.catch(() => {});
        }
        await pipeline.cleanup();
    }
});
