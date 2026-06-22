"use strict";

// Phase 2 — stuck-state visibility + zero-loss recovery (data-level).
//
// These tests prove that when a tier (dispatcher / worker / Redis) is stuck, the symptom
// is OBSERVABLE in shared state (disk backlog count, Redis stream length) and that NO
// accepted event is lost once the tier recovers. They assert on real data movement, so
// they fail on actual loss. The health/debug/ops *status* surface is asserted separately
// in pipeline-health.spec.js (which activates once the backend health module lands).

const test = require("node:test");
const assert = require("node:assert/strict");

const {
    createPipeline,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
    sleep,
} = require("./helpers/pipeline-harness");
const { startEvent, interactionEvent } = require("./helpers/events");

// ─── dispatcher stopped: events accepted, stranded on disk (visible), drain on restart ──

test("dispatcher stopped: HTTP still 200, disk backlog is visible and NOT in Redis", async () => {
    const pipeline = createPipeline();
    const N = 50;
    try {
        // No dispatcher running (we simply never call drainDiskToRedis): the original incident.
        for (let i = 0; i < N; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`sess-disp-${i}`)), 200);
        }

        const stats = await pipeline.services.diskQueue.getQueueStats();
        const backlog = stats.pending.fileCount + stats.ready.fileCount + stats.processing.fileCount;
        assert.ok(stats.pending.totalBytes > 0, "events are stranded on disk");
        assert.ok(backlog > 0, "disk backlog is visible via getQueueStats");

        // Nothing reached Redis because the bridge never ran.
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), 0, "no events in Redis while dispatcher stuck");
        assert.equal(pipeline.bigquery.state.inserted.length, 0, "nothing inserted while dispatcher stuck");
    } finally {
        await pipeline.cleanup();
    }
});

test("dispatcher restart: stranded disk backlog drains to BigQuery with zero loss", async () => {
    const pipeline = createPipeline();
    const N = 50;
    try {
        for (let i = 0; i < N; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`sess-disp2-${i}`)), 200);
        }
        // Dispatcher was down; now it "restarts" and drains the backlog.
        const drained = await drainDiskToRedis(pipeline.services);
        assert.equal(drained, N, "every stranded event bridged off disk after restart");

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        assert.equal(pipeline.bigquery.state.inserted.length, N, "every stranded event inserted, zero loss");

        const stats = await pipeline.services.diskQueue.getQueueStats();
        assert.equal(stats.pending.totalBytes + stats.ready.totalBytes + stats.processing.totalBytes, 0, "disk drained");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── worker stopped: stream length grows (visible), drains on worker restart ──────────

test("worker stopped: Redis stream length grows and is visible; restart drains zero-loss", async () => {
    const pipeline = createPipeline();
    const N = 40;
    try {
        for (let i = 0; i < N; i += 1) {
            assert.equal(await runController(pipeline.services.controller, interactionEvent(`sess-wstop-${i}`, `t-${i}`)), 200);
        }
        const enqueued = await drainDiskToRedis(pipeline.services);
        assert.equal(enqueued, N);

        // Worker is stopped -> the stream backs up and the depth is observable via XLEN.
        assert.equal(pipeline.redis.state.streamLength("pixel:events"), N, "stream length grows while worker stuck");
        assert.equal(pipeline.bigquery.state.inserted.length, 0, "nothing consumed while worker stuck");

        // Worker restarts and drains everything.
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        assert.equal(pipeline.bigquery.state.inserted.length, N, "every event inserted after worker restart, zero loss");
        assert.equal(pipeline.redis.state.streamLength("pixel:events"), 0, "stream fully drained");
    } finally {
        await pipeline.cleanup();
    }
});

// ─── redis down then recovery: enqueue fails (not silent), drains after recovery ──────

test("redis down then recovery: buffered on disk, no silent loss after recovery", async () => {
    const pipeline = createPipeline();
    const N = 35;
    try {
        pipeline.redis.state.setFault(true);
        for (let i = 0; i < N; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`sess-rdown-${i}`)), 200);
        }
        await assert.rejects(() => drainDiskToRedis(pipeline.services), /FAKE_REDIS_DOWN|unavailable/);

        pipeline.redis.state.setFault(false);
        await sleep(250); // cooldown
        const drained = await drainDiskToRedis(pipeline.services);
        assert.equal(drained, N, "all buffered events drain after Redis recovery");

        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        assert.equal(pipeline.bigquery.state.inserted.length, N, "zero loss across the Redis outage");
    } finally {
        await pipeline.cleanup();
    }
});
