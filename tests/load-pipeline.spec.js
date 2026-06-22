"use strict";

// CONTRACT scenarios 8 and 9 — load reconciliation through the full in-process pipeline.
//
// 8. 5k `start` load: accepted == enqueued == consumed == inserted (no loss).
// 9. mixed 5k load (start + interactions + store_trigger + end): per-stage counts
//    reconcile and total inserted == accepted (no loss, no over-dedup).
//
// Drives the REAL services via the same code path as scripts/load-pipeline.js.

const test = require("node:test");
const assert = require("node:assert/strict");

const {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
} = require("./helpers/pipeline-harness");
const { startEvent, interactionEvent, storeTriggerEvent, endEvent } = require("./helpers/events");

const EVENTS = 5000;

test("5k start load: accepted == enqueued == consumed == inserted (zero loss)", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture(); // silence chatty per-insert logs
    try {
        let accepted = 0;
        for (let i = 0; i < EVENTS; i += 1) {
            const status = await runController(
                pipeline.services.controller,
                startEvent(`load-start-${i}`, { playableId: `pl-${i % 7}` })
            );
            if (status === 200) {
                accepted += 1;
            }
        }

        const enqueued = await drainDiskToRedis(pipeline.services);
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state, { timeoutMs: 120_000 });
        capture.restore();

        const xadd = pipeline.redis.state.xaddCount("pixel:events");
        const consumed = pipeline.redis.state.counters.consumedByRead;
        const inserted = pipeline.bigquery.state.inserted.length;

        assert.equal(accepted, EVENTS, "every request accepted (HTTP 200)");
        assert.equal(enqueued, EVENTS, "every accepted event bridged off disk");
        assert.equal(xadd, EVENTS, "every event enqueued to Redis (no dedup over-drop)");
        assert.equal(consumed, EVENTS, "worker consumed every event");
        assert.equal(inserted, EVENTS, "every event inserted into BigQuery (zero loss)");
        assert.equal(pipeline.redis.state.counters.dedupeSkip, 0, "no false dedup on unique events");
        assert.equal(pipeline.redis.state.streamLength("pixel:events"), 0, "stream fully drained");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});

test("mixed 5k load: per-stage counts reconcile and nothing is lost", async () => {
    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        const perSession = 5;
        const sessions = Math.ceil(EVENTS / perSession);
        const stageCounts = { start: 0, interaction: 0, store_trigger: 0, end: 0 };
        let accepted = 0;
        let emitted = 0;

        for (let s = 0; s < sessions && emitted < EVENTS; s += 1) {
            const sid = `load-mixed-${s}`;
            const playableId = `pl-${s % 11}`;
            const stages = [
                ["start", startEvent(sid, { playableId })],
                ["interaction", interactionEvent(sid, "tap_play", { playableId })],
                ["interaction", interactionEvent(sid, "tap_again", { playableId })],
                ["store_trigger", storeTriggerEvent(sid, "cta_click", { playableId })],
                ["end", endEvent(sid, 2, { playableId })],
            ];
            for (const [stage, query] of stages) {
                if (emitted >= EVENTS) {
                    break;
                }
                const status = await runController(pipeline.services.controller, query);
                if (status === 200) {
                    accepted += 1;
                    stageCounts[stage] += 1;
                }
                emitted += 1;
            }
        }

        const enqueued = await drainDiskToRedis(pipeline.services);
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state, { timeoutMs: 120_000 });
        capture.restore();

        const inserted = pipeline.bigquery.state.inserted.length;

        assert.equal(accepted, emitted, "every mixed request accepted");
        assert.equal(enqueued, emitted, "every accepted event bridged off disk");
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), emitted, "every event enqueued");
        assert.equal(inserted, emitted, "every event inserted (zero loss)");

        // Per-stage reconciliation: inserted rows match the requested per-stage counts.
        const insertedByStage = pipeline.bigquery.state.inserted.reduce((acc, row) => {
            acc[row.event_name] = (acc[row.event_name] || 0) + 1;
            return acc;
        }, {});
        for (const stage of Object.keys(stageCounts)) {
            assert.equal(
                insertedByStage[stage] || 0,
                stageCounts[stage],
                `inserted ${stage} count reconciles with requested`
            );
        }
        assert.equal(pipeline.redis.state.counters.dedupeSkip, 0, "no over-dedup across distinct stages/sessions");
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
});
