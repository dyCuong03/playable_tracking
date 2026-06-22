"use strict";

// Evidence generator for REPORT2: drives ONE event through every stage and prints its
// event_hash alongside the matching contract log line at each stage
// (HTTP/disk -> dispatcher/redis -> worker/BigQuery), then demonstrates that the
// dispatcher-stopped and worker-stopped conditions flip /debug/pipeline's pipeline_status.
//
// Real services in-process, fake Redis + mock BigQuery. Zero external deps:
//   node scripts/trace-event.js

const path = require("path");
const request = require("supertest");
const {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
} = require(path.join(__dirname, "..", "tests", "helpers", "pipeline-harness"));
const { interactionEvent, startEvent } = require(path.join(__dirname, "..", "tests", "helpers", "events"));

const line = (label, entry) => {
    console.log(`\n  [${label}]`);
    console.log(`    ${JSON.stringify(entry)}`);
};

const traceSingleEvent = async () => {
    console.log("=".repeat(80));
    console.log("EVIDENCE 1 — single event traced by event_hash through ALL stages");
    console.log("=".repeat(80));

    const pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        await runController(pipeline.services.controller, interactionEvent("trace-session-1", "tap_play"));
        const diskLog = capture.byType("disk-persist-success")[0];
        const hash = diskLog.event_hash;

        await drainDiskToRedis(pipeline.services);
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);
        capture.restore();

        const enqueue = capture.byType("redis-enqueue-success").find((e) => e.event_hash === hash);
        const attempt = capture.byType("bigquery-insert-attempt").find(
            (e) => (e.event_hashes || []).includes(hash) || e.event_hash === hash
        );
        const insert = capture.byType("bigquery-insert-success").find((e) => (e.event_hashes || []).includes(hash));
        const inSink = pipeline.bigquery.state.inserted.some((row) => row.event_hash === hash);

        console.log(`\n  event_hash = ${hash}`);
        line("HTTP / disk  (disk-persist-success)", diskLog);
        line("dispatcher / redis (redis-enqueue-success)", enqueue);
        line("worker (bigquery-insert-attempt)", attempt);
        line("worker (bigquery-insert-success)", insert);
        console.log(`\n  mock BigQuery sink contains row with this event_hash: ${inSink}`);
        console.log(`\n  SAME event_hash at every stage: ${[diskLog.event_hash, enqueue.event_hash, (insert.event_hashes || [])[0]].every((h) => h === hash)}`);
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }
};

const statusFlip = async () => {
    console.log("\n" + "=".repeat(80));
    console.log("EVIDENCE 2 — stuck states flip /debug/pipeline pipeline_status");
    console.log("=".repeat(80));

    // dispatcher stopped: accept events, never drain.
    let pipeline = createPipeline();
    const capture = createConsoleCapture();
    try {
        for (let i = 0; i < 20; i += 1) {
            await runController(pipeline.services.controller, startEvent(`flip-disp-${i}`));
        }
        const res = await request(pipeline.services.app).get("/debug/pipeline").expect(200);
        capture.restore();
        console.log("\n  DISPATCHER STOPPED (events accepted, bridge not running):");
        console.log(`    pipeline_status = ${res.body.pipeline_status}`);
        console.log(`    unhealthy_reasons = ${JSON.stringify(res.body.unhealthy_reasons)}`);
        console.log(`    disk_queue.total = ${res.body.disk_queue.total}  (backlog visible)`);
        console.log(`    stream_length = ${res.body.stream_length}`);
    } finally {
        capture.restore();
        await pipeline.cleanup();
    }

    // worker stopped: drain to redis, never run worker.
    pipeline = createPipeline();
    const capture2 = createConsoleCapture();
    try {
        for (let i = 0; i < 20; i += 1) {
            await runController(pipeline.services.controller, interactionEvent(`flip-wrk-${i}`, `t-${i}`));
        }
        await drainDiskToRedis(pipeline.services);
        // mark dispatcher fresh so the verdict isolates the worker symptom
        await pipeline.services.health.recordHeartbeat("dispatcher", { running: true, lastSuccessAt: new Date().toISOString(), dispatcher_id: "ev" });
        const res = await request(pipeline.services.app).get("/debug/pipeline").expect(200);
        capture2.restore();
        console.log("\n  WORKER STOPPED (events in Redis stream, no worker consuming):");
        console.log(`    pipeline_status = ${res.body.pipeline_status}`);
        console.log(`    unhealthy_reasons = ${JSON.stringify(res.body.unhealthy_reasons)}`);
        console.log(`    degraded_reasons = ${JSON.stringify(res.body.degraded_reasons)}`);
        console.log(`    stream_length = ${res.body.stream_length}  (depth visible)`);
    } finally {
        capture2.restore();
        await pipeline.cleanup();
    }
};

(async () => {
    await traceSingleEvent();
    await statusFlip();
    console.log("\nDONE.");
})().catch((error) => {
    console.error("trace-event failed:", error);
    process.exit(1);
});
