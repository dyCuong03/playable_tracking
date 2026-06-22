"use strict";

// Phase 2 — pipeline-health visibility surface.
//
// A. computePipelineStatus(snapshot) unit table — each CONTRACT2 rule maps to the right
//    pipeline_status + a matching reason.
// B. /debug/pipeline endpoint reflects stuck states and exposes the cross-process
//    snapshot (disk backlog, stream length, heartbeats) with no secrets.
//
// Shapes match src/services/pipeline-health.service.js exactly:
//   snapshot = { now, redisReachable, streamLength, rejectedLength, diskBacklogFiles,
//                diskBacklogItems, heartbeats: { web, dispatcher, workers[] } }
// Heartbeat entries carry ISO timestamps; ages are computed as now - Date.parse(field).
// Thresholds default to 30s (stale) / 5000 (disk warn) / 10000 (stream warn).

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const request = require("supertest");

const HEALTH_MODULE = path.join(__dirname, "..", "src", "services", "pipeline-health.service.js");
const SKIP = fs.existsSync(HEALTH_MODULE) ? false : "awaiting backend src/services/pipeline-health.service.js";

const { createPipeline, drainDiskToRedis, runController, runWorkerUntilDrained } = require("./helpers/pipeline-harness");
const { startEvent } = require("./helpers/events");

const FRESH = 1_000; // < 30s stale thresholds
const STALE = 300_000; // >> 30s
const isoAgo = (ms) => new Date(Date.now() - ms).toISOString();

const freshWorker = () => ({
    worker_id: "w-1",
    lastConsumeAt: isoAgo(FRESH),
    lastInsertAt: isoAgo(FRESH),
    bqFailureCount: 0,
});

const snapshot = (overrides = {}) => ({
    now: Date.now(),
    redisReachable: true,
    streamLength: 0,
    rejectedLength: 0,
    diskBacklogFiles: 0,
    diskBacklogItems: 0,
    heartbeats: {
        web: { lastAcceptAt: isoAgo(FRESH) },
        dispatcher: { running: true, lastSuccessAt: isoAgo(FRESH), lastErrorAt: null, dispatcher_id: "disp-1" },
        workers: [freshWorker()],
    },
    ...overrides,
});

const allReasons = (result) =>
    [...(result.unhealthy_reasons || []), ...(result.degraded_reasons || [])].join(" | ").toLowerCase();

test("computePipelineStatus rule table maps each condition to the right status", { skip: SKIP }, () => {
    const pipeline = createPipeline();
    try {
        const { computePipelineStatus } = require("../src/services/pipeline-health.service");

        // HEALTHY — idle, empty, fresh.
        assert.equal(computePipelineStatus(snapshot()).pipeline_status, "healthy");

        // UNHEALTHY — redis unreachable.
        assert.equal(computePipelineStatus(snapshot({ redisReachable: false })).pipeline_status, "unhealthy");

        // UNHEALTHY — dispatcher stale while disk backlog > 0 (THE ORIGINAL INCIDENT).
        let r = computePipelineStatus(snapshot({
            diskBacklogFiles: 3,
            diskBacklogItems: 30,
            heartbeats: { web: null, dispatcher: { running: false, lastSuccessAt: isoAgo(STALE) }, workers: [freshWorker()] },
        }));
        assert.equal(r.pipeline_status, "unhealthy");
        assert.match(allReasons(r), /dispatch|backlog|strand/);

        // UNHEALTHY — dispatcher heartbeat missing while backlog > 0.
        r = computePipelineStatus(snapshot({
            diskBacklogFiles: 1,
            diskBacklogItems: 5,
            heartbeats: { web: null, dispatcher: null, workers: [freshWorker()] },
        }));
        assert.equal(r.pipeline_status, "unhealthy");

        // UNHEALTHY — accepting but nothing inserted while stream_len > 0.
        r = computePipelineStatus(snapshot({
            streamLength: 50,
            heartbeats: {
                web: { lastAcceptAt: isoAgo(FRESH) },
                dispatcher: { running: true, lastSuccessAt: isoAgo(FRESH) },
                workers: [{ worker_id: "w-1", lastConsumeAt: isoAgo(FRESH), lastInsertAt: isoAgo(STALE), bqFailureCount: 0 }],
            },
        }));
        assert.equal(r.pipeline_status, "unhealthy");
        assert.match(allReasons(r), /insert|bigquery/);

        // UNHEALTHY — no workers while stream_len > 0.
        r = computePipelineStatus(snapshot({
            streamLength: 30,
            heartbeats: { web: null, dispatcher: { running: true, lastSuccessAt: isoAgo(FRESH) }, workers: [] },
        }));
        assert.equal(r.pipeline_status, "unhealthy");
        assert.match(allReasons(r), /worker|consum/);

        // DEGRADED — disk backlog items over warn (dispatcher fresh, so not unhealthy).
        r = computePipelineStatus(snapshot({ diskBacklogFiles: 1, diskBacklogItems: 6_000 }));
        assert.equal(r.pipeline_status, "degraded");

        // DEGRADED — stream length over warn.
        assert.equal(computePipelineStatus(snapshot({ streamLength: 20_000 })).pipeline_status, "degraded");

        // DEGRADED — worker consume stale while stream non-empty.
        r = computePipelineStatus(snapshot({
            streamLength: 5,
            heartbeats: {
                web: { lastAcceptAt: isoAgo(STALE) },
                dispatcher: { running: true, lastSuccessAt: isoAgo(FRESH) },
                workers: [{ worker_id: "w-1", lastConsumeAt: isoAgo(STALE), lastInsertAt: isoAgo(FRESH), bqFailureCount: 0 }],
            },
        }));
        assert.equal(r.pipeline_status, "degraded");

        // DEGRADED — rejected rows present.
        assert.equal(computePipelineStatus(snapshot({ rejectedLength: 3 })).pipeline_status, "degraded");

        // DEGRADED — bq failure count.
        r = computePipelineStatus(snapshot({
            heartbeats: {
                web: { lastAcceptAt: isoAgo(FRESH) },
                dispatcher: { running: true, lastSuccessAt: isoAgo(FRESH) },
                workers: [{ worker_id: "w-1", lastConsumeAt: isoAgo(FRESH), lastInsertAt: isoAgo(FRESH), bqFailureCount: 4 }],
            },
        }));
        assert.equal(r.pipeline_status, "degraded");
    } finally {
        pipeline.cleanup();
    }
});

test("/debug/pipeline reports unhealthy for a stuck dispatcher + visible disk backlog, no secrets", { skip: SKIP }, async () => {
    const pipeline = createPipeline();
    try {
        for (let i = 0; i < 20; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`dbg-${i}`)), 200);
        }

        const res = await request(pipeline.services.app).get("/debug/pipeline").expect(200);
        const body = res.body;

        assert.equal(body.pipeline_status, "unhealthy", "stuck dispatcher + backlog is unhealthy");
        assert.match(body.unhealthy_reasons.join(" ").toLowerCase(), /dispatch|backlog|strand/);
        assert.ok(body.disk_queue && body.disk_queue.total > 0, "disk backlog count visible");
        assert.equal(typeof body.stream_length, "number");
        assert.equal(body.redis_reachable, true);
        assert.equal(body.queue_type, "stream");
        // No secrets.
        const serialized = JSON.stringify(body);
        assert.ok(!/redis:\/\/[^"']*:[^@"']*@/.test(serialized), "no credentials in any redis url");
    } finally {
        await pipeline.cleanup();
    }
});

test("/debug/pipeline reports healthy once disk + stream fully drained", { skip: SKIP }, async () => {
    const pipeline = createPipeline();
    try {
        for (let i = 0; i < 12; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`dbg2-${i}`)), 200);
        }
        await drainDiskToRedis(pipeline.services);
        await runWorkerUntilDrained(pipeline.services, pipeline.redis.state);

        // Mark the dispatcher alive (in prod its loop heartbeats; here we drove the bridge directly).
        await pipeline.services.health.recordHeartbeat("dispatcher", {
            running: true,
            lastSuccessAt: new Date().toISOString(),
            dispatcher_id: "test-dispatcher",
        });

        const res = await request(pipeline.services.app).get("/debug/pipeline").expect(200);
        assert.equal(res.body.pipeline_status, "healthy", `expected healthy, got ${JSON.stringify(res.body.unhealthy_reasons)}`);
        assert.equal(res.body.disk_queue.total, 0, "disk drained");
        assert.equal(res.body.stream_length, 0, "stream drained");
    } finally {
        await pipeline.cleanup();
    }
});
