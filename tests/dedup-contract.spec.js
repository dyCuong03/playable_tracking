"use strict";

// CONTRACT scenario 7: dedup must drop ONLY true duplicates.
//
// The Redis dedupe key is `pixel:dedupe:<stream>:<event_hash>`, so dedupe behaviour is
// governed entirely by buildRow/hashEvent. This spec validates the backend's event_hash
// widening fix: an EXACT duplicate resend collapses to one enqueue (intentional
// idempotency preserved), while interactions that differ in name/stage, session,
// playable_id, package_name OR env are all DISTINCT and every one is enqueued.
//
// This is the test that would FAIL if the hash were too narrow (the original bug:
// cross-env / cross-playable collisions silently dedup-dropped valid events) OR if dedupe
// were disabled (true duplicates would double-enqueue).

const test = require("node:test");
const assert = require("node:assert/strict");

const { createPipeline, drainDiskToRedis, runController } = require("./helpers/pipeline-harness");
const { interactionEvent } = require("./helpers/events");

const enqueueAll = async (pipeline, queries) => {
    for (const query of queries) {
        assert.equal(await runController(pipeline.services.controller, query), 200);
    }
    return drainDiskToRedis(pipeline.services);
};

test("event_hash level: exact duplicate hashes identically; any distinct field differs", () => {
    const pipeline = createPipeline();
    try {
        const { hashEvent } = pipeline.services.bigquery;
        const base = {
            sid: "s1",
            event: "interaction",
            eventTime: "2026-06-20T10:00:00.000Z",
            params: { name: "tap" },
            playableId: "pl-1",
            packageName: "com.a",
            trackingEnvironment: "test",
        };

        // Exact duplicate -> identical hash (intentional idempotency).
        assert.equal(hashEvent(base), hashEvent({ ...base }));

        // Each distinct dimension must change the hash.
        assert.notEqual(hashEvent(base), hashEvent({ ...base, params: { name: "swipe" } }), "different event name");
        assert.notEqual(hashEvent(base), hashEvent({ ...base, sid: "s2" }), "different session");
        assert.notEqual(hashEvent(base), hashEvent({ ...base, playableId: "pl-2" }), "different playable_id");
        assert.notEqual(hashEvent(base), hashEvent({ ...base, packageName: "com.b" }), "different package_name");
        assert.notEqual(hashEvent(base), hashEvent({ ...base, trackingEnvironment: "production" }), "different env");
    } finally {
        pipeline.cleanup();
    }
});

test("exact duplicate resend collapses to a single Redis XADD", async () => {
    const pipeline = createPipeline();
    try {
        const dup = interactionEvent("sess-dup", "tap");
        // Both identical events reach disk and are read by the bridge (2 items)...
        const itemsRead = await enqueueAll(pipeline, [dup, { ...dup }]);
        assert.equal(itemsRead, 2, "both resends are durably persisted and read by the bridge");

        // ...but dedupe collapses them to exactly ONE Redis XADD.
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), 1, "duplicate resend enqueues exactly once");
        assert.equal(pipeline.redis.state.counters.dedupeSkip, 1, "second send recorded as a dedup skip");
    } finally {
        await pipeline.cleanup();
    }
});

test("distinct interactions are all enqueued (name, session, playable, package, env)", async () => {
    const pipeline = createPipeline();
    try {
        // Same session/time but different event name + stage.
        const a = interactionEvent("sess-x", "tap_play");
        const b = interactionEvent("sess-x", "tap_again");
        // Different session.
        const c = interactionEvent("sess-y", "tap_play");
        // Different playable.
        const d = interactionEvent("sess-x", "tap_play", { playableId: "playable-Z" });
        // Different package.
        const e = interactionEvent("sess-x", "tap_play", { pid: "com.other.game" });
        // Different env.
        const f = interactionEvent("sess-x", "tap_play", { env: "production" });

        const queries = [a, b, c, d, e, f];
        const enqueued = await enqueueAll(pipeline, queries);

        assert.equal(enqueued, queries.length, "every distinct interaction must be enqueued");
        assert.equal(pipeline.redis.state.xaddCount("pixel:events"), queries.length);
        assert.equal(pipeline.redis.state.counters.dedupeSkip, 0, "no distinct interaction is dedup-dropped");
    } finally {
        await pipeline.cleanup();
    }
});
