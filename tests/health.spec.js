require("./helpers/isolate-queue-dir"); // MUST be first: redirect disk queue to a temp dir before src loads
const test = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

const app = require("../src/app");
const {
    resetQueueState,
} = require("../src/services/bigquery-queue.service");

test.beforeEach(() => {
    return resetQueueState();
});

test.after(() => {
    return resetQueueState();
});

test("GET /health skips Redis stats by default", async () => {
    const response = await request(app)
        .get("/health")
        .expect(200);

    assert.equal(response.body.ok, true);
    assert.equal(typeof response.body.bigQuery.enabled, "boolean");
    assert.equal(typeof response.body.bigQuery.configured, "boolean");
    assert.ok(Array.isArray(response.body.bigQuery.issues));
    assert.equal(response.body.queue.ok, true);
    assert.equal(response.body.queue.skipped, true);
    assert.equal(response.body.dispatcher.bridge.running, false);
    assert.equal(typeof response.body.dispatcher.durableQueue.pending.fileCount, "number");
});

test("GET /health?queue=1 degrades gracefully when Redis is unavailable", async () => {
    const response = await request(app)
        .get("/health")
        .query({ queue: "1" })
        .expect(200);

    assert.equal(response.body.ok, true);
    assert.equal(typeof response.body.queue.ok, "boolean");

    if (response.body.queue.ok === false) {
        assert.equal(typeof response.body.queue.error, "string");
        assert.ok(response.body.queue.error.length > 0);
    }
});
