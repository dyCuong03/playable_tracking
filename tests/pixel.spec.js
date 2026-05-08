const test = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");

const app = require("../src/app");
const {
    getQueueStats,
    resetQueueState,
} = require("../src/services/bigquery-queue.service");

test.beforeEach(() => {
    return resetQueueState();
});

test.after(() => {
    return resetQueueState();
});

test("GET /p.gif returns a pixel immediately", async () => {
    const response = await request(app)
        .get("/p.gif")
        .query({
            e: "tracking_snapshot",
            pid: "com.demo.game",
            sid: "session-1",
            env: "test",
        })
        .expect(200);

    assert.equal(response.headers["content-type"], "image/gif");
    assert.ok(Buffer.isBuffer(response.body));
    assert.ok(response.body.length > 0);
});

test("GET /p.gif persists the event to the durable queue before returning", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            e: "tracking_snapshot",
            pid: "com.demo.game",
            sid: "session-2",
            env: "test",
        })
        .expect(200);

    let totalFiles = 0;
    let totalBytes = 0;

    for (let attempt = 0; attempt < 5; attempt += 1) {
        const stats = await getQueueStats();
        totalFiles = stats.pending.fileCount + stats.ready.fileCount + stats.processing.fileCount;
        totalBytes = stats.pending.totalBytes + stats.ready.totalBytes + stats.processing.totalBytes;

        if (totalFiles > 0 && totalBytes > 0) {
            break;
        }

        await new Promise((resolve) => setTimeout(resolve, 25));
    }

    assert.ok(totalFiles > 0);
    assert.ok(totalBytes > 0);
});
