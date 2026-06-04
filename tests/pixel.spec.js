const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const request = require("supertest");

const app = require("../src/app");
const {
    getQueueStats,
    resetQueueState,
    parseQueueFile,
    PENDING_DIR,
} = require("../src/services/bigquery-queue.service");

test.beforeEach(() => resetQueueState());
test.after(() => resetQueueState());

// ─── Helpers ─────────────────────────────────────────────────────────────────

const waitForQueueData = async () => {
    for (let attempt = 0; attempt < 10; attempt += 1) {
        const stats = await getQueueStats();
        const totalBytes =
            stats.pending.totalBytes +
            stats.ready.totalBytes +
            stats.processing.totalBytes;

        if (totalBytes > 0) {
            return stats;
        }

        await new Promise((resolve) => setTimeout(resolve, 25));
    }

    return null;
};

const readAllQueuedRows = async () => {
    const entries = await fs.promises.readdir(PENDING_DIR).catch(() => []);
    const rows = [];

    for (const entry of entries) {
        if (!entry.endsWith(".ndjson")) {
            continue;
        }

        const items = await parseQueueFile(path.join(PENDING_DIR, entry));

        for (const item of items) {
            if (item && item.row) {
                rows.push(item.row);
            }
        }
    }

    return rows;
};

// ─── 1×1 GIF response for all four event types ───────────────────────────────

test("GET /p.gif: start event returns 200 image/gif immediately", async () => {
    const response = await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-start-1",
            event_name: "start",
            event_time: "2026-06-04T10:00:00.000Z",
            event_params: JSON.stringify({ platform: "android", campaign: {} }),
        })
        .expect(200);

    assert.equal(response.headers["content-type"], "image/gif");
    assert.ok(Buffer.isBuffer(response.body) && response.body.length > 0);
});

test("GET /p.gif: interaction event returns 200 image/gif immediately", async () => {
    const response = await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-interact-1",
            event_name: "interaction",
            event_time: "2026-06-04T10:01:00.000Z",
            event_params: JSON.stringify({ name: "btn_play" }),
        })
        .expect(200);

    assert.equal(response.headers["content-type"], "image/gif");
    assert.ok(Buffer.isBuffer(response.body) && response.body.length > 0);
});

test("GET /p.gif: store_trigger event returns 200 image/gif immediately", async () => {
    const response = await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-store-1",
            event_name: "store_trigger",
            event_time: "2026-06-04T10:02:00.000Z",
            event_params: JSON.stringify({ name: "store_open" }),
        })
        .expect(200);

    assert.equal(response.headers["content-type"], "image/gif");
    assert.ok(Buffer.isBuffer(response.body) && response.body.length > 0);
});

test("GET /p.gif: end event returns 200 image/gif immediately", async () => {
    const response = await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-end-1",
            event_name: "end",
            event_time: "2026-06-04T10:03:00.000Z",
            event_params: JSON.stringify({ interact_count: 5 }),
        })
        .expect(200);

    assert.equal(response.headers["content-type"], "image/gif");
    assert.ok(Buffer.isBuffer(response.body) && response.body.length > 0);
});

// ─── Queue persistence ────────────────────────────────────────────────────────

test("GET /p.gif persists event to durable queue before responding", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-queue-1",
            event_name: "start",
            event_time: "2026-06-04T10:00:00.000Z",
            event_params: JSON.stringify({ platform: "ios", campaign: {} }),
        })
        .expect(200);

    const stats = await waitForQueueData();
    assert.ok(stats !== null, "Queue should have data after request");

    const totalFiles =
        stats.pending.fileCount +
        stats.ready.fileCount +
        stats.processing.fileCount;
    assert.ok(totalFiles > 0);
    assert.ok(
        stats.pending.totalBytes + stats.ready.totalBytes + stats.processing.totalBytes > 0
    );
});

// ─── Row content: event_time comes from client, received_at from server ───────

test("queued row preserves client-provided event_time (not server time)", async () => {
    const clientTime = "2020-01-01T00:00:00.000Z"; // clearly in the past

    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-time-1",
            event_name: "start",
            event_time: clientTime,
            event_params: JSON.stringify({ platform: "android", campaign: {} }),
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-time-1");

    assert.ok(row, "Row for sess-time-1 not found in queue");
    assert.equal(row.event_time, clientTime, "event_time must match client-supplied value");
});

test("queued row has received_at added by server", async () => {
    const before = Date.now();

    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-recv-1",
            event_name: "interaction",
            event_time: "2026-06-04T10:01:00.000Z",
            event_params: JSON.stringify({ name: "tap" }),
        })
        .expect(200);

    const after = Date.now();

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-recv-1");

    assert.ok(row, "Row for sess-recv-1 not found in queue");
    assert.equal(typeof row.received_at, "string", "received_at must be a string");

    const ms = Date.parse(row.received_at);
    assert.ok(Number.isFinite(ms), "received_at must be a valid ISO timestamp");
    assert.ok(ms >= before && ms <= after + 1000,
        `received_at ${row.received_at} should be between ${new Date(before).toISOString()} and ${new Date(after + 1000).toISOString()}`);
});

// ─── Row content: event_params structure ─────────────────────────────────────

test("queued row for start event has event_params with platform and campaign", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-start-params",
            event_name: "start",
            event_time: "2026-06-04T10:00:00.000Z",
            event_params: JSON.stringify({ platform: "android", campaign: { network: "meta", id: 7 } }),
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-start-params");

    assert.ok(row, "Row for sess-start-params not found");
    assert.equal(typeof row.event_params, "object");
    assert.equal(row.event_params.platform, "android");
    assert.deepEqual(row.event_params.campaign, { network: "meta", id: 7 });
});

test("queued row for start event: legacy platform + campaign_raw params work too", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-legacy-start",
            event_name: "start",
            event_time: "2026-06-04T10:00:00.000Z",
            platform: "ios",
            campaign_raw: JSON.stringify({ network: "goog" }),
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-legacy-start");

    assert.ok(row, "Row for sess-legacy-start not found");
    assert.equal(row.event_params.platform, "ios");
    assert.deepEqual(row.event_params.campaign, { network: "goog" });
});

test("queued row for end event has event_params with interact_count", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-end-params",
            event_name: "end",
            event_time: "2026-06-04T10:03:00.000Z",
            event_params: JSON.stringify({ interact_count: 11 }),
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-end-params");

    assert.ok(row, "Row for sess-end-params not found");
    assert.equal(row.event_params.interact_count, 11);
});

// ─── Row content: no forbidden top-level fields ───────────────────────────────

test("queued row has no top-level platform, campaign_raw, user_agent, ip, or referer fields", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-nofld-1",
            event_name: "end",
            event_time: "2026-06-04T10:03:00.000Z",
            event_params: JSON.stringify({ interact_count: 3 }),
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-nofld-1");

    assert.ok(row, "Row for sess-nofld-1 not found");

    const forbidden = ["platform", "campaign_raw", "user_agent", "ip", "referer"];
    for (const key of forbidden) {
        assert.equal(key in row, false, `Row must not have top-level field "${key}"`);
    }
});

test("queued row carries package_name + playable_id from query (pid / playable_id)", async () => {
    await request(app)
        .get("/p.gif")
        .query({
            session_id: "sess-pkg-1",
            event_name: "interaction",
            event_time: "2026-06-04T10:04:00.000Z",
            event_params: JSON.stringify({ name: "tap" }),
            pid: "com.archer.cat.kitchen",
            playable_id: "PA0006",
        })
        .expect(200);

    await waitForQueueData();
    const rows = await readAllQueuedRows();
    const row = rows.find((r) => r.session_id === "sess-pkg-1");

    assert.ok(row, "Row for sess-pkg-1 not found");
    assert.equal(row.package_name, "com.archer.cat.kitchen");
    assert.equal(row.playable_id, "PA0006");
});
