const test = require("node:test");
const assert = require("node:assert/strict");

const { buildEvent } = require("../src/services/event.service");

const makeReq = (query) => ({ query });

// ─── event identity fields ────────────────────────────────────────────────────

test("buildEvent: maps event_name → event.event and session_id → event.sid", () => {
    const event = buildEvent(makeReq({
        event_name: "start",
        session_id: "sess-001",
        event_time: "2026-06-04T10:00:00.000Z",
    }));

    assert.equal(event.event, "start");
    assert.equal(event.sid, "sess-001");
});

test("buildEvent: legacy e / sid aliases still work", () => {
    const event = buildEvent(makeReq({
        e: "interaction",
        sid: "sess-legacy",
        event_time: "2026-06-04T10:01:00.000Z",
    }));

    assert.equal(event.event, "interaction");
    assert.equal(event.sid, "sess-legacy");
});

// ─── event_time resolution ────────────────────────────────────────────────────

test("buildEvent: event_time ISO string is preserved (normalised to ISO)", () => {
    const clientTime = "2026-06-04T10:00:00.000Z";
    const event = buildEvent(makeReq({
        session_id: "sess-time-1",
        event_name: "start",
        event_time: clientTime,
    }));

    assert.equal(event.eventTime, clientTime);
});

test("buildEvent: ts alias also accepted as event_time", () => {
    const clientTime = "2026-06-04T09:00:00.000Z";
    const event = buildEvent(makeReq({
        session_id: "sess-ts",
        event_name: "end",
        ts: clientTime,
    }));

    assert.equal(event.eventTime, clientTime);
});

test("buildEvent: epoch-ms string is converted to ISO", () => {
    const epochMs = "1748995200000"; // 2025-06-04T00:00:00.000Z
    const event = buildEvent(makeReq({
        session_id: "sess-epoch",
        event_name: "end",
        event_time: epochMs,
    }));

    const parsed = Date.parse(event.eventTime);
    assert.ok(Number.isFinite(parsed), "eventTime must be a valid ISO string");
    assert.equal(parsed, Number(epochMs));
});

test("buildEvent: falls back to server time when event_time absent", () => {
    const before = Date.now();
    const event = buildEvent(makeReq({
        session_id: "sess-notime",
        event_name: "interaction",
    }));
    const after = Date.now();

    const ms = Date.parse(event.eventTime);
    assert.ok(Number.isFinite(ms), "eventTime must be a valid ISO string");
    assert.ok(ms >= before && ms <= after);
});

// ─── event_params (event.params): new-model JSON object in query ──────────────

test("buildEvent start: parses event_params JSON into event.params", () => {
    const params = { platform: "android", campaign: { network: "meta", id: 42 } };
    const event = buildEvent(makeReq({
        session_id: "sess-start-1",
        event_name: "start",
        event_time: "2026-06-04T10:00:00.000Z",
        event_params: JSON.stringify(params),
    }));

    assert.deepEqual(event.params, params);
});

test("buildEvent interaction: parses event_params JSON with name", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-interact-1",
        event_name: "interaction",
        event_time: "2026-06-04T10:01:00.000Z",
        event_params: JSON.stringify({ name: "btn_play" }),
    }));

    assert.deepEqual(event.params, { name: "btn_play" });
});

test("buildEvent store_trigger: parses event_params JSON with name", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-store-1",
        event_name: "store_trigger",
        event_time: "2026-06-04T10:02:00.000Z",
        event_params: JSON.stringify({ name: "store_open" }),
    }));

    assert.deepEqual(event.params, { name: "store_open" });
});

test("buildEvent end: parses event_params JSON with interact_count", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-end-1",
        event_name: "end",
        event_time: "2026-06-04T10:03:00.000Z",
        event_params: JSON.stringify({ interact_count: 7 }),
    }));

    assert.deepEqual(event.params, { interact_count: 7 });
});

// ─── event.params: legacy-compat (separate query params for start) ────────────

test("buildEvent start: legacy platform + campaign_raw network merged into event.params", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-legacy-start",
        event_name: "start",
        event_time: "2026-06-04T10:00:00.000Z",
        platform: "IOS",
        campaign_raw: JSON.stringify({ network: "goog", id: 9 }),
    }));

    assert.equal(event.params.platform, "IOS");
    assert.equal(event.params.network, "goog");
    assert.equal(event.params.campaign, undefined);
});

test("buildEvent start: event_params platform takes precedence over legacy platform param", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-precedence",
        event_name: "start",
        event_time: "2026-06-04T10:00:00.000Z",
        event_params: JSON.stringify({ platform: "android", campaign: {} }),
        platform: "ios", // should be ignored
    }));

    assert.equal(event.params.platform, "android");
});

// ─── no forbidden top-level fields ───────────────────────────────────────────

test("buildEvent: no top-level ip, ua, ref, platform, campaignRaw, pid fields", () => {
    const event = buildEvent(makeReq({
        session_id: "sess-nofld",
        event_name: "start",
        event_time: "2026-06-04T10:00:00.000Z",
        platform: "android",
        campaign_raw: "{}",
        pid: "com.game.test",
    }));

    const forbidden = ["ua", "platform", "campaignRaw", "pid", "user_agent"];
    for (const key of forbidden) {
        assert.equal(key in event, false, `event must not have top-level field "${key}"`);
    }
});
