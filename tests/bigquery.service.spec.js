const test = require("node:test");
const assert = require("node:assert/strict");

const {
    buildRow,
    formatRowForInsert,
    normalizeValueForBigQueryType,
    validateFormattedRowForInsert,
} = require("../src/services/bigquery.service");

// Construct an event object as buildEvent() would produce, using the internal
// field names that buildRow() and hashEvent() actually read.
const makeEvent = (overrides = {}) => ({
    sid: "sess-001",
    event: "start",
    eventTime: "2026-06-04T10:00:00.000Z",
    params: { platform: "android", campaign: {} },
    trackingEnvironment: "test",
    ...overrides,
});

// ─── buildRow: new shape ──────────────────────────────────────────────────────

test("buildRow produces correct new shape: session_id, event_name, event_time, event_params, received_at, event_hash", () => {
    const row = buildRow(makeEvent({
        sid: "sess-001",
        event: "start",
        eventTime: "2026-06-04T10:00:00.000Z",
        params: { platform: "android", campaign: { network: "meta" } },
    }));

    assert.equal(row.session_id, "sess-001");
    assert.equal(row.event_name, "start");
    assert.equal(row.event_time, "2026-06-04T10:00:00.000Z");
    assert.deepEqual(row.event_params, { platform: "android", campaign: { network: "meta" } });
    assert.equal(typeof row.received_at, "string");
    assert.ok(row.received_at.length > 0);
    assert.equal(typeof row.event_hash, "string");
    assert.equal(row.event_hash.length, 64, "event_hash must be a 64-char SHA-256 hex string");
});

test("buildRow does not include legacy top-level fields", () => {
    const row = buildRow(makeEvent({
        sid: "sess-002",
        event: "interaction",
        params: { name: "tap" },
    }));

    const forbidden = [
        "platform", "campaign_raw", "user_agent", "ip",
        "referer", "package_name", "playable_id",
        "e", "pid", "ua", "ref", "time",
    ];
    for (const key of forbidden) {
        assert.equal(key in row, false, `buildRow must not include top-level field "${key}"`);
    }
});

test("buildRow: received_at is a valid server-generated ISO timestamp", () => {
    const before = Date.now();
    const row = buildRow(makeEvent({ sid: "sess-recv", event: "end", params: { interact_count: 5 } }));
    const after = Date.now();

    const ms = Date.parse(row.received_at);
    assert.ok(Number.isFinite(ms), "received_at must be a valid ISO string");
    assert.ok(ms >= before && ms <= after);
});

test("buildRow: event_hash is deterministic for identical input", () => {
    const input = makeEvent({
        sid: "sess-hash",
        event: "start",
        eventTime: "2026-06-04T10:00:00.000Z",
        params: { platform: "android", campaign: {} },
    });

    const row1 = buildRow(input);
    const row2 = buildRow(input);

    assert.equal(row1.event_hash, row2.event_hash);
});

test("buildRow: event_hash differs when session_id (sid) differs", () => {
    const base = { event: "start", eventTime: "2026-06-04T10:00:00.000Z", params: {} };

    const rowA = buildRow(makeEvent({ ...base, sid: "sess-a" }));
    const rowB = buildRow(makeEvent({ ...base, sid: "sess-b" }));

    assert.notEqual(rowA.event_hash, rowB.event_hash);
});

test("buildRow: event_hash differs when event_name (event) differs", () => {
    const base = { sid: "sess-hash2", eventTime: "2026-06-04T10:00:00.000Z", params: {} };

    const rowStart = buildRow(makeEvent({ ...base, event: "start" }));
    const rowEnd   = buildRow(makeEvent({ ...base, event: "end" }));

    assert.notEqual(rowStart.event_hash, rowEnd.event_hash);
});

test("buildRow: event_hash differs when event_time differs", () => {
    const base = { sid: "sess-hash3", event: "interaction", params: { name: "tap" } };

    const row1 = buildRow(makeEvent({ ...base, eventTime: "2026-06-04T10:00:00.000Z" }));
    const row2 = buildRow(makeEvent({ ...base, eventTime: "2026-06-04T10:01:00.000Z" }));

    assert.notEqual(row1.event_hash, row2.event_hash);
});

test("buildRow: event_params kept as object (not serialised pre-insert)", () => {
    const row = buildRow(makeEvent({
        sid: "sess-obj",
        event: "end",
        eventTime: "2026-06-04T10:03:00.000Z",
        params: { interact_count: 3 },
    }));

    assert.equal(typeof row.event_params, "object");
    assert.deepEqual(row.event_params, { interact_count: 3 });
});

// ─── formatRowForInsert ───────────────────────────────────────────────────────

test("formatRowForInsert stringifies object event_params for STRING columns", () => {
    const row = {
        session_id: "sess-fmt",
        event_params: { platform: "android", campaign: { network: "meta" } },
    };
    const fieldTypes = new Map([
        ["session_id", "STRING"],
        ["event_params", "STRING"],
    ]);
    const formatted = formatRowForInsert(row, fieldTypes);

    assert.equal(formatted.session_id, "sess-fmt");
    assert.equal(
        formatted.event_params,
        JSON.stringify({ platform: "android", campaign: { network: "meta" } })
    );
});

test("formatRowForInsert keeps JSON string as-is for JSON columns", () => {
    const row = { event_params: "{\"name\":\"tap\"}" };
    const fieldTypes = new Map([["event_params", "JSON"]]);
    const formatted = formatRowForInsert(row, fieldTypes);

    assert.equal(formatted.event_params, "{\"name\":\"tap\"}");
});

test("formatRowForInsert: native object for JSON column (jsonMode: native)", () => {
    const row = { event_params: { name: "tap" } };
    const fieldTypes = new Map([["event_params", "JSON"]]);
    const formatted = formatRowForInsert(row, fieldTypes, { jsonMode: "native" });

    assert.deepEqual(formatted.event_params, { name: "tap" });
});

// ─── normalizeValueForBigQueryType ────────────────────────────────────────────

test("normalizeValueForBigQueryType: plain string encoded as JSON for JSON column", () => {
    assert.equal(
        normalizeValueForBigQueryType("plain-text", "JSON"),
        "\"plain-text\""
    );
});

test("normalizeValueForBigQueryType: native object kept for JSON column (jsonMode: native)", () => {
    assert.deepEqual(
        normalizeValueForBigQueryType({ platform: "ios" }, "JSON", { jsonMode: "native" }),
        { platform: "ios" }
    );
});

// ─── validateFormattedRowForInsert ────────────────────────────────────────────

test("validateFormattedRowForInsert reports invalid field types with field names", () => {
    const row = {
        event_time: "not-a-timestamp",
        event_params: "{bad json",
        session_id: 12345,
    };
    const fieldTypes = new Map([
        ["event_time", "TIMESTAMP"],
        ["event_params", "JSON"],
        ["session_id", "STRING"],
    ]);

    const issues = validateFormattedRowForInsert(row, fieldTypes);

    assert.deepEqual(
        issues.map((item) => item.field),
        ["event_time", "event_params", "session_id"]
    );
    assert.equal(issues[0].type, "TIMESTAMP");
    assert.equal(issues[1].type, "JSON");
    assert.equal(issues[2].type, "STRING");
});

test("validateFormattedRowForInsert returns empty array for a valid row", () => {
    const row = {
        event_time: "2026-06-04T10:00:00.000Z",
        event_params: "{\"name\":\"tap\"}",
        session_id: "sess-valid",
    };
    const fieldTypes = new Map([
        ["event_time", "TIMESTAMP"],
        ["event_params", "JSON"],
        ["session_id", "STRING"],
    ]);

    const issues = validateFormattedRowForInsert(row, fieldTypes);
    assert.deepEqual(issues, []);
});
