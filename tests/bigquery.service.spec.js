const test = require("node:test");
const assert = require("node:assert/strict");

const {
    buildRow,
    formatRowForInsert,
    normalizeValueForBigQueryType,
    validateFormattedRowForInsert,
} = require("../src/services/bigquery.service");

test("buildRow keeps structured payloads as objects before insert formatting", () => {
    const row = buildRow({
        time: "2026-05-18T10:00:00.000Z",
        event: "install",
        pid: "com.demo.game",
        playableId: "playable-1",
        sid: "session-1",
        platform: "android",
        campaignRaw: { network: "meta", campaignId: 123 },
        params: { level: "1", source: "ad" },
        ip: "127.0.0.1",
        ua: "test-agent",
        ref: "https://example.com",
    });

    assert.deepEqual(row.campaign_raw, { network: "meta", campaignId: 123 });
    assert.deepEqual(row.event_params, { level: "1", source: "ad" });
});

test("formatRowForInsert stringifies structured payloads for STRING columns", () => {
    const row = {
        campaign_raw: { network: "meta", campaignId: 123 },
        event_params: { level: "1", source: "ad" },
    };
    const fieldTypes = new Map([
        ["campaign_raw", "STRING"],
        ["event_params", "STRING"],
    ]);

    const formatted = formatRowForInsert(row, fieldTypes);

    assert.equal(formatted.campaign_raw, "{\"network\":\"meta\",\"campaignId\":123}");
    assert.equal(formatted.event_params, "{\"level\":\"1\",\"source\":\"ad\"}");
});

test("formatRowForInsert parses legacy JSON strings for JSON columns", () => {
    const row = {
        campaign_raw: "{\"network\":\"meta\",\"campaignId\":123}",
        event_params: "{\"level\":\"1\",\"source\":\"ad\"}",
    };
    const fieldTypes = new Map([
        ["campaign_raw", "JSON"],
        ["event_params", "JSON"],
    ]);

    const formatted = formatRowForInsert(row, fieldTypes);

    assert.equal(formatted.campaign_raw, "{\"network\":\"meta\",\"campaignId\":123}");
    assert.equal(formatted.event_params, "{\"level\":\"1\",\"source\":\"ad\"}");
});

test("normalizeValueForBigQueryType encodes plain strings as valid JSON for JSON columns", () => {
    assert.equal(
        normalizeValueForBigQueryType("plain-text", "JSON"),
        "\"plain-text\""
    );
});

test("normalizeValueForBigQueryType can keep native objects for JSON columns", () => {
    assert.deepEqual(
        normalizeValueForBigQueryType({ network: "unknown" }, "JSON", { jsonMode: "native" }),
        { network: "unknown" }
    );
});

test("formatRowForInsert can keep native objects for JSON columns", () => {
    const row = {
        campaign_raw: { network: "unknown" },
        event_params: { reason: "debug" },
    };
    const fieldTypes = new Map([
        ["campaign_raw", "JSON"],
        ["event_params", "JSON"],
    ]);

    const formatted = formatRowForInsert(row, fieldTypes, { jsonMode: "native" });

    assert.deepEqual(formatted.campaign_raw, { network: "unknown" });
    assert.deepEqual(formatted.event_params, { reason: "debug" });
});

test("validateFormattedRowForInsert reports invalid field types with field names", () => {
    const row = {
        event_time: "not-a-timestamp",
        campaign_raw: "{bad json",
        event_params: "{\"ok\":true}",
        package_name: 123,
    };
    const fieldTypes = new Map([
        ["event_time", "TIMESTAMP"],
        ["campaign_raw", "JSON"],
        ["event_params", "JSON"],
        ["package_name", "STRING"],
    ]);

    const issues = validateFormattedRowForInsert(row, fieldTypes);

    assert.deepEqual(
        issues.map((item) => item.field),
        ["event_time", "campaign_raw", "package_name"]
    );
    assert.equal(issues[0].type, "TIMESTAMP");
    assert.equal(issues[1].type, "JSON");
    assert.equal(issues[2].type, "STRING");
});
