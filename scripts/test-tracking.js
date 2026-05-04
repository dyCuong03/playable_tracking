#!/usr/bin/env node

const crypto = require("crypto");

const DEFAULTS = {
    baseUrl: "http://127.0.0.1:8080/p.gif",
    sessions: 10,
    snapshots: 5,
    intervalMs: 500,
    concurrency: 5,
    env: "test",
    playableId: "PA0006",
    packageName: "com.archer.cat.kitchen",
    platform: "web",
    includeHealth: true,
};

const parseNumber = (value, fallback) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
};

const parseBoolean = (value, fallback) => {
    if (value === undefined) {
        return fallback;
    }

    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) {
        return true;
    }

    if (["0", "false", "no", "n", "off"].includes(normalized)) {
        return false;
    }

    return fallback;
};

const parseArgs = () => {
    const args = process.argv.slice(2);
    const config = { ...DEFAULTS };

    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (!arg.startsWith("--")) {
            continue;
        }

        const [rawKey, inlineValue] = arg.slice(2).split("=", 2);
        const key = rawKey.trim();
        const nextValue = inlineValue !== undefined ? inlineValue : args[index + 1];
        const consumesNext = inlineValue === undefined;

        switch (key) {
        case "url":
            config.baseUrl = nextValue || config.baseUrl;
            break;
        case "sessions":
            config.sessions = Math.max(1, parseNumber(nextValue, config.sessions));
            break;
        case "snapshots":
            config.snapshots = Math.max(1, parseNumber(nextValue, config.snapshots));
            break;
        case "interval-ms":
            config.intervalMs = Math.max(0, parseNumber(nextValue, config.intervalMs));
            break;
        case "concurrency":
            config.concurrency = Math.max(1, parseNumber(nextValue, config.concurrency));
            break;
        case "env":
            config.env = nextValue || config.env;
            break;
        case "playable-id":
            config.playableId = nextValue || config.playableId;
            break;
        case "package-name":
            config.packageName = nextValue || config.packageName;
            break;
        case "platform":
            config.platform = nextValue || config.platform;
            break;
        case "health":
            config.includeHealth = parseBoolean(nextValue, config.includeHealth);
            break;
        default:
            break;
        }

        if (consumesNext) {
            index += 1;
        }
    }

    return config;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const sanitizeBaseUrl = (value) => String(value || "").replace(/\/+$/, "");

const buildCampaignPayload = (sessionIndex) => ({
    network: "test_network",
    campaign_id: `camp_${Math.ceil((sessionIndex + 1) / 10)}`,
    campaign_name: "tracking_load_test",
    adgroup_id: `ag_${(sessionIndex % 5) + 1}`,
    creative_id: `creative_${(sessionIndex % 3) + 1}`,
    click_id: `click_${sessionIndex + 1}`,
    country: "VN",
    language: "vi",
});

const createSessionId = (sessionIndex) => `test-${sessionIndex + 1}-${crypto.randomUUID()}`;

const buildSnapshotParams = (config, sessionId, sessionIndex, snapshotIndex, sessionStartMs) => {
    const now = Date.now();
    const elapsedSec = Math.max(0, Math.floor((now - sessionStartMs) / 1000));
    const hitCount = snapshotIndex + 1;
    const quadrantValue = Number((100 / 4).toFixed(2));

    return new URLSearchParams({
        e: "tracking_snapshot",
        pid: config.packageName,
        playable_id: config.playableId,
        sid: sessionId,
        ref: "https://playable-load-test.local",
        ts: String(now),
        r: Math.random().toString(),
        plf: config.platform,
        env: config.env,
        reason: snapshotIndex === 0 ? "onLoad" : "interval",
        duration_sec: String(elapsedSec),
        play_duration_sec: String(elapsedSec),
        input_count: String(hitCount),
        input_per_second: String(hitCount),
        first_input_captured: "1",
        first_input_time_sec: "0",
        total_hits: String(hitCount),
        top_left_pct: String(quadrantValue),
        top_right_pct: String(quadrantValue),
        bottom_left_pct: String(quadrantValue),
        bottom_right_pct: String(quadrantValue),
        camp: JSON.stringify(buildCampaignPayload(sessionIndex)),
    });
};

const sendPixel = async (config, params) => {
    const response = await fetch(`${sanitizeBaseUrl(config.baseUrl)}?${params.toString()}`, {
        method: "GET",
        cache: "no-store",
    });

    if (!response.ok) {
        throw new Error(`Tracking request failed with status ${response.status}`);
    }
};

const runSession = async (config, sessionIndex) => {
    const sessionId = createSessionId(sessionIndex);
    const sessionStartMs = Date.now();

    for (let snapshotIndex = 0; snapshotIndex < config.snapshots; snapshotIndex += 1) {
        const params = buildSnapshotParams(
            config,
            sessionId,
            sessionIndex,
            snapshotIndex,
            sessionStartMs
        );

        await sendPixel(config, params);

        if (snapshotIndex < config.snapshots - 1 && config.intervalMs > 0) {
            await sleep(config.intervalMs);
        }
    }

    return {
        sessionId,
        requestsSent: config.snapshots,
    };
};

const runWithConcurrency = async (items, limit, worker) => {
    const results = [];
    let cursor = 0;

    const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
        while (cursor < items.length) {
            const currentIndex = cursor;
            cursor += 1;
            results[currentIndex] = await worker(items[currentIndex], currentIndex);
        }
    });

    await Promise.all(runners);
    return results;
};

const fetchHealth = async (baseUrl) => {
    const trackingUrl = new URL(sanitizeBaseUrl(baseUrl));
    const healthUrl = `${trackingUrl.origin}/health`;
    const response = await fetch(healthUrl, { method: "GET", cache: "no-store" });

    if (!response.ok) {
        throw new Error(`Health check failed with status ${response.status}`);
    }

    return response.json();
};

const main = async () => {
    const config = parseArgs();
    const startedAt = Date.now();
    const sessionIndexes = Array.from({ length: config.sessions }, (_, index) => index);

    console.log(JSON.stringify({
        level: "info",
        type: "tracking-load-test",
        message: "Starting tracking load test",
        config,
    }));

    const sessionResults = await runWithConcurrency(
        sessionIndexes,
        config.concurrency,
        (_, sessionIndex) => runSession(config, sessionIndex)
    );

    const summary = {
        sessions: sessionResults.length,
        totalRequests: sessionResults.reduce((sum, item) => sum + item.requestsSent, 0),
        durationMs: Date.now() - startedAt,
        baseUrl: config.baseUrl,
    };

    console.log(JSON.stringify({
        level: "info",
        type: "tracking-load-test",
        message: "Tracking load test completed",
        summary,
    }));

    if (config.includeHealth) {
        const health = await fetchHealth(config.baseUrl);
        console.log(JSON.stringify({
            level: "info",
            type: "tracking-load-test",
            message: "Health snapshot after test",
            health,
        }));
    }
};

if (require.main === module) {
    main().catch((error) => {
        console.error(JSON.stringify({
            level: "error",
            type: "tracking-load-test",
            message: error.message,
        }));
        process.exit(1);
    });
}

module.exports = {
    parseArgs,
};
