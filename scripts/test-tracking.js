#!/usr/bin/env node

const crypto = require("crypto");

const DEFAULTS = {
    baseUrl: "http://127.0.0.1:8080/p.gif",
    sessions: 10,
    interactions: 3,
    intervalMs: 500,
    concurrency: 5,
    env: "test",
    platform: "web",
    includeHealth: true,
};

const INTERACTION_NAMES = ["tap", "swipe", "tap", "drag", "tap"];

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
        case "baseUrl":
            config.baseUrl = nextValue || config.baseUrl;
            break;
        case "sessions":
            config.sessions = Math.max(1, parseNumber(nextValue, config.sessions));
            break;
        case "interactions":
            config.interactions = Math.max(0, parseNumber(nextValue, config.interactions));
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

const nowIso = () => new Date().toISOString();

const createSessionId = () => `test-${crypto.randomUUID()}`;

const buildCampaignPayload = (sessionIndex) => ({
    network: "test_network",
    campaign_id: `camp_${Math.ceil((sessionIndex + 1) / 10)}`,
    campaign_name: "tracking_load_test",
    adgroup_id: `ag_${(sessionIndex % 5) + 1}`,
    creative_id: `creative_${(sessionIndex % 3) + 1}`,
    click_id: `click_${sessionIndex + 1}`,
    country: "VN",
});

const buildStartParams = (config, sessionId, sessionIndex) => new URLSearchParams({
    e: "start",
    sid: sessionId,
    event_time: nowIso(),
    event_params: JSON.stringify({
        platform: config.platform,
        campaign: buildCampaignPayload(sessionIndex),
    }),
    env: config.env,
});

const buildInteractionParams = (config, sessionId, interactionIndex) => new URLSearchParams({
    e: "interaction",
    sid: sessionId,
    event_time: nowIso(),
    event_params: JSON.stringify({
        name: INTERACTION_NAMES[interactionIndex % INTERACTION_NAMES.length],
    }),
    env: config.env,
});

const buildStoreTriggerParams = (config, sessionId) => new URLSearchParams({
    e: "store_trigger",
    sid: sessionId,
    event_time: nowIso(),
    event_params: JSON.stringify({
        name: "tap_cta",
    }),
    env: config.env,
});

const buildEndParams = (config, sessionId, interactCount) => new URLSearchParams({
    e: "end",
    sid: sessionId,
    event_time: nowIso(),
    event_params: JSON.stringify({
        interact_count: interactCount,
    }),
    env: config.env,
});

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
    const sessionId = createSessionId();
    let requestsSent = 0;

    // start
    await sendPixel(config, buildStartParams(config, sessionId, sessionIndex));
    requestsSent += 1;

    if (config.intervalMs > 0) {
        await sleep(config.intervalMs);
    }

    // interactions
    for (let index = 0; index < config.interactions; index += 1) {
        await sendPixel(config, buildInteractionParams(config, sessionId, index));
        requestsSent += 1;

        if (index < config.interactions - 1 && config.intervalMs > 0) {
            await sleep(config.intervalMs);
        }
    }

    // store_trigger: ~30% of sessions trigger the CTA
    const triggersStore = (sessionIndex % 10) < 3;
    if (triggersStore) {
        if (config.intervalMs > 0) {
            await sleep(config.intervalMs);
        }

        await sendPixel(config, buildStoreTriggerParams(config, sessionId));
        requestsSent += 1;
    }

    // end
    if (config.intervalMs > 0) {
        await sleep(config.intervalMs);
    }

    await sendPixel(config, buildEndParams(config, sessionId, config.interactions));
    requestsSent += 1;

    return {
        sessionId,
        requestsSent,
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
