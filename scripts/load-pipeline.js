"use strict";

// Load harness for the playable_tracking pixel pipeline.
//
// Drives the REAL services in-process (pixel.controller -> disk queue -> dispatcher
// bridge -> redis-queue -> bigquery worker) against an in-memory fake Redis and a mock
// BigQuery sink, then prints the LOAD_RESULTS reconciliation table:
//
//   requested / accepted HTTP / redis enqueued / worker consumed / dedup skipped /
//   inserted / failed / lost
//
// Usage:
//   node scripts/load-pipeline.js --events=5000 --mix=start
//   node scripts/load-pipeline.js --events=5000 --mix=mixed
//
// Zero external dependencies — no Redis server, no BigQuery credentials.

const path = require("path");
const {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
} = require(path.join(__dirname, "..", "tests", "helpers", "pipeline-harness"));
const { startEvent, interactionEvent, storeTriggerEvent, endEvent } = require(
    path.join(__dirname, "..", "tests", "helpers", "events")
);

const parseArgs = (argv) => {
    const args = { events: 5000, mix: "start" };
    for (const token of argv) {
        const match = /^--([^=]+)=(.*)$/.exec(token);
        if (!match) {
            continue;
        }
        const [, key, value] = match;
        if (key === "events") {
            args.events = Math.max(1, Number(value) || 0);
        } else if (key === "mix") {
            args.mix = value === "mixed" ? "mixed" : "start";
        }
    }
    return args;
};

// Build the per-request query list and the expected per-stage counts.
const buildRequests = (mix, totalEvents) => {
    const requests = [];
    const stageCounts = { start: 0, interaction: 0, store_trigger: 0, end: 0 };

    if (mix === "start") {
        for (let i = 0; i < totalEvents; i += 1) {
            requests.push(startEvent(`load-start-${i}`, { playableId: `pl-${i % 7}` }));
            stageCounts.start += 1;
        }
        return { requests, stageCounts };
    }

    // mixed: each session emits start + 2 interactions + 1 store_trigger + end (5 events).
    const perSession = 5;
    const sessions = Math.ceil(totalEvents / perSession);
    let emitted = 0;

    for (let s = 0; s < sessions && emitted < totalEvents; s += 1) {
        const sid = `load-mixed-${s}`;
        const playableId = `pl-${s % 11}`;
        const stages = [
            ["start", startEvent(sid, { playableId })],
            ["interaction", interactionEvent(sid, "tap_play", { playableId })],
            ["interaction", interactionEvent(sid, "tap_again", { playableId })],
            ["store_trigger", storeTriggerEvent(sid, "cta_click", { playableId })],
            ["end", endEvent(sid, 2, { playableId })],
        ];

        for (const [stage, query] of stages) {
            if (emitted >= totalEvents) {
                break;
            }
            requests.push(query);
            stageCounts[stage] += 1;
            emitted += 1;
        }
    }

    return { requests, stageCounts };
};

const pad = (label, width) => String(label).padEnd(width);
const padNum = (value, width) => String(value).padStart(width);

const printTable = (rows) => {
    const labelWidth = Math.max(...rows.map(([label]) => label.length)) + 2;
    const valueWidth = Math.max(...rows.map(([, value]) => String(value).length)) + 2;
    const line = `+${"-".repeat(labelWidth + 2)}+${"-".repeat(valueWidth + 2)}+`;

    console.log(line);
    console.log(`| ${pad("metric", labelWidth)} | ${pad("value", valueWidth)} |`);
    console.log(line);
    for (const [label, value] of rows) {
        console.log(`| ${pad(label, labelWidth)} | ${padNum(value, valueWidth)} |`);
    }
    console.log(line);
};

const run = async () => {
    const args = parseArgs(process.argv.slice(2));
    const { requests, stageCounts } = buildRequests(args.mix, args.events);

    const pipeline = createPipeline();
    const { services, redis, bigquery } = pipeline;

    let acceptedHttp = 0;
    let rejectedHttp = 0;
    let diskPersisted = 0;

    // Suppress the (very chatty) per-insert JSON logs while the load runs.
    const capture = createConsoleCapture();
    const started = Date.now();

    try {
        for (const query of requests) {
            const status = await runController(services.controller, query);
            if (status === 200) {
                acceptedHttp += 1;
            } else {
                rejectedHttp += 1;
            }
        }

        diskPersisted = await drainDiskToRedis(services);
        await runWorkerUntilDrained(services, redis.state, { timeoutMs: 120_000 });
    } finally {
        capture.restore();
    }

    const elapsedMs = Date.now() - started;

    const xaddMain = redis.state.xaddCount("pixel:events");
    const xaddRejected = redis.state.xaddCount("pixel:rejected");
    const dedupSkipped = redis.state.counters.dedupeSkip;
    const consumed = redis.state.counters.consumedByRead; // initial deliveries
    const inserted = bigquery.state.inserted.length;
    const failed = xaddRejected;
    const stillPending =
        redis.state.streamLength("pixel:events") +
        redis.state.pendingCount("pixel:events", "pixel-workers");
    const lost = acceptedHttp - dedupSkipped - inserted - failed - stillPending;

    await pipeline.cleanup();

    console.log("");
    console.log(`LOAD_RESULTS  (mix=${args.mix}, events=${args.events}, elapsed=${elapsedMs}ms)`);
    printTable([
        ["requested", requests.length],
        ["accepted HTTP (200)", acceptedHttp],
        ["rejected HTTP (4xx/5xx)", rejectedHttp],
        ["disk persisted (NDJSON)", diskPersisted],
        ["redis enqueued (XADD)", xaddMain],
        ["worker consumed", consumed],
        ["dedup skipped", dedupSkipped],
        ["inserted (BigQuery)", inserted],
        ["failed (rejected stream)", failed],
        ["still pending", stillPending],
        ["LOST", lost],
    ]);

    if (args.mix === "mixed") {
        console.log("");
        console.log("Per-stage requested:");
        printTable(Object.entries(stageCounts).map(([stage, count]) => [stage, count]));
    }

    console.log("");
    if (lost === 0 && inserted + failed + dedupSkipped === acceptedHttp) {
        console.log("RESULT: PASS — accepted == inserted + failed + dedup (zero loss).");
        process.exitCode = 0;
    } else {
        console.log("RESULT: FAIL — reconciliation mismatch (see LOST above).");
        process.exitCode = 1;
    }
};

run().catch((error) => {
    console.error("load-pipeline failed:", error);
    process.exit(1);
});
