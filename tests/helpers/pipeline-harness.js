"use strict";

// Shared test harness: installs module mocks for `redis` and `@google-cloud/bigquery`
// via Module._load interception, sets a deterministic environment, freshly requires the
// REAL production services against the fakes, and exposes a console capture so tests can
// assert on the contract log events (which the backend emits to stdout/stderr).
//
// Everything below drives the actual pipeline modules — nothing about persistence,
// rotation, enqueue, consume, classification, retry or insert is reimplemented here.

const fs = require("fs");
const os = require("os");
const path = require("path");
const Module = require("module");

const { createFakeRedis } = require("./fake-redis");
const { createFakeBigQuery } = require("./fake-bigquery");

const SERVICE_MODULES = [
    "../../src/config",
    "../../src/config/env",
    "../../src/services/log.service",
    "../../src/services/bigquery.service",
    "../../src/services/bigquery-queue.service",
    "../../src/services/redis-queue.service",
    "../../src/services/request-dispatcher.service",
    "../../src/services/bigquery-worker.service",
    "../../src/services/event.service",
    "../../src/services/pixel.service",
    "../../src/controllers/pixel.controller",
    "../../src/routes/pixel.route",
    "../../src/routes/health.route",
    "../../src/app",
];

const clearServiceCache = () => {
    for (const relative of SERVICE_MODULES) {
        try {
            delete require.cache[require.resolve(relative)];
        } catch (_) {
            // not loaded yet
        }
    }
};

const installModuleMocks = (mocks) => {
    const originalLoad = Module._load;
    Module._load = function load(request, parent, isMain) {
        if (Object.prototype.hasOwnProperty.call(mocks, request)) {
            return mocks[request];
        }
        return originalLoad.call(this, request, parent, isMain);
    };
    return () => {
        Module._load = originalLoad;
    };
};

const createConsoleCapture = () => {
    const original = { log: console.log, error: console.error, warn: console.warn };
    const entries = [];

    const capture = (level) => (...args) => {
        const text = args.length === 1 ? args[0] : args.join(" ");
        if (typeof text === "string" && text.startsWith("{")) {
            try {
                entries.push({ level, ...JSON.parse(text) });
                return;
            } catch (_) {
                // not JSON; ignore for assertions
            }
        }
    };

    console.log = capture("log");
    console.error = capture("error");
    console.warn = capture("warn");

    return {
        entries,
        byType(type) {
            return entries.filter((entry) => entry.type === type);
        },
        restore() {
            console.log = original.log;
            console.error = original.error;
            console.warn = original.warn;
        },
    };
};

// Set the deterministic env BEFORE requiring config/services.
const applyEnv = (overrides = {}) => {
    const env = {
        REDIS_URL: "redis://fake:6379",
        REDIS_UNAVAILABLE_COOLDOWN_MS: "150",
        REDIS_CONNECT_TIMEOUT_MS: "500",
        REDIS_COMMAND_TIMEOUT_MS: "1000",
        REDIS_ERROR_LOG_INTERVAL_MS: "0",
        BIGQUERY_ENABLED: "true",
        BIGQUERY_DATASET: "fake_dataset",
        BIGQUERY_TABLE: "pixel_events_ver_2",
        BIGQUERY_BATCH_SIZE: "500",
        BIGQUERY_QUEUE_READ_BATCH: "5000",
        BIGQUERY_WORKER_POLL_MS: "250",
        BIGQUERY_WORKER_LEASE_MS: "1000",
        BIGQUERY_RETRY_DELAY_MS: "1000",
        BIGQUERY_ERROR_LOG_INTERVAL_MS: "0",
        BIGQUERY_MAX_RETRIES: "5",
        BIGQUERY_QUEUE_SHARDS: "4",
        REQUEST_QUEUE_ROTATE_MIN_BYTES: "1",
        REQUEST_QUEUE_ROTATE_MAX_AGE_MS: "0",
        REQUEST_QUEUE_BRIDGE_MAX_FILES_PER_RUN: "16",
        REQUEST_QUEUE_BRIDGE_BATCH_SIZE: "500",
        ...overrides,
    };
    const previous = {};
    for (const [key, value] of Object.entries(env)) {
        previous[key] = process.env[key];
        process.env[key] = String(value);
    }
    return () => {
        for (const [key, value] of Object.entries(previous)) {
            if (value === undefined) {
                delete process.env[key];
            } else {
                process.env[key] = value;
            }
        }
    };
};

// Create an isolated environment: temp dirs, fakes installed, services freshly loaded.
const createPipeline = (options = {}) => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-pipeline-"));
    const queueDir = path.join(tempRoot, "queue");
    const logDir = path.join(tempRoot, "logs");

    const restoreEnv = applyEnv({
        BIGQUERY_QUEUE_DIR: queueDir,
        PIXEL_LOG_DIR: logDir,
        ...(options.env || {}),
    });

    // realRedis: do not mock the `redis` module — connect to a real server at REDIS_URL
    // (used by the docker-gated integration spec). BigQuery is still mocked.
    const redis = options.realRedis ? null : createFakeRedis();
    const bigquery = createFakeBigQuery(options.bigquery || {});

    const mocks = { "@google-cloud/bigquery": bigquery.module };
    if (redis) {
        mocks.redis = redis.module;
    }
    const uninstallMocks = installModuleMocks(mocks);

    clearServiceCache();

    const services = {
        config: require("../../src/config"),
        log: require("../../src/services/log.service"),
        bigquery: require("../../src/services/bigquery.service"),
        diskQueue: require("../../src/services/bigquery-queue.service"),
        redisQueue: require("../../src/services/redis-queue.service"),
        dispatcher: require("../../src/services/request-dispatcher.service"),
        worker: require("../../src/services/bigquery-worker.service"),
        event: require("../../src/services/event.service"),
        controller: require("../../src/controllers/pixel.controller"),
        app: require("../../src/app"),
    };

    return {
        redis,
        bigquery,
        services,
        paths: { tempRoot, queueDir, logDir },
        async cleanup() {
            uninstallMocks();
            restoreEnv();
            clearServiceCache();
            await fs.promises.rm(tempRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 25 });
        },
    };
};

// Drive the disk -> Redis bridge using the REAL queue + redis-queue exports
// (the same calls request-dispatcher.runBridgeLoop makes), until the disk drains.
const drainDiskToRedis = async (services, maxRounds = 50) => {
    const { diskQueue, redisQueue, dispatcher } = services;
    let totalItems = 0;

    for (let round = 0; round < maxRounds; round += 1) {
        await diskQueue.rotatePendingFiles();
        const claimed = await diskQueue.claimReadyFiles("test-dispatcher", 16);

        if (!claimed.length) {
            const stats = await diskQueue.getQueueStats();
            const remaining = stats.pending.totalBytes + stats.ready.totalBytes;
            if (remaining === 0) {
                break;
            }
            continue;
        }

        for (const claim of claimed) {
            const items = await diskQueue.parseQueueFile(claim.processingFile);
            if (items.length) {
                // persistRequest is disk-only; the dispatcher pushes batches to Redis.
                await redisQueue.enqueueEventBatch(items);
                totalItems += items.length;
            }
            await diskQueue.completeProcessingFile(claim.processingFile);
        }
    }

    return totalItems;
};

// Build a mock req/res pair and run the REAL controller.
const runController = async (controller, query) => {
    const req = {
        query,
        ip: "127.0.0.1",
        originalUrl: `/p.gif?${new URLSearchParams(query).toString()}`,
        url: "/p.gif",
        path: "/p.gif",
        get() {
            return "";
        },
    };

    let statusCode = 200;
    const res = {
        status(code) {
            statusCode = code;
            return this;
        },
        set() {
            return this;
        },
        end() {
            return this;
        },
    };

    await controller.trackPixel(req, res);
    return statusCode;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Start the REAL worker (startWorker) against the fakes and poll until the main stream
// is fully drained (no live entries, no pending), then stop it. Returns nothing — assert
// against the fake BigQuery sink / redis counters afterwards.
const runWorkerUntilDrained = async (services, redisState, options = {}) => {
    const {
        stream = "pixel:events",
        group = "pixel-workers",
        timeoutMs = 20_000,
        quietRounds = 3,
    } = options;

    const workerPromise = services.worker.startWorker();
    const deadline = Date.now() + timeoutMs;
    let consecutiveEmpty = 0;

    try {
        while (Date.now() < deadline) {
            await sleep(100);
            const len = redisState.streamLength(stream);
            const pending = redisState.pendingCount(stream, group);

            if (len === 0 && pending === 0) {
                consecutiveEmpty += 1;
                if (consecutiveEmpty >= quietRounds) {
                    break;
                }
            } else {
                consecutiveEmpty = 0;
            }
        }
    } finally {
        services.worker.stopWorker();
        await workerPromise;
    }
};

module.exports = {
    createPipeline,
    createConsoleCapture,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
    installModuleMocks,
    sleep,
};
