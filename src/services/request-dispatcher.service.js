const os = require("os");

const {
    requestQueueBridgePollMs,
    requestQueueBridgeBatchSize,
    requestQueueRetryDelayMs,
    bigQueryErrorLogIntervalMs,
} = require("../config");
const {
    enqueueEvent,
    rotatePendingFiles,
    claimReadyFile,
    parseQueueFile,
    completeProcessingFile,
    releaseProcessingFile,
    getQueueStats,
} = require("./bigquery-queue.service");
const { enqueueEventBatch } = require("./redis-queue.service");

const workerName = `${os.hostname()}-${process.pid}`.replace(/[^a-zA-Z0-9_-]/g, "-");

let started = false;
let stopping = false;
let bridgePromise = null;
let lastErrorLogAt = 0;
let bridgeState = {
    running: false,
    processingFile: null,
    lastSuccessAt: null,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const shouldLogNow = () => (Date.now() - lastErrorLogAt) >= bigQueryErrorLogIntervalMs;

const logBridgeError = (message, error, details = {}) => {
    if (!shouldLogNow()) {
        return;
    }

    lastErrorLogAt = Date.now();
    console.error(JSON.stringify({
        level: "error",
        type: "request-dispatcher",
        message,
        reason: error.message,
        worker: workerName,
        ...details,
    }));
};

const persistRequest = async (item) => enqueueEvent(item);

const flushItemsToRedis = async (items) => {
    const batchSize = Math.max(1, requestQueueBridgeBatchSize);

    for (let index = 0; index < items.length; index += batchSize) {
        const batch = items.slice(index, index + batchSize);
        await enqueueEventBatch(batch);
    }
};

const runBridgeLoop = async () => {
    while (!stopping) {
        let processingFile = null;

        try {
            await rotatePendingFiles();

            processingFile = await claimReadyFile(workerName);
            bridgeState.processingFile = processingFile;

            if (!processingFile) {
                await sleep(Math.max(25, requestQueueBridgePollMs));
                continue;
            }

            const items = await parseQueueFile(processingFile);

            if (items.length > 0) {
                await flushItemsToRedis(items);
            }

            await completeProcessingFile(processingFile);
            bridgeState.lastSuccessAt = new Date().toISOString();
        } catch (error) {
            if (processingFile) {
                await releaseProcessingFile(processingFile).catch(() => {});
            }

            logBridgeError("Failed to dispatch durable queue to Redis", error, {
                processingFile,
            });
            await sleep(Math.max(250, requestQueueRetryDelayMs));
        } finally {
            bridgeState.processingFile = null;
        }
    }
};

const startDispatcher = () => {
    if (started) {
        return bridgePromise;
    }

    started = true;
    stopping = false;
    bridgeState.running = true;

    bridgePromise = runBridgeLoop().finally(() => {
        bridgeState.running = false;
        started = false;
    });

    return bridgePromise;
};

const stopDispatcher = () => {
    stopping = true;
};

const getDispatcherStats = async () => ({
    bridge: {
        running: bridgeState.running,
        processingFile: bridgeState.processingFile,
        lastSuccessAt: bridgeState.lastSuccessAt,
        worker: workerName,
    },
    durableQueue: await getQueueStats(),
});

module.exports = {
    persistRequest,
    startDispatcher,
    stopDispatcher,
    getDispatcherStats,
};
