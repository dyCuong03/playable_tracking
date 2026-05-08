const os = require("os");

const {
    requestQueueBridgePollMs,
    requestQueueBridgeBatchSize,
    requestQueueBridgeMaxFilesPerRun,
    requestQueuePublishTimeoutMs,
    requestQueueRetryDelayMs,
    bigQueryErrorLogIntervalMs,
} = require("../config");
const {
    enqueueEvent: enqueueDiskEvent,
    rotatePendingFiles,
    claimReadyFiles,
    parseQueueFile,
    completeProcessingFile,
    releaseProcessingFile,
    getQueueStats,
} = require("./bigquery-queue.service");
const {
    enqueueEvent: enqueueRedisEvent,
    enqueueEventBatch,
} = require("./redis-queue.service");

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

let lastDirectPublishErrorLogAt = 0;

const logDirectPublishFallback = (error) => {
    const now = Date.now();
    if ((now - lastDirectPublishErrorLogAt) < bigQueryErrorLogIntervalMs) {
        return;
    }

    lastDirectPublishErrorLogAt = now;
    console.warn(JSON.stringify({
        level: "warn",
        type: "request-publish-fallback",
        message: "Direct Redis publish failed; falling back to durable queue",
        reason: error.message,
        worker: workerName,
    }));
};

const persistRequest = async (item) => {
    try {
        await enqueueRedisEvent(item, {
            timeoutMs: Math.max(50, requestQueuePublishTimeoutMs),
            timeoutMessage: `Direct Redis publish timed out after ${requestQueuePublishTimeoutMs}ms`,
        });
        return "redis";
    } catch (error) {
        logDirectPublishFallback(error);
        await enqueueDiskEvent(item);
        return "disk";
    }
};

const flushItemsToRedis = async (items) => {
    const batchSize = Math.max(1, requestQueueBridgeBatchSize);

    for (let index = 0; index < items.length; index += batchSize) {
        const batch = items.slice(index, index + batchSize);
        await enqueueEventBatch(batch);
    }
};

const getApproximateItemCount = (stats) => {
    const bytes = (
        stats.pending.totalBytes +
        stats.ready.totalBytes +
        stats.processing.totalBytes
    );

    return Math.max(0, Math.floor(bytes / 450));
};

const runBridgeLoop = async () => {
    while (!stopping) {
        let claimedFiles = [];
        let processingFile = null;

        try {
            await rotatePendingFiles();

            claimedFiles = await claimReadyFiles(
                workerName,
                Math.max(1, requestQueueBridgeMaxFilesPerRun)
            );
            processingFile = claimedFiles.length > 0
                ? claimedFiles.map((item) => item.processingFile).join(",")
                : null;
            bridgeState.processingFile = processingFile;

            if (!processingFile) {
                await sleep(Math.max(25, requestQueueBridgePollMs));
                continue;
            }

            const fileBatches = await Promise.all(
                claimedFiles.map(async (claimedFile) => ({
                    claimedFile,
                    items: await parseQueueFile(claimedFile.processingFile),
                }))
            );
            const items = fileBatches.flatMap((entry) => entry.items);

            if (items.length > 0) {
                await flushItemsToRedis(items);
            }

            await Promise.all(
                fileBatches.map((entry) => completeProcessingFile(entry.claimedFile.processingFile))
            );
            bridgeState.lastSuccessAt = new Date().toISOString();
        } catch (error) {
            if (claimedFiles.length > 0) {
                await Promise.all(
                    claimedFiles.map((claimedFile) => releaseProcessingFile(
                        claimedFile.processingFile,
                        claimedFile.readyFile
                    ).catch(() => {}))
                );
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

const getDispatcherSummary = async () => {
    const durableQueue = await getQueueStats();

    return {
        bridge: {
            running: bridgeState.running,
            processingFile: bridgeState.processingFile,
            lastSuccessAt: bridgeState.lastSuccessAt,
            worker: workerName,
        },
        durableQueue,
        approxItems: getApproximateItemCount(durableQueue),
    };
};

module.exports = {
    persistRequest,
    startDispatcher,
    stopDispatcher,
    getDispatcherStats,
    getDispatcherSummary,
};
