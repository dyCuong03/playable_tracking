const os = require("os");

const {
    requestQueueBridgePollMs,
    requestQueueBridgeBatchSize,
    requestQueueBridgeMaxFilesPerRun,
    requestQueueRetryDelayMs,
    bigQueryErrorLogIntervalMs,
    pipelineHeartbeatIntervalMs,
} = require("../config");
const { recordHeartbeat, summarizeDiskQueue } = require("./pipeline-health.service");
const {
    enqueueEvent: enqueueDiskEvent,
    rotatePendingFiles,
    claimReadyFiles,
    parseQueueFile,
    completeProcessingFile,
    releaseProcessingFile,
    getQueueStats,
} = require("./bigquery-queue.service");
const { enqueueEventBatch } = require("./redis-queue.service");
const logService = require("./log.service");

const workerName = `${os.hostname()}-${process.pid}`.replace(/[^a-zA-Z0-9_-]/g, "-");

let started = false;
let stopping = false;
let bridgePromise = null;
let lastErrorLogAt = 0;
let bridgeState = {
    running: false,
    processingFile: null,
    lastSuccessAt: null,
    lastErrorAt: null,
};
let lastDispatcherHealthAt = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const shouldLogNow = () => (Date.now() - lastErrorLogAt) >= bigQueryErrorLogIntervalMs;

const logDispatcher = (level, type, message, details = {}, silent = false) => {
    const entry = {
        ts: new Date().toISOString(),
        level,
        type,
        message,
        worker: workerName,
        ...details,
    };

    if (!silent) {
        if (level === "error") {
            console.error(JSON.stringify(entry));
        } else if (level === "warn") {
            console.warn(JSON.stringify(entry));
        } else {
            console.log(JSON.stringify(entry));
        }
    }

    logService.writeDaily("dispatcher", entry, true);
};

const logBridgeError = (message, error, details = {}) => {
    if (!shouldLogNow()) {
        return;
    }

    lastErrorLogAt = Date.now();
    logDispatcher("error", "request-dispatcher", message, {
        reason: error.message,
        ...details,
    });
};

const persistRequest = async (item) => enqueueDiskEvent(item);

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

// Publish the dispatcher's cross-process liveness to Redis + structured logs. Throttled to
// the heartbeat interval on idle loops, but forced after a real dispatch so lastSuccessAt is
// always fresh. Best-effort: a health write must never break the dispatch loop.
const writeDispatcherHealth = async (force, itemsDispatched) => {
    const now = Date.now();
    if (!force && (now - lastDispatcherHealthAt) < Math.max(1000, pipelineHeartbeatIntervalMs)) {
        return;
    }

    lastDispatcherHealthAt = now;

    const diskStats = await getQueueStats().catch(() => null);
    const disk = summarizeDiskQueue(diskStats);

    await recordHeartbeat("dispatcher", {
        running: bridgeState.running,
        lastSuccessAt: bridgeState.lastSuccessAt,
        lastErrorAt: bridgeState.lastErrorAt,
        dispatcher_id: workerName,
        diskBacklog: disk.total,
    });

    logDispatcher("info", "dispatcher-status", "Dispatcher heartbeat", {
        running: bridgeState.running,
        lastSuccessAt: bridgeState.lastSuccessAt,
        lastErrorAt: bridgeState.lastErrorAt,
        dispatcher_id: workerName,
    });

    logDispatcher("info", "dispatcher-backlog-summary", "Disk backlog summary", {
        pending: disk.pending,
        ready: disk.ready,
        processing: disk.processing,
        total: disk.total,
        itemsDispatched: Number(itemsDispatched || 0),
        dispatcher_id: workerName,
    });
};

const runBridgeLoop = async () => {
    while (!stopping) {
        let claimedFiles = [];
        let processingFile = null;

        await writeDispatcherHealth(false, 0);

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
            logDispatcher("info", "request-dispatcher-batch", "Dispatched durable queue files to Redis", {
                processingFile,
                fileCount: claimedFiles.length,
                itemCount: items.length,
            });
            await writeDispatcherHealth(true, items.length);
        } catch (error) {
            bridgeState.lastErrorAt = new Date().toISOString();

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
    logDispatcher("info", "request-dispatcher-start", "Request dispatcher started");

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
