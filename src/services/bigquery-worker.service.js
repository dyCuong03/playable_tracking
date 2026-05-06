const os = require("os");

const {
    bigQueryBatchSize,
    bigQueryMaxRetries,
    bigQueryRetryDelayMs,
    bigQueryErrorLogIntervalMs,
    bigQueryWorkerPollMs,
    bigQueryWorkerLeaseMs,
    bigQueryWorkerName,
} = require("../config");
const {
    insertBatch,
    isBigQueryConfigured,
    logInsertError,
} = require("./bigquery.service");
const {
    ensureQueueReady,
    readQueueBatch,
    claimPendingBatch,
    acknowledgeMessages,
    requeueItems,
    rejectItems,
} = require("./redis-queue.service");

const workerName = (bigQueryWorkerName || `${os.hostname()}-${process.pid}`)
    .replace(/[^a-zA-Z0-9_-]/g, "-");

let stopping = false;
let lastErrorLogAt = 0;
let lastWarnLogAt = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const shouldLogNow = (lastLogAt) => (Date.now() - lastLogAt) >= bigQueryErrorLogIntervalMs;

const logWorker = (level, type, message, details = {}) => {
    if (level === "error") {
        if (!shouldLogNow(lastErrorLogAt)) {
            return;
        }

        lastErrorLogAt = Date.now();
        console.error(JSON.stringify({ level, type, message, worker: workerName, ...details }));
        return;
    }

    if (level === "warn") {
        if (!shouldLogNow(lastWarnLogAt)) {
            return;
        }

        lastWarnLogAt = Date.now();
        console.warn(JSON.stringify({ level, type, message, worker: workerName, ...details }));
        return;
    }

    console.log(JSON.stringify({ level, type, message, worker: workerName, ...details }));
};

const getFailedInsertIds = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors
        .map((entry) => entry && entry.row && entry.row.insertId)
        .filter(Boolean);
};

const getChunkItemByInsertId = (chunk) => chunk.items.reduce((acc, item) => {
    if (item && item.row && item.row.event_hash) {
        acc.set(item.row.event_hash, item);
    }

    return acc;
}, new Map());

const getRowErrors = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors.filter(Boolean);
};

const getRowErrorReasons = (rowError) => {
    if (!Array.isArray(rowError && rowError.errors)) {
        return [];
    }

    return rowError.errors
        .map((entry) => String((entry && entry.reason) || "").trim())
        .filter(Boolean);
};

const isRetryableReason = (reason) => [
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "quotaExceeded",
    "timeout",
    "stopped",
].includes(reason);

const isRetryableRowError = (rowError) => {
    const reasons = getRowErrorReasons(rowError);

    if (!reasons.length) {
        return false;
    }

    return reasons.every(isRetryableReason);
};

const buildRowErrorMessage = (error, rowError) => {
    const reasons = getRowErrorReasons(rowError);
    if (!reasons.length) {
        return error.message;
    }

    return `${error.message} [${reasons.join(",")}]`;
};

const buildChunks = (items) => {
    const grouped = items.reduce((acc, item) => {
        if (!acc.has(item.tableName)) {
            acc.set(item.tableName, []);
        }

        acc.get(item.tableName).push(item);
        return acc;
    }, new Map());

    const chunks = [];
    const chunkSize = Math.max(1, bigQueryBatchSize);

    for (const [tableName, tableItems] of grouped.entries()) {
        for (let index = 0; index < tableItems.length; index += chunkSize) {
            chunks.push({
                tableName,
                items: tableItems.slice(index, index + chunkSize),
            });
        }
    }

    return chunks;
};

const buildRetriedItem = (item, errorMessage) => ({
    tableName: item.tableName,
    row: item.row,
    attempts: Number(item.attempts || 0) + 1,
    lastError: errorMessage,
    lastAttemptAt: new Date().toISOString(),
});

const splitRetryableItems = (items, errorMessage) => items.reduce((acc, item) => {
    const attempts = Number(item.attempts || 0);

    if ((attempts + 1) > Math.max(1, bigQueryMaxRetries)) {
        acc.rejected.push({
            item,
            message: `${errorMessage} [max_retries_exceeded]`,
        });
        return acc;
    }

    acc.retryItems.push(buildRetriedItem(item, errorMessage));
    return acc;
}, { retryItems: [], rejected: [] });

const logRejectedItems = async (rejectedItems, tableName) => {
    if (!rejectedItems.length) {
        return;
    }

    await rejectItems(
        rejectedItems.map((rejected) => ({
            tableName,
            row: rejected.item.row,
            rejectedAt: new Date().toISOString(),
            attempts: Number(rejected.item.attempts || 0),
            error: rejected.message,
        }))
    );

    for (const rejected of rejectedItems) {
        logInsertError(
            { message: rejected.message, code: null },
            rejected.item.row,
            null,
            null,
            tableName
        );
    }
};

const processMessages = async (items) => {
    if (!items.length) {
        return;
    }

    const chunks = buildChunks(items);

    for (let index = 0; index < chunks.length; index += 1) {
        const chunk = chunks[index];
        const chunkMessageIds = chunk.items.map((item) => item.messageId);

        try {
            await insertBatch(
                chunk.tableName,
                chunk.items.map((item) => item.row)
            );
            await acknowledgeMessages(chunkMessageIds);
        } catch (error) {
            const failedInsertIds = new Set(getFailedInsertIds(error));

            if (failedInsertIds.size > 0) {
                const chunkItemsByInsertId = getChunkItemByInsertId(chunk);
                const rowErrors = getRowErrors(error);
                const rowErrorByInsertId = rowErrors.reduce((acc, rowError) => {
                    const insertId = rowError && rowError.row && rowError.row.insertId;
                    if (insertId) {
                        acc.set(insertId, rowError);
                    }

                    return acc;
                }, new Map());
                const retryItems = [];
                const rejectedItems = [];

                for (const item of chunk.items) {
                    const insertId = item && item.row && item.row.event_hash;

                    if (!insertId || !failedInsertIds.has(insertId)) {
                        continue;
                    }

                    const rowError = rowErrorByInsertId.get(insertId);

                    if (rowError && isRetryableRowError(rowError)) {
                        const retrySplit = splitRetryableItems(
                            [chunkItemsByInsertId.get(insertId)],
                            buildRowErrorMessage(error, rowError)
                        );
                        retryItems.push(...retrySplit.retryItems);
                        rejectedItems.push(...retrySplit.rejected);
                        continue;
                    }

                    rejectedItems.push({
                        item: chunkItemsByInsertId.get(insertId),
                        message: buildRowErrorMessage(error, rowError),
                    });
                }

                if (retryItems.length > 0) {
                    await requeueItems(retryItems);
                    logWorker("warn", "bigquery-worker-retry", "Re-queued retryable rows after partial BigQuery failure", {
                        tableName: chunk.tableName,
                        count: retryItems.length,
                        reason: error.message,
                    });
                    await sleep(Math.max(1000, bigQueryRetryDelayMs));
                }

                if (rejectedItems.length > 0) {
                    logWorker("warn", "bigquery-worker-drop", "Dropped rows rejected by BigQuery", {
                        tableName: chunk.tableName,
                        count: rejectedItems.length,
                        reason: error.message,
                    });
                }

                await logRejectedItems(rejectedItems, chunk.tableName);
                await acknowledgeMessages(chunkMessageIds);

                continue;
            }

            const retrySplit = splitRetryableItems(chunk.items, error.message);

            if (retrySplit.retryItems.length > 0) {
                await requeueItems(retrySplit.retryItems);
            }

            await logRejectedItems(retrySplit.rejected, chunk.tableName);
            await acknowledgeMessages(chunkMessageIds);

            logWorker("error", "bigquery-worker", "BigQuery insert failed; re-queued stream chunk", {
                tableName: chunk.tableName,
                count: chunk.items.length,
                reason: error.message,
            });

            await sleep(Math.max(1000, bigQueryRetryDelayMs));
        }
    }
};

const startWorker = async () => {
    if (!isBigQueryConfigured()) {
        throw new Error("BigQuery is not fully configured");
    }

    await ensureQueueReady();
    logWorker("info", "bigquery-worker", "BigQuery Redis worker started");

    while (!stopping) {
        try {
            const reclaimedItems = await claimPendingBatch(
                workerName,
                Math.max(1, bigQueryBatchSize),
                Math.max(1000, bigQueryWorkerLeaseMs)
            );

            if (reclaimedItems.length > 0) {
                await processMessages(reclaimedItems);
                continue;
            }

            const items = await readQueueBatch(
                workerName,
                Math.max(1, bigQueryBatchSize),
                Math.max(250, bigQueryWorkerPollMs)
            );

            if (!items.length) {
                continue;
            }

            await processMessages(items);
        } catch (error) {
            logWorker("error", "bigquery-worker", "Worker loop failed", {
                reason: error.message,
            });
            await sleep(Math.max(1000, bigQueryRetryDelayMs));
        }
    }
};

const stopWorker = () => {
    stopping = true;
};

module.exports = {
    startWorker,
    stopWorker,
};
