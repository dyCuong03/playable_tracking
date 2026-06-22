const os = require("os");

const {
    bigQueryBatchSize,
    bigQueryQueueReadBatch,
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
const logService = require("./log.service");

const workerName = (bigQueryWorkerName || `${os.hostname()}-${process.pid}`)
    .replace(/[^a-zA-Z0-9_-]/g, "-");

let stopping = false;
let lastErrorLogAt = 0;
let lastWarnLogAt = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const shouldLogNow = (lastLogAt) => (Date.now() - lastLogAt) >= bigQueryErrorLogIntervalMs;

const logWorker = (level, type, message, details = {}) => {
    const entry = {
        ts: new Date().toISOString(),
        level,
        type,
        message,
        worker: workerName,
        ...details,
    };

    if (level === "error") {
        if (!shouldLogNow(lastErrorLogAt)) {
            return;
        }

        lastErrorLogAt = Date.now();
        console.error(JSON.stringify(entry));
        logService.writeDaily("worker", entry, true);
        return;
    }

    if (level === "warn") {
        if (!shouldLogNow(lastWarnLogAt)) {
            return;
        }

        lastWarnLogAt = Date.now();
        console.warn(JSON.stringify(entry));
        logService.writeDaily("worker", entry, true);
        return;
    }

    console.log(JSON.stringify(entry));
    logService.writeDaily("worker", entry, true);
};

// Contract events (redis-consume-*, bigquery-insert-*, worker-batch-summary). Unlike
// logWorker(), these are NEVER rate-limited or suppressed — every consume/insert outcome
// must be visible so a single event is traceable end-to-end and zero loss is provable.
const emitWorkerEvent = (type, message, details = {}) => {
    const entry = {
        ts: new Date().toISOString(),
        level: type.endsWith("-failed") ? "error" : "info",
        type,
        message,
        worker_id: workerName,
        queue_backend: "redis-stream",
        ...details,
    };

    if (entry.level === "error") {
        console.error(JSON.stringify(entry));
    } else {
        console.log(JSON.stringify(entry));
    }

    logService.writeDaily("worker", entry, true);
};

const collectEventHashes = (items) => items
    .map((item) => (item && item.row && item.row.event_hash) || null)
    .filter(Boolean);

const summarizeItems = (items) => items.map((item) => {
    const row = item && item.row ? item.row : {};
    const data = item && item.urlData
        ? item.urlData
        : { event_params: row.event_params || {} };

    return {
        message_id: item && item.messageId ? item.messageId : null,
        attempts: Number(item && item.attempts ? item.attempts : 0),
        tableName: item && item.tableName ? item.tableName : null,
        event_hash: row.event_hash || null,
        session_id: row.session_id || null,
        event_name: row.event_name || null,
        event_time: row.event_time || null,
        received_at: row.received_at || null,
        data,
    };
});

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

const getErrorDetails = (error) => {
    if (!error || !error.details) {
        return null;
    }

    return error.details;
};

const getErrorRowDiagnostics = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors.map((rowError) => ({
        insertId: rowError && rowError.row ? rowError.row.insertId || null : null,
        errors: Array.isArray(rowError && rowError.errors)
            ? rowError.errors.map((entry) => ({
                reason: entry && entry.reason ? entry.reason : null,
                message: entry && entry.message ? entry.message : null,
                location: entry && entry.location ? entry.location : null,
            }))
            : [],
    }));
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
    urlData: item.urlData || null,
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
            urlData: rejected.item.urlData || null,
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
    const summary = { inserted: 0, retried: 0, dropped: 0 };

    if (!items.length) {
        return summary;
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
            summary.inserted += chunk.items.length;
            logWorker("info", "bigquery-worker-insert", "Inserted Redis chunk to BigQuery", {
                tableName: chunk.tableName,
                count: chunk.items.length,
                messageIds: chunkMessageIds,
                data: summarizeItems(chunk.items),
            });
            emitWorkerEvent("bigquery-insert-success", "Inserted Redis chunk to BigQuery", {
                tableName: chunk.tableName,
                count: chunk.items.length,
                event_hashes: collectEventHashes(chunk.items),
            });
        } catch (error) {
            emitWorkerEvent("bigquery-insert-failed", "BigQuery insert failed for Redis chunk", {
                tableName: chunk.tableName,
                count: chunk.items.length,
                message: error.message,
                event_hashes: collectEventHashes(chunk.items),
            });
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

                summary.retried += retryItems.length;
                summary.dropped += rejectedItems.length;

                if (retryItems.length > 0) {
                    await requeueItems(retryItems);
                    logWorker("warn", "bigquery-worker-retry", "Re-queued retryable rows after partial BigQuery failure", {
                        tableName: chunk.tableName,
                        count: retryItems.length,
                        reason: error.message,
                        details: getErrorDetails(error),
                        rowErrors: getErrorRowDiagnostics(error),
                        data: summarizeItems(retryItems),
                    });
                    await sleep(Math.max(1000, bigQueryRetryDelayMs));
                }

                if (rejectedItems.length > 0) {
                    logWorker("warn", "bigquery-worker-drop", "Dropped rows rejected by BigQuery", {
                        tableName: chunk.tableName,
                        count: rejectedItems.length,
                        reason: error.message,
                        details: getErrorDetails(error),
                        rowErrors: getErrorRowDiagnostics(error),
                        data: summarizeItems(rejectedItems.map((rejected) => rejected.item)),
                    });
                }

                await logRejectedItems(rejectedItems, chunk.tableName);
                await acknowledgeMessages(chunkMessageIds);

                continue;
            }

            const retrySplit = splitRetryableItems(chunk.items, error.message);

            summary.retried += retrySplit.retryItems.length;
            summary.dropped += retrySplit.rejected.length;

            if (retrySplit.retryItems.length > 0) {
                await requeueItems(retrySplit.retryItems);
            }

            await logRejectedItems(retrySplit.rejected, chunk.tableName);
            await acknowledgeMessages(chunkMessageIds);

            logWorker("error", "bigquery-worker", "BigQuery insert failed; re-queued stream chunk", {
                tableName: chunk.tableName,
                count: chunk.items.length,
                reason: error.message,
                stack: error && error.stack ? error.stack : null,
                errorName: error && error.name ? error.name : null,
                errorCode: error && error.code ? error.code : null,
                details: getErrorDetails(error),
                rowErrors: getErrorRowDiagnostics(error),
                sampleRow: chunk.items[0] && chunk.items[0].row ? chunk.items[0].row : null,
                data: summarizeItems(chunk.items),
            });

            await sleep(Math.max(1000, bigQueryRetryDelayMs));
        }
    }

    return summary;
};

const startWorker = async () => {
    if (!isBigQueryConfigured()) {
        throw new Error("BigQuery is not fully configured");
    }

    logWorker("info", "bigquery-worker", "BigQuery Redis worker started");

    while (!stopping) {
        try {
            await ensureQueueReady();

            const readBatchSize = Math.max(1, bigQueryQueueReadBatch);

            const reclaimedItems = await claimPendingBatch(
                workerName,
                readBatchSize,
                Math.max(1000, bigQueryWorkerLeaseMs)
            );

            if (reclaimedItems.length > 0) {
                emitWorkerEvent("redis-consume-success", "Reclaimed pending Redis stream entries", {
                    source: "xautoclaim",
                    count: reclaimedItems.length,
                    event_hashes: collectEventHashes(reclaimedItems),
                });
                const reclaimSummary = await processMessages(reclaimedItems);
                emitWorkerEvent("worker-batch-summary", "Worker batch complete", {
                    source: "xautoclaim",
                    consumed: reclaimedItems.length,
                    inserted: reclaimSummary.inserted,
                    retried: reclaimSummary.retried,
                    dropped: reclaimSummary.dropped,
                    dedup: 0,
                });
                continue;
            }

            let items;
            try {
                items = await readQueueBatch(
                    workerName,
                    readBatchSize,
                    Math.max(250, bigQueryWorkerPollMs)
                );
            } catch (error) {
                emitWorkerEvent("redis-consume-failed", "Failed to read from Redis stream", {
                    source: "xreadgroup",
                    message: error.message,
                });
                throw error;
            }

            if (!items.length) {
                continue;
            }

            emitWorkerEvent("redis-consume-success", "Consumed Redis stream entries", {
                source: "xreadgroup",
                count: items.length,
                event_hashes: collectEventHashes(items),
            });

            const batchSummary = await processMessages(items);
            emitWorkerEvent("worker-batch-summary", "Worker batch complete", {
                source: "xreadgroup",
                consumed: items.length,
                inserted: batchSummary.inserted,
                retried: batchSummary.retried,
                dropped: batchSummary.dropped,
                dedup: 0,
            });
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
