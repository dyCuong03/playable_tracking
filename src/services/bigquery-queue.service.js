const {
    bigQueryBatchSize,
    bigQueryFlushIntervalMs,
    bigQueryMaxQueueSize,
    bigQueryRetryDelayMs,
    bigQueryErrorLogIntervalMs,
} = require("../config");
const {
    insertBatch,
    isBigQueryConfigured,
} = require("./bigquery.service");

const queue = [];

let timer;
let processing = false;
let pausedUntil = 0;
let nextItemId = 1;
let lastErrorLogAt = 0;
let lastDropLogAt = 0;

const shouldLogNow = (lastLogAt) => (Date.now() - lastLogAt) >= bigQueryErrorLogIntervalMs;

const logQueue = (level, type, message, details = {}) => {
    if (level === "error") {
        if (!shouldLogNow(lastErrorLogAt)) {
            return;
        }

        lastErrorLogAt = Date.now();
        console.error(JSON.stringify({ level, type, message, ...details }));
        return;
    }

    if (!shouldLogNow(lastDropLogAt)) {
        return;
    }

    lastDropLogAt = Date.now();
    console.warn(JSON.stringify({ level, type, message, ...details }));
};

const getFailedInsertIds = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors
        .map((entry) => entry && entry.row && entry.row.insertId)
        .filter(Boolean);
};

const removeItemsById = (itemIds) => {
    if (!itemIds.length) {
        return;
    }

    const idSet = new Set(itemIds);

    for (let index = queue.length - 1; index >= 0; index -= 1) {
        if (idSet.has(queue[index].id)) {
            queue.splice(index, 1);
        }
    }
};

const buildBatch = () => queue.slice(0, Math.max(1, bigQueryBatchSize));

const groupBatchByTable = (items) => items.reduce((acc, item) => {
    if (!acc.has(item.tableName)) {
        acc.set(item.tableName, []);
    }

    acc.get(item.tableName).push(item);
    return acc;
}, new Map());

const processTableItems = async (tableName, items) => {
    try {
        await insertBatch(
            tableName,
            items.map((item) => item.row)
        );

        return {
            status: "success",
            removeIds: items.map((item) => item.id),
        };
    } catch (error) {
        const failedInsertIds = new Set(getFailedInsertIds(error));

        if (failedInsertIds.size > 0) {
            logQueue("warn", "bigquery-queue-drop", "Dropping rows rejected by BigQuery", {
                tableName,
                count: items.length,
                reason: error.message,
            });

            return {
                status: "drop",
                removeIds: items.map((item) => item.id),
            };
        }

        logQueue("error", "bigquery-queue", "BigQuery insert failed; queue worker will retry", {
            tableName,
            count: items.length,
            reason: error.message,
            queueSize: queue.length,
        });

        return {
            status: "retry",
            removeIds: [],
        };
    }
};

const processQueue = async () => {
    if (processing || !queue.length || Date.now() < pausedUntil) {
        return;
    }

    if (!isBigQueryConfigured()) {
        return;
    }

    processing = true;

    try {
        const batch = buildBatch();
        const grouped = groupBatchByTable(batch);
        const removeIds = [];

        for (const [tableName, items] of grouped.entries()) {
            const result = await processTableItems(tableName, items);

            if (result.status === "retry") {
                pausedUntil = Date.now() + Math.max(1000, bigQueryRetryDelayMs);
                break;
            }

            removeIds.push(...result.removeIds);
        }

        removeItemsById(removeIds);
    } finally {
        processing = false;
    }
};

const enqueueEvent = async (queueItem) => {
    if (queue.length >= Math.max(1, bigQueryMaxQueueSize)) {
        logQueue("warn", "bigquery-queue-drop", "Dropping event because local queue is full", {
            queueSize: queue.length,
            maxQueueSize: bigQueryMaxQueueSize,
            eventHash: queueItem && queueItem.row ? queueItem.row.event_hash : null,
        });
        return false;
    }

    queue.push({
        id: nextItemId,
        tableName: queueItem.tableName,
        row: queueItem.row,
    });
    nextItemId += 1;

    if (queue.length >= Math.max(1, bigQueryBatchSize)) {
        void processQueue();
    }

    return true;
};

const initBigQueryQueue = () => {
    if (timer) {
        return;
    }

    timer = setInterval(() => {
        void processQueue();
    }, Math.max(250, bigQueryFlushIntervalMs));

    if (typeof timer.unref === "function") {
        timer.unref();
    }
};

module.exports = {
    enqueueEvent,
    initBigQueryQueue,
};
