const fs = require("fs");
const os = require("os");
const path = require("path");

const {
    bigQueryBatchSize,
    bigQueryRetryDelayMs,
    bigQueryErrorLogIntervalMs,
    bigQueryQueueShards,
    bigQueryWorkerPollMs,
    bigQueryWorkerLeaseMs,
    bigQueryWorkerName,
} = require("../config");
const {
    insertBatch,
    isBigQueryConfigured,
} = require("./bigquery.service");
const {
    ensureQueueDir,
    serializeItem,
    getPendingFilePath,
    getReadyFilePath,
    getProcessingFilePath,
    READY_DIR,
    PROCESSING_DIR,
} = require("./bigquery-queue.service");

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

const readQueueFile = async (filePath) => {
    const content = await fs.promises.readFile(filePath, "utf8");

    if (!content.trim()) {
        return [];
    }

    return content
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line));
};

const touchFile = async (filePath) => {
    const now = new Date();
    await fs.promises.utimes(filePath, now, now).catch(() => {});
};

const writeQueueFile = async (filePath, items) => {
    const payload = items.map(serializeItem).join("");
    await fs.promises.writeFile(filePath, payload, "utf8");
};

const rotatePendingFile = async () => {
    for (let shard = 0; shard < Math.max(1, bigQueryQueueShards); shard += 1) {
        const pendingFile = getPendingFilePath(shard);

        try {
            const stat = await fs.promises.stat(pendingFile);

            if (!stat.size) {
                continue;
            }

            const readyFile = getReadyFilePath(`${Date.now()}-${shard}-${Math.random().toString(16).slice(2, 8)}`);
            await fs.promises.rename(pendingFile, readyFile);
            return readyFile;
        } catch (error) {
            if (error.code === "ENOENT") {
                continue;
            }
        }
    }

    return null;
};

const recoverExpiredProcessingFiles = async () => {
    const entries = await fs.promises.readdir(PROCESSING_DIR);
    const now = Date.now();

    for (const entry of entries) {
        if (!entry.endsWith(".ndjson")) {
            continue;
        }

        const filePath = path.join(PROCESSING_DIR, entry);
        const stat = await fs.promises.stat(filePath).catch(() => null);

        if (!stat) {
            continue;
        }

        if ((now - stat.mtimeMs) < Math.max(1000, bigQueryWorkerLeaseMs)) {
            continue;
        }

        const readyFile = getReadyFilePath(`recovered-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`);

        try {
            await fs.promises.rename(filePath, readyFile);
            logWorker("warn", "bigquery-worker", "Recovered expired processing file", {
                source: entry,
            });
        } catch (error) {
            if (error.code !== "ENOENT") {
                throw error;
            }
        }
    }
};

const claimReadyFile = async () => {
    const entries = (await fs.promises.readdir(READY_DIR))
        .filter((entry) => entry.endsWith(".ndjson"))
        .sort();

    for (const entry of entries) {
        const readyFile = path.join(READY_DIR, entry);
        const processingFile = getProcessingFilePath(workerName, `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`);

        try {
            await fs.promises.rename(readyFile, processingFile);
            return processingFile;
        } catch (error) {
            if (error.code === "ENOENT") {
                continue;
            }
        }
    }

    return null;
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

const processFile = async (filePath) => {
    const items = await readQueueFile(filePath);

    if (!items.length) {
        await fs.promises.unlink(filePath).catch(() => {});
        return;
    }

    const chunks = buildChunks(items);
    const remaining = [];

    for (let index = 0; index < chunks.length; index += 1) {
        const chunk = chunks[index];

        try {
            await insertBatch(
                chunk.tableName,
                chunk.items.map((item) => item.row)
            );
            await touchFile(filePath);
        } catch (error) {
            const failedInsertIds = new Set(getFailedInsertIds(error));

            if (failedInsertIds.size > 0) {
                logWorker("warn", "bigquery-worker-drop", "Dropping rows rejected by BigQuery", {
                    tableName: chunk.tableName,
                    count: failedInsertIds.size,
                    reason: error.message,
                });

                continue;
            }

            remaining.push(...chunk.items);

            for (let pendingIndex = index + 1; pendingIndex < chunks.length; pendingIndex += 1) {
                remaining.push(...chunks[pendingIndex].items);
            }

            const readyFile = getReadyFilePath(`retry-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`);
            await writeQueueFile(readyFile, remaining);
            await fs.promises.unlink(filePath).catch(() => {});

            logWorker("error", "bigquery-worker", "BigQuery insert failed; re-queued file chunk", {
                tableName: chunk.tableName,
                count: remaining.length,
                reason: error.message,
            });

            await sleep(Math.max(1000, bigQueryRetryDelayMs));
            return;
        }
    }

    await fs.promises.unlink(filePath).catch(() => {});
};

const startWorker = async () => {
    if (!isBigQueryConfigured()) {
        throw new Error("BigQuery is not fully configured");
    }

    await ensureQueueDir();
    logWorker("info", "bigquery-worker", "BigQuery file worker started");

    while (!stopping) {
        try {
            await recoverExpiredProcessingFiles();

            let filePath = await claimReadyFile();

            if (!filePath) {
                await rotatePendingFile();
                filePath = await claimReadyFile();
            }

            if (!filePath) {
                await sleep(Math.max(250, bigQueryWorkerPollMs));
                continue;
            }

            await processFile(filePath);
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
