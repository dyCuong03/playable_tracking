const fs = require("fs");
const path = require("path");

const {
    bigQueryQueueDir,
    bigQueryQueueShards,
} = require("../config");

const BASE_DIR = path.resolve(process.cwd(), bigQueryQueueDir);
const PENDING_DIR = path.join(BASE_DIR, "pending");
const READY_DIR = path.join(BASE_DIR, "ready");
const PROCESSING_DIR = path.join(BASE_DIR, "processing");
const REJECTED_DIR = path.join(BASE_DIR, "rejected");

let ensurePromise;
let nextShard = 0;
const appendChains = new Map();

const ensureQueueDir = async () => {
    if (!ensurePromise) {
        ensurePromise = Promise.all([
            fs.promises.mkdir(PENDING_DIR, { recursive: true }),
            fs.promises.mkdir(READY_DIR, { recursive: true }),
            fs.promises.mkdir(PROCESSING_DIR, { recursive: true }),
            fs.promises.mkdir(REJECTED_DIR, { recursive: true }),
        ]);
    }

    await ensurePromise;
};

const serializeItem = (queueItem) => `${JSON.stringify(queueItem)}\n`;

const getPendingFilePath = (shard) => path.join(PENDING_DIR, `pending-${shard}.ndjson`);

const getReadyFilePath = (suffix) => path.join(READY_DIR, `ready-${suffix}.ndjson`);

const getProcessingFilePath = (workerName, suffix) => path.join(PROCESSING_DIR, `processing-${workerName}-${suffix}.ndjson`);
const getRejectedFilePath = (suffix) => path.join(REJECTED_DIR, `rejected-${suffix}.ndjson`);

const getDirectoryStats = async (dirPath) => {
    const entries = await fs.promises.readdir(dirPath).catch(() => []);
    const ndjsonEntries = entries.filter((entry) => entry.endsWith(".ndjson"));
    const fileStats = await Promise.all(
        ndjsonEntries.map(async (entry) => {
            const fullPath = path.join(dirPath, entry);
            const stat = await fs.promises.stat(fullPath).catch(() => null);
            return stat ? { name: entry, size: stat.size } : null;
        })
    );
    const validStats = fileStats.filter(Boolean);

    return {
        fileCount: validStats.length,
        totalBytes: validStats.reduce((sum, item) => sum + item.size, 0),
        files: validStats,
    };
};

const getQueueStats = async () => {
    await ensureQueueDir();

    const [pending, ready, processing, rejected] = await Promise.all([
        getDirectoryStats(PENDING_DIR),
        getDirectoryStats(READY_DIR),
        getDirectoryStats(PROCESSING_DIR),
        getDirectoryStats(REJECTED_DIR),
    ]);

    return {
        baseDir: BASE_DIR,
        pending,
        ready,
        processing,
        rejected,
    };
};

const getNextShard = () => {
    const shardCount = Math.max(1, bigQueryQueueShards);
    const shard = nextShard % shardCount;
    nextShard += 1;
    return shard;
};

const enqueueEvent = async (queueItem) => {
    await ensureQueueDir();

    const shard = getNextShard();
    const targetFile = getPendingFilePath(shard);
    const payload = serializeItem(queueItem);
    const chain = appendChains.get(targetFile) || Promise.resolve();

    const nextChain = chain.catch(() => {}).then(() => fs.promises.appendFile(
        targetFile,
        payload,
        "utf8"
    ));

    appendChains.set(targetFile, nextChain);
    await nextChain;
    return true;
};

module.exports = {
    enqueueEvent,
    ensureQueueDir,
    serializeItem,
    getPendingFilePath,
    getReadyFilePath,
    getProcessingFilePath,
    getRejectedFilePath,
    getQueueStats,
    BASE_DIR,
    PENDING_DIR,
    READY_DIR,
    PROCESSING_DIR,
    REJECTED_DIR,
};
