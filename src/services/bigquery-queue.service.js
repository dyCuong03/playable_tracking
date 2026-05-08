const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const {
    bigQueryQueueDir,
    bigQueryQueueShards,
    requestQueueRotateMinBytes,
    requestQueueRotateMaxAgeMs,
} = require("../config");

const BASE_DIR = path.resolve(process.cwd(), bigQueryQueueDir);
const PENDING_DIR = path.join(BASE_DIR, "pending");
const READY_DIR = path.join(BASE_DIR, "ready");
const PROCESSING_DIR = path.join(BASE_DIR, "processing");
const REJECTED_DIR = path.join(BASE_DIR, "rejected");

let ensurePromise;
let nextShard = 0;
const appendStates = new Map();

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

const getProcessingToken = (workerName, readyFile) => {
    const basename = path.basename(readyFile);
    const hash = crypto.createHash("sha1").update(basename).digest("hex").slice(0, 12);
    return `${workerName}-${hash}`;
};

const getProcessingFilePath = (workerName, readyFile) => path.join(
    PROCESSING_DIR,
    `processing--${getProcessingToken(workerName, readyFile)}.ndjson`
);
const getRejectedFilePath = (suffix) => path.join(REJECTED_DIR, `rejected-${suffix}.ndjson`);

const getFileSuffix = () => `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2, 10)}`;

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

const getAppendState = (targetFile) => {
    if (!appendStates.has(targetFile)) {
        appendStates.set(targetFile, {
            entries: [],
            flushing: false,
            scheduled: false,
            waiters: [],
        });
    }

    return appendStates.get(targetFile);
};

const flushPendingAppends = async (targetFile) => {
    const state = getAppendState(targetFile);

    if (state.flushing) {
        await new Promise((resolve, reject) => {
            state.waiters.push({ resolve, reject });
        });
        return;
    }

    if (!state.entries.length) {
        return;
    }

    state.flushing = true;
    state.scheduled = false;

    const entries = state.entries.splice(0, state.entries.length);
    const payload = entries.map((entry) => entry.payload).join("");

    try {
        await fs.promises.appendFile(targetFile, payload, "utf8");
        entries.forEach((entry) => entry.resolve(true));
    } catch (error) {
        entries.forEach((entry) => entry.reject(error));
        throw error;
    } finally {
        state.flushing = false;

        const waiters = state.waiters.splice(0, state.waiters.length);
        waiters.forEach((waiter) => waiter.resolve());

        if (state.entries.length > 0) {
            await flushPendingAppends(targetFile);
        }
    }
};

const scheduleFlush = (targetFile) => {
    const state = getAppendState(targetFile);

    if (state.scheduled) {
        return;
    }

    state.scheduled = true;
    setImmediate(() => {
        flushPendingAppends(targetFile).catch((error) => {
            const waiters = state.waiters.splice(0, state.waiters.length);
            waiters.forEach((waiter) => waiter.reject(error));
        });
    });
};

const enqueueEvent = async (queueItem) => {
    await ensureQueueDir();

    const shard = getNextShard();
    const targetFile = getPendingFilePath(shard);
    const payload = serializeItem(queueItem);
    const state = getAppendState(targetFile);

    return new Promise((resolve, reject) => {
        state.entries.push({
            payload,
            resolve,
            reject,
        });
        scheduleFlush(targetFile);
    });
};

const rotatePendingShard = async (shard) => {
    await ensureQueueDir();

    const targetFile = getPendingFilePath(shard);
    await flushPendingAppends(targetFile);

    const stat = await fs.promises.stat(targetFile).catch(() => null);

    if (!stat || stat.size === 0) {
        return null;
    }

    const fileAgeMs = Math.max(0, Date.now() - Number(stat.mtimeMs || 0));
    const shouldRotateForSize = stat.size >= Math.max(1, requestQueueRotateMinBytes);
    const shouldRotateForAge = fileAgeMs >= Math.max(0, requestQueueRotateMaxAgeMs);

    if (!shouldRotateForSize && !shouldRotateForAge) {
        return null;
    }

    const readyFile = getReadyFilePath(getFileSuffix());
    await fs.promises.rename(targetFile, readyFile);
    return readyFile;
};

const rotatePendingFiles = async () => {
    const shardCount = Math.max(1, bigQueryQueueShards);
    const rotated = await Promise.all(
        Array.from({ length: shardCount }, (_, shard) => rotatePendingShard(shard))
    );

    return rotated.filter(Boolean);
};

const listReadyFiles = async () => {
    await ensureQueueDir();

    const entries = await fs.promises.readdir(READY_DIR).catch(() => []);
    return entries
        .filter((entry) => entry.endsWith(".ndjson"))
        .sort()
        .map((entry) => path.join(READY_DIR, entry));
};

const claimReadyFile = async (workerName) => {
    const readyFiles = await listReadyFiles();

    for (let index = 0; index < readyFiles.length; index += 1) {
        const readyFile = readyFiles[index];
        const processingFile = getProcessingFilePath(workerName, readyFile);

        try {
            await fs.promises.rename(readyFile, processingFile);
            return {
                readyFile,
                processingFile,
            };
        } catch (error) {
            if (error && error.code === "ENOENT") {
                continue;
            }

            throw error;
        }
    }

    return null;
};

const claimReadyFiles = async (workerName, limit) => {
    const maxFiles = Math.max(1, limit);
    const claimedFiles = [];

    for (let index = 0; index < maxFiles; index += 1) {
        const claimedFile = await claimReadyFile(workerName);

        if (!claimedFile) {
            break;
        }

        claimedFiles.push(claimedFile);
    }

    return claimedFiles;
};

const parseQueueFile = async (filePath) => {
    const content = await fs.promises.readFile(filePath, "utf8");

    return content
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => JSON.parse(line));
};

const completeProcessingFile = async (processingFile) => {
    await fs.promises.unlink(processingFile).catch((error) => {
        if (error && error.code !== "ENOENT") {
            throw error;
        }
    });
};

const releaseProcessingFile = async (processingFile, readyFile) => {
    if (!readyFile) {
        return null;
    }

    await fs.promises.rename(processingFile, readyFile);
    return readyFile;
};

const resetQueueState = async () => {
    appendStates.clear();
    ensurePromise = null;
    nextShard = 0;
    await fs.promises.rm(BASE_DIR, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 50,
    });
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
    rotatePendingFiles,
    claimReadyFile,
    claimReadyFiles,
    parseQueueFile,
    completeProcessingFile,
    releaseProcessingFile,
    resetQueueState,
    BASE_DIR,
    PENDING_DIR,
    READY_DIR,
    PROCESSING_DIR,
    REJECTED_DIR,
};
