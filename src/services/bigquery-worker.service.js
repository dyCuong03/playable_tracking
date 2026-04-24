const os = require("os");

const {
    bigQueryBatchSize,
    bigQueryRetryDelayMs,
    bigQueryErrorLogIntervalMs,
    redisStreamKey,
    redisStreamConsumerGroup,
    redisStreamConsumerName,
    redisStreamBlockMs,
    redisStreamClaimIdleMs,
} = require("../config");
const {
    insertBatch,
    isBigQueryConfigured,
} = require("./bigquery.service");
const { getRedisClient } = require("./redis.service");

const consumerName = redisStreamConsumerName || `${os.hostname()}-${process.pid}`;

let lastWorkerLogAt = 0;
let stopping = false;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const shouldLogNow = () => (Date.now() - lastWorkerLogAt) >= bigQueryErrorLogIntervalMs;

const logWorker = (level, message, details = {}) => {
    if (level === "error" && !shouldLogNow()) {
        return;
    }

    if (level === "error") {
        lastWorkerLogAt = Date.now();
    }

    const entry = {
        level,
        type: "bigquery-worker",
        message,
        consumer: consumerName,
        ...details,
    };

    const payload = JSON.stringify(entry);
    if (level === "error") {
        console.error(payload);
        return;
    }

    console.log(payload);
};

const ensureConsumerGroup = async (redis) => {
    try {
        await redis.xGroupCreate(
            redisStreamKey,
            redisStreamConsumerGroup,
            "0",
            { MKSTREAM: true }
        );
    } catch (error) {
        if (!String(error.message || "").includes("BUSYGROUP")) {
            throw error;
        }
    }
};

const parseQueueItem = (message) => {
    try {
        return JSON.parse(message.message.payload);
    } catch (error) {
        return null;
    }
};

const ackMessages = async (redis, messages) => {
    const ids = messages.map((message) => message.id);

    if (!ids.length) {
        return;
    }

    await redis.xAck(redisStreamKey, redisStreamConsumerGroup, ids);
    await redis.xDel(redisStreamKey, ids);
};

const getFailedInsertIds = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors
        .map((entry) => entry && entry.row && entry.row.insertId)
        .filter(Boolean);
};

const processTableBatch = async (redis, items) => {
    const rows = items.map((item) => item.payload.row);

    try {
        await insertBatch(items[0].payload.tableName, rows);
        await ackMessages(redis, items);
        return true;
    } catch (error) {
        const failedInsertIds = new Set(getFailedInsertIds(error));

        if (failedInsertIds.size > 0) {
            logWorker("error", "Dropping rows rejected by BigQuery", {
                tableName: items[0].payload.tableName,
                count: failedInsertIds.size,
                reason: error.message,
            });
            await ackMessages(redis, items);
            return true;
        }

        logWorker("error", "BigQuery insert failed; worker will retry", {
            tableName: items[0].payload.tableName,
            count: items.length,
            reason: error.message,
        });
        await sleep(Math.max(1000, bigQueryRetryDelayMs));
        return false;
    }
};

const groupMessagesByTable = (messages) => messages.reduce((acc, message) => {
    const payload = parseQueueItem(message);

    if (!payload || !payload.tableName || !payload.row) {
        acc.invalid.push(message);
        return acc;
    }

    if (!acc.tables.has(payload.tableName)) {
        acc.tables.set(payload.tableName, []);
    }

    acc.tables.get(payload.tableName).push({
        id: message.id,
        message: message.message,
        payload,
    });

    return acc;
}, { tables: new Map(), invalid: [] });

const processMessages = async (redis, messages) => {
    const grouped = groupMessagesByTable(messages);

    if (grouped.invalid.length) {
        logWorker("error", "Dropping invalid stream messages", {
            count: grouped.invalid.length,
        });
        await ackMessages(redis, grouped.invalid);
    }

    for (const items of grouped.tables.values()) {
        const chunks = [];

        for (let index = 0; index < items.length; index += Math.max(1, bigQueryBatchSize)) {
            chunks.push(items.slice(index, index + Math.max(1, bigQueryBatchSize)));
        }

        for (const chunk of chunks) {
            const success = await processTableBatch(redis, chunk);

            if (!success) {
                return false;
            }
        }
    }

    return true;
};

const claimPendingMessages = async (redis) => {
    const claimed = await redis.xAutoClaim(
        redisStreamKey,
        redisStreamConsumerGroup,
        consumerName,
        Math.max(1000, redisStreamClaimIdleMs),
        "0-0",
        { COUNT: Math.max(1, bigQueryBatchSize) }
    );

    return claimed && Array.isArray(claimed.messages) ? claimed.messages.filter(Boolean) : [];
};

const readNewMessages = async (redis) => {
    const response = await redis.xReadGroup(
        redisStreamConsumerGroup,
        consumerName,
        {
            key: redisStreamKey,
            id: ">",
        },
        {
            COUNT: Math.max(1, bigQueryBatchSize),
            BLOCK: Math.max(1000, redisStreamBlockMs),
        }
    );

    if (!response || !response.length) {
        return [];
    }

    return response.flatMap((stream) => stream.messages || []);
};

const startWorker = async () => {
    if (!isBigQueryConfigured()) {
        throw new Error("BigQuery is not fully configured");
    }

    const redis = await getRedisClient();

    if (!redis) {
        throw new Error("Redis is not configured");
    }

    await ensureConsumerGroup(redis);
    logWorker("info", "BigQuery worker started", {
        stream: redisStreamKey,
        group: redisStreamConsumerGroup,
    });

    while (!stopping) {
        try {
            const claimed = await claimPendingMessages(redis);

            if (claimed.length) {
                const success = await processMessages(redis, claimed);
                if (!success) {
                    continue;
                }
            }

            const freshMessages = await readNewMessages(redis);

            if (freshMessages.length) {
                await processMessages(redis, freshMessages);
            }
        } catch (error) {
            logWorker("error", "Worker loop failed", {
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
