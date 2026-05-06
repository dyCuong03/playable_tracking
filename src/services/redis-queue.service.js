const { createClient } = require("redis");
const {
    redisUrl,
    redisQueueStream,
    redisQueueGroup,
    redisRejectedStream,
    redisQueueMaxLen,
} = require("../config");

let client;
let clientPromise;
let consumerGroupPromise;

const logRedisError = (error) => {
    console.error(JSON.stringify({
        level: "error",
        type: "redis-queue",
        message: error.message,
    }));
};

const getClient = async () => {
    if (client && client.isOpen) {
        return client;
    }

    if (!clientPromise) {
        client = createClient({
            url: redisUrl,
            socket: {
                reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
            },
        });

        client.on("error", logRedisError);

        clientPromise = client.connect()
            .then(() => client)
            .catch((error) => {
                clientPromise = null;
                throw error;
            });
    }

    return clientPromise;
};

const sendRedisCommand = async (args) => {
    const queueClient = await getClient();
    return queueClient.sendCommand(args.map((value) => String(value)));
};

const encodeItem = (item) => JSON.stringify({
    ...item,
    attempts: Number(item && item.attempts ? item.attempts : 0),
    enqueuedAt: item && item.enqueuedAt ? item.enqueuedAt : new Date().toISOString(),
});

const decodeEntryFields = (fields) => {
    const data = {};

    for (let index = 0; index < fields.length; index += 2) {
        data[fields[index]] = fields[index + 1];
    }

    return data;
};

const parseStreamEntries = (response) => {
    if (!Array.isArray(response) || response.length === 0) {
        return [];
    }

    return response.flatMap((streamEntry) => {
        const entries = Array.isArray(streamEntry) ? streamEntry[1] : [];

        if (!Array.isArray(entries)) {
            return [];
        }

        return entries.map((entry) => {
            const messageId = entry[0];
            const fields = decodeEntryFields(entry[1] || []);
            const payload = fields.payload ? JSON.parse(fields.payload) : {};

            return {
                messageId,
                ...payload,
            };
        });
    });
};

const ensureQueueReady = async () => {
    if (!consumerGroupPromise) {
        consumerGroupPromise = (async () => {
            await sendRedisCommand(["XGROUP", "CREATE", redisQueueStream, redisQueueGroup, "0", "MKSTREAM"])
                .catch((error) => {
                    if (!String(error.message || "").includes("BUSYGROUP")) {
                        throw error;
                    }
                });
        })().catch((error) => {
            consumerGroupPromise = null;
            throw error;
        });
    }

    await consumerGroupPromise;
};

const appendToStream = async (streamName, item) => sendRedisCommand([
    "XADD",
    streamName,
    "MAXLEN",
    "~",
    Math.max(1000, redisQueueMaxLen),
    "*",
    "payload",
    encodeItem(item),
]);

const enqueueEvent = async (queueItem) => {
    await ensureQueueReady();
    return appendToStream(redisQueueStream, queueItem);
};

const requeueItems = async (items) => {
    if (!items.length) {
        return;
    }

    await ensureQueueReady();
    await Promise.all(items.map((item) => appendToStream(redisQueueStream, item)));
};

const rejectItems = async (items) => {
    if (!items.length) {
        return;
    }

    await Promise.all(items.map((item) => appendToStream(redisRejectedStream, item)));
};

const readQueueBatch = async (consumerName, count, blockMs) => {
    await ensureQueueReady();

    const response = await sendRedisCommand([
        "XREADGROUP",
        "GROUP",
        redisQueueGroup,
        consumerName,
        "COUNT",
        Math.max(1, count),
        "BLOCK",
        Math.max(0, blockMs),
        "STREAMS",
        redisQueueStream,
        ">",
    ]);

    return parseStreamEntries(response);
};

const claimPendingBatch = async (consumerName, count, minIdleMs) => {
    await ensureQueueReady();

    const response = await sendRedisCommand([
        "XAUTOCLAIM",
        redisQueueStream,
        redisQueueGroup,
        consumerName,
        Math.max(1000, minIdleMs),
        "0-0",
        "COUNT",
        Math.max(1, count),
    ]);

    if (!Array.isArray(response) || response.length < 2) {
        return [];
    }

    return parseStreamEntries([[redisQueueStream, response[1]]]);
};

const acknowledgeMessages = async (messageIds) => {
    if (!messageIds.length) {
        return;
    }

    await ensureQueueReady();
    await sendRedisCommand(["XACK", redisQueueStream, redisQueueGroup, ...messageIds]);
    await sendRedisCommand(["XDEL", redisQueueStream, ...messageIds]);
};

const parsePendingSummary = (response) => {
    if (!Array.isArray(response) || response.length < 4) {
        return {
            count: 0,
            minId: null,
            maxId: null,
            consumers: [],
        };
    }

    return {
        count: Number(response[0] || 0),
        minId: response[1] || null,
        maxId: response[2] || null,
        consumers: Array.isArray(response[3])
            ? response[3].map((entry) => ({
                name: entry[0],
                pending: Number(entry[1] || 0),
            }))
            : [],
    };
};

const getQueueStats = async () => {
    await ensureQueueReady();

    const [length, pending, rejectedLength] = await Promise.all([
        sendRedisCommand(["XLEN", redisQueueStream]),
        sendRedisCommand(["XPENDING", redisQueueStream, redisQueueGroup]).catch(() => [0, null, null, []]),
        sendRedisCommand(["XLEN", redisRejectedStream]).catch(() => 0),
    ]);

    return {
        backend: "redis-stream",
        redisUrl,
        stream: redisQueueStream,
        group: redisQueueGroup,
        length: Number(length || 0),
        pending: parsePendingSummary(pending),
        rejectedLength: Number(rejectedLength || 0),
    };
};

module.exports = {
    ensureQueueReady,
    enqueueEvent,
    requeueItems,
    rejectItems,
    readQueueBatch,
    claimPendingBatch,
    acknowledgeMessages,
    getQueueStats,
};
