const { createClient } = require("redis");
const {
    redisUrl,
    redisQueueStream,
    redisQueueGroup,
    redisRejectedStream,
    redisQueueMaxLen,
    redisConnectTimeoutMs,
    redisCommandTimeoutMs,
    redisUnavailableCooldownMs,
    redisErrorLogIntervalMs,
} = require("../config");

let client;
let clientPromise;
let consumerGroupPromise;
let redisUnavailableUntil = 0;
let lastRedisErrorLogAt = 0;

const isBusyGroupError = (error) => String(error && error.message ? error.message : "").includes("BUSYGROUP");
const normalizeRedisArgs = (args) => args.map((value) => String(value));

const withTimeout = (promise, timeoutMs, message) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
        reject(new Error(message));
    }, timeoutMs);

    promise
        .then((value) => {
            clearTimeout(timer);
            resolve(value);
        })
        .catch((error) => {
            clearTimeout(timer);
            reject(error);
        });
});

const logRedisError = (error) => {
    const now = Date.now();
    if ((now - lastRedisErrorLogAt) < redisErrorLogIntervalMs) {
        return;
    }

    lastRedisErrorLogAt = now;
    console.error(JSON.stringify({
        level: "error",
        type: "redis-queue",
        message: error.message,
    }));
};

const resetClientState = () => {
    consumerGroupPromise = null;
    clientPromise = null;

    if (!client) {
        return;
    }

    const currentClient = client;
    client = null;

    try {
        currentClient.removeAllListeners("error");
        if (currentClient.isOpen) {
            currentClient.disconnect();
        }
    } catch (_) {
        // Ignore disconnect errors while entering degraded mode.
    }
};

const markRedisUnavailable = (error) => {
    redisUnavailableUntil = Date.now() + redisUnavailableCooldownMs;
    resetClientState();

    if (error) {
        logRedisError(error);
    }
};

const getClient = async () => {
    if (client && client.isOpen) {
        return client;
    }

    if (Date.now() < redisUnavailableUntil) {
        throw new Error("Redis queue temporarily unavailable");
    }

    if (!clientPromise) {
        client = createClient({
            url: redisUrl,
            socket: {
                connectTimeout: Math.max(100, redisConnectTimeoutMs),
                reconnectStrategy: false,
            },
        });

        client.on("error", logRedisError);

        clientPromise = withTimeout(
            client.connect(),
            Math.max(100, redisConnectTimeoutMs),
            `Redis connect timed out after ${redisConnectTimeoutMs}ms`
        )
            .then(() => client)
            .catch((error) => {
                markRedisUnavailable(error);
                throw error;
            });
    }

    return clientPromise;
};

const executeRedisCommand = async (args, options = {}) => {
    const {
        markUnavailableOnError = true,
        timeoutMs = Math.max(100, redisCommandTimeoutMs),
        timeoutMessage = `Redis command timed out after ${timeoutMs}ms`,
    } = options;
    const queueClient = await getClient();

    return withTimeout(
        queueClient.sendCommand(normalizeRedisArgs(args)),
        timeoutMs,
        timeoutMessage
    ).catch((error) => {
        if (markUnavailableOnError) {
            markRedisUnavailable(error);
        }

        throw error;
    });
};

const sendRedisCommand = async (args) => executeRedisCommand(args);

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
            await executeRedisCommand(
                ["XGROUP", "CREATE", redisQueueStream, redisQueueGroup, "0", "MKSTREAM"],
                { markUnavailableOnError: false }
            )
                .catch((error) => {
                    if (isBusyGroupError(error)) {
                        return;
                    }

                    markRedisUnavailable(error);
                    if (!isBusyGroupError(error)) {
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

const produceToStream = async (streamName, item, options = {}) => executeRedisCommand(
    [
        "XADD",
        streamName,
        "MAXLEN",
        "~",
        Math.max(1000, redisQueueMaxLen),
        "*",
        "payload",
        encodeItem(item),
    ],
    options
);

const appendToStreamBatch = async (streamName, items, options = {}) => {
    if (!items.length) {
        return [];
    }

    const {
        timeoutMs = Math.max(
            Math.max(100, redisCommandTimeoutMs),
            Math.min(30_000, Math.max(1, items.length) * 25)
        ),
        timeoutMessage = `Redis pipeline timed out after ${timeoutMs}ms`,
        markUnavailableOnError = true,
    } = options;

    const queueClient = await getClient();
    const pipeline = queueClient.MULTI();

    for (let index = 0; index < items.length; index += 1) {
        pipeline.addCommand([
            "XADD",
            streamName,
            "MAXLEN",
            "~",
            Math.max(1000, redisQueueMaxLen),
            "*",
            "payload",
            encodeItem(items[index]),
        ].map((value) => String(value)));
    }

    return withTimeout(
        pipeline.execAsPipeline(),
        timeoutMs,
        timeoutMessage
    ).catch((error) => {
        if (markUnavailableOnError) {
            markRedisUnavailable(error);
        }

        throw error;
    });
};

const enqueueEvent = async (queueItem, options = {}) => {
    return produceToStream(redisQueueStream, queueItem, options);
};

const enqueueEventBatch = async (items) => {
    if (!items.length) {
        return [];
    }

    return appendToStreamBatch(redisQueueStream, items);
};

const requeueItems = async (items) => {
    if (!items.length) {
        return;
    }

    await appendToStreamBatch(redisQueueStream, items);
};

const rejectItems = async (items) => {
    if (!items.length) {
        return;
    }

    await appendToStreamBatch(redisRejectedStream, items);
};

const readQueueBatch = async (consumerName, count, blockMs) => {
    await ensureQueueReady();
    const effectiveBlockMs = Math.max(0, blockMs);
    const timeoutMs = Math.max(
        Math.max(100, redisCommandTimeoutMs),
        effectiveBlockMs + 1_000
    );

    const response = await executeRedisCommand(
        [
            "XREADGROUP",
            "GROUP",
            redisQueueGroup,
            consumerName,
            "COUNT",
            Math.max(1, count),
            "BLOCK",
            effectiveBlockMs,
            "STREAMS",
            redisQueueStream,
            ">",
        ],
        {
            timeoutMs,
            timeoutMessage: `Redis blocking read timed out after ${timeoutMs}ms`,
        }
    );

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
    const queueClient = await getClient();
    const pipeline = queueClient.MULTI();
    const timeoutMs = Math.max(
        Math.max(100, redisCommandTimeoutMs),
        Math.min(30_000, Math.max(1, messageIds.length) * 10)
    );

    pipeline.addCommand(normalizeRedisArgs(["XACK", redisQueueStream, redisQueueGroup, ...messageIds]));
    pipeline.addCommand(normalizeRedisArgs(["XDEL", redisQueueStream, ...messageIds]));

    await withTimeout(
        pipeline.execAsPipeline(),
        timeoutMs,
        `Redis acknowledge pipeline timed out after ${timeoutMs}ms`
    ).catch((error) => {
        markRedisUnavailable(error);
        throw error;
    });
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
    produceToStream,
    enqueueEvent,
    enqueueEventBatch,
    requeueItems,
    rejectItems,
    readQueueBatch,
    claimPendingBatch,
    acknowledgeMessages,
    getQueueStats,
};
