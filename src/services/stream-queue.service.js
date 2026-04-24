const {
    redisStreamKey,
    redisStreamMaxLen,
    bigQueryErrorLogIntervalMs,
} = require("../config");
const { getRedisClient } = require("./redis.service");

let lastLogAt = 0;

const shouldLogNow = () => (Date.now() - lastLogAt) >= bigQueryErrorLogIntervalMs;

const logStreamError = (message, error) => {
    if (!shouldLogNow()) {
        return;
    }

    lastLogAt = Date.now();
    console.error(JSON.stringify({
        level: "error",
        type: "redis-stream",
        message,
        reason: error ? error.message : null,
    }));
};

const enqueueEvent = async (queueItem) => {
    const redis = await getRedisClient();

    if (!redis) {
        return false;
    }

    try {
        await redis.xAdd(
            redisStreamKey,
            "*",
            {
                payload: JSON.stringify(queueItem),
            },
            {
                TRIM: {
                    strategy: "MAXLEN",
                    strategyModifier: "~",
                    threshold: Math.max(1000, redisStreamMaxLen),
                },
            }
        );

        return true;
    } catch (error) {
        logStreamError("Failed to enqueue event into Redis stream", error);
        return false;
    }
};

module.exports = {
    enqueueEvent,
};
