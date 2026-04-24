const { createClient } = require("redis");

const { redisUrl } = require("../config");

let client;
let connectPromise;
let isLoggedDisabled = false;

const logDisabled = () => {
    if (isLoggedDisabled) {
        return;
    }

    console.warn("Redis features disabled: REDIS_URL is not configured");
    isLoggedDisabled = true;
};

const getRedisClient = async () => {
    if (!redisUrl) {
        logDisabled();
        return null;
    }

    if (!client) {
        client = createClient({ url: redisUrl });
        client.on("error", (error) => {
            console.error("Redis client error", error);
        });
    }

    if (client.isOpen) {
        return client;
    }

    if (!connectPromise) {
        connectPromise = client.connect().finally(() => {
            connectPromise = null;
        });
    }

    await connectPromise;
    return client;
};

module.exports = {
    getRedisClient,
};
