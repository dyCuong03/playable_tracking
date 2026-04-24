const {
    rateLimitWindowMs,
    rateLimitMax,
    rateLimitPrefix,
} = require("../config");
const { getRedisClient } = require("../services/redis.service");

const RATE_LIMIT_LUA = `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return current
`;

module.exports = async (req, res, next) => {
    const ip = req.ip || "unknown";
    const key = `${rateLimitPrefix}:${ip}`;

    try {
        const redis = await getRedisClient();

        if (!redis) {
            return next();
        }

        const current = await redis.eval(RATE_LIMIT_LUA, {
            keys: [key],
            arguments: [String(rateLimitWindowMs)],
        });

        req.rateLimitCount = Number(current);
        req.rateLimitExceeded = Number(current) > rateLimitMax;

        next();
    } catch (error) {
        console.error("Redis rate limit failed", error);
        next();
    }
};
