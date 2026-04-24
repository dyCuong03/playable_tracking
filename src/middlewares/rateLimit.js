const {
    rateLimitWindowMs,
    rateLimitMax,
} = require("../config");
const hits = new Map();

module.exports = async (req, res, next) => {
    const ip = req.ip || "unknown";
    const now = Date.now();

    let record = hits.get(ip);

    if (!record || (now - record.start) > rateLimitWindowMs) {
        record = { count: 0, start: now };
        hits.set(ip, record);
    }

    record.count += 1;
    req.rateLimitCount = record.count;
    req.rateLimitExceeded = record.count > rateLimitMax;

    next();
};
