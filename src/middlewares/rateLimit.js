// src/middlewares/rateLimit.js

const hits = new Map();

module.exports = (req, res, next) => {
    const ip = req.ip || "unknown";
    const now = Date.now();

    const WINDOW_MS = 10_000; // 10 seconds
    const LIMIT = 200;       // max requests per IP per window

    let record = hits.get(ip);

    if (!record) {
        record = { count: 1, start: now };
        hits.set(ip, record);
        return next();
    }

    if (now - record.start > WINDOW_MS) {
        record.count = 1;
        record.start = now;
        return next();
    }

    record.count += 1;

    if (record.count > LIMIT) {
        // IMPORTANT: still return pixel, don't block playable
        res.status(200).end();
        return;
    }

    next();
};
