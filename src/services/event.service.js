const RESERVED_QUERY_KEYS = new Set([
    "e",
    "event",
    "pid",
    "project_id",
    "sid",
    "session_id",
    "playableId",
    "playable_id",
]);

const buildParams = (query) => {
    if (!query) {
        return {};
    }

    return Object.keys(query).reduce((acc, key) => {
        if (RESERVED_QUERY_KEYS.has(key)) {
            return acc;
        }

        acc[key] = query[key];
        return acc;
    }, {});
};

exports.buildEvent = (req) => {
    const query = req.query || {};

    return {
        time: new Date().toISOString(),
        event: query.e || "",
        pid: query.pid || "",
        playableId: query.playableId || query.playable_id || "",
        sid: query.sid || "",
        params: buildParams(query),
        ip: req.ip || "",
        ua: req.get("user-agent") || "",
        ref: req.get("referer") || "",
    };
};
