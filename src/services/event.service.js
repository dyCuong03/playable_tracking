const RESERVED_QUERY_KEYS = new Set([
    "e",
    "event",
    "pid",
    "package_name",
    "project_id",
    "platform",
    "plf",
    "campaign_raw",
    "camp",
    "sid",
    "session_id",
    "playableId",
    "playable_id",
]);

const parseJSONField = (value) => {
    if (!value) {
        return {};
    }

    if (typeof value === "object" && value !== null) {
        return value;
    }

    if (typeof value !== "string") {
        return {};
    }

    try {
        const parsed = JSON.parse(value);
        if (parsed && typeof parsed === "object") {
            return parsed;
        }
    } catch (error) {
        return {};
    }

    return {};
};

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
        pid: query.pid || query.package_name || query.project_id || "",
        playableId: query.playableId || query.playable_id || "",
        sid: query.sid || "",
        platform: query.platform || query.plf || "",
        campaignRaw: parseJSONField(query.campaign_raw || query.camp),
        params: buildParams(query),
        ip: req.ip || "",
        ua: req.get("user-agent") || "",
        ref: req.get("referer") || "",
    };
};
