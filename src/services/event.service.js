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
    "env",
    "environment",
]);

const resolveTrackingEnvironment = (query) => {
    const rawValue = query.env || query.environment || "";
    const normalized = String(rawValue).trim().toLowerCase();

    if (normalized === "production" || normalized === "prod") {
        return "production";
    }

    return "test";
};

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
    const clientTimestamp = query.ts || query.timestamp || query.client_ts || "";

    return {
        time: new Date().toISOString(),
        clientTimestamp: String(clientTimestamp || ""),
        event: query.e || "",
        pid: query.pid || query.package_name || query.project_id || "",
        playableId: query.playableId || query.playable_id || "",
        sid: query.sid || query.session_id || "",
        platform: query.platform || query.plf || "",
        campaignRaw: parseJSONField(query.campaign_raw || query.camp),
        trackingEnvironment: resolveTrackingEnvironment(query),
        params: buildParams(query),
        ip: req.ip || "",
        ua: req.get("user-agent") || "",
        ref: query.ref || req.get("referer") || "",
    };
};
