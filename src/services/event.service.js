const RESERVED_QUERY_KEYS = new Set([
    "e",
    "event",
    "event_name",
    "sid",
    "session_id",
    "ts",
    "event_time",
    "event_params",
    "env",
    "environment",
    // legacy aliases — still reserved so they are never captured as free params
    "platform",
    "plf",
    "campaign_raw",
    "camp",
    "pid",
    "package_name",
    "project_id",
    "playableId",
    "playable_id",
]);

const resolveTrackingEnvironment = (query) => {
    const rawValue = query.env || query.environment || "";
    const normalized = String(rawValue).trim().toLowerCase();

    if (normalized === "production" || normalized === "prod") {
        return "production";
    }

    return "test";
};

// Parse a URL query value into a plain object. Returns {} on failure, non-object, or array.
const parseEventParams = (value) => {
    if (!value) {
        return {};
    }

    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        return value;
    }

    if (typeof value !== "string") {
        return {};
    }

    try {
        const parsed = JSON.parse(value);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            return parsed;
        }
    } catch (_) {
        // fall through
    }

    return {};
};

// Resolve event_time: prefer client-supplied ISO or epoch-ms string; fall back to server now.
const resolveEventTime = (query) => {
    const raw = query.event_time || query.ts || "";
    const str = String(raw).trim();

    if (!str) {
        return new Date().toISOString();
    }

    // ISO 8601 or any string Date.parse accepts
    const asDate = Date.parse(str);
    if (Number.isFinite(asDate)) {
        return new Date(asDate).toISOString();
    }

    // Legacy: epoch-ms integer string (e.g. "1736179200000")
    const asMs = Number(str);
    if (Number.isFinite(asMs) && asMs > 0) {
        return new Date(asMs).toISOString();
    }

    return new Date().toISOString();
};

// For "start" events: if event_params is missing platform or campaign, pull from legacy
// query params (plf/platform and camp/campaign_raw) so old clients keep working.
const mergeLegacyStartParams = (eventParams, query) => {
    const merged = { ...eventParams };

    if (!merged.platform) {
        const legacyPlatform = query.platform || query.plf || "";
        if (legacyPlatform) {
            merged.platform = legacyPlatform;
        }
    }

    if (!merged.campaign) {
        const legacyCampaign = query.campaign_raw || query.camp || "";
        if (legacyCampaign) {
            const parsed = parseEventParams(legacyCampaign);
            if (Object.keys(parsed).length > 0) {
                merged.campaign = parsed;
            }
        }
    }

    return merged;
};

exports.buildEvent = (req) => {
    const query = req.query || {};
    const eventName = query.e || query.event || query.event_name || "";
    const rawParams = parseEventParams(query.event_params);
    const params = eventName === "start"
        ? mergeLegacyStartParams(rawParams, query)
        : rawParams;

    return {
        event: eventName,
        sid: query.sid || query.session_id || "",
        eventTime: resolveEventTime(query),
        params,
        trackingEnvironment: resolveTrackingEnvironment(query),
    };
};
