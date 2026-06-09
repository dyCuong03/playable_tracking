const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const { buildRow, resolveTableName } = require("../services/bigquery.service");
const { persistRequest } = require("../services/request-dispatcher.service");
const logService = require("../services/log.service");

const EVENT_NAME_WHITELIST = new Set(["start", "interaction", "store_trigger", "end"]);

const normalizeQueryValue = (value) => {
    if (Array.isArray(value)) {
        return value.map((entry) => String(entry));
    }

    if (value === undefined || value === null) {
        return "";
    }

    return String(value);
};

const buildUrlData = (req, event) => {
    const query = req.query || {};
    const queryData = Object.fromEntries(
        Object.entries(query).map(([key, value]) => [key, normalizeQueryValue(value)])
    );

    return {
        request_uri: req.originalUrl || req.url || "",
        path: req.path || "",
        query: queryData,
        event_params_parsed: event.params || {},
    };
};

const logServerRequest = (entry) => {
    logService.writeDaily("server", {
        level: entry.statusCode >= 500 ? "error" : (entry.statusCode >= 400 ? "warn" : "info"),
        type: "pixel-server-request",
        ...entry,
    }, false);
};

// Returns an array of error objects. Empty array means valid.
// Validation failures skip BigQuery writes but the pixel is always served.
const validateEvent = (event) => {
    const errors = [];
    const params = event.params || {};

    if (!event.sid) {
        errors.push({ field: "sid", issue: "required" });
    }

    if (!event.event) {
        errors.push({ field: "e", issue: "required" });
    } else if (!EVENT_NAME_WHITELIST.has(event.event)) {
        errors.push({
            field: "e",
            issue: `must be one of: ${[...EVENT_NAME_WHITELIST].join(", ")}`,
            received: event.event,
        });
    }

    // Stop here — per-event checks require a valid event name.
    if (errors.length > 0) {
        return errors;
    }

    switch (event.event) {
    case "start": {
        const VALID_PLATFORMS = new Set(["Windows", "Android", "IOS"]);
        if (!params.platform) {
            errors.push({ field: "event_params.platform", issue: "required for start event" });
        } else if (!VALID_PLATFORMS.has(params.platform)) {
            errors.push({
                field: "event_params.platform",
                issue: "must be one of: Windows, Android, IOS",
                received: params.platform,
            });
        }
        if (!params.network) {
            errors.push({ field: "event_params.network", issue: "required for start event" });
        }
        break;
    }
    case "interaction":
    case "store_trigger":
        if (!params.name) {
            errors.push({ field: "event_params.name", issue: `required for ${event.event} event` });
        }
        break;
    case "end":
        if (params.interact_count === undefined || params.interact_count === null) {
            errors.push({ field: "event_params.interact_count", issue: "required for end event" });
        }
        break;
    default:
        break;
    }

    return errors;
};

exports.trackPixel = async (req, res) => {
    const serverStartAt = Date.now();
    const event = buildEvent(req);
    const urlData = buildUrlData(req, event);
    const errors = validateEvent(event);

    if (errors.length > 0) {
        const durationMs = Date.now() - serverStartAt;
        console.error(JSON.stringify({
            ts: new Date().toISOString(),
            level: "warn",
            type: "event-validation",
            message: "Invalid event payload — skipping BigQuery write",
            errors,
            sid: event.sid,
            eventName: event.event,
            server_duration_ms: durationMs,
        }));

        sendPixel(res, 400);
        logServerRequest({
            message: "Invalid event payload",
            statusCode: 400,
            server_duration_ms: durationMs,
            event_name: event.event || null,
            session_id: event.sid || null,
            data: urlData,
            errors,
        });
        return;
    }

    const row = buildRow(event);
    const tableName = resolveTableName(event);

    try {
        await persistRequest({
            tableName,
            row,
            urlData,
        });
        sendPixel(res);
        logServerRequest({
            message: "Tracking request persisted to durable queue",
            statusCode: 200,
            server_duration_ms: Date.now() - serverStartAt,
            tableName,
            event_hash: row.event_hash || null,
            event_name: row.event_name || null,
            session_id: row.session_id || null,
            data: urlData,
        });
    } catch (error) {
        const durationMs = Date.now() - serverStartAt;
        console.error(JSON.stringify({
            ts: new Date().toISOString(),
            level: "error",
            type: "request-persist",
            message: "Failed to persist tracking request",
            reason: error.message,
            server_duration_ms: durationMs,
        }));

        sendPixel(res, 503);
        logServerRequest({
            message: "Failed to persist tracking request",
            statusCode: 503,
            server_duration_ms: durationMs,
            tableName,
            event_hash: row.event_hash || null,
            event_name: row.event_name || null,
            session_id: row.session_id || null,
            data: urlData,
            reason: error.message,
        });
    }
};
