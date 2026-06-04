const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const { buildRow, resolveTableName } = require("../services/bigquery.service");
const { persistRequest } = require("../services/request-dispatcher.service");

const EVENT_NAME_WHITELIST = new Set(["start", "interaction", "store_trigger", "end"]);

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
    case "start":
        if (!params.platform) {
            errors.push({ field: "event_params.platform", issue: "required for start event" });
        }
        if (!params.campaign || typeof params.campaign !== "object") {
            errors.push({ field: "event_params.campaign", issue: "required object for start event" });
        }
        break;
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
    const event = buildEvent(req);
    const errors = validateEvent(event);

    if (errors.length > 0) {
        console.error(JSON.stringify({
            level: "warn",
            type: "event-validation",
            message: "Invalid event payload — skipping BigQuery write",
            errors,
            sid: event.sid,
            eventName: event.event,
        }));

        sendPixel(res, 400);
        return;
    }

    const row = buildRow(event);
    const tableName = resolveTableName(event);

    try {
        await persistRequest({
            tableName,
            row,
        });
        sendPixel(res);
    } catch (error) {
        console.error(JSON.stringify({
            level: "error",
            type: "request-persist",
            message: "Failed to persist tracking request",
            reason: error.message,
        }));

        sendPixel(res, 503);
    }
};
