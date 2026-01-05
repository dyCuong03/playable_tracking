const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const logService = require("../services/log.service");
const { insertEvent, buildRow } = require("../services/bigquery.service");

const parseTimestamp = (value) => {
    if (value === undefined || value === null || value === "") {
        return null;
    }

    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
        if (numeric < 1_000_000_000_000) {
            return numeric * 1000;
        }
        return numeric;
    }

    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
        return parsed;
    }

    return null;
};

const calculateDelayTime = (receivedAt) => (tsValue) => {
    const receivedMs = Date.parse(receivedAt);
    const tsMs = parseTimestamp(tsValue);

    if (Number.isNaN(receivedMs) || tsMs === null) {
        return null;
    }

    return receivedMs - tsMs;
};

exports.trackPixel = (req, res) => {
    const event = buildEvent(req);
    const row = buildRow(event);
    const getDelay = calculateDelayTime(row.received_at);

    logService.write({
        ...row,
        event_params: event.params,
        delay_time: getDelay(event.params.ts),
    });
    insertEvent(event);
    sendPixel(res);
};
