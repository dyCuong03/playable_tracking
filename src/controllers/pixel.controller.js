const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const { buildRow, resolveTableName } = require("../services/bigquery.service");
const { persistRequest } = require("../services/request-dispatcher.service");

exports.trackPixel = async (req, res) => {
    const event = buildEvent(req);
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
