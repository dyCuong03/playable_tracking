const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const { buildRow, resolveTableName } = require("../services/bigquery.service");
const { enqueueEvent } = require("../services/bigquery-queue.service");

exports.trackPixel = async (req, res) => {
    const event = buildEvent(req);
    const row = buildRow(event);

    try {
        await enqueueEvent({
            tableName: resolveTableName(event),
            row,
        });
    } catch (error) {
        console.error("Failed to queue BigQuery event", error);
    }

    sendPixel(res);
};
