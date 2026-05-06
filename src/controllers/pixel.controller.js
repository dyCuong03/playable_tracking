const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const { buildRow, resolveTableName } = require("../services/bigquery.service");
const { enqueueEvent } = require("../services/bigquery-queue.service");

exports.trackPixel = (req, res) => {
    const event = buildEvent(req);
    const row = buildRow(event);
    const tableName = resolveTableName(event);

    sendPixel(res);

    setImmediate(() => {
        enqueueEvent({
            tableName,
            row,
        }).catch((error) => {
            console.error("Failed to queue BigQuery event", error);
        });
    });
};
