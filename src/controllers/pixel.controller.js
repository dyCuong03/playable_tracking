const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const logService = require("../services/log.service");
const { insertEvent, buildRow } = require("../services/bigquery.service");

exports.trackPixel = (req, res) => {
    const event = buildEvent(req);
    const row = buildRow(event);

    logService.write({
        ...row,
        event_params: event.params,
    });
    insertEvent(event);
    sendPixel(res);
};
