const { buildEvent } = require("../services/event.service");
const { sendPixel } = require("../services/pixel.service");
const logService = require("../services/log.service");

exports.trackPixel = (req, res) => {
    const event = buildEvent(req);
    logService.write(event);
    sendPixel(res);
};
