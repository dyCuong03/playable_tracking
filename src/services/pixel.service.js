const { trackingResponseMode } = require("../config");
const { PIXEL_GIF } = require("../utils/gif");

exports.sendPixel = (res) => {
    if (String(trackingResponseMode).trim().toLowerCase() !== "pixel") {
        res
            .status(200)
            .set({
                "Content-Type": "text/plain; charset=utf-8",
                "Content-Length": "2",
            })
            .end("OK");
        return;
    }

    res
        .status(200)
        .set({
            "Content-Type": "image/gif",
            "Content-Length": PIXEL_GIF.length,
        })
        .end(PIXEL_GIF);
};
