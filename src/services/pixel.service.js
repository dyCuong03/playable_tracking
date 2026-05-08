const { PIXEL_GIF } = require("../utils/gif");

exports.sendPixel = (res, statusCode = 200) => {
    res
        .status(statusCode)
        .set({
            "Content-Type": "image/gif",
            "Content-Length": PIXEL_GIF.length,
        })
        .end(PIXEL_GIF);
};
