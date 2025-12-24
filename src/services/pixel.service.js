const { PIXEL_GIF } = require("../utils/gif");

exports.sendPixel = (res) => {
    res
        .status(200)
        .set({
            "Content-Type": "image/gif",
            "Content-Length": PIXEL_GIF.length,
        })
        .end(PIXEL_GIF);
};
