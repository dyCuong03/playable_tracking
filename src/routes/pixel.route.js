const express = require("express");
const { trackPixel } = require("../controllers/pixel.controller");
const rateLimit = require("../middlewares/rateLimit");
const noCache = require("../middlewares/noCache");

const router = express.Router();

router.get("/p.gif", rateLimit, noCache, trackPixel);

module.exports = router;
