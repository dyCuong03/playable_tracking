// src/routes/health.route.js

const express = require("express");

const router = express.Router();

router.get("/health", (req, res) => {
    res.status(200).json({ ok: true });
});

module.exports = router;
