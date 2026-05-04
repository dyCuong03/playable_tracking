// src/routes/health.route.js

const express = require("express");
const { getQueueStats } = require("../services/bigquery-queue.service");

const router = express.Router();

router.get("/health", async (req, res) => {
    const queue = await getQueueStats().catch(() => null);
    res.status(200).json({ ok: true, queue });
});

module.exports = router;
