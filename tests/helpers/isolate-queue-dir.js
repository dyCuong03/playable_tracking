"use strict";

// Redirects the disk queue (and logs) away from the repo's real ./data/bigquery-queue into
// a per-PROCESS temp dir, BEFORE any src module loads. This removes the shared-real-dir
// contention class entirely: each `node --test` process (and any parallel CI job) gets its
// own isolated queue dir, so specs can never stomp each other's NDJSON shards.
//
// Why per-process and not per-spec: bigquery-queue.service computes BASE_DIR once at module
// load (path.resolve(cwd, BIGQUERY_QUEUE_DIR)) and is cached across all specs in a process,
// so a per-spec env change after first load would not take effect. A per-process temp dir
// achieves the same isolation goal without rewriting every spec's top-level requires.
//
// Require this at the TOP of any spec that touches the disk queue, BEFORE requiring
// ../src/* modules.

const fs = require("fs");
const os = require("os");
const path = require("path");

if (!global.__PIXEL_TEST_QDIR__) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-test-qdir-"));
    global.__PIXEL_TEST_QDIR__ = root;

    // Only set defaults if the spec/harness has not already chosen its own dirs.
    if (!process.env.BIGQUERY_QUEUE_DIR) {
        process.env.BIGQUERY_QUEUE_DIR = path.join(root, "bigquery-queue");
    }
    if (!process.env.PIXEL_LOG_DIR) {
        process.env.PIXEL_LOG_DIR = path.join(root, "logs");
    }

    process.on("exit", () => {
        try {
            fs.rmSync(root, { recursive: true, force: true });
        } catch (_) {
            // best effort
        }
    });
}

module.exports = {
    root: global.__PIXEL_TEST_QDIR__,
    queueDir: process.env.BIGQUERY_QUEUE_DIR,
    logDir: process.env.PIXEL_LOG_DIR,
};
