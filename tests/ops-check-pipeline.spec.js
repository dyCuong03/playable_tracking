"use strict";

// Phase 2 — ops:check-pipeline exit codes (CONTRACT2 §OPS CHECK).
//
// In HTTP mode `scripts/check-pipeline.js` GETs /debug/pipeline and maps pipeline_status
// to an exit code: 0 healthy, 1 unhealthy, 2 degraded — so cron can alert.
//
// We start the REAL app (fake Redis + mock BigQuery) on a port and spawn the script as a
// child process; the child only performs an HTTP GET, so it transparently sees our
// in-process fake-backed state. Activates once scripts/check-pipeline.js exists.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const SCRIPT = path.join(__dirname, "..", "scripts", "check-pipeline.js");
const SKIP = fs.existsSync(SCRIPT) ? false : "awaiting backend scripts/check-pipeline.js";

const { createPipeline, runController } = require("./helpers/pipeline-harness");
const { startEvent } = require("./helpers/events");

const listen = (app) =>
    new Promise((resolve) => {
        const server = app.listen(0, "127.0.0.1", () => resolve(server));
    });

// Use async spawn (NOT spawnSync): the in-process HTTP server must keep serving while the
// child probes it, so the parent event loop must not block.
const runCheck = (port, args = []) =>
    new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [SCRIPT, ...args], {
            env: { ...process.env, PORT: String(port) },
        });
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk) => {
            stdout += chunk;
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk;
        });
        child.on("error", reject);
        child.on("close", (status) => resolve({ status, stdout, stderr }));
    });

test("ops:check-pipeline exits 0 when the pipeline is healthy", { skip: SKIP }, async () => {
    const pipeline = createPipeline();
    const server = await listen(pipeline.services.app);
    try {
        // Idle, empty queues -> healthy.
        const result = await runCheck(server.address().port);
        assert.equal(result.status, 0, `expected exit 0 (healthy); got ${result.status}. stdout: ${result.stdout} stderr: ${result.stderr}`);
    } finally {
        server.close();
        await pipeline.cleanup();
    }
});

test("ops:check-pipeline exits 1 when the pipeline is unhealthy (stuck dispatcher + backlog)", { skip: SKIP }, async () => {
    const pipeline = createPipeline();
    const server = await listen(pipeline.services.app);
    try {
        // Accept events but never drain -> disk backlog stranded, no dispatcher heartbeat.
        for (let i = 0; i < 20; i += 1) {
            assert.equal(await runController(pipeline.services.controller, startEvent(`ops-${i}`)), 200);
        }
        const result = await runCheck(server.address().port);
        assert.equal(result.status, 1, `expected exit 1 (unhealthy); got ${result.status}. stdout: ${result.stdout} stderr: ${result.stderr}`);
        assert.match(String(result.stdout) + String(result.stderr), /unhealthy|dispatch|backlog/i, "prints the reason");
    } finally {
        server.close();
        await pipeline.cleanup();
    }
});
