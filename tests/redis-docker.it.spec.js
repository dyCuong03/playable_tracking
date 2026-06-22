"use strict";

// Docker-gated integration test: runs the pipeline against a REAL Redis 7 server
// (no fake redis), proving the production redis-queue.service XADD/XREADGROUP/XAUTOCLAIM
// commands work against an actual Redis Stream. BigQuery is still mocked (no creds).
//
// Skipped unless RUN_REDIS_IT=1 AND docker is available, so the default `npm test` stays
// dependency-free. Run it with:
//   RUN_REDIS_IT=1 node --test tests/redis-docker.it.spec.js
//
// Inspect the live stream while debugging:
//   docker exec <container> redis-cli XLEN pixel:events
//   docker exec <container> redis-cli XINFO GROUPS pixel:events

const test = require("node:test");
const assert = require("node:assert/strict");
const { execFileSync } = require("child_process");

const {
    createPipeline,
    drainDiskToRedis,
    runController,
    runWorkerUntilDrained,
    sleep,
} = require("./helpers/pipeline-harness");
const { startEvent } = require("./helpers/events");

const ENABLED = process.env.RUN_REDIS_IT === "1";

const dockerAvailable = () => {
    try {
        execFileSync("docker", ["version"], { stdio: "ignore" });
        return true;
    } catch (_) {
        return false;
    }
};

test(
    "real Redis (docker): 200 events flow disk -> Redis stream -> worker -> BigQuery mock",
    { skip: !ENABLED ? "set RUN_REDIS_IT=1 to run" : (!dockerAvailable() ? "docker not available" : false) },
    async () => {
        const port = 6390;
        const name = `pixel-it-redis-${process.pid}`;

        execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" });
        execFileSync("docker", [
            "run", "-d", "--name", name, "-p", `${port}:6379`,
            "redis:7-alpine", "redis-server", "--save", "", "--appendonly", "no",
        ], { stdio: "ignore" });

        // Wait for Redis to accept connections.
        let ready = false;
        for (let i = 0; i < 30 && !ready; i += 1) {
            try {
                const out = execFileSync("docker", ["exec", name, "redis-cli", "ping"], { encoding: "utf8" });
                ready = out.trim() === "PONG";
            } catch (_) {
                await sleep(250);
            }
        }
        assert.ok(ready, "Redis container became ready");

        const pipeline = createPipeline({
            realRedis: true,
            env: { REDIS_URL: `redis://127.0.0.1:${port}`, BIGQUERY_WORKER_LEASE_MS: "1000" },
        });

        try {
            const N = 200;
            for (let i = 0; i < N; i += 1) {
                assert.equal(await runController(pipeline.services.controller, startEvent(`it-${i}`, { playableId: `pl-${i % 5}` })), 200);
            }

            const enqueued = await drainDiskToRedis(pipeline.services);
            assert.equal(enqueued, N);

            const xlen = execFileSync("docker", ["exec", name, "redis-cli", "XLEN", "pixel:events"], { encoding: "utf8" }).trim();
            assert.equal(Number(xlen), N, "real Redis stream holds all N entries before worker drains");

            await runWorkerUntilDrained(pipeline.services, {
                streamLength: () => Number(execFileSync("docker", ["exec", name, "redis-cli", "XLEN", "pixel:events"], { encoding: "utf8" }).trim()),
                pendingCount: () => {
                    const out = execFileSync("docker", ["exec", name, "redis-cli", "XPENDING", "pixel:events", "pixel-workers"], { encoding: "utf8" }).trim();
                    return Number(out.split("\n")[0] || 0);
                },
            }, { timeoutMs: 60_000 });

            assert.equal(pipeline.bigquery.state.inserted.length, N, "every event inserted into BigQuery mock (zero loss)");
        } finally {
            await pipeline.cleanup();
            execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" });
        }
    }
);
