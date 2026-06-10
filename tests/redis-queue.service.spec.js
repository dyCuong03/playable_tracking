const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const Module = require("module");

const clearQueueModules = () => {
    [
        "../src/services/redis-queue.service",
        "../src/services/log.service",
        "../src/config",
        "../src/config/env",
    ].forEach((modulePath) => {
        try {
            delete require.cache[require.resolve(modulePath)];
        } catch (_) {
            // Module may not have been loaded yet.
        }
    });
};

const withFakeRedis = async (handler) => {
    const originalLoad = Module._load;
    const commands = [];
    let nextId = 1;

    Module._load = function load(request, parent, isMain) {
        if (request !== "redis") {
            return originalLoad.apply(this, arguments);
        }

        return {
            createClient: () => ({
                isOpen: false,
                on: () => {},
                removeAllListeners: () => {},
                disconnect: () => {},
                connect: async function connect() {
                    this.isOpen = true;
                    return this;
                },
                sendCommand: async (args) => {
                    commands.push(args);
                    return `${nextId++}-0`;
                },
                MULTI: () => {
                    const pipelineCommands = [];

                    return {
                        addCommand: (args) => {
                            pipelineCommands.push(args);
                        },
                        execAsPipeline: async () => {
                            commands.push(...pipelineCommands);
                            return pipelineCommands.map(() => `${nextId++}-0`);
                        },
                    };
                },
            }),
        };
    };

    try {
        await handler(commands);
    } finally {
        Module._load = originalLoad;
        clearQueueModules();
    }
};

test("enqueueEvent writes exact Redis payload to the daily redis queue audit log", async () => {
    const previousLogDir = process.env.PIXEL_LOG_DIR;
    const previousRedisUrl = process.env.REDIS_URL;
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-redis-logs-"));

    try {
        process.env.PIXEL_LOG_DIR = tempDir;
        process.env.REDIS_URL = "redis://fake:6379";
        clearQueueModules();

        await withFakeRedis(async (commands) => {
            const { enqueueEvent } = require("../src/services/redis-queue.service");

            await enqueueEvent({
                tableName: "pixel_events_ver_2",
                row: {
                    event_hash: "hash-1",
                    session_id: "session-1",
                    event_name: "interaction",
                    event_time: "2026-06-10T01:02:03.000Z",
                    event_params: {
                        name: "tap",
                    },
                    received_at: "2026-06-10T01:02:04.000Z",
                },
                urlData: {
                    request_uri: "/p.gif?event_name=interaction&session_id=session-1",
                    query: {
                        event_name: "interaction",
                        session_id: "session-1",
                    },
                    event_params_parsed: {
                        name: "tap",
                    },
                },
            });

            const xadd = commands.find((args) => args[0] === "XADD");
            assert.ok(xadd, "XADD command was not sent");

            const payload = JSON.parse(xadd[xadd.length - 1]);
            assert.equal(payload.attempts, 0);
            assert.equal(typeof payload.enqueuedAt, "string");
            assert.equal(payload.row.session_id, "session-1");

            const now = new Date();
            const day = [
                now.getFullYear(),
                String(now.getMonth() + 1).padStart(2, "0"),
                String(now.getDate()).padStart(2, "0"),
            ].join("-");
            const logFile = path.join(tempDir, "redis-queue", `${day}.ndjson`);
            const entry = JSON.parse(fs.readFileSync(logFile, "utf8").trim());
            const auditPayload = JSON.parse(entry.redis_payload);

            assert.equal(entry.type, "redis-enqueue");
            assert.equal(entry.stream, "pixel:events");
            assert.equal(entry.message_id, "1-0");
            assert.deepEqual(auditPayload, payload);
            assert.deepEqual(entry.queue_item, payload);
            assert.equal(entry.data.query.session_id, "session-1");
        });
    } finally {
        if (previousLogDir === undefined) {
            delete process.env.PIXEL_LOG_DIR;
        } else {
            process.env.PIXEL_LOG_DIR = previousLogDir;
        }

        if (previousRedisUrl === undefined) {
            delete process.env.REDIS_URL;
        } else {
            process.env.REDIS_URL = previousRedisUrl;
        }

        clearQueueModules();
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
});
