const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const loadLogService = () => {
    delete require.cache[require.resolve("../src/services/log.service")];
    return require("../src/services/log.service");
};

test("writeDailyBatch writes NDJSON lines under a daily log file", () => {
    const previousLogDir = process.env.PIXEL_LOG_DIR;
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-logs-"));

    try {
        process.env.PIXEL_LOG_DIR = tempDir;
        const logService = loadLogService();

        logService.writeDailyBatch("redis-queue", [
            {
                level: "info",
                type: "redis-enqueue",
                stream: "pixel:events",
                data: { name: "tap" },
            },
        ]);

        const now = new Date();
        const day = [
            now.getFullYear(),
            String(now.getMonth() + 1).padStart(2, "0"),
            String(now.getDate()).padStart(2, "0"),
        ].join("-");
        const logFile = path.join(tempDir, "redis-queue", `${day}.ndjson`);
        const lines = fs.readFileSync(logFile, "utf8").trim().split("\n");
        const entry = JSON.parse(lines[0]);

        assert.equal(lines.length, 1);
        assert.equal(entry.type, "redis-enqueue");
        assert.equal(entry.stream, "pixel:events");
        assert.equal(entry.data.name, "tap");
        assert.equal(typeof entry.ts, "string");
    } finally {
        if (previousLogDir === undefined) {
            delete process.env.PIXEL_LOG_DIR;
        } else {
            process.env.PIXEL_LOG_DIR = previousLogDir;
        }
        delete require.cache[require.resolve("../src/services/log.service")];
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
});

test("writeDailyBatch splits entries into the daily file matching each timestamp", () => {
    const previousLogDir = process.env.PIXEL_LOG_DIR;
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pixel-logs-"));

    try {
        process.env.PIXEL_LOG_DIR = tempDir;
        const logService = loadLogService();

        logService.writeDailyBatch("redis-queue", [
            {
                ts: "2026-06-09T12:00:00.000",
                type: "redis-enqueue",
                session_id: "day-1",
            },
            {
                ts: "2026-06-10T12:00:00.000",
                type: "redis-enqueue",
                session_id: "day-2",
            },
        ]);

        const dayOneFile = path.join(tempDir, "redis-queue", "2026-06-09.ndjson");
        const dayTwoFile = path.join(tempDir, "redis-queue", "2026-06-10.ndjson");
        const dayOneEntry = JSON.parse(fs.readFileSync(dayOneFile, "utf8").trim());
        const dayTwoEntry = JSON.parse(fs.readFileSync(dayTwoFile, "utf8").trim());

        assert.equal(dayOneEntry.session_id, "day-1");
        assert.equal(dayTwoEntry.session_id, "day-2");
    } finally {
        if (previousLogDir === undefined) {
            delete process.env.PIXEL_LOG_DIR;
        } else {
            process.env.PIXEL_LOG_DIR = previousLogDir;
        }
        delete require.cache[require.resolve("../src/services/log.service")];
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
});
