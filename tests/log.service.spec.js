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
