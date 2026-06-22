require("./helpers/isolate-queue-dir"); // MUST be first: redirect disk queue to a temp dir before src loads
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");

const {
    enqueueEvent,
    getQueueStats,
    resetQueueState,
    rotatePendingFiles,
} = require("../src/services/bigquery-queue.service");

test.beforeEach(() => resetQueueState());
test.after(() => resetQueueState());

test("rotatePendingFiles ignores ENOENT when another dispatcher already moved the shard", async () => {
    const originalRename = fs.promises.rename;

    try {
        await enqueueEvent({
            tableName: "pixel_events_ver_2",
            row: {
                event_hash: "hash-1",
                payload: "x".repeat(300_000),
            },
        });

        fs.promises.rename = async (source, destination) => {
            if (
                /[\\/]pending[\\/]/.test(String(source)) &&
                /[\\/]ready[\\/]/.test(String(destination))
            ) {
                const error = new Error("simulated race");
                error.code = "ENOENT";
                throw error;
            }

            return originalRename(source, destination);
        };

        const rotated = await rotatePendingFiles();
        const stats = await getQueueStats();

        assert.deepEqual(rotated, []);
        assert.equal(stats.ready.fileCount, 0);
    } finally {
        fs.promises.rename = originalRename;
    }
});
