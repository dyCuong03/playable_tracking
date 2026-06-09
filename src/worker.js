const { startWorker, stopWorker } = require("./services/bigquery-worker.service");

const shutdown = () => {
    stopWorker();
};

if (require.main === module) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    startWorker().catch((error) => {
        console.error(JSON.stringify({
            ts: new Date().toISOString(),
            level: "error",
            type: "bigquery-worker-startup",
            message: "Failed to start BigQuery worker",
            reason: error.message,
        }));
        process.exit(1);
    });
}

module.exports = {
    shutdown,
};
