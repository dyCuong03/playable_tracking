const { startWorker, stopWorker } = require("./services/bigquery-worker.service");

const shutdown = () => {
    stopWorker();
};

if (require.main === module) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    startWorker().catch((error) => {
        console.error(error.message);
        process.exit(1);
    });
}

module.exports = {
    shutdown,
};
