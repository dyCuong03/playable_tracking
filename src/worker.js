const { startWorker, stopWorker } = require("./services/bigquery-worker.service");

const shutdown = () => {
    stopWorker();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

startWorker().catch((error) => {
    console.error(error.message);
    process.exit(1);
});
