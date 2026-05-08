const cluster = require("cluster");
const app = require("./app");
const {
    startDispatcher,
    stopDispatcher,
} = require("./services/request-dispatcher.service");
const {
    PORT,
    webConcurrency,
} = require("./config");

const resolveWorkerCount = () => Math.max(1, webConcurrency);

const startHttpServer = () => {
    startDispatcher().catch((error) => {
        console.error(`Request dispatcher failed: ${error.message}`);
    });

    app.listen(PORT, () => {
        console.log(`Pixel server running on port ${PORT} (pid: ${process.pid})`);
    });
};

const workerCount = resolveWorkerCount();

const shutdown = () => {
    stopDispatcher();
    process.exit(0);
};

if (workerCount <= 1) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
    startHttpServer();
} else if (cluster.isPrimary) {
    console.log(`Starting pixel server cluster with ${workerCount} workers`);

    for (let index = 0; index < workerCount; index += 1) {
        cluster.fork();
    }

    cluster.on("exit", (worker, code, signal) => {
        console.error(`Worker ${worker.process.pid} exited (code=${code}, signal=${signal}); restarting`);
        cluster.fork();
    });
} else {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
    startHttpServer();
}
