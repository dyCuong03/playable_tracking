const cluster = require("cluster");
const os = require("os");
const app = require("./app");
const {
    PORT,
    webConcurrency,
} = require("./config");

const resolveWorkerCount = () => {
    if (webConcurrency > 0) {
        return Math.max(1, webConcurrency);
    }

    if (typeof os.availableParallelism === "function") {
        return Math.max(1, os.availableParallelism());
    }

    return Math.max(1, os.cpus().length);
};

const startHttpServer = () => {
    app.listen(PORT, () => {
        console.log(`Pixel server running on port ${PORT} (pid: ${process.pid})`);
    });
};

if (cluster.isPrimary) {
    const workerCount = resolveWorkerCount();

    console.log(`Starting pixel server cluster with ${workerCount} workers`);

    for (let index = 0; index < workerCount; index += 1) {
        cluster.fork();
    }

    cluster.on("exit", (worker, code, signal) => {
        console.error(`Worker ${worker.process.pid} exited (code=${code}, signal=${signal}); restarting`);
        cluster.fork();
    });
} else {
    startHttpServer();
}
