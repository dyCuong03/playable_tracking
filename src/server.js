const cluster = require("cluster");
const app = require("./app");
const { stopDispatcher } = require("./services/request-dispatcher.service");
const logService = require("./services/log.service");
const {
    PORT,
    webConcurrency,
    requestQueueBridgeEnabled,
} = require("./config");

const resolveWorkerCount = () => Math.max(1, webConcurrency);

const logServerRuntime = (level, type, message, details = {}) => {
    const entry = {
        level,
        type,
        message,
        pid: process.pid,
        ...details,
    };

    const payload = {
        ts: new Date().toISOString(),
        ...entry,
    };

    if (level === "error") {
        console.error(JSON.stringify(payload));
    } else if (level === "warn") {
        console.warn(JSON.stringify(payload));
    } else {
        console.log(JSON.stringify(payload));
    }

    logService.writeDaily("server", entry, true);
};

const startHttpServer = () => {
    app.listen(PORT, () => {
        logServerRuntime("info", "pixel-server-start", "Pixel server running", {
            port: PORT,
            requestQueueBridgeEnabled,
        });
        if (requestQueueBridgeEnabled) {
            logServerRuntime(
                "warn",
                "pixel-server-queue-bridge",
                "REQUEST_QUEUE_BRIDGE_ENABLED is true in web server; prefer dedicated dispatcher worker"
            );
        }
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
    logServerRuntime("info", "pixel-server-cluster-start", "Starting pixel server cluster", {
        workerCount,
    });

    for (let index = 0; index < workerCount; index += 1) {
        cluster.fork();
    }

    cluster.on("exit", (worker, code, signal) => {
        logServerRuntime("error", "pixel-server-worker-exit", "Cluster worker exited; restarting", {
            workerPid: worker.process.pid,
            code,
            signal,
        });
        cluster.fork();
    });
} else {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
    startHttpServer();
}
