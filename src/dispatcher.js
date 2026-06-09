const {
    startDispatcher,
    stopDispatcher,
} = require("./services/request-dispatcher.service");
const logService = require("./services/log.service");

const shutdown = () => {
    stopDispatcher();
};

if (require.main === module) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    startDispatcher().catch((error) => {
        const entry = {
            level: "error",
            type: "dispatcher-startup",
            message: "Failed to start dispatcher",
            reason: error.message,
        };
        console.error(JSON.stringify({
            ts: new Date().toISOString(),
            ...entry,
        }));
        logService.writeDaily("dispatcher", entry, true);
        process.exit(1);
    });
}

module.exports = {
    shutdown,
};
