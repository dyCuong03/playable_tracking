const {
    startDispatcher,
    stopDispatcher,
} = require("./services/request-dispatcher.service");

const shutdown = () => {
    stopDispatcher();
};

if (require.main === module) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    startDispatcher().catch((error) => {
        console.error(error.message);
        process.exit(1);
    });
}

module.exports = {
    shutdown,
};
