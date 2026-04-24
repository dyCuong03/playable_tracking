module.exports = {
    apps: [
        {
            name: "pixel-server",
            script: "src/server.js",
            instances: 1,
            exec_mode: "fork",
            env: {
                NODE_ENV: "production",
                PORT: 8080,
            },
        },
        {
            name: "pixel-worker",
            script: "src/worker.js",
            instances: 1,
            exec_mode: "fork",
            env: {
                NODE_ENV: "production",
            },
        },
    ],
};
