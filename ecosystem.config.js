module.exports = {
    apps: [{
        name: "pixel-server",
        script: "server.js",
        instances: 1,
        exec_mode: "fork",
        env: {
            NODE_ENV: "production",
            PORT: 8080
        }
    }]
};
