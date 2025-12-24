const express = require("express");
const cors = require("cors");
const helmet = require("helmet");

const { trustProxy } = require("./config");
const pixelRoutes = require("./routes/pixel.route");
const healthRoutes = require("./routes/health.route");

const app = express();

app.set("trust proxy", trustProxy);
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: "*", methods: ["GET"] }));

app.use("/", healthRoutes);
app.use("/", pixelRoutes);

module.exports = app;
