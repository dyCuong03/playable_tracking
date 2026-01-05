require("dotenv").config();

const parseBoolean = (value, fallback = false) => {
    if (value === undefined) {
        return fallback;
    }

    if (typeof value === "boolean") {
        return value;
    }

    return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
};

module.exports = {
    PORT: process.env.PORT || 8080,
    NODE_ENV: process.env.NODE_ENV || "development",
    trustProxy: true,
    bigQueryEnabled: parseBoolean(process.env.BIGQUERY_ENABLED, false),
    bigQueryDataset: process.env.BIGQUERY_DATASET || "",
    bigQueryTable: process.env.BIGQUERY_TABLE || "",
};
