const crypto = require("crypto");
const fs = require("fs");
const { BigQuery } = require("@google-cloud/bigquery");
const {
    bigQueryDataset,
    bigQueryTable,
    bigQueryEnabled,
} = require("../config");
const logService = require("./log.service");

let client;
const tableRefs = new Map();
const tableSchemaRefs = new Map();

const TABLE_BY_ENVIRONMENT = {
    production: "pixel_events_production",
    test: "pixel_events_ver_2",
};

const isConfigured = Boolean(
    bigQueryEnabled &&
    bigQueryDataset
);

const getBigQueryStatus = () => {
    const credentialsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || "";
    const credentialsPresent = credentialsPath
        ? fs.existsSync(credentialsPath)
        : false;

    return {
        enabled: bigQueryEnabled,
        dataset: bigQueryDataset || "",
        defaultTable: bigQueryTable || "",
        configured: isConfigured,
        credentialsPath,
        credentialsPresent,
        issues: [
            !bigQueryEnabled ? "BIGQUERY_ENABLED is false" : null,
            !bigQueryDataset ? "BIGQUERY_DATASET is empty" : null,
            !credentialsPath ? "GOOGLE_APPLICATION_CREDENTIALS is empty" : null,
            (credentialsPath && !credentialsPresent) ? "GOOGLE_APPLICATION_CREDENTIALS file not found" : null,
        ].filter(Boolean),
    };
};

const getClient = () => {
    if (!client) {
        client = new BigQuery();
    }

    return client;
};

const getTable = (tableName) => {
    if (!tableRefs.has(tableName)) {
        tableRefs.set(
            tableName,
            getClient().dataset(bigQueryDataset).table(tableName)
        );
    }

    return tableRefs.get(tableName);
};

const resolveTableName = (event) => {
    const trackingEnvironment = String(event && event.trackingEnvironment ? event.trackingEnvironment : "")
        .trim()
        .toLowerCase();

    if (TABLE_BY_ENVIRONMENT[trackingEnvironment]) {
        return TABLE_BY_ENVIRONMENT[trackingEnvironment];
    }

    if (bigQueryTable) {
        return bigQueryTable;
    }

    return TABLE_BY_ENVIRONMENT.test;
};

const hashEvent = (event) => {
    const payload = JSON.stringify({
        requestTime: event.time || "",
        clientTimestamp: event.clientTimestamp || "",
        event: event.event || "",
        pid: event.pid || "",
        playableId: event.playableId || "",
        sid: event.sid || "",
        platform: event.platform || "",
        trackingEnvironment: event.trackingEnvironment || "test",
        campaignRaw: event.campaignRaw || {},
        params: event.params || {},
        ip: event.ip || "",
        ua: event.ua || "",
        ref: event.ref || "",
    });

    return crypto.createHash("sha256").update(payload).digest("hex");
};

const buildRow = (event) => ({
    event_time: event.time,
    event_name: event.event,
    package_name: event.pid,
    playable_id: event.playableId || "",
    session_id: event.sid,
    platform: event.platform || "",
    campaign_raw: event.campaignRaw || {},
    event_params: event.params || {},
    ip: event.ip,
    user_agent: event.ua,
    referer: event.ref,
    received_at: new Date().toISOString(),
    event_hash: hashEvent(event),
});

const safeParseJSON = (value) => {
    if (!value || typeof value !== "string") {
        return {};
    }

    try {
        return JSON.parse(value);
    } catch (error) {
        return {};
    }
};

const isPlainObject = (value) => typeof value === "object" && value !== null && !Array.isArray(value);

const isJSONStringCandidate = (value) => {
    const normalized = String(value || "").trim();

    if (!normalized) {
        return false;
    }

    return (
        normalized.startsWith("{") ||
        normalized.startsWith("[") ||
        normalized === "null" ||
        normalized === "true" ||
        normalized === "false" ||
        /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(normalized)
    );
};

const parseJSONValue = (value) => {
    if (typeof value !== "string") {
        return value;
    }

    if (!isJSONStringCandidate(value)) {
        return value;
    }

    try {
        return JSON.parse(value);
    } catch (_) {
        return value;
    }
};

const normalizeValueForBigQueryType = (value, fieldType) => {
    const normalizedType = String(fieldType || "").toUpperCase();

    if (normalizedType === "JSON") {
        if (value === undefined || value === null) {
            return "{}";
        }

        return JSON.stringify(parseJSONValue(value));
    }

    if (normalizedType === "STRING") {
        if (value === undefined || value === null) {
            return "";
        }

        if (typeof value === "string") {
            return value;
        }

        if (Array.isArray(value) || isPlainObject(value)) {
            return JSON.stringify(value);
        }

        return String(value);
    }

    return value;
};

const getTableSchema = async (tableName) => {
    if (!tableSchemaRefs.has(tableName)) {
        tableSchemaRefs.set(
            tableName,
            getTable(tableName)
                .getMetadata()
                .then(([metadata]) => {
                    const fields = (((metadata || {}).schema || {}).fields || []);

                    return fields.reduce((acc, field) => {
                        if (field && field.name) {
                            acc.set(String(field.name).toLowerCase(), String(field.type || "").toUpperCase());
                        }

                        return acc;
                    }, new Map());
                })
                .catch((error) => {
                    tableSchemaRefs.delete(tableName);
                    throw error;
                })
        );
    }

    return tableSchemaRefs.get(tableName);
};

const formatRowForInsert = (row, fieldTypes = new Map()) => Object.keys(row || {}).reduce((acc, key) => {
    acc[key] = normalizeValueForBigQueryType(
        row[key],
        fieldTypes.get(String(key).toLowerCase())
    );
    return acc;
}, {});

const normalizeLogEntry = (event, row, logEntry) => {
    if (logEntry) {
        return logEntry;
    }

    const payload = { ...(row || (event ? buildRow(event) : {})) };

    if (
        typeof payload.event_params !== "object" ||
        payload.event_params === null
    ) {
        payload.event_params = (event && event.params) || safeParseJSON(payload.event_params);
    }

    if (
        typeof payload.campaign_raw !== "object" ||
        payload.campaign_raw === null
    ) {
        payload.campaign_raw = (event && event.campaignRaw) || safeParseJSON(payload.campaign_raw);
    }

    return payload;
};

const logInsertError = (error, row, logEntry, event, tableName) => {
    const entry = normalizeLogEntry(event, row, logEntry);

    logService.write(
        {
            ...entry,
            bigquery_table: tableName,
            bigquery_status: "failed",
            bigquery_error: {
                message: error.message,
                code: error.code || null,
            },
        },
        true
    );

    console.error(
        JSON.stringify({
            level: "error",
            type: "bigquery-insert",
            message: error.message,
            tableName,
            eventHash: (row && row.event_hash) || null,
        })
    );
};

const insertBatch = async (tableName, rows) => {
    const fieldTypes = await getTableSchema(tableName);

    return getTable(tableName).insert(
        rows.map((row) => ({
            insertId: row.event_hash,
            json: formatRowForInsert(row, fieldTypes),
        })),
        { raw: true }
    );
};

const insertEvent = (event, row, logEntry) => {
    if (!isConfigured) {
        return Promise.resolve(false);
    }

    const payload = row || buildRow(event);
    const tableName = resolveTableName(event);

    try {
        return insertBatch(tableName, [payload])
            .then(() => true)
            .catch((error) => {
                logInsertError(error, payload, logEntry, event, tableName);
                return false;
            });
    } catch (error) {
        logInsertError(error, payload, logEntry, event, tableName);
        return Promise.resolve(false);
    }
};

module.exports = {
    insertEvent,
    insertBatch,
    buildRow,
    formatRowForInsert,
    getBigQueryStatus,
    hashEvent,
    isBigQueryConfigured: () => isConfigured,
    normalizeValueForBigQueryType,
    resolveTableName,
    logInsertError,
};
