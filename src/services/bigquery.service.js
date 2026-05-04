const crypto = require("crypto");
const { BigQuery } = require("@google-cloud/bigquery");
const {
    bigQueryDataset,
    bigQueryTable,
    bigQueryEnabled,
} = require("../config");
const logService = require("./log.service");

let client;
const tableRefs = new Map();

const TABLE_BY_ENVIRONMENT = {
    production: "pixel_events_production",
    test: "pixel_events_ver_2",
};

const isConfigured = Boolean(
    bigQueryEnabled &&
    bigQueryDataset
);

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
    campaign_raw: JSON.stringify(event.campaignRaw || {}),
    event_params: JSON.stringify(event.params || {}),
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

const insertBatch = (tableName, rows) => {
    return getTable(tableName).insert(
        rows.map((row) => ({
            insertId: row.event_hash,
            json: row,
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
    hashEvent,
    isBigQueryConfigured: () => isConfigured,
    resolveTableName,
    logInsertError,
};
