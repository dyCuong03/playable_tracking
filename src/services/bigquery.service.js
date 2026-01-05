const crypto = require("crypto");
const { BigQuery } = require("@google-cloud/bigquery");
const {
    bigQueryDataset,
    bigQueryTable,
    bigQueryEnabled,
} = require("../config");
const logService = require("./log.service");

let client;
let tableRef;
const isConfigured = Boolean(
    bigQueryEnabled &&
    bigQueryDataset &&
    bigQueryTable
);

const getClient = () => {
    if (!client) {
        client = new BigQuery();
    }

    return client;
};

const getTable = () => {
    if (!tableRef) {
        tableRef = getClient().dataset(bigQueryDataset).table(bigQueryTable);
    }

    return tableRef;
};

const hashEvent = (event) => {
    const payload = JSON.stringify({
        event: event.event || "",
        pid: event.pid || "",
        playableId: event.playableId || "",
        sid: event.sid || "",
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
    project_id: event.pid,
    playable_id: event.playableId || "",
    session_id: event.sid,
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

    return payload;
};

const logInsertError = (error, row, logEntry, event) => {
    const entry = normalizeLogEntry(event, row, logEntry);

    logService.write(
        {
            ...entry,
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
            eventHash: (row && row.event_hash) || null,
        })
    );
};

const insertEvent = (event, row, logEntry) => {
    if (!isConfigured) {
        return Promise.resolve(false);
    }

    const payload = row || buildRow(event);

    try {
        return getTable()
            .insert([payload])
            .then(() => true)
            .catch((error) => {
                logInsertError(error, payload, logEntry, event);
                return false;
            });
    } catch (error) {
        logInsertError(error, payload, logEntry, event);
        return Promise.resolve(false);
    }
};

module.exports = {
    insertEvent,
    buildRow,
    hashEvent,
};
