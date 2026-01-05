const crypto = require("crypto");
const { BigQuery } = require("@google-cloud/bigquery");
const {
    bigQueryDataset,
    bigQueryTable,
    bigQueryEnabled,
} = require("../config");

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

const logInsertError = (error, row) => {
    console.error(
        JSON.stringify({
            level: "error",
            type: "bigquery-insert",
            message: error.message,
            eventHash: row.event_hash,
        })
    );
};

const insertEvent = (event) => {
    if (!isConfigured) {
        return Promise.resolve(false);
    }

    const row = buildRow(event);

    try {
        return getTable()
            .insert([row])
            .then(() => true)
            .catch((error) => {
                logInsertError(error, row);
                return false;
            });
    } catch (error) {
        logInsertError(error, row);
        return Promise.resolve(false);
    }
};

module.exports = {
    insertEvent,
    buildRow,
    hashEvent,
};
