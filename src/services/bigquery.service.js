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
        sid: event.sid || "",
        event: event.event || "",
        eventTime: event.eventTime || "",
        params: event.params || {},
    });

    return crypto.createHash("sha256").update(payload).digest("hex");
};

const buildRow = (event) => ({
    session_id: event.sid,
    event_name: event.event,
    event_time: event.eventTime,
    package_name: event.packageName || "",
    playable_id: event.playableId || "",
    ip: event.ip || "",
    referer: event.referer || "",
    received_at: new Date().toISOString(),
    event_params: event.params || {},
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

const previewValue = (value) => {
    try {
        if (typeof value === "string") {
            return value.length > 200 ? `${value.slice(0, 200)}...` : value;
        }

        const serialized = JSON.stringify(value);
        return serialized.length > 200 ? `${serialized.slice(0, 200)}...` : serialized;
    } catch (_) {
        return String(value);
    }
};

const isValidTimestamp = (value) => {
    if (value instanceof Date) {
        return Number.isFinite(value.getTime());
    }

    if (typeof value !== "string" || !value.trim()) {
        return false;
    }

    return Number.isFinite(Date.parse(value));
};

const normalizeValueForBigQueryType = (value, fieldType, options = {}) => {
    const {
        jsonMode = "string",
    } = options;
    const normalizedType = String(fieldType || "").toUpperCase();

    if (normalizedType === "JSON") {
        const normalizedJsonValue = value === undefined || value === null
            ? {}
            : parseJSONValue(value);

        if (jsonMode === "native") {
            return normalizedJsonValue;
        }

        return JSON.stringify(normalizedJsonValue);
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

const formatRowForInsert = (row, fieldTypes = new Map(), options = {}) => Object.keys(row || {}).reduce((acc, key) => {
    const normalizedKey = String(key).toLowerCase();

    if (!fieldTypes.has(normalizedKey)) {
        return acc;
    }

    acc[key] = normalizeValueForBigQueryType(
        row[key],
        fieldTypes.get(normalizedKey),
        options
    );
    return acc;
}, {});

const buildInsertRows = (rows, fieldTypes, options = {}) => rows.map((row) => ({
    insertId: row.event_hash,
    json: formatRowForInsert(row, fieldTypes, options),
}));

const validateValueForBigQueryType = (value, fieldType, options = {}) => {
    const {
        jsonMode = "string",
    } = options;
    const normalizedType = String(fieldType || "").toUpperCase();

    if (!normalizedType) {
        return null;
    }

    if (normalizedType === "TIMESTAMP" && !isValidTimestamp(value)) {
        return "Expected ISO timestamp string";
    }

    if (normalizedType === "JSON") {
        if (jsonMode === "native") {
            if (value === undefined) {
                return "Expected JSON-compatible value";
            }

            try {
                JSON.stringify(value);
                return null;
            } catch (_) {
                return "Expected JSON-compatible value";
            }
        }

        try {
            if (typeof value !== "string") {
                return "Expected JSON-encoded string payload";
            }

            JSON.parse(value);
        } catch (_) {
            return "Expected valid JSON text";
        }
    }

    if (normalizedType === "STRING" && typeof value !== "string") {
        return "Expected string value";
    }

    return null;
};

const validateFormattedRowForInsert = (row, fieldTypes = new Map(), options = {}) => Object.keys(row || {}).reduce((acc, key) => {
    const fieldType = fieldTypes.get(String(key).toLowerCase());
    const issue = validateValueForBigQueryType(row[key], fieldType, options);

    if (issue) {
        acc.push({
            field: key,
            type: fieldType || "UNKNOWN",
            issue,
            valuePreview: previewValue(row[key]),
        });
    }

    return acc;
}, []);

const createValidationError = (tableName, invalidRows) => {
    const error = new Error("BigQuery row validation failed before insert");
    error.code = "BIGQUERY_ROW_VALIDATION";
    error.details = {
        tableName,
        invalidRows,
    };
    return error;
};

const summarizeFieldTypes = (fieldTypes = new Map()) => Array.from(fieldTypes.entries()).reduce((acc, [key, value]) => {
    acc[key] = value;
    return acc;
}, {});

const buildInsertDiagnostics = (tableName, insertRows, fieldTypes, options = {}) => ({
    tableName,
    jsonMode: options.jsonMode || "string",
    fieldTypes: summarizeFieldTypes(fieldTypes),
    sample: insertRows.slice(0, 1).map((entry) => ({
        insertId: entry.insertId || null,
        json: entry.json,
    })),
});

const getValueType = (value) => {
    if (value === null) {
        return "null";
    }

    if (Array.isArray(value)) {
        return "array";
    }

    if (value instanceof Date) {
        return "date";
    }

    return typeof value;
};

const buildSampleTypes = (row) => Object.keys(row || {}).reduce((acc, key) => {
    acc[key] = getValueType(row[key]);
    return acc;
}, {});

const attachErrorDetails = (error, details) => {
    if (!error || !details) {
        return error;
    }

    error.details = {
        ...(error.details || {}),
        ...details,
    };

    return error;
};

const assertValidInsertRows = (tableName, insertRows, fieldTypes) => {
    const invalidRows = insertRows.reduce((acc, entry) => {
        const issues = validateFormattedRowForInsert(entry.json, fieldTypes);

        if (issues.length > 0) {
            acc.push({
                insertId: entry.insertId || null,
                issues,
            });
        }

        return acc;
    }, []);

    if (invalidRows.length > 0) {
        throw createValidationError(tableName, invalidRows);
    }
};

const assertValidFormattedRows = (tableName, rows, fieldTypes, options = {}) => {
    const invalidRows = rows.reduce((acc, row) => {
        const issues = validateFormattedRowForInsert(row, fieldTypes, options);

        if (issues.length > 0) {
            acc.push({
                issues,
            });
        }

        return acc;
    }, []);

    if (invalidRows.length > 0) {
        throw createValidationError(tableName, invalidRows);
    }
};

const buildRowErrorDetails = (error) => {
    if (!Array.isArray(error && error.errors)) {
        return [];
    }

    return error.errors.map((rowError) => ({
        insertId: rowError && rowError.row ? rowError.row.insertId || null : null,
        reasons: Array.isArray(rowError && rowError.errors)
            ? rowError.errors.map((entry) => ({
                reason: entry && entry.reason ? entry.reason : null,
                message: entry && entry.message ? entry.message : null,
                location: entry && entry.location ? entry.location : null,
            }))
            : [],
    }));
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

const logInsertError = (error, row, logEntry, event, tableName) => {
    const entry = normalizeLogEntry(event, row, logEntry);
    const details = error && error.details ? error.details : null;
    const rowErrors = buildRowErrorDetails(error);

    logService.write(
        {
            ...entry,
            bigquery_table: tableName,
            bigquery_status: "failed",
            bigquery_error: {
                message: error.message,
                code: error.code || null,
                details,
                rowErrors,
            },
        },
        true
    );

    console.error(
        JSON.stringify({
            ts: new Date().toISOString(),
            level: "error",
            type: "bigquery-insert",
            message: error.message,
            tableName,
            eventHash: (row && row.event_hash) || null,
            details,
            rowErrors,
        })
    );
};

const insertBatch = async (tableName, rows) => {
    const fieldTypes = await getTableSchema(tableName);
    const table = getTable(tableName);
    const formattedRows = rows.map((row) => formatRowForInsert(row, fieldTypes, { jsonMode: "string" }));

    assertValidFormattedRows(tableName, formattedRows, fieldTypes, { jsonMode: "string" });

    console.error(JSON.stringify({
        ts: new Date().toISOString(),
        level: "info",
        type: "bigquery-debug",
        message: "Prepared BigQuery insert payload",
        tableName,
        fieldTypes: Object.fromEntries(fieldTypes.entries()),
        sample: formattedRows[0] || null,
        sampleTypes: buildSampleTypes(formattedRows[0] || {}),
    }));

    return table.insert(formattedRows).catch((error) => {
        attachErrorDetails(
            error,
            buildInsertDiagnostics(
                tableName,
                formattedRows.map((row, index) => ({
                    insertId: rows[index] && rows[index].event_hash ? rows[index].event_hash : null,
                    json: row,
                })),
                fieldTypes,
                { jsonMode: "string" }
            )
        );
        throw error;
    });
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
    validateFormattedRowForInsert,
};
