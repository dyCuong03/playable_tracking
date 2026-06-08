#!/usr/bin/env node
/**
 * ops/bin/bq-upload.js
 *
 * BigQuery NDJSON uploader for ops metrics (nginx + redis).
 *
 * Usage:
 *   node ops/bin/bq-upload.js --table <project.dataset.table> --file <path.ndjson> [--dry-run] [--create]
 *
 * Options:
 *   --table    BigQuery table in project.dataset.table format (required)
 *   --file     Path to NDJSON file, one JSON object per line with an insert_id field
 *   --dry-run  Validate + count rows; do NOT insert
 *   --create   Create table if not exists using the matching schema SQL before uploading
 *   --help     Print usage
 *
 * Output: ONE JSON line to stdout:
 *   { "uploaded": N, "errors": N, "status": "ok|auth_missing|config_missing|failed|dry_run", "message": "..." }
 *
 * Exit 0  - success, dry_run, or graceful degrade (auth/config missing)
 * Exit 1+ - unexpected crash only
 *
 * Auth: GOOGLE_APPLICATION_CREDENTIALS env var (same as src/services/bigquery.service.js).
 * Never prints credential contents.
 */

"use strict";

const fs   = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

const argv = process.argv.slice(2);

function getArg(name) {
    const idx = argv.indexOf(name);
    return idx !== -1 ? (argv[idx + 1] || null) : null;
}

function hasFlag(name) {
    return argv.includes(name);
}

const tableArg  = getArg("--table");
const fileArg   = getArg("--file");
const dryRun    = hasFlag("--dry-run");
const doCreate  = hasFlag("--create");
const showHelp  = hasFlag("--help") || hasFlag("-h");

// ---------------------------------------------------------------------------
// Output helpers (all non-result output goes to stderr)
// ---------------------------------------------------------------------------

function printResult(result) {
    process.stdout.write(JSON.stringify(result) + "\n");
}

function logErr(obj) {
    process.stderr.write(JSON.stringify(obj) + "\n");
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

if (showHelp) {
    process.stderr.write([
        "Usage: node ops/bin/bq-upload.js --table <project.dataset.table> --file <ndjson> [--dry-run] [--create]",
        "",
        "Options:",
        "  --table    project.dataset.table  (required)",
        "  --file     NDJSON file path; each line must be valid JSON with an insert_id field",
        "  --dry-run  validate + count rows, skip insert",
        "  --create   create table if not exists (uses ops/bigquery/schema/<table>.sql)",
        "  --help     show this help",
        "",
        "Exit 0 on success or graceful degrade; nonzero only on unexpected crash.",
        "",
        "Status values:",
        "  ok             - upload completed (or no rows)",
        "  dry_run        - --dry-run completed, no insert",
        "  auth_missing   - GOOGLE_APPLICATION_CREDENTIALS not set / file not found",
        "  config_missing - --table or --file missing / malformed",
        "  failed         - insert or create failed (partial or full)",
    ].join("\n") + "\n");
    printResult({ uploaded: 0, errors: 0, status: "ok", message: "help shown" });
    process.exit(0);
}

// ---------------------------------------------------------------------------
// Parse project.dataset.table
// ---------------------------------------------------------------------------

function parseTable(raw) {
    if (!raw || typeof raw !== "string") {
        return null;
    }
    const parts = raw.split(".");
    if (parts.length < 3 || parts.some((p) => !p.trim())) {
        return null;
    }
    return { projectId: parts[0].trim(), datasetId: parts[1].trim(), tableId: parts[2].trim() };
}

// ---------------------------------------------------------------------------
// Read NDJSON (sync — rows fit in ops metrics files, never huge)
// ---------------------------------------------------------------------------

function readNdjson(filePath) {
    const content = fs.readFileSync(filePath, "utf8");
    const lines   = content.split("\n").filter((l) => l.trim());
    return lines.map((line, i) => {
        try {
            return JSON.parse(line);
        } catch (err) {
            throw new Error("Invalid JSON on line " + (i + 1) + ": " + err.message);
        }
    });
}

// ---------------------------------------------------------------------------
// Locate schema SQL for a given table name suffix
// ---------------------------------------------------------------------------

const SCHEMA_DIR = path.resolve(__dirname, "..", "bigquery", "schema");

const TABLE_SCHEMA_MAP = {
    "nginx_requests": path.join(SCHEMA_DIR, "nginx_requests.sql"),
    "redis_metrics":  path.join(SCHEMA_DIR, "redis_metrics.sql"),
};

function findSchemaFile(tableId) {
    // Direct match
    if (TABLE_SCHEMA_MAP[tableId]) {
        return TABLE_SCHEMA_MAP[tableId];
    }
    // Suffix match (e.g. prod_nginx_requests → nginx_requests)
    for (const [suffix, filePath] of Object.entries(TABLE_SCHEMA_MAP)) {
        if (tableId.endsWith("_" + suffix) || tableId === suffix) {
            return filePath;
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// BigQuery client (lazy — mirrors bigquery.service.js pattern)
// ---------------------------------------------------------------------------

let _bqClient = null;

function getBigQueryClient(projectId) {
    if (!_bqClient) {
        const { BigQuery } = require("@google-cloud/bigquery");
        _bqClient = new BigQuery({ projectId });
    }
    return _bqClient;
}

// ---------------------------------------------------------------------------
// Create table via DDL query
// ---------------------------------------------------------------------------

async function createTable(projectId, datasetId, tableId) {
    const schemaFile = findSchemaFile(tableId);
    if (!schemaFile) {
        return {
            ok: false,
            message: "No schema SQL found for table '" + tableId + "'. Expected one of: " + Object.keys(TABLE_SCHEMA_MAP).join(", "),
        };
    }

    let ddl;
    try {
        ddl = fs.readFileSync(schemaFile, "utf8");
    } catch (err) {
        return { ok: false, message: "Cannot read schema file " + schemaFile + ": " + err.message };
    }

    // Substitute template placeholders: %%PROJECT%%, %%DATASET%%, %%TABLE%%
    const resolvedDdl = ddl
        .replace(/%%PROJECT%%/g, projectId)
        .replace(/%%DATASET%%/g, datasetId)
        .replace(/%%TABLE%%/g, tableId);

    try {
        const client = getBigQueryClient(projectId);
        const [job] = await client.createQueryJob({ query: resolvedDdl, location: "US" });
        await job.getQueryResults();
        logErr({ level: "info", type: "bq-create", message: "Table created (or already exists)", table: tableId });
        return { ok: true, message: "Table " + projectId + "." + datasetId + "." + tableId + " created (or already exists)" };
    } catch (err) {
        return { ok: false, message: "DDL query failed: " + err.message };
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
    // --- Validate --table ---
    if (!tableArg) {
        printResult({ uploaded: 0, errors: 0, status: "config_missing", message: "--table is required (format: project.dataset.table)" });
        process.exit(0);
    }

    const parsed = parseTable(tableArg);
    if (!parsed) {
        printResult({ uploaded: 0, errors: 0, status: "config_missing", message: "--table must be project.dataset.table with non-empty parts" });
        process.exit(0);
    }

    const { projectId, datasetId, tableId } = parsed;

    // --- --file is required for insert / dry-run (validate early for --dry-run without creds) ---
    // (full --file-required check is repeated below; here just to allow dry-run without creds)
    if (!fileArg && !doCreate) {
        printResult({ uploaded: 0, errors: 0, status: "config_missing", message: "--file is required (unless using --create alone)" });
        process.exit(0);
    }

    // --- Dry run: no credentials needed ---
    if (dryRun && fileArg) {
        let rows;
        try {
            rows = readNdjson(fileArg);
        } catch (err) {
            printResult({ uploaded: 0, errors: 1, status: "failed", message: "Failed to read NDJSON: " + err.message });
            process.exit(1);
        }
        const missing = rows.filter((r) => !r || !r.insert_id).length;
        printResult({
            uploaded: 0,
            errors: missing,
            status: "dry_run",
            message: "Dry run: " + rows.length + " rows parsed" + (missing ? ", " + missing + " missing insert_id" : "") + ", no insert performed",
        });
        process.exit(0);
    }

    // --- Check credentials (required for real insert or --create) ---
    const credPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || "";
    if (!credPath) {
        printResult({ uploaded: 0, errors: 0, status: "auth_missing", message: "GOOGLE_APPLICATION_CREDENTIALS is not set" });
        process.exit(0);
    }
    if (!fs.existsSync(credPath)) {
        printResult({ uploaded: 0, errors: 0, status: "auth_missing", message: "GOOGLE_APPLICATION_CREDENTIALS file not found (path withheld for safety)" });
        process.exit(0);
    }

    // --- --create: run DDL ---
    if (doCreate) {
        const result = await createTable(projectId, datasetId, tableId);
        if (!result.ok) {
            printResult({ uploaded: 0, errors: 1, status: "failed", message: result.message });
            // Exit 0 — non-crash failure; caller can inspect status
            process.exit(0);
        }
        // If --file is also given, fall through to insert; otherwise done
        if (!fileArg) {
            printResult({ uploaded: 0, errors: 0, status: "ok", message: result.message });
            process.exit(0);
        }
    }

    // --- --file is required for insert ---
    if (!fileArg) {
        printResult({ uploaded: 0, errors: 0, status: "config_missing", message: "--file is required when --create is not the only operation" });
        process.exit(0);
    }

    // --- Read NDJSON ---
    let rows;
    try {
        rows = readNdjson(fileArg);
    } catch (err) {
        printResult({ uploaded: 0, errors: 1, status: "failed", message: "Failed to read NDJSON: " + err.message });
        process.exit(1);
    }

    // --- Insert ---
    if (rows.length === 0) {
        printResult({ uploaded: 0, errors: 0, status: "ok", message: "No rows to insert" });
        process.exit(0);
    }

    // Map to {insertId, json} for BigQuery streaming dedup
    const insertRows = rows.map((row) => ({
        insertId: String(row.insert_id || ""),
        json: row,
    }));

    try {
        const client  = getBigQueryClient(projectId);
        const table   = client.dataset(datasetId).table(tableId);

        // raw: true — we provide {insertId, json} ourselves (matches bigquery.service.js dedup pattern)
        await table.insert(insertRows, { raw: true });

        printResult({ uploaded: insertRows.length, errors: 0, status: "ok", message: "Inserted " + insertRows.length + " rows into " + tableArg });
    } catch (err) {
        // PartialFailureError: some rows failed, some may have succeeded
        if (err.name === "PartialFailureError" && Array.isArray(err.errors)) {
            const errCount = err.errors.length;
            const okCount  = insertRows.length - errCount;

            logErr({
                level: "error",
                type: "bq-upload",
                message: "Partial insert failure",
                table: tableArg,
                failedRows: errCount,
                sampleErrors: err.errors.slice(0, 3).map((e) => ({
                    insertId: e.row && e.row.insertId ? e.row.insertId : null,
                    reasons: Array.isArray(e.errors) ? e.errors.map((r) => ({ reason: r.reason, message: r.message })) : [],
                })),
            });

            printResult({
                uploaded: okCount,
                errors: errCount,
                status: "failed",
                message: errCount + " rows failed out of " + insertRows.length,
            });
            process.exit(0); // graceful partial failure
        }

        logErr({ level: "error", type: "bq-upload", message: err.message, table: tableArg });
        printResult({ uploaded: 0, errors: insertRows.length, status: "failed", message: err.message });
        process.exit(0); // graceful — non-crash
    }
}

main().catch((err) => {
    logErr({ level: "error", type: "bq-upload", message: "Unexpected crash: " + err.message, stack: err.stack });
    printResult({ uploaded: 0, errors: 0, status: "failed", message: "Unexpected crash: " + err.message });
    process.exit(1);
});
