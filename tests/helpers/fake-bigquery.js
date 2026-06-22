"use strict";

// In-memory fake of the @google-cloud/bigquery module surface used by
// src/services/bigquery.service.js:
//   new BigQuery().dataset(name).table(name).getMetadata() -> [{ schema: { fields } }]
//   ... .table(name).insert(rows) -> Promise
//
// The real bigquery.service code path (schema fetch + caching, type normalization,
// local validation, insertId dedupe) runs unmodified; only the network sink is faked.
// Failure injection lets tests exercise partial-failure classification, retry, and
// transient-outage recovery — all without real credentials.

const DEFAULT_FIELDS = [
    { name: "session_id", type: "STRING" },
    { name: "event_name", type: "STRING" },
    { name: "event_time", type: "TIMESTAMP" },
    { name: "package_name", type: "STRING" },
    { name: "playable_id", type: "STRING" },
    { name: "ip", type: "STRING" },
    { name: "referer", type: "STRING" },
    { name: "received_at", type: "TIMESTAMP" },
    { name: "event_params", type: "JSON" },
    { name: "event_hash", type: "STRING" },
    // tracking environment is carried on the row; include it so it is not dropped.
    { name: "env", type: "STRING" },
    { name: "tracking_environment", type: "STRING" },
];

// Build a BigQuery PartialFailureError-shaped error. `rowOutcomes` is an array of
// { insertId, reason } for the rows that failed (others are treated as inserted).
const buildPartialFailureError = (rowOutcomes) => {
    const error = new Error("A failure occurred during this request.");
    error.name = "PartialFailureError";
    error.errors = rowOutcomes.map((outcome) => ({
        row: { insertId: outcome.insertId },
        errors: [
            {
                reason: outcome.reason,
                message: `simulated ${outcome.reason}`,
                location: "fake",
            },
        ],
    }));
    return error;
};

const createFakeBigQuery = (options = {}) => {
    const fields = options.fields || DEFAULT_FIELDS;

    const state = {
        inserted: [], // every row object that was accepted
        insertCalls: 0,
        // failure plan: a function (rows, callIndex) => null | Error
        failPlan: options.failPlan || null,
        insertedByTable: {},
    };

    const recordInserted = (tableName, rows) => {
        state.inserted.push(...rows);
        state.insertedByTable[tableName] = (state.insertedByTable[tableName] || 0) + rows.length;
    };

    class FakeTable {
        constructor(tableName) {
            this.tableName = tableName;
        }

        async getMetadata() {
            return [{ schema: { fields } }];
        }

        async insert(rows) {
            const callIndex = state.insertCalls;
            state.insertCalls += 1;

            if (typeof state.failPlan === "function") {
                const failure = state.failPlan(rows, callIndex, this.tableName);

                if (failure instanceof Error) {
                    // Partial failure: record the rows NOT named in error.errors as inserted.
                    if (Array.isArray(failure.errors) && failure.errors.length) {
                        const failedIds = new Set(
                            failure.errors
                                .map((entry) => entry && entry.row && entry.row.insertId)
                                .filter(Boolean)
                        );
                        const accepted = rows.filter((row) => !failedIds.has(row.event_hash));
                        recordInserted(this.tableName, accepted);
                    }
                    throw failure;
                }
            }

            recordInserted(this.tableName, rows);
            return [{}];
        }
    }

    class FakeDataset {
        table(tableName) {
            return new FakeTable(tableName);
        }
    }

    class BigQuery {
        dataset() {
            return new FakeDataset();
        }
    }

    return {
        module: { BigQuery },
        state,
    };
};

module.exports = { createFakeBigQuery, buildPartialFailureError, DEFAULT_FIELDS };
