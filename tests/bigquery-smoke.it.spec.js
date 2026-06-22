"use strict";

// Optional real-BigQuery smoke test. Skipped unless RUN_BQ_SMOKE=1 so CI never needs prod
// credentials. When enabled it performs ONE real streaming insert against a throwaway
// table using the production bigquery.service (no mocks), proving the row shape +
// type normalization match a live schema.
//
// Run with:
//   RUN_BQ_SMOKE=1 BIGQUERY_DATASET=<ds> BQ_SMOKE_TABLE=<table> \
//   GOOGLE_APPLICATION_CREDENTIALS=/path/key.json node --test tests/bigquery-smoke.it.spec.js

const test = require("node:test");
const assert = require("node:assert/strict");

const ENABLED = process.env.RUN_BQ_SMOKE === "1";

test(
    "real BigQuery streaming insert of a single row succeeds",
    { skip: !ENABLED ? "set RUN_BQ_SMOKE=1 (+ creds, dataset, BQ_SMOKE_TABLE) to run" : false },
    async () => {
        process.env.BIGQUERY_ENABLED = "true";
        const table = process.env.BQ_SMOKE_TABLE;
        assert.ok(table, "BQ_SMOKE_TABLE must be set");
        assert.ok(process.env.BIGQUERY_DATASET, "BIGQUERY_DATASET must be set");
        assert.ok(process.env.GOOGLE_APPLICATION_CREDENTIALS, "GOOGLE_APPLICATION_CREDENTIALS must be set");

        delete require.cache[require.resolve("../src/config")];
        delete require.cache[require.resolve("../src/config/env")];
        delete require.cache[require.resolve("../src/services/bigquery.service")];
        const bigquery = require("../src/services/bigquery.service");

        const row = bigquery.buildRow({
            sid: `smoke-${Date.now()}`,
            event: "interaction",
            eventTime: new Date().toISOString(),
            params: { name: "smoke" },
            playableId: "smoke-playable",
            packageName: "com.smoke",
            trackingEnvironment: "test",
        });

        await assert.doesNotReject(() => bigquery.insertBatch(table, [row]), "real insert should succeed");
    }
);
