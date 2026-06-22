#!/usr/bin/env node
// scripts/check-pipeline.js
//
// Read-only pipeline health probe for cron / ops alerting. Reports the cross-process
// pipeline_status and exits with a code an alerting wrapper can branch on:
//   0 = healthy, 1 = unhealthy, 2 = degraded, 3 = unknown/error.
//
// Modes:
//   (default)  HTTP GET http://127.0.0.1:${PORT}/debug/pipeline  — checks the live web tier.
//   --direct   compute straight from Redis + disk (for a VPS cron where the web port is not
//              reachable, e.g. the dispatcher/worker host). Uses the same code path as the
//              endpoint, so verdicts match.
//
// Never mutates Redis or disk. Honors --timeout=<ms> and --url=<url>.

const http = require("http");

const EXIT = { healthy: 0, unhealthy: 1, degraded: 2, unknown: 3 };

const parseArgs = (argv) => {
    const args = { direct: false, timeoutMs: 2000, url: null, json: false };

    for (const raw of argv.slice(2)) {
        if (raw === "--direct") {
            args.direct = true;
        } else if (raw === "--json") {
            args.json = true;
        } else if (raw.startsWith("--timeout=")) {
            const value = Number(raw.slice("--timeout=".length));
            if (Number.isFinite(value) && value > 0) {
                args.timeoutMs = value;
            }
        } else if (raw.startsWith("--url=")) {
            args.url = raw.slice("--url=".length);
        }
    }

    return args;
};

const fetchHttp = (url, timeoutMs) => new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
            body += chunk;
        });
        res.on("end", () => {
            try {
                resolve(JSON.parse(body));
            } catch (error) {
                reject(new Error(`Invalid JSON from ${url}: ${error.message}`));
            }
        });
    });

    req.setTimeout(timeoutMs, () => {
        req.destroy(new Error(`HTTP request to ${url} timed out after ${timeoutMs}ms`));
    });
    req.on("error", reject);
});

const getReportHttp = async (args) => {
    const port = process.env.PORT || 8080;
    const url = args.url || `http://127.0.0.1:${port}/debug/pipeline`;
    return fetchHttp(url, args.timeoutMs);
};

const getReportDirect = async () => {
    // Lazy-require so default HTTP mode does not pull in BigQuery/Redis clients.
    const { buildPipelineReport } = require("../src/services/pipeline-health.service");
    const { getBigQueryStatus } = require("../src/services/bigquery.service");

    const report = await buildPipelineReport();
    const bq = getBigQueryStatus();
    report.bigquery = {
        configured: Boolean(bq && bq.configured),
        lastInsertAt: report.bigquery ? report.bigquery.lastInsertAt : null,
        failureCount: report.bigquery ? report.bigquery.failureCount : 0,
    };
    return report;
};

const main = async () => {
    const args = parseArgs(process.argv);

    let report;
    try {
        report = args.direct ? await getReportDirect() : await getReportHttp(args);
    } catch (error) {
        console.error(JSON.stringify({
            ts: new Date().toISOString(),
            level: "error",
            type: "ops-check-pipeline",
            mode: args.direct ? "direct" : "http",
            pipeline_status: "unknown",
            error: error.message,
        }));
        process.exit(EXIT.unknown);
        return;
    }

    const status = (report && report.pipeline_status) || "unknown";
    const unhealthy = (report && report.unhealthy_reasons) || [];
    const degraded = (report && report.degraded_reasons) || [];

    const summary = {
        ts: new Date().toISOString(),
        level: status === "healthy" ? "info" : (status === "degraded" ? "warn" : "error"),
        type: "ops-check-pipeline",
        mode: args.direct ? "direct" : "http",
        pipeline_status: status,
        unhealthy_reasons: unhealthy,
        degraded_reasons: degraded,
        redis_reachable: report ? report.redis_reachable : null,
        stream_length: report ? report.stream_length : null,
        disk_queue: report ? report.disk_queue : null,
        workers: report ? (report.workers || []).length : 0,
    };

    if (args.json) {
        console.log(JSON.stringify(report));
    } else {
        console.log(JSON.stringify(summary));
        if (unhealthy.length) {
            console.error(`UNHEALTHY: ${unhealthy.join("; ")}`);
        } else if (degraded.length) {
            console.error(`DEGRADED: ${degraded.join("; ")}`);
        }
    }

    process.exit(EXIT[status] !== undefined ? EXIT[status] : EXIT.unknown);
};

main().catch((error) => {
    console.error(JSON.stringify({
        ts: new Date().toISOString(),
        level: "error",
        type: "ops-check-pipeline",
        pipeline_status: "unknown",
        error: error.message,
    }));
    process.exit(EXIT.unknown);
});
