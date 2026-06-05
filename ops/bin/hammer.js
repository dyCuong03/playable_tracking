#!/usr/bin/env node
// Raw load generator for the /p.gif endpoint. Floods at a fixed concurrency for
// a fixed duration, then prints one JSON summary line: RPS, latency percentiles,
// error + timeout counts. Used by ops/bin/stress.sh ramping stages.
//
// Usage: node hammer.js --url=http://127.0.0.1:9000/p.gif --concurrency=200 \
//                       --duration=15 --timeout=2000
"use strict";

const crypto = require("crypto");

const parse = () => {
    const cfg = {
        url: "http://127.0.0.1:9000/p.gif",
        concurrency: 100,
        duration: 15,   // seconds
        timeout: 2000,  // ms per request
        env: "test",
    };
    for (const a of process.argv.slice(2)) {
        const m = a.match(/^--([^=]+)=(.*)$/);
        if (!m) continue;
        const [, k, v] = m;
        if (k in cfg) cfg[k] = isNaN(Number(v)) || k === "url" || k === "env" ? v : Number(v);
    }
    cfg.concurrency = Number(cfg.concurrency);
    cfg.duration = Number(cfg.duration);
    cfg.timeout = Number(cfg.timeout);
    return cfg;
};

const pct = (sorted, p) => {
    if (!sorted.length) return 0;
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
};

const main = async () => {
    const cfg = parse();
    const deadline = Date.now() + cfg.duration * 1000;
    const lat = [];
    let ok = 0, http_err = 0, net_err = 0, timeouts = 0, sent = 0;

    const events = ["start", "interaction", "store_trigger", "end"];
    // Per-event valid params so the controller passes validateEvent() and the row
    // is actually persisted (200). Without these every request 400s and the test
    // only measures the validation-reject path, never the disk-persist path.
    const paramsFor = (e) => {
        switch (e) {
        case "start": return { platform: "android", campaign: { id: "load-camp" } };
        case "interaction": return { name: "tap" };
        case "store_trigger": return { name: "cta" };
        case "end": return { interact_count: 3 };
        default: return {};
        }
    };

    const worker = async () => {
        while (Date.now() < deadline) {
            const sid = `load-${crypto.randomUUID()}`;
            const e = events[sent % events.length];
            const ep = encodeURIComponent(JSON.stringify(paramsFor(e)));
            const url = `${cfg.url}?e=${e}&sid=${sid}&pid=load.pkg&playableId=load-pl&env=${cfg.env}`
                + `&event_time=${encodeURIComponent(new Date().toISOString())}&event_params=${ep}`;
            sent++;
            const ac = new AbortController();
            const t = setTimeout(() => ac.abort(), cfg.timeout);
            const t0 = Date.now();
            try {
                const r = await fetch(url, { signal: ac.signal, cache: "no-store" });
                lat.push(Date.now() - t0);
                if (r.ok) ok++; else http_err++;
                // drain body so the socket frees
                await r.arrayBuffer().catch(() => {});
            } catch (err) {
                if (err.name === "AbortError") timeouts++; else net_err++;
            } finally {
                clearTimeout(t);
            }
        }
    };

    const t0 = Date.now();
    await Promise.all(Array.from({ length: cfg.concurrency }, worker));
    const elapsed = (Date.now() - t0) / 1000;
    lat.sort((a, b) => a - b);

    const total = ok + http_err + net_err + timeouts;
    const summary = {
        type: "hammer",
        ts: new Date().toISOString(),
        url: cfg.url,
        concurrency: cfg.concurrency,
        duration_s: Number(elapsed.toFixed(2)),
        sent: total,
        rps: Number((total / elapsed).toFixed(1)),
        ok,
        http_err,
        net_err,
        timeouts,
        error_rate: Number(((http_err + net_err + timeouts) / Math.max(1, total)).toFixed(4)),
        latency_ms: {
            p50: pct(lat, 50),
            p95: pct(lat, 95),
            p99: pct(lat, 99),
            max: lat[lat.length - 1] || 0,
        },
    };
    console.log(JSON.stringify(summary));
};

main().catch((e) => {
    console.error(JSON.stringify({ type: "hammer", level: "error", message: e.message }));
    process.exit(1);
});
