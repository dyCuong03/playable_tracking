// src/services/pipeline-health.service.js
//
// Cross-process pipeline liveness via Redis-backed heartbeats. In production the web,
// dispatcher and worker tiers run as SEPARATE containers (see scripts/deploy-prod.sh), so
// the web process cannot see the dispatcher/worker in-memory state. Each tier instead
// writes a short-TTL heartbeat key to the one store every tier already shares — Redis —
// and the health endpoints / ops check read them back. A MISSING (expired) key means that
// tier is dead or stuck, which is exactly the silent-failure mode phase 2 must surface.

const {
    pipelineHeartbeatTtlSeconds,
    pipelineDispatcherStaleMs,
    pipelineWorkerStaleMs,
    pipelineBqStaleMs,
    pipelineWebAcceptRecentMs,
    pipelineDiskBacklogWarn,
    pipelineStreamWarn,
    pipelineHealthKeyPrefix,
} = require("../config");
const { sendCommandSafe, getQueueStats: getRedisQueueStats } = require("./redis-queue.service");
const { getQueueStats: getDiskQueueStats } = require("./bigquery-queue.service");

const PREFIX = pipelineHealthKeyPrefix || "pixel:health:";
const HEALTH_READ_TIMEOUT_MS = 750;

const healthKey = (role) => `${PREFIX}${role}`;

// ── Writers ──────────────────────────────────────────────────────────────────

// Best-effort heartbeat. NEVER throws into the caller's hot path (sendCommandSafe swallows
// errors). role ∈ {"web", "dispatcher", "worker:<id>"}.
const recordHeartbeat = async (role, fields = {}) => {
    if (!role) {
        return false;
    }

    const payload = JSON.stringify({
        role,
        ts: new Date().toISOString(),
        ...fields,
    });
    const ttl = Math.max(1, pipelineHeartbeatTtlSeconds);

    const result = await sendCommandSafe(["SET", healthKey(role), payload, "EX", ttl]);
    return result === "OK";
};

// ── Readers ──────────────────────────────────────────────────────────────────

const ageMsFromTs = (ts, now) => {
    const parsed = Date.parse(String(ts || ""));
    if (!Number.isFinite(parsed)) {
        return null;
    }

    return Math.max(0, now - parsed);
};

const parseHeartbeat = (raw, now) => {
    if (!raw) {
        return null;
    }

    try {
        const parsed = JSON.parse(raw);
        return {
            ...parsed,
            heartbeatAgeMs: ageMsFromTs(parsed.ts, now),
        };
    } catch (_) {
        return null;
    }
};

const scanWorkerKeys = async () => {
    const matchPattern = `${PREFIX}worker:*`;
    const keys = [];
    let cursor = "0";

    // Bounded SCAN loop — guard against a pathological key space so a health read can never
    // hang the endpoint. COUNT is a hint; the iteration count cap is the hard stop.
    for (let iterations = 0; iterations < 1000; iterations += 1) {
        const response = await sendCommandSafe(["SCAN", cursor, "MATCH", matchPattern, "COUNT", "200"]);

        if (!Array.isArray(response) || response.length < 2) {
            break;
        }

        cursor = String(response[0]);
        const batch = Array.isArray(response[1]) ? response[1] : [];
        batch.forEach((key) => keys.push(String(key)));

        if (cursor === "0") {
            break;
        }
    }

    return keys;
};

const readHeartbeats = async () => {
    const now = Date.now();
    const web = parseHeartbeat(await sendCommandSafe(["GET", healthKey("web")]), now);
    const dispatcher = parseHeartbeat(await sendCommandSafe(["GET", healthKey("dispatcher")]), now);

    const workerKeys = await scanWorkerKeys();
    const workers = [];
    for (const key of workerKeys) {
        const worker = parseHeartbeat(await sendCommandSafe(["GET", key]), now);
        if (worker) {
            workers.push(worker);
        }
    }

    return { web, dispatcher, workers };
};

// ── Status computation ────────────────────────────────────────────────────────

// snapshot = {
//   now, redisReachable, streamLength, rejectedLength,
//   diskBacklogFiles, diskBacklogItems,
//   heartbeats: { web, dispatcher, workers[] },
// }
const computePipelineStatus = (snapshot = {}) => {
    const now = Number.isFinite(snapshot.now) ? snapshot.now : Date.now();
    const heartbeats = snapshot.heartbeats || {};
    const web = heartbeats.web || null;
    const dispatcher = heartbeats.dispatcher || null;
    const workers = Array.isArray(heartbeats.workers) ? heartbeats.workers : [];

    const streamLength = Number(snapshot.streamLength || 0);
    const rejectedLength = Number(snapshot.rejectedLength || 0);
    const diskBacklogFiles = Number(snapshot.diskBacklogFiles || 0);
    const diskBacklogItems = Number(snapshot.diskBacklogItems || 0);

    const unhealthy_reasons = [];
    const degraded_reasons = [];

    const age = (entry, field) => {
        if (!entry) {
            return Infinity;
        }
        if (!field) {
            return Number.isFinite(entry.heartbeatAgeMs) ? entry.heartbeatAgeMs : Infinity;
        }
        const parsed = Date.parse(String(entry[field] || ""));
        return Number.isFinite(parsed) ? Math.max(0, now - parsed) : Infinity;
    };

    // ── UNHEALTHY ──
    if (snapshot.redisReachable === false) {
        unhealthy_reasons.push("redis unreachable");
    }

    // THE ORIGINAL INCIDENT: dispatcher dead/stuck while disk files are stranded.
    const dispatcherStale = !dispatcher || age(dispatcher, "lastSuccessAt") > pipelineDispatcherStaleMs;
    if (dispatcherStale && diskBacklogFiles > 0) {
        unhealthy_reasons.push("dispatcher stuck; disk backlog stranded");
    }

    // Accepting traffic but nothing reaching BigQuery while the stream has entries.
    const webAccepting = Boolean(web) && age(web, "lastAcceptAt") <= pipelineWebAcceptRecentMs;
    const freshestInsertAge = workers.length
        ? Math.min(...workers.map((worker) => age(worker, "lastInsertAt")))
        : Infinity;
    if (webAccepting && streamLength > 0 && freshestInsertAge > pipelineBqStaleMs) {
        unhealthy_reasons.push("events accepted but nothing inserted to BigQuery");
    }

    // No workers alive at all while the stream is non-empty.
    if (workers.length === 0 && streamLength > 0) {
        unhealthy_reasons.push("no workers consuming");
    }

    // ── DEGRADED (only matters if not already unhealthy) ──
    if (diskBacklogItems > pipelineDiskBacklogWarn) {
        degraded_reasons.push(`disk backlog high (${diskBacklogItems} > ${pipelineDiskBacklogWarn})`);
    }

    if (streamLength > pipelineStreamWarn) {
        degraded_reasons.push(`stream length high (${streamLength} > ${pipelineStreamWarn}); worker behind`);
    }

    const freshestConsumeAge = workers.length
        ? Math.min(...workers.map((worker) => age(worker, "lastConsumeAt")))
        : Infinity;
    if (streamLength > 0 && freshestConsumeAge > pipelineWorkerStaleMs) {
        degraded_reasons.push("worker consume stale while stream non-empty");
    }

    const totalBqFailures = workers.reduce((sum, worker) => sum + Number(worker.bqFailureCount || 0), 0);
    if (totalBqFailures > 0 || rejectedLength > 0) {
        degraded_reasons.push(`bigquery failures (bqFailureCount=${totalBqFailures}, rejected=${rejectedLength})`);
    }

    if (dispatcher && age(dispatcher, "lastErrorAt") <= pipelineDispatcherStaleMs) {
        degraded_reasons.push("dispatcher reported a recent error");
    }

    let pipeline_status = "healthy";
    if (unhealthy_reasons.length > 0) {
        pipeline_status = "unhealthy";
    } else if (degraded_reasons.length > 0) {
        pipeline_status = "degraded";
    }

    return { pipeline_status, degraded_reasons, unhealthy_reasons };
};

// ── Aggregated read-only report (shared by /debug/pipeline, /health, ops check) ──

const withTimeout = (promise, timeoutMs) => new Promise((resolve) => {
    const timer = setTimeout(() => resolve(null), timeoutMs);
    promise
        .then((value) => {
            clearTimeout(timer);
            resolve(value);
        })
        .catch(() => {
            clearTimeout(timer);
            resolve(null);
        });
});

const summarizeDiskQueue = (diskStats) => {
    if (!diskStats) {
        return { pending: 0, ready: 0, processing: 0, total: 0, approxItems: 0 };
    }

    const pending = Number((diskStats.pending || {}).fileCount || 0);
    const ready = Number((diskStats.ready || {}).fileCount || 0);
    const processing = Number((diskStats.processing || {}).fileCount || 0);
    const bytes = Number((diskStats.pending || {}).totalBytes || 0)
        + Number((diskStats.ready || {}).totalBytes || 0)
        + Number((diskStats.processing || {}).totalBytes || 0);

    return {
        pending,
        ready,
        processing,
        total: pending + ready + processing,
        // ~450 bytes/event, matching request-dispatcher.getApproximateItemCount.
        approxItems: Math.max(0, Math.floor(bytes / 450)),
    };
};

const buildPipelineReport = async () => {
    const now = Date.now();

    const [redisStats, diskStats, heartbeats] = await Promise.all([
        withTimeout(getRedisQueueStats(), HEALTH_READ_TIMEOUT_MS),
        withTimeout(getDiskQueueStats(), HEALTH_READ_TIMEOUT_MS),
        withTimeout(readHeartbeats(), HEALTH_READ_TIMEOUT_MS),
    ]);

    const redisReachable = Boolean(redisStats);
    const streamLength = redisReachable ? Number(redisStats.length || 0) : null;
    const rejectedLength = redisReachable ? Number(redisStats.rejectedLength || 0) : null;
    const disk = summarizeDiskQueue(diskStats);
    const beats = heartbeats || { web: null, dispatcher: null, workers: [] };
    const workers = Array.isArray(beats.workers) ? beats.workers : [];

    const status = computePipelineStatus({
        now,
        redisReachable,
        streamLength: streamLength || 0,
        rejectedLength: rejectedLength || 0,
        diskBacklogFiles: disk.total,
        diskBacklogItems: disk.approxItems,
        heartbeats: beats,
    });

    const dispatcherBeat = beats.dispatcher || null;
    const freshestInsert = workers.reduce((best, worker) => {
        const ts = Date.parse(String(worker.lastInsertAt || ""));
        return Number.isFinite(ts) && ts > best ? ts : best;
    }, 0);

    return {
        ...status,
        queue_backend: "redis-stream",
        queue_type: "stream",
        queue_key: redisReachable ? (redisStats.stream || null) : null,
        group: redisReachable ? (redisStats.group || null) : null,
        redis_reachable: redisReachable,
        redis_url: redisReachable ? (redisStats.redisUrl || null) : null, // already redacted upstream
        stream_length: streamLength,
        pending: redisReachable ? redisStats.pending : null,
        rejected_length: rejectedLength,
        disk_queue: {
            pending: disk.pending,
            ready: disk.ready,
            processing: disk.processing,
            total: disk.total,
            approxItems: disk.approxItems,
        },
        dispatcher: {
            running: Boolean(dispatcherBeat && dispatcherBeat.running),
            lastSuccessAt: dispatcherBeat ? (dispatcherBeat.lastSuccessAt || null) : null,
            lastErrorAt: dispatcherBeat ? (dispatcherBeat.lastErrorAt || null) : null,
            dispatcher_id: dispatcherBeat ? (dispatcherBeat.dispatcher_id || null) : null,
            heartbeatAgeMs: dispatcherBeat ? (dispatcherBeat.heartbeatAgeMs ?? null) : null,
            diskBacklog: dispatcherBeat ? (dispatcherBeat.diskBacklog ?? null) : null,
        },
        web: beats.web
            ? {
                lastAcceptAt: beats.web.lastAcceptAt || null,
                heartbeatAgeMs: beats.web.heartbeatAgeMs ?? null,
            }
            : null,
        workers: workers.map((worker) => ({
            worker_id: worker.worker_id || null,
            lastConsumeAt: worker.lastConsumeAt || null,
            lastInsertAt: worker.lastInsertAt || null,
            bqFailureCount: Number(worker.bqFailureCount || 0),
            heartbeatAgeMs: worker.heartbeatAgeMs ?? null,
        })),
        bigquery: {
            lastInsertAt: freshestInsert > 0 ? new Date(freshestInsert).toISOString() : null,
            failureCount: workers.reduce((sum, worker) => sum + Number(worker.bqFailureCount || 0), 0),
        },
    };
};

module.exports = {
    recordHeartbeat,
    readHeartbeats,
    computePipelineStatus,
    buildPipelineReport,
    summarizeDiskQueue,
    healthKey,
};
