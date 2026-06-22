# PATCH2 — backend-reliability-engineer (phase 2: silent-failure prevention)

Builds on commit 2bdbe8a. Scope: `src/**` + `scripts/check-pipeline.js` + `package.json`
script + `ops/bin/` wrapper. `npm test` → **74 pass / 0 fail / 2 skipped** (the 2 skipped are
the `RUN_BQ_SMOKE=1` / `RUN_REDIS_IT=1` docker-gated integration specs). Verified stable
across 3 consecutive full runs.

## Root problem solved
`/debug/pipeline` (phase 1) ran in the WEB process and read the web process's OWN in-memory
`bridgeState`. In prod the dispatcher and workers are SEPARATE containers (deploy-prod.sh),
so the endpoint always showed `dispatcher.running=false` and had ZERO visibility into the
worker / BigQuery tiers — the original silent-failure mode. Fixed with Redis-backed
heartbeats (the one store all tiers already share) + a status computation + an ops check.

## New files
- **`src/services/pipeline-health.service.js`** — the core module:
  - `recordHeartbeat(role, fields)` → `SET pixel:health:<role> <json> EX <ttl>` (ttl =
    `PIPELINE_HEARTBEAT_TTL_SECONDS`, default 30). Best-effort via redis-queue
    `sendCommandSafe` — never throws into a hot path, never trips the queue cooldown.
  - `readHeartbeats()` → GET `pixel:health:web` / `:dispatcher` + bounded `SCAN
    pixel:health:worker:*`; a MISSING (expired) key = that tier dead/stuck; attaches
    `heartbeatAgeMs` from each beat's `ts`.
  - `computePipelineStatus(snapshot)` → `{ pipeline_status, degraded_reasons[],
    unhealthy_reasons[] }` implementing CONTRACT2 §RULES exactly (see below).
  - `buildPipelineReport()` → aggregates redis depth (redis getQueueStats) + disk backlog
    (bigquery-queue getQueueStats, shared mount) + heartbeats into the full read-only shape;
    shared by `/debug/pipeline`, `/health?queue=1`, and the ops check `--direct` mode.
  - `summarizeDiskQueue()` exported for the dispatcher heartbeat.
- **`scripts/check-pipeline.js`** — read-only ops probe. Default = HTTP GET
  `http://127.0.0.1:${PORT}/debug/pipeline`; `--direct` = compute from Redis+disk in-process
  (for a VPS cron with no reachable web port). Exit **0 healthy / 1 unhealthy / 2 degraded /
  3 unknown**; prints reasons. Honors `--timeout=<ms>`, `--url=`, `--json`. Never mutates.
- **`ops/bin/pipeline-check-loop.sh`** — interval wrapper (default `--direct`, mode via
  `PIPELINE_CHECK_MODE=http`), follows the existing ops loop pattern (common.sh, pidfile,
  jlog severity by exit code). Never dies on a single failed iteration.

## pipeline_status rules (computePipelineStatus)
UNHEALTHY (any): redis unreachable; dispatcher heartbeat missing OR `lastSuccessAt` older
than `PIPELINE_DISPATCHER_STALE_MS` **while disk backlog files > 0** (THE ORIGINAL INCIDENT);
web accepting recently BUT no worker `lastInsertAt` within `PIPELINE_BQ_STALE_MS` while
`stream_length>0`; all worker heartbeats missing while `stream_length>0`.
DEGRADED (if not unhealthy): disk backlog items > `PIPELINE_DISK_BACKLOG_WARN`;
`stream_length` > `PIPELINE_STREAM_WARN`; worker `lastConsumeAt` stale while stream non-empty;
`bqFailureCount>0` or `rejected_length>0`; dispatcher `lastErrorAt` recent.
HEALTHY: none of the above (idle + empty queues = healthy, not unhealthy).

## Writers (heartbeats + new logs)
- **WEB** — `src/controllers/pixel.controller.js`: `bumpWebHeartbeat()` writes
  `pixel:health:web {lastAcceptAt}` COALESCED to ≤1/sec, fire-and-forget (no hot-path await).
  New logs `disk-persist-success` / `disk-persist-failed` around the awaited `persistRequest`.
- **DISPATCHER** — `src/services/request-dispatcher.service.js`: `writeDispatcherHealth()`
  each loop (throttled to `PIPELINE_HEARTBEAT_INTERVAL_MS`, forced after a real dispatch)
  writes `pixel:health:dispatcher {running,lastSuccessAt,lastErrorAt,dispatcher_id,
  diskBacklog}` + new logs `dispatcher-status` and `dispatcher-backlog-summary`
  (pending/ready/processing + itemsDispatched). Added `bridgeState.lastErrorAt`.
- **WORKER** — `src/services/bigquery-worker.service.js`: `writeWorkerHealth()` each loop
  (throttled, forced after consume) writes `pixel:health:worker:<id> {lastConsumeAt,
  lastInsertAt,bqFailureCount,worker_id}`. New log `bigquery-insert-attempt` before
  `insertBatch`; `workerLastInsertAt` set on success, `workerBqFailureCount++` on failure.
  (Phase-1 redis-consume-*/bigquery-insert-*/worker-batch-summary retained.)

## Endpoints (read-only, no secrets — redactRedisUrl kept; SA JSON never touched)
- `src/routes/health.route.js`: `/debug/pipeline` now returns the full `buildPipelineReport()`
  shape — `pipeline_status`, `degraded_reasons[]`, `unhealthy_reasons[]`, `disk_queue
  {pending,ready,processing,total,approxItems}`, `dispatcher{running,lastSuccessAt,lastErrorAt,
  dispatcher_id,heartbeatAgeMs,diskBacklog}`, `redis_reachable`, `queue_backend`, `queue_key`,
  `queue_type:"stream"`, `stream_length`, `pending`, `rejected_length`,
  `workers[]{worker_id,lastConsumeAt,lastInsertAt,bqFailureCount,heartbeatAgeMs}`,
  `bigquery{configured,lastInsertAt,failureCount}`. `/health?queue=1` now also embeds the
  report under `pipeline`. In-process bridgeState is merged as a dev/single-process fallback.

## Plumbing
- `src/services/redis-queue.service.js`: added + exported `sendCommandSafe(args)` —
  best-effort command runner (markUnavailableOnError:false, returns null on error) used by
  heartbeats so health writes/reads never degrade the enqueue path.
- `src/config/env.js`: `PIPELINE_*` thresholds with defaults (TTL 30s, interval 5s,
  dispatcher/worker/bq stale 30s, web-accept-recent 30s, disk-backlog-warn 5000,
  stream-warn 10000, health key prefix `pixel:health:`).
- `package.json`: `"ops:check-pipeline": "node scripts/check-pipeline.js"`.

## EOL hygiene
All edited `src/**` files are LF (matching HEAD); `git diff --ignore-all-space` == full diff
per file (no whitespace/EOL noise). New files are LF. Did NOT touch the pre-existing
CRLF-dirty files (ops/*, CLAUDE.md, scripts/deploy-prod.sh, etc.) or qa-owned files
(tests/**, scripts/load-pipeline.js). Never `git add -A`. NOTHING COMMITTED (per team-lead).

## Verification done
- `npm test` 74/0/2-skip, stable ×3.
- `node scripts/check-pipeline.js --direct` with no Redis → `unhealthy: redis unreachable`,
  exit 1 (graceful, no crash).
- qa specs `ops-check-pipeline`, `pipeline-health`, `stuck-state`, `persist-failure`,
  `pipeline-integration`, `health` all green against the real modules.
