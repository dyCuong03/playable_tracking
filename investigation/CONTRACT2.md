# Phase 2 — silent-failure prevention. Shared team contract.

Builds on commit 2bdbe8a. Goal: dispatcher/Redis/worker/BigQuery stuck states are
VISIBLE in health/debug/logs and an ops check exits non-zero. Read before editing.

## THE CORE PROBLEM (why phase-1 /debug/pipeline is not enough)

/debug/pipeline runs in the WEB process and calls getDispatcherSummary(), which returns the
web process's OWN in-memory bridgeState. In prod the dispatcher and workers are SEPARATE
containers (deploy-prod.sh) — so the endpoint always shows dispatcher.running=false and has
ZERO visibility into worker / BigQuery. Cross-process state must be shared.

## SOLUTION: Redis-backed heartbeats (single shared store all tiers already use)

New module `src/services/pipeline-health.service.js`:
- `recordHeartbeat(role, fields)` -> `SET pixel:health:<role> <json> EX <ttl>` (ttl = 3x writer interval,
  default 30s). Best-effort, never throws into the hot path. role ∈ {web, dispatcher, worker:<id>}.
- `readHeartbeats()` -> reads pixel:health:web, pixel:health:dispatcher, and SCAN pixel:health:worker:*.
  A MISSING key (expired) = that tier is DEAD/stuck. Parse JSON, attach ageMs from `ts`.
- `computePipelineStatus(snapshot)` -> { pipeline_status: healthy|degraded|unhealthy,
  degraded_reasons:[], unhealthy_reasons:[] } using the rules below.

Writers:
- WEB (pixel.controller / app): on each accepted+persisted event bump pixel:health:web
  { lastAcceptAt, ts }. (Coalesce — once per N events or per second, not every request.)
- DISPATCHER (request-dispatcher runBridgeLoop): every loop write pixel:health:dispatcher
  { running:true, lastSuccessAt, lastErrorAt, dispatcher_id, diskBacklog, ts }.
- WORKER (bigquery-worker loop): every loop write pixel:health:worker:<id>
  { lastConsumeAt, lastInsertAt, bqFailureCount, worker_id, ts }.

Disk backlog: read locally via bigquery-queue getQueueStats (data dir is a shared mount) — pending+ready+processing counts. Redis depth: XLEN. Both already available.

## pipeline_status RULES (computePipelineStatus)

UNHEALTHY (any):
- redis_reachable=false
- dispatcher heartbeat missing OR dispatcher.lastSuccessAt older than DISPATCHER_STALE_MS (default 30000)
  while disk backlog > 0  -> "dispatcher stuck; disk backlog stranded"  (THE ORIGINAL INCIDENT)
- web accepting (lastAcceptAt recent) BUT no worker lastInsertAt within BQ_STALE_MS while stream_len>0
  -> "events accepted but nothing inserted to BigQuery"
- all worker heartbeats missing while stream_len > 0 -> "no workers consuming"

DEGRADED (any, if not already unhealthy):
- disk backlog > DISK_BACKLOG_WARN (default 5000)
- stream_length > STREAM_WARN (default 10000) -> worker behind
- worker lastConsumeAt stale (> WORKER_STALE_MS) while stream_len>0
- bqFailureCount increasing / rejected_length > 0
- dispatcher.lastErrorAt recent

HEALTHY: none of the above. (No accepted traffic + empty queues = healthy/idle, not unhealthy.)

All thresholds via src/config/env.js (PIPELINE_* env vars) with sane defaults.

## /debug/pipeline + /health?queue=1 MUST expose (read-only, no secrets)

pipeline_status, degraded_reasons[], unhealthy_reasons[], disk_queue:{pending,ready,processing,total},
dispatcher:{running,lastSuccessAt,lastErrorAt,dispatcher_id,heartbeatAgeMs}, redis_reachable,
queue_backend, queue_key, queue_type("stream"), stream_length, pending, rejected_length,
workers:[{worker_id,lastConsumeAt,lastInsertAt,bqFailureCount,heartbeatAgeMs}],
bigquery:{configured,lastInsertAt,failureCount}. KEEP redactRedisUrl; never emit REDIS_URL or SA JSON.

## NEW LOGS to add (phase-1 already has redis-enqueue/consume/insert + worker-batch-summary + dedup-skip)

- disk-persist-success / disk-persist-failed (pixel.controller or bigquery-queue enqueueDiskEvent)
- dispatcher-status (per loop: running, lastSuccessAt, lastErrorAt, dispatcher_id)
- dispatcher-backlog-summary (per loop: pending/ready/processing counts, itemsDispatched)
- bigquery-insert-attempt (worker, before insertBatch)

Every log includes when applicable: event_hash, event_name, session_id, playable_id, package_name,
env, queue_key, queue_backend, dispatcher_id, worker_id.

## OPS CHECK (requirement 4)

`scripts/check-pipeline.js` + `npm run ops:check-pipeline`. Two modes:
- default: HTTP GET http://127.0.0.1:${PORT}/debug/pipeline, parse pipeline_status.
- `--direct`: compute from Redis + disk directly (for cron on VPS without the web port).
Exit codes: 0 healthy, 1 unhealthy, 2 degraded (so cron can alert). Prints reasons. Read-only;
no destructive Redis. Also wire an ops/bin wrapper if the ops/ harness pattern wants it.

## TESTS (requirement 5) — qa owns. Mock BQ default; RUN_REDIS_IT=1 / RUN_BQ_SMOKE=1 gated.

single e2e by event_hash through all stages; dispatcher-stopped -> health degraded/unhealthy + backlog
visible; dispatcher-restart drains 0-loss; redis-down -> enqueue-failed logs + unhealthy; redis-recovery
drains; worker-stopped -> stream grows + degraded/unhealthy; worker-restart drains; bq-temp-failure
retried no loss; dedup keeps valid repeats; exact-dup deduped; 5k start 0-loss; 5k mixed 0-loss.
Plus a computePipelineStatus unit test table (each reason triggers correct status) and an
ops:check-pipeline exit-code test (healthy=0, unhealthy=1).

## OWNERSHIP (no cross-edits)
- backend-reliability-engineer: src/** + scripts/check-pipeline.js + package.json script + ops/bin wrapper.
- qa-load-tester: tests/** + tests/helpers/** + load harness updates.
- pipeline-architect: READ-ONLY. Verify flow + same-disk/redis/table config + trace one event_hash
  end-to-end. Write investigation/REPORT2.md with all 9 deliverables incl. final verdict.

## EOL WARNING (from phase 1): tree is CRLF/LF-dirty. Preserve each file's existing EOL; use
`git diff --ignore-all-space` to confirm only real changes; never `git add -A`.

Outputs: backend->PATCH2.md, qa->TEST2.md+LOAD2.md, architect->REPORT2.md.
