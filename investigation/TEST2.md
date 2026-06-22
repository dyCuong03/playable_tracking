# TEST2.md — Phase 2 QA (silent-failure prevention)

Builds on phase-1 harness. All tests drive the **real** services
(`pixel.controller` → disk queue → dispatcher bridge → `redis-queue.service` →
`bigquery-worker.service` → `pipeline-health.service`) against an in-memory fake Redis
(now with heartbeat-key support) and a mock BigQuery sink. Default `npm test` needs zero
external deps.

Runner: `node:test` + `node:assert/strict`, `supertest`, sequential (`--test-concurrency=1`).

## Harness additions (phase 2)

- **`tests/helpers/fake-redis.js`** — extended with the heartbeat surface
  `computePipelineStatus` / `/debug/pipeline` need: `GET`, `SET ... EX`, `TTL`, `EXISTS`,
  `DEL`, `SCAN MATCH`, `KEYS`, `scanIterator`, plus the high-level redis v4 methods
  (`get/set/ttl/exists/del/keys/scanIterator`) — so the backend works whether it uses
  `sendCommand([...])` or the convenience API. Real TTL expiry (an expired key reads as
  missing = that tier dead). State seed/inspect helpers: `setKey/getKey/scanKeys/deleteKey`.
  Targeted fault injection `setFault(true, ["XADD"])` (SET succeeds, XADD fails) for the
  `redis-enqueue-failed` path.
- **`tests/helpers/pipeline-harness.js`** — loads the optional `pipeline-health.service`
  when present; `drainDiskToRedis` now releases ALL claimed files back to `ready/` on
  error (mirrors `runBridgeLoop`) so a mid-drain Redis outage strands nothing.

## Files added (phase 2)

| File | Scenarios |
|------|-----------|
| `tests/stuck-state.spec.js` | dispatcher stopped (backlog visible) + restart drains; worker stopped (stream grows) + restart drains; redis down→recovery, all zero-loss |
| `tests/pipeline-health.spec.js` | `computePipelineStatus` rule table; `/debug/pipeline` unhealthy on stuck dispatcher + healthy after drain; no secrets |
| `tests/ops-check-pipeline.spec.js` | `scripts/check-pipeline.js` exit codes: healthy=0, unhealthy=1 (HTTP mode against in-process server) |
| `tests/trace2.spec.js` | one event_hash traced HTTP→disk→redis→worker→mock-BQ (incl. new logs); real dispatcher loop writes heartbeat + backlog-summary |

Phase-1 specs (pipeline-integration, dedup-contract, persist-failure, load-pipeline,
redis-docker.it, bigquery-smoke.it) still run and pass.

## What each phase-2 test proves

- **computePipelineStatus rule table** — every CONTRACT2 rule maps to the right status:
  - dispatcher stale/missing while disk backlog > 0 → **unhealthy** ("dispatcher stuck; disk backlog stranded") — THE ORIGINAL INCIDENT
  - web accepting + stream>0 + no recent BQ insert → **unhealthy** ("events accepted but nothing inserted to BigQuery")
  - no worker heartbeats while stream>0 → **unhealthy** ("no workers consuming")
  - redis unreachable → **unhealthy**
  - disk backlog > 5000 / stream > 10000 / worker consume stale / bqFailures or rejected>0 / dispatcher recent error → **degraded**
  - idle + empty + fresh heartbeats → **healthy**
  Asserts against the exact snapshot shape `{ now, redisReachable, streamLength,
  rejectedLength, diskBacklogFiles, diskBacklogItems, heartbeats:{web,dispatcher,workers[]} }`
  with ISO-timestamp ages (matches the backend implementation).
- **/debug/pipeline** — with events stranded on disk and no dispatcher, the endpoint returns
  `pipeline_status:"unhealthy"`, exposes `disk_queue.total>0`, `stream_length`,
  `redis_reachable`, `queue_type:"stream"`, and leaks no Redis credentials. After a full
  drain (+ a fresh dispatcher heartbeat) it returns `healthy` with disk+stream at 0. This is
  the cross-process visibility the phase-1 in-memory `/debug` lacked.
- **ops:check-pipeline** — child process exits 0 when healthy, 1 when unhealthy, and prints
  the reason. (Uses async `spawn`, not `spawnSync`, so the in-process server can answer.)
- **stuck-state** — proves the symptom is observable (disk backlog count via
  `getQueueStats`; Redis depth via `XLEN`) AND that no accepted event is lost once the tier
  recovers. Fails on real loss.
- **trace2** — same `event_hash` appears at HTTP (`disk-persist-success`), on the disk NDJSON
  row, in Redis (`redis-enqueue-success`), and at the worker (`bigquery-insert-attempt` +
  `bigquery-insert-success`), and the row lands in the mock sink. Plus the real dispatcher
  loop writes `pixel:health:dispatcher` and `dispatcher-backlog-summary`.

## Exact run commands

```bash
npm test                                            # full default suite (zero deps)

node --test tests/stuck-state.spec.js
node --test tests/pipeline-health.spec.js
node --test tests/ops-check-pipeline.spec.js
node --test tests/trace2.spec.js

node scripts/load-pipeline.js --events=5000 --mix=start
node scripts/load-pipeline.js --events=5000 --mix=mixed

# ops check by hand (HTTP mode against a running web tier)
node scripts/check-pipeline.js            # exit 0/1/2/3
node scripts/check-pipeline.js --direct   # compute from Redis+disk (VPS cron)
npm run ops:check-pipeline

# gated integration (unchanged from phase 1)
RUN_REDIS_IT=1 node --test tests/redis-docker.it.spec.js
RUN_BQ_SMOKE=1 ... node --test tests/bigquery-smoke.it.spec.js
```

## Latest run result

```
# tests 78
# pass 76
# fail 0
# skipped 2      (redis-docker.it + bigquery-smoke.it — gated)
```

## Evidence dump (`node scripts/trace-event.js`) — for REPORT2

### EVIDENCE 1 — one event_hash visible at every stage

```
event_hash = bd5f56799ac671e6cb3625e2f62b11ec4b190b1ed7a1959c7a8e99cb526a4bc7

[HTTP / disk  (disk-persist-success)]
  {"type":"disk-persist-success","event_hash":"bd5f...4bc7","event_name":"interaction","session_id":"trace-session-1","playable_id":"playable-A","package_name":"com.archer.game","env":"test", ...}

[dispatcher / redis (redis-enqueue-success)]
  {"type":"redis-enqueue-success","event_hash":"bd5f...4bc7","session_id":"trace-session-1","queue_key":"pixel:events","message_id":"1-0","stream_len":1, ...}

[worker (bigquery-insert-attempt)]
  {"type":"bigquery-insert-attempt","worker_id":"Admin-PC-...","event_hashes":["bd5f...4bc7"], ...}

[worker (bigquery-insert-success)]
  {"type":"bigquery-insert-success","worker_id":"Admin-PC-...","event_hashes":["bd5f...4bc7"], ...}

mock BigQuery sink contains row with this event_hash: true
SAME event_hash at every stage: true
```

### EVIDENCE 2 — stuck states flip /debug/pipeline pipeline_status

```
DISPATCHER STOPPED (events accepted, bridge not running):
  pipeline_status = unhealthy
  unhealthy_reasons = ["dispatcher stuck; disk backlog stranded"]   <-- THE ORIGINAL INCIDENT
  disk_queue.total = 4   (backlog visible)
  stream_length = 0

WORKER STOPPED (events in Redis stream, no worker consuming):
  pipeline_status = unhealthy
  unhealthy_reasons = ["events accepted but nothing inserted to BigQuery","no workers consuming"]
  degraded_reasons = ["worker consume stale while stream non-empty"]
  stream_length = 20   (depth visible)
```

Regenerate any time with `node scripts/trace-event.js` (real services, fake Redis + mock BQ).

## Notes
- `pipeline-health.spec.js` and `ops-check-pipeline.spec.js` self-skip if the backend
  health module / ops script are absent; with both landed they are active and green.
- Snapshot field names verified against `src/services/pipeline-health.service.js` (commit
  with `computePipelineStatus`): `redisReachable`, `streamLength`, `rejectedLength`,
  `diskBacklogFiles`, `diskBacklogItems`, `heartbeats.{web,dispatcher,workers}`. Thresholds
  use the shipped defaults (PIPELINE_*_MS=30000, DISK_BACKLOG_WARN=5000, STREAM_WARN=10000).
- docker real-Redis path self-skips here (Docker Desktop WSL integration off); runnable on
  a VPS via `RUN_REDIS_IT=1`.
