# Pixel pipeline investigation — shared team contract

Single source of truth for the 3-agent team. Read before editing. Do NOT renegotiate
field names — converge on this so backend logs and qa tests match without races.

## Pipeline (confirmed by main thread)

```
client /p.gif
  -> pixel.controller.trackPixel (validate + buildRow + event_hash)
  -> request-dispatcher.persistRequest -> bigquery-queue.enqueueDiskEvent  [DISK NDJSON, "durable queue"]
  -> dispatcher process runBridgeLoop: rotate -> claim -> parseQueueFile -> enqueueEventBatch  [REDIS XADD stream pixel:events]
  -> bigquery-worker readQueueBatch (XREADGROUP) -> insertBatch -> BigQuery
```

KEY FACT: web log "Tracking request persisted to durable queue" = **disk write only**, NOT Redis.
Redis XADD happens ONLY in the dispatcher process. If `npm run start:dispatcher` is not running,
events sit on disk forever and Redis stays empty — matches the reported symptom.

- Queue backend: Redis **Stream** (XADD/XREADGROUP/XAUTOCLAIM). Key = `pixel:events` (REDIS_QUEUE_STREAM).
- Rejected stream: `pixel:rejected`. Group: `pixel-workers` (REDIS_QUEUE_GROUP).
- Dedup: `SET pixel:dedupe:<stream>:<event_hash> 1 NX EX 86400` (redis-queue.service.js getDedupeKey/shouldEnqueueItem).
- event_hash = sha256({sid,event,eventTime,params}) — MISSING playable_id, package_name, env.

## Confirmed/suspect defects

1. event_hash too narrow: omits playable_id, package_name, env -> cross-env/playable collisions dedup-drop valid events.
2. `env` never reaches the row/queue item -> cannot log env, cannot include in hash.
3. Redis client `reconnectStrategy:false` -> after one blip, cooldown 5s, all enqueues throw; worker also can't reconnect.
4. Disk->Redis bridge runs in a SEPARATE process; easy to forget -> Redis empty (likely original root cause).
5. enqueue logging only on success (`redis-enqueue`); no attempt/failed events, no queue length.

## LOG EVENT CONTRACT (backend implements, qa asserts) — type names exact

| type                     | where                              |
|--------------------------|------------------------------------|
| redis-enqueue-attempt    | before XADD batch                  |
| redis-enqueue-success    | after XADD ok (per item or batch)  |
| redis-enqueue-failed     | XADD/pipeline throws               |
| redis-consume-success    | worker after XREADGROUP returns N  |
| redis-consume-failed     | worker read throws                 |
| bigquery-insert-success  | after insertBatch ok               |
| bigquery-insert-failed   | insert throws / partial reject     |
| redis-dedup-skip         | already exists, keep               |
| worker-batch-summary     | per worker loop: consumed/inserted/retried/dropped |

Every entry includes when available: `ts, event_hash, event_name, session_id, playable_id,
package_name, env, queue_key, queue_backend("redis-stream"), worker_id, stream_len`.

## ACCEPTANCE CRITERIA (architect verifies all)

- [ ] single event traceable end-to-end by event_hash across server/dispatcher/worker logs
- [ ] statusCode:200 only when durably persisted (disk write awaited — already true; document it)
- [ ] redis enqueue success AND failure visible in logs (+ stream length)
- [ ] worker consume success/failure visible in logs
- [ ] bigquery insert success/failure visible in logs
- [ ] 5k start load: zero unexplained loss (accepted==enqueued==consumed==inserted+failed+dedup)
- [ ] redis restart mid-load: no silent loss of accepted events (disk buffer drains after recovery)
- [ ] bigquery temp failure: retried, not silently lost
- [ ] dedup drops only exact duplicate resend, NOT valid distinct interactions
- [ ] final report states the proven root cause from the enumerated list

## OWNERSHIP (no cross-edits)

- pipeline-architect: READ-ONLY. Diagnosis, coordinate, fill REPORT.md, verify criteria. Do not edit src/tests.
- backend-reliability-engineer: edits ONLY src/**. Implements log contract, hash fix, env threading, reconnect, health/debug.
- qa-load-tester: edits ONLY tests/** and scripts/**. Tests + mock sink + load harness using real modules.

## Local env reality

- redis-cli / redis-server NOT installed on host. Docker IS available (docker, docker-compose).
- Load/integration tests that need Redis must either spin Redis via docker, OR use an in-memory
  fake redis client. qa: provide BOTH a no-redis unit path and a docker-gated integration path.
- BigQuery: gate real inserts behind env; default to a mock/stub sink.

## Outputs

- backend -> investigation/PATCH_SUMMARY.md
- qa -> investigation/TEST_SUMMARY.md + investigation/LOAD_RESULTS.md (the result table)
- architect -> investigation/REPORT.md (root cause, evidence, rollback, prod checklist)
