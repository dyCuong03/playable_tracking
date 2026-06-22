# TEST_SUMMARY — qa-load-tester

Tests + mock sink + load harness for the playable_tracking pixel pipeline. Everything
drives the **real** production services (`pixel.controller` → disk queue → dispatcher
bridge → `redis-queue.service` → `bigquery-worker.service`) against an in-memory fake
Redis Stream and a mock BigQuery sink. Zero external deps for the default suite.

Runner: Node's built-in `node:test` + `node:assert/strict`, `supertest` for HTTP, run
sequentially via `--test-concurrency=1` (already the `npm test` config). Queue specs that
touch disk reset state per pipeline (each test builds an isolated temp queue/log dir and
tears it down in `finally`).

## Files added

### Helpers (`tests/helpers/`, not run as specs)
- **`fake-redis.js`** — in-memory Redis: real-enough Stream + consumer-group + KV.
  Implements `SET NX EX`, `XADD`, `XGROUP CREATE` (BUSYGROUP), `XREADGROUP >`,
  `XAUTOCLAIM`, `XACK`, `XDEL`, `XLEN`, `XPENDING` via the same `sendCommand` /
  `MULTI().addCommand/execAsPipeline` surface the production client uses. Records counters
  (xadd-per-stream, dedupe skips, consumed, acked, deleted) so loss is proven at the data
  level, and supports fault injection — total outage `setFault(true)` or targeted
  `setFault(true, ["XADD"])` (SET succeeds, pipeline XADD fails) for the enqueue-failed path.
- **`fake-bigquery.js`** — injectable `@google-cloud/bigquery`. The real `bigquery.service`
  code runs unchanged (schema fetch/cache, type normalization, local validation, insertId
  dedupe); only the network sink is faked. `failPlan(rows, callIndex, table)` injects
  whole-batch errors or `PartialFailureError`s (per-row `backendError`/`invalid`/…).
- **`pipeline-harness.js`** — installs the module mocks via `Module._load`, sets a
  deterministic env, freshly requires the real services, and exposes: `createPipeline`,
  `runController` (mock req/res → real controller), `drainDiskToRedis` (the exact
  rotate→claim→parse→`enqueueEventBatch` calls the dispatcher makes), `runWorkerUntilDrained`
  (starts the real `startWorker`, polls until stream + PEL empty, stops it),
  `createConsoleCapture` (collects JSON contract log events). `realRedis: true` skips the
  redis mock for the docker path.
- **`events.js`** — valid `/p.gif` query builders per stage (start/interaction/store_trigger/end).

### Specs
- **`tests/pipeline-integration.spec.js`** (7 tests) — scenarios 1, 4, 5, 6, 10, 11, 12 + end-to-end event_hash traceability (server → dispatcher → worker).
- **`tests/dedup-contract.spec.js`** (3 tests) — scenario 7 (the event_hash fix).
- **`tests/persist-failure.spec.js`** (2 tests) — scenarios 2, 3.
- **`tests/load-pipeline.spec.js`** (2 tests) — scenarios 8, 9 (5k start + 5k mixed).
- **`tests/redis-docker.it.spec.js`** (1 test, gated) — real Redis 7 via docker.
- **`tests/bigquery-smoke.it.spec.js`** (1 test, gated) — real BigQuery insert.

### Load harness
- **`scripts/load-pipeline.js`** — `--events=N --mix=start|mixed`; prints the LOAD_RESULTS
  reconciliation table. Exits non-zero on any reconciliation mismatch.

## What each test proves (and how it would FAIL)

| # | Test | Proves | Fails if |
|---|------|--------|----------|
| 1 | single event → one disk → one XADD | exactly-once bridge | event lost, or double-enqueued |
| 2 | durable persist failure → 503, no success log | 200 only when durable | swallows failure / returns 200 |
| 3 | Redis down → HTTP still 200, enqueue throws + `redis-enqueue-failed` | disk decouples web; enqueue not silent | HTTP blocked, or enqueue silently drops |
| 4 | worker consumes all queued | XREADGROUP delivers all | any event left unconsumed |
| 5 | worker inserts into BQ mock | insert path + row shape | inserted ≠ N |
| 6 | partial insert failure | retryable re-queued, bad row → `pixel:rejected`, both logged | row swallowed / not logged / wrong classification |
| 7 | dedup only true duplicates | event_hash wide (env/playable/package/name/session); exact resend = 1 | narrow hash dedup-drops distinct events, or dedup disabled double-enqueues |
| 8 | 5k start load | accepted==enqueued==consumed==inserted | any loss / false dedup |
| 9 | mixed 5k load | per-stage counts reconcile | stage miscount / loss |
| 10 | Redis outage mid-load | disk buffers, drains on recovery, zero loss | accepted events lost across outage |
| 11 | worker restart | XAUTOCLAIM reclaims dead consumer's PEL | in-flight events lost on crash |
| 12 | transient BQ failure | retried → eventual insert, zero loss, nothing rejected | transient error drops rows |
| 5b | event_hash traceability | one hash appears in server + dispatcher(enqueue) + worker logs (console + daily files) | a stage omits/changes the hash |

Scenario 7 is the direct validation of the backend `event_hash` widening: it asserts the
hash changes for a difference in **name, session, playable_id, package_name, or env**, and
is identical for an exact resend. With the original narrow hash (`sid,event,eventTime,params`
only), the playable/package/env sub-tests fail — exactly the cross-env/cross-playable
dedup-drop bug.

## Exact run commands

```bash
# Full default suite (unit + integration + load; zero external deps)
npm test

# Single areas
node --test tests/pipeline-integration.spec.js
node --test tests/dedup-contract.spec.js
node --test tests/persist-failure.spec.js
node --test tests/load-pipeline.spec.js

# Load harness (prints LOAD_RESULTS table)
node scripts/load-pipeline.js --events=5000 --mix=start
node scripts/load-pipeline.js --events=5000 --mix=mixed

# Docker-gated real-Redis integration (needs a reachable docker daemon)
RUN_REDIS_IT=1 node --test tests/redis-docker.it.spec.js
# Inspect the live stream while it runs:
#   docker exec <container> redis-cli XLEN pixel:events
#   docker exec <container> redis-cli XINFO GROUPS pixel:events
#   docker exec <container> redis-cli XPENDING pixel:events pixel-workers

# Optional real-BigQuery smoke (needs creds — never required by CI)
RUN_BQ_SMOKE=1 BIGQUERY_DATASET=<ds> BQ_SMOKE_TABLE=<table> \
  GOOGLE_APPLICATION_CREDENTIALS=/path/key.json node --test tests/bigquery-smoke.it.spec.js
```

## Latest run result

```
# tests 66
# pass 64
# fail 0
# skipped 2      (redis-docker.it + bigquery-smoke.it — gated)
# duration_ms ~58s
```

(64 = 30 pre-existing + 13 new active + 21 other pre-existing specs; the 2 skips are the
gated docker/BQ integration tests.)

## Local environment notes
- `redis-cli` / `redis-server` are **not** installed on the host — covered by the
  fake-redis path (default) and the docker path (gated).
- Docker CLI is present but Docker Desktop's **WSL integration is currently off**, so the
  daemon is unreachable; `redis-docker.it.spec.js` detects this (it probes `docker version`
  = server reachability) and self-skips. Enable WSL integration in Docker Desktop to run it.
- BigQuery: default suite uses the mock sink; no credentials needed. Real inserts are gated
  behind `RUN_BQ_SMOKE=1`.
