# Pixel pipeline — missing-events investigation REPORT

Owner: pipeline-architect (read-only on src/tests). Source of truth: `investigation/CONTRACT.md`.
Status: **COMPLETE.** Root cause proven; backend patch (4 src files) + qa tests/load landed and
independently re-verified by the architect (`npm test` 67/65/0/2-gated; both 5k loads zero-loss).
Working tree is **uncommitted** — the commit/deploy decision is the user's (team-lead to surface).

## Acceptance criteria status (CONTRACT §ACCEPTANCE)

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| 1 | single event traceable end-to-end by `event_hash` | ✅ | contract log events on all tiers carry `event_hash` (§2.3); spec 1 |
| 2 | 200 only when durably persisted (disk write awaited) | ✅ | `pixel.controller.js:139` awaits persist before `sendPixel`; spec 2 (503 on failure) |
| 3 | redis enqueue success AND failure visible (+ stream length) | ✅ | `redis-enqueue-attempt/-success/-failed` + `stream_len`; spec 3 |
| 4 | worker consume success/failure visible | ✅ | `redis-consume-success/-failed` + `worker-batch-summary` |
| 5 | bigquery insert success/failure visible | ✅ | `bigquery-insert-success/-failed`; spec 6 |
| 6 | 5k start load: zero unexplained loss | ✅ | §6 — accepted=enqueued=consumed=inserted=5000, LOST=0 |
| 7 | redis restart mid-load: no silent loss (disk drains) | ✅* | §6 outage run: 40/40 drained on recovery (fake-redis); live path gated behind `RUN_REDIS_IT=1` |
| 8 | bigquery temp failure: retried, not lost | ✅ | §6 transient + partial BQ runs; specs 6,12 |
| 9 | dedup drops only exact duplicate resend, not distinct | ✅ | `event_hash` widened (§2.1); `tests/dedup-contract.spec.js` (scenario 7) |
| 10 | report states proven root cause from the list | ✅ | §1 verdict |

\* #7's live-Redis confirmation requires a reachable Docker daemon (Docker Desktop WSL integration is off on
this host, redis-cli not installed). It is validated against the fake-redis stream now and is runnable on the
VPS via the gated `redis-docker.it.spec.js` + the §7 redis-cli inspection. This is the only residual
host-environment limitation; logic is covered.

---

## 1. Root cause + evidence

End-to-end flow (verified file:line):

```
client GET /p.gif
 -> routes/pixel.route.js:8            router.get("/p.gif", rateLimit, noCache, trackPixel)
 -> controllers/pixel.controller.js:103 trackPixel  (validate -> buildRow -> resolveTableName)
 -> :139  await persistRequest({tableName,row,urlData})
 -> request-dispatcher.service.js:73   persistRequest = enqueueDiskEvent   <-- DISK NDJSON ONLY
 -> :144 sendPixel(res) + log "Tracking request persisted to durable queue" statusCode:200
 == process boundary ==
 dispatcher process (src/dispatcher.js -> request-dispatcher.service.js:94 runBridgeLoop)
 -> :100 rotatePendingFiles -> :102 claimReadyFiles -> :119 parseQueueFile
 -> :125 flushItemsToRedis -> :80 enqueueEventBatch  <-- REDIS XADD pixel:events (first Redis touch)
 == process boundary ==
 worker process (src/worker.js -> bigquery-worker.service.js:381 startWorker)
 -> :405 readQueueBatch (XREADGROUP) -> :271 insertBatch -> BigQuery
```

**KEY FACT (confirmed):** the web log `"Tracking request persisted to durable queue"` + `statusCode:200`
means a **disk write only** (`pixel.controller.js:146-154`). The first and only Redis `XADD` is in the
**separate dispatcher process** (`request-dispatcher.service.js:80,125`). The web tier never touches Redis.

### Proven root causes (from the CONTRACT enumerated list)

**A. Dedup too aggressive — silent loss of valid, distinct events (PRIMARY code defect).**
- `event_hash = sha256(JSON({sid, event, eventTime, params}))` — `bigquery.service.js:82-91`. It **omits
  `playable_id`, `package_name`, and `env`.**
- Dedup key `pixel:dedupe:pixel:events:<event_hash>` set via `SET … NX EX 86400` —
  `redis-queue.service.js:153-162` (`getDedupeKey`) + `:297-310` (`shouldEnqueueItem`), invoked for every
  batch enqueue (`enqueueEventBatch` → `appendToStreamBatch(..., {dedupe:true})`, `:369`).
- Consequence: any second event sharing the same `{sid, event, eventTime, params}` is dropped for 24h.
  This collides across **different playables**, **different package_names**, and **prod-vs-test env**, and
  on rapid identical-timestamp interactions. These are valid distinct interactions, dropped as "duplicates."
- This is provable from code alone, independent of any ops state — it explains valid events missing from
  BigQuery even when every process is healthy.

**B. Disk→Redis bridge is a separate process; web reports success on disk-write only — the "Redis empty"
operational symptom.**
- Web path persists to disk and returns 200 (`pixel.controller.js:139-154`); Redis is populated only by
  the dispatcher (`request-dispatcher.service.js:94-155`). If the dispatcher process is **not running /
  crash-looping / omitted from a deploy variant**, events accumulate on disk forever and Redis `XLEN
  pixel:events` stays 0 while the web tier logs success. No log line distinguishes "disk-persisted" from
  "Redis-enqueued."
- Topology IS wired correctly in the script deploy: `scripts/deploy-prod.sh` launches a dispatcher
  container (`:253-284`, `DISPATCHER_COUNT` default 1), a worker pool (`:214-251`, `WORKER_COUNT` 4), and
  app replicas (`:149-186`), **all sharing** `REDIS_URL=redis://pixel-redis:6379`,
  `REDIS_QUEUE_STREAM=pixel:events`, `REDIS_QUEUE_GROUP=pixel-workers` (`:37-39,171-173,235-237,267-269`).
  `.github/workflows/deploy.yml:127` runs exactly this script on the VPS.
- Therefore "wrong key / wrong instance / worker not consuming" are **ruled out** for the script deploy.
  The residual risk is purely operational + invisible: a dead/absent dispatcher silently strands events
  with zero alerting, because there is no `redis-enqueue-attempt/failed` log and no stream-length signal.

### Contributing defects (amplify A/B, not standalone root cause)

- **`reconnectStrategy:false` + 5s cooldown** — `redis-queue.service.js:103` disables the client's socket
  auto-reconnect; on any error `markRedisUnavailable` (`:80-87`) sets a `redisUnavailableCooldownMs` (5s
  default) window during which `getClient` throws (`:94-96`). Dispatcher enqueues throw → file is released
  back to `ready/` and retried (`request-dispatcher.service.js:138-150`) so **no loss, just delay**; worker
  reads throw and the loop sleeps/retries. Recovery works (a fresh client is created after cooldown), but
  every Redis blip causes a burst of failures and a throughput cliff. Worth fixing for resilience.
- **`env` never written to the row** — `buildRow` (`bigquery.service.js:93-104`) has no `env`/`environment`
  field; `trackingEnvironment` is consumed only by `resolveTableName` (`:66-80`). So env can't be logged
  and can't be added to the hash without threading it through. Prereq for fixing A.
- **Observability gap** — only successful enqueues are logged (`redis-enqueue`, `redis-queue.service.js:196`);
  there is no attempt/failed event, no stream length, and worker consume success/failure is not emitted per
  the LOG EVENT CONTRACT. This is *why* defect B is invisible in production.

### Ruled out
- **Wrong key / wrong instance:** all tiers share `pixel:events` @ `redis://pixel-redis:6379` (deploy-prod.sh).
- **Enqueue not awaited:** dispatcher awaits `enqueueEventBatch` (`request-dispatcher.service.js:80`); web
  awaits the disk persist (`pixel.controller.js:139`). The issue is *where* enqueue happens, not a missing await.
- **Worker not consuming:** `XREADGROUP > … COUNT … BLOCK` + `XAUTOCLAIM` reclaim are correct
  (`redis-queue.service.js:447-497`).
- **BigQuery insert failure → silent loss:** partial failures are classified; retryable rows re-`XADD`'d,
  non-retryable → `pixel:rejected` + logged; chunk always `XACK+XDEL` (`bigquery-worker.service.js:259-379`).
- **Rate limiter dropping events:** `middlewares/rateLimit.js` sets `req.rateLimitExceeded` but **always
  calls `next()`** and nothing consumes the flag — currently a no-op, not a drop path.
- **Dashboard query / client not sending enough:** out of scope; no evidence.

**VERDICT:** Two confirmed root causes — (A) **dedup too aggressive** via a too-narrow `event_hash`
(silent loss of valid distinct events, provable from code) and (B) **the disk→Redis bridge running only in a
separate dispatcher process while the web tier returns 200 on a disk write**, which produces the reported
"Redis empty" symptom whenever the dispatcher is down/absent and is invisible due to the observability gap.

---

## 2. Patch summary

Backend (`investigation/PATCH_SUMMARY.md`), confined to `src/**`; architect-verified against source:

1. **`event_hash` widened** (`bigquery.service.js:82-100`) — payload now
   `{sid, event, eventTime, params, playableId, packageName, env(trackingEnvironment)}`. Exact-duplicate
   resends still hash identically (idempotency kept); distinct playables/packages/envs no longer collide.
   **Fixes root cause A.**
2. **`env` threaded into the row** (`bigquery.service.js:buildRow`) — `env: event.trackingEnvironment || ""`.
   BigQuery-safe: `formatRowForInsert` drops keys absent from the live schema, so it can't break inserts.
3. **LOG EVENT CONTRACT implemented** — `redis-enqueue-attempt/-success/-failed` on both `produceToStream`
   and `appendToStreamBatch` (the real dispatcher path), with `message_id` + best-effort `stream_len`
   (`getStreamLengthSafe` XLEN); `redis-consume-success/-failed` + `bigquery-insert-success/-failed` +
   `worker-batch-summary {consumed,inserted,retried,dropped,dedup,worker_id}` in the worker;
   `redis-dedup-skip` kept. All carry `event_hash/event_name/session_id/playable_id/package_name/env/
   queue_key/queue_backend:"redis-stream"`. **Makes root cause B observable.**
4. **`reconnectStrategy:false` → bounded backoff** (`redis-queue.service.js:155`) —
   `retries => retries>20 ? Error : min(retries*100, 3000)`, then falls through to the existing
   cooldown + fresh-client path. Enqueue confirmed awaited end-to-end.
5. **`GET /debug/pipeline`** (`health.route.js:56`) — read-only liveness: `queue_backend, queue_key, group,
   stream_length, pending, rejected_length, redis_reachable, dispatcher.running, dispatcher.lastSuccessAt,
   bigquery_configured`. Directly exposes the dead-dispatcher condition behind root cause B.
6. **Security fix (bonus):** `redactRedisUrl` (`redis-queue.service.js:61`) strips `user:password` from the
   URL returned by `getQueueStats()` — previously leaked at `/health?queue=1`; now redacted there and at
   `/debug/pipeline`.

---

## 3. Files changed

**The fix is exactly 4 source files** — verified clean: `git diff --stat HEAD -- src/` →
`4 files changed, 276 insertions(+), 23 deletions(-)` (an earlier 13-file stat was CRLF/EOL churn, since
renormalized by backend; the 9 unrelated `src/*` files are no longer in the diff):

- `src/services/bigquery.service.js` (+13) — `hashEvent` widened; `buildRow` adds `env`.
- `src/services/redis-queue.service.js` (+147) — bounded `reconnectStrategy`; `redactRedisUrl`;
  `getStreamLengthSafe`; `redis-enqueue-attempt/-success/-failed`.
- `src/services/bigquery-worker.service.js` (+101) — `redis-consume-*`, `bigquery-insert-*`,
  `worker-batch-summary`; `processMessages` returns `{inserted,retried,dropped}`.
- `src/routes/health.route.js` (+38) — new `GET /debug/pipeline` (read-only) + redacted Redis URL.

qa (separate ownership): modified `tests/redis-queue.service.spec.js`, `tests/pixel.spec.js`,
`tests/log.service.spec.js`; new `tests/dedup-contract.spec.js`, `tests/pipeline-integration.spec.js`,
`tests/helpers/`, `scripts/load-pipeline.js`.

---

## 4. Tests added

qa-load-tester (`investigation/TEST_SUMMARY.md`), `tests/**` + `scripts/**` only. All specs drive the **real**
services (`pixel.controller` → disk queue → dispatcher bridge → `redis-queue.service` → `bigquery-worker`)
against an in-memory fake-Redis Stream + mock BigQuery sink, asserting on actual data movement (XADD / consume /
insert counts) so they fail on real loss or over-aggressive dedup.

- Helpers: `tests/helpers/{fake-redis,fake-bigquery,pipeline-harness,events}.js` — fake-redis implements
  `SET NX EX`, `XADD`, `XGROUP/BUSYGROUP`, `XREADGROUP >`, `XAUTOCLAIM`, `XACK/XDEL/XLEN/XPENDING` + fault
  injection; fake-bigquery runs the real `bigquery.service` and fakes only the network sink.
- `tests/pipeline-integration.spec.js` — scenarios 1,4,5,6,10,11,12, incl. a dedicated end-to-end
  **traceability** test: one `event_hash` traced across server (`pixel-server-request`), dispatcher
  (`redis-enqueue-success`) and worker (`bigquery-insert-success`) — asserted in both console contract
  events and the on-disk `logs/server` / `logs/worker` daily ndjson files.
- `tests/dedup-contract.spec.js` — scenario 7 (validates the `event_hash` widening; fails on the old narrow hash).
- `tests/persist-failure.spec.js` — scenarios 2,3 (503 on durable-persist failure; HTTP 200 + `redis-enqueue-failed`
  when Redis down).
- `tests/load-pipeline.spec.js` — scenarios 8,9 (5k start + 5k mixed, per-stage reconciliation).
- `tests/redis-docker.it.spec.js` (gated `RUN_REDIS_IT=1`) — live Redis 7 XADD/XREADGROUP/XAUTOCLAIM path.
- `tests/bigquery-smoke.it.spec.js` (gated `RUN_BQ_SMOKE=1`) — real BigQuery insert.
- `scripts/load-pipeline.js` — `--events=N --mix=start|mixed`; prints the §6 table, non-zero exit on mismatch.

---

## 5. Exact commands run + results

All run independently by the architect (read-only verification):

- `npm test` → **67 tests, 65 pass, 0 fail, 2 skipped** (the 2 skips are the docker/BQ gated integration
  specs; ~61s; Node `node:test`, sequential `--test-concurrency=1`). Matches qa's reported run.
- `node scripts/load-pipeline.js --events=5000 --mix=start` → `RESULT: PASS` — accepted=enqueued=consumed=
  inserted=5000, dedup=0, **LOST=0**.
- `node scripts/load-pipeline.js --events=5000 --mix=mixed` → `RESULT: PASS` — same totals, **LOST=0**.
- `git diff --stat HEAD -- src/` → 4 files, 276 insertions / 23 deletions (the fix; §3).
- `grep` audit of `src/` confirms all 9 contract event-type strings are emitted; `reconnectStrategy` is a
  function (not `false`); `redactRedisUrl`/`getStreamLengthSafe` present; `/debug/pipeline` + `redis_reachable`
  + `stream_length` present in `health.route.js`.
- Architect read-only code trace — all file:line citations in §1 confirmed by direct read.
- Gated (NOT runnable on this host — Docker Desktop WSL integration off, daemon unreachable; redis-cli not
  installed): `RUN_REDIS_IT=1 node --test tests/redis-docker.it.spec.js` (live Redis path; self-skips here).

---

## 6. Load result table

From `investigation/LOAD_RESULTS.md`, re-run and confirmed by the architect. Invariant:
**accepted HTTP == inserted + failed + dedup**, and **LOST == 0** (counts observed at the data level —
XADD / worker delivery / mock-sink inserts — not parsed from logs).

| metric | 5k start | 5k mixed |
|---|---|---|
| requested | 5000 | 5000 |
| accepted HTTP (200) | 5000 | 5000 |
| rejected HTTP (4xx/5xx) | 0 | 0 |
| redis enqueued (XADD) | 5000 | 5000 |
| worker consumed | 5000 | 5000 |
| dedup skipped | 0 | 0 |
| inserted (BigQuery) | 5000 | 5000 |
| failed (rejected stream) | 0 | 0 |
| still pending | 0 | 0 |
| **LOST** | **0** | **0** |

Mixed run reconciles per stage: start 1000 / interaction 2000 / store_trigger 1000 / end 1000 (inserted rows
grouped by `event_name` match requested exactly).

Failure-mode runs (zero loss in every case):

| Scenario | Observed |
|---|---|
| Redis outage during accept (40) | all 40 accepted (HTTP 200, disk), enqueue threw + `redis-enqueue-failed` while down, all 40 drained + inserted on recovery |
| Worker crash before ack (30) | 30 left in dead consumer's PEL, reclaimed via `XAUTOCLAIM`, all 30 inserted |
| Transient BQ failure (8) | whole batch retried → all 8 inserted, 0 rejected |
| Partial BQ failure (3: ok/retry/reject) | ok+retry inserted (2), invalid → `pixel:rejected` + logged `bigquery-insert-failed` (1), 0 silently lost |

---

## 7. Production verification checklist (read-only)

Redis is **not** installed on the host; run these inside the redis container:
`docker exec -it pixel-redis redis-cli <cmd>`. **No destructive commands.**

- [ ] Containers up: `docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'pixel-(redis|nginx|server|worker|dispatcher)'`
      — expect 1 redis, 1 nginx, `APP_REPLICAS` servers, `WORKER_COUNT` workers, **≥1 `pixel-dispatcher`** (defect B).
- [ ] Dispatcher alive recently: `docker logs --since 5m pixel-dispatcher-1 | grep request-dispatcher-batch` (should advance).
- [ ] Stream exists + depth: `redis-cli TYPE pixel:events` (→ `stream`); `redis-cli XLEN pixel:events`.
- [ ] Discover any stray keys: `redis-cli --scan --pattern '*event*'` and `--pattern 'pixel:*'`.
- [ ] Consumer group health: `redis-cli XINFO GROUPS pixel:events` (group `pixel-workers`, `consumers>0`, `lag` low).
- [ ] Pending (unacked) entries: `redis-cli XPENDING pixel:events pixel-workers` (large/old ⇒ workers stuck).
- [ ] Rejected stream: `redis-cli XLEN pixel:rejected` (growth ⇒ non-retryable insert failures — inspect logs).
- [ ] Dedup footprint: `redis-cli --scan --pattern 'pixel:dedupe:*' | wc -l` (unexpectedly high ⇒ over-dedup, defect A).
- [ ] App health w/ queue stats: `curl -s 'http://127.0.0.1:9000/health?queue=1'` — check `dispatcher.bridge.running`,
      `dispatcher.bridge.lastSuccessAt` fresh, `queue.length`, `queue.pending`, `bigQuery.configured:true`.
- [ ] New log event types present (post-patch), traceable by a single `event_hash` across tiers:
      `grep -h '"type":"redis-enqueue-attempt"' logs/dispatcher/*.ndjson`
      `grep -h '"type":"redis-enqueue-success"' logs/redis-queue/*.ndjson`
      `grep -h '"type":"redis-consume-success"' logs/worker/*.ndjson`
      `grep -h '"type":"bigquery-insert-success"' logs/worker/*.ndjson`
      `grep -Rh '<event_hash>' logs/server logs/dispatcher logs/worker logs/redis-queue` (single event, full path).
- [ ] Failure visibility: `grep -h '"type":"redis-enqueue-failed"\|"type":"bigquery-insert-failed"' logs/**/*.ndjson`.

---

## 8. Rollback plan

**Current state: uncommitted.** All fix changes live in the working tree (the 4 source files in §3); per
team-lead, nothing is committed until the real user authorizes. The commit/deploy decision is the user's.

- **Pre-commit (now):** to discard the fix, restore just the 4 fix files from `HEAD` —
  `git checkout HEAD -- src/services/bigquery.service.js src/services/redis-queue.service.js
  src/services/bigquery-worker.service.js src/routes/health.route.js`
  (or `git stash push -- <those 4 files>` to keep them). This leaves the ~9 pre-existing dirty files untouched.
- **Post-commit (after the user authorizes a commit/deploy):** the fix should land as a single commit of
  exactly those 4 files (keep the pre-existing dirty files out so the revert stays surgical). Roll back with
  **`git revert <SHA>`**, then re-run `scripts/deploy-prod.sh` (CI: push the revert to `main` →
  `.github/workflows/deploy.yml` redeploys). No schema migration is required (widening `event_hash` only
  changes future `insertId`s; existing rows are unaffected).
- **No destructive Redis commands** in either case (see below).
- **No destructive Redis commands.** Do **not** `FLUSHALL`/`DEL pixel:events`/`XGROUP DESTROY`. The stream is
  capped (`XADD MAXLEN ~`) and self-trims; the durable buffer is the on-disk `data/bigquery-queue` (treat as
  prod state — do not delete).
- Reverting the widened hash re-narrows dedup; the only effect is reverting to the prior (buggy) dedup
  behavior — acceptable as an emergency rollback. Dedupe keys (`pixel:dedupe:*`) expire on their own (24h TTL);
  no manual cleanup needed.
- If a hotfix must disable dedup entirely without a deploy, that is a code path (`dedupe:false`), not a redis
  command — prefer revert over live mutation.
