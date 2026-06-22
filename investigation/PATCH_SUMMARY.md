# PATCH SUMMARY — backend-reliability-engineer

Scope: `src/**` only. `npm test` → **51 pass / 0 fail**. Public `/p.gif` API and existing
behavior preserved. All new logs follow the LOG EVENT CONTRACT type names exactly.

## Root-cause defects fixed (from CONTRACT.md §"Confirmed/suspect defects")

- **#1 event_hash too narrow** → cross-env / cross-playable / cross-package collisions
  caused the Redis dedupe `SET … NX` to drop *valid distinct* events.
- **#2 `env` never reached the row/queue item** → could not log env, could not include it
  in the hash, could not split prod vs test traffic in audit.
- **#3 `reconnectStrategy:false`** → after one transient Redis blip the client died, the
  unavailable-cooldown tripped, and every enqueue threw "temporarily unavailable" forever.
- **#5 enqueue logging only on success** (`redis-enqueue`) → XADD failures and queue depth
  were invisible, so loss between disk→Redis could not be detected.
- Plus observability gaps on the worker (consume/insert outcomes, per-loop accounting) and
  a **credential leak**: `getQueueStats()` returned the raw `REDIS_URL` (incl. user:pass)
  and it was already exposed at `/health?queue=1`.

> Note #4 (disk→Redis bridge is a separate process; if `npm run start:dispatcher` is not
> running, Redis stays empty) is an **operational** root cause, not a code defect. The new
> `/debug/pipeline` endpoint surfaces `dispatcher.running` + `lastSuccessAt` so this is now
> directly observable. Left for architect's prod checklist.

## Changes by file

### `src/services/bigquery.service.js`
- `hashEvent()` (was ~L82): hash payload widened from `{sid,event,eventTime,params}` to also
  include `playableId`, `packageName`, and `env` (`event.trackingEnvironment`). Still a pure
  function of the event, so an EXACT duplicate resend hashes identically (idempotency kept),
  but distinct sessions/interactions/playables/packages/envs now differ. `event.trackingEnvironment`
  is already on the event object built by `event.service.buildEvent`, so `hashEvent` receives it.
- `buildRow()` (was ~L93): added `env: event.trackingEnvironment || ""` to the row so env is
  threaded through the queue item → Redis → all logs. **BigQuery-safe**: `formatRowForInsert`
  drops any key absent from the live schema (`if (!fieldTypes.has(normalizedKey)) return acc;`),
  so carrying `env` on the row never breaks `table.insert` even when there is no `env` column.

### `src/services/redis-queue.service.js`
- `reconnectStrategy` (getClient, ~L99): `false` → bounded backoff
  `retries => retries>20 ? Error : min(retries*100, 3000)ms`. A blip now recovers in place;
  only after exhaustion does it fall through to `markRedisUnavailable()` (cooldown) and a
  fresh client on the next call. Command timeout + unavailable-cooldown safety net unchanged.
- `redactRedisUrl()` (new): strips `user:password` from the URL → `redis://host:port`.
  `getQueueStats()` now returns `redisUrl: redactRedisUrl(redisUrl)` so credentials never
  leave the process (fixes the `/health?queue=1` + `/debug/pipeline` leak).
- Contract enqueue events (new `emitEnqueueEvent` + `describeQueueItem`): emit
  `redis-enqueue-attempt` (before XADD), `redis-enqueue-success` (after XADD ok, per item,
  with `message_id` + best-effort `stream_len` via new `getStreamLengthSafe` XLEN), and
  `redis-enqueue-failed` (XADD/pipeline throws). Wired into BOTH `produceToStream` (single)
  and `appendToStreamBatch` (the real dispatcher batch path). Each carries `event_hash,
  event_name, session_id, playable_id, package_name, env, queue_key, queue_backend:"redis-stream"`.
  These go to **stdout/stderr only** (not the `redis-queue` daily audit batch) so the existing
  index-based audit-log assertions in `tests/redis-queue.service.spec.js` stay green.
- `redis-dedup-skip` kept as-is. The existing `redis-enqueue` daily audit entry kept as-is
  (and enriched with `playable_id`, `package_name`, `env`).
- **Enqueue is awaited**: dispatcher `flushItemsToRedis` does `await enqueueEventBatch(batch)`
  → `await appendToStreamBatch(...)` which awaits the piped `execAsPipeline()`. Confirmed.

### `src/services/bigquery-worker.service.js`
- New always-on `emitWorkerEvent()` (NOT rate-limited, unlike `logWorker`) for contract events.
- `redis-consume-success` / `redis-consume-failed` around `readQueueBatch` (and a
  consume-success for `claimPendingBatch`/XAUTOCLAIM reclaims).
- `bigquery-insert-success` / `bigquery-insert-failed` around `insertBatch` (the existing
  `bigquery-worker-insert` success log is kept; the contract-named events are added alongside).
- `worker-batch-summary` per loop with `consumed/inserted/retried/dropped/dedup` + `worker_id`.
  `processMessages` now accumulates and returns `{inserted, retried, dropped}`.
- Partial-insert errors are **not swallowed**: existing classify → retry (`requeueItems`) /
  reject (`pixel:rejected` + `logInsertError`) logic preserved; counts now feed the summary.
- Worker survives Redis disconnect: the `while(!stopping)` loop still catches, logs, and
  sleeps; combined with the new bounded `reconnectStrategy` it resumes after recovery.

### `src/routes/health.route.js`
- New read-only `GET /debug/pipeline` exposing: `queue_backend`, `queue_key` (stream),
  `group`, `stream_length`, `pending`, `rejected_length`, `redis_reachable` (bool),
  `dispatcher.running` + `dispatcher.lastSuccessAt`, `bigquery_configured` (bool).
  **No secrets**: Redis URL is redacted upstream and not returned here; service-account
  contents are never touched. Mounted via the existing `app.use("/", healthRoutes)`.

## Acceptance-criteria notes for the architect
- 200 only when durably persisted: `pixel.controller.trackPixel` `await persistRequest(...)`
  before `sendPixel(res)` (disk append awaited) — unchanged, documented here.
- Single event traceable by `event_hash`: server request log → `redis-enqueue-*` →
  `redis-consume-*` / `bigquery-insert-*` / `worker-batch-summary` all carry `event_hash`.
- dedup now drops only exact duplicate resends (hash widened); distinct interactions differ.
