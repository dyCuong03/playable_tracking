# Pixel pipeline — Phase 2 (silent-failure prevention) REPORT2

Owner: pipeline-architect (READ-ONLY on src/tests). Source of truth: `investigation/CONTRACT2.md`.
Builds on commit `2bdbe8a` (phase-1 fix). **Status: COMPLETE** — all phase-2 changes landed, independently
re-verified by the architect (`npm test` 78/76/0/2-gated; live `/debug/pipeline` + `ops:check-pipeline`
captured; both 5k loads zero-loss). Working tree is **uncommitted** — commit/deploy decision is the user's.

> Sources: backend `investigation/PATCH2.md`, qa `investigation/TEST2.md` + `investigation/LOAD2.md`, plus the
> architect's own direct source reads (all file:line citations confirmed) and independent re-runs.

---

## 0. Architect verification (CONTRACT2 requirement 1)

- **Baseline:** `git log -1` → `2bdbe8a Fix silent event loss…` (phase-1, 4 files committed).
- **Flow mapped** client→nginx→pixel-server→disk queue→dispatcher→Redis→worker→BigQuery (file:line below in §1).
- **Config alignment VERIFIED ✅:**
  - server & dispatcher share the disk queue — `BASE_DIR = resolve(cwd, bigQueryQueueDir)`
    (`bigquery-queue.service.js:12`); `deploy-prod.sh` sets `BIGQUERY_QUEUE_DIR` + mounts
    `${QUEUE_ROOT_DIR}:/app/data` for server (`:165,182`) and dispatcher (`:261,280`).
  - dispatcher & worker share Redis — `REDIS_URL`/`REDIS_QUEUE_STREAM`/`REDIS_QUEUE_GROUP` set identically
    for all tiers (`deploy-prod.sh:171-173,235-237,267-269` → `redis://pixel-redis:6379`, `pixel:events`,
    `pixel-workers`); consumed at `redis-queue.service.js:423,447-476`.
  - worker writes the expected dataset/table — `resolveTableName`/`TABLE_BY_ENVIRONMENT`
    (`bigquery.service.js:15-18,66-80`); dataset `playable_tracking`.
- **One event traced by `event_hash`:** `tests/trace2.spec.js` asserts the same hash at HTTP
  (`disk-persist-success`) → disk NDJSON row → Redis (`redis-enqueue-success`) → worker
  (`bigquery-insert-attempt` + `-success`) → mock sink; architect re-ran the suite green.
- **CONTRACT2 core problem confirmed & fixed:** phase-1 `/debug/pipeline` returned the web process's own
  in-memory `bridgeState` (`request-dispatcher.service.js` `getDispatcherSummary`), so in prod's separate
  containers it always showed `dispatcher.running=false` with zero worker/BQ visibility. Phase 2 replaces this
  with Redis-backed cross-process heartbeats (see §1).

---

## 1. Patch summary (from PATCH2.md, verified from source)

**New `src/services/pipeline-health.service.js`** (cross-process liveness via Redis heartbeats):
- `recordHeartbeat(role, fields)` → `SET pixel:health:<role> <json> EX ttl` via `sendCommandSafe`
  (best-effort, never throws into the hot path). `role ∈ {web, dispatcher, worker:<id>}` (`:32-46`).
- `readHeartbeats()` → `GET pixel:health:web|dispatcher` + bounded `SCAN pixel:health:worker:*`; a missing
  (expired) key = that tier dead/stuck; attaches `heartbeatAgeMs` from `ts` (`:101-116`).
- `computePipelineStatus(snapshot)` → `{pipeline_status, degraded_reasons[], unhealthy_reasons[]}` implementing
  the CONTRACT2 rules exactly (`:125-209`): **unhealthy** = redis unreachable / dispatcher stale while disk
  backlog>0 ("dispatcher stuck; disk backlog stranded" — the original incident) / web accepting + stream>0 +
  no recent BQ insert / no workers while stream>0; **degraded** = disk backlog>warn / stream>warn / worker
  consume stale / bqFailures or rejected>0 / dispatcher recent error.
- `buildPipelineReport()` (`:248-324`) aggregates redis stats + disk stats + heartbeats into the full
  read-only report (with `withTimeout` guards so a health read can't hang).

**Heartbeat writers wired:**
- web — `pixel.controller.js:23` `void recordHeartbeat("web", {lastAcceptAt})`, coalesced (once per second).
- dispatcher — `request-dispatcher.service.js:112` per loop `recordHeartbeat("dispatcher", {running,lastSuccessAt,lastErrorAt,dispatcher_id,diskBacklog})`.
- worker — `bigquery-worker.service.js:53` per loop `recordHeartbeat("worker:<id>", {lastConsumeAt,lastInsertAt,bqFailureCount,worker_id})`.

**Endpoints** (`health.route.js`): `GET /debug/pipeline` (`:95`) and `GET /health?queue=1` (`:46-58`) both
return `buildFullReport()` — the full CONTRACT2 §54 field set (`pipeline_status`, `degraded_reasons[]`,
`unhealthy_reasons[]`, `disk_queue{pending,ready,processing,total}`, `dispatcher{...}`, `redis_reachable`,
`queue_type:"stream"`, `stream_length`, `pending`, `rejected_length`, `workers[]`, `bigquery{configured,
lastInsertAt,failureCount}`). Redis URL redacted upstream (`redis_url:null` when unreachable); SA JSON never
touched. A dev-mode fallback merges in-process `bridgeState` when no dispatcher heartbeat exists (`:77-90`).

**New logs:** `disk-persist-success` / `disk-persist-failed` (controller), `dispatcher-status` +
`dispatcher-backlog-summary` (dispatcher loop), `bigquery-insert-attempt` (worker) — all present in src.

**Ops check:** `scripts/check-pipeline.js` + `npm run ops:check-pipeline` (`package.json:11`). Exit codes
**0 healthy / 1 unhealthy / 2 degraded / 3 unknown**; default HTTP mode (`GET /debug/pipeline`) + `--direct`
mode (computes from Redis+disk via the same `buildPipelineReport`, for VPS cron). Read-only; no destructive
Redis. Plus an `ops/bin/pipeline-check-loop.sh` wrapper.

**Config:** 9 `PIPELINE_*` env vars in `src/config/env.js:70-78` with defaults (TTL, dispatcher/worker/BQ
stale = 30000ms, disk backlog warn = 5000, stream warn = 10000, health key prefix `pixel:health:`).

---

## 2. Files changed

New (untracked): `src/services/pipeline-health.service.js`, `scripts/check-pipeline.js`,
`ops/bin/pipeline-check-loop.sh`, `tests/{stuck-state,pipeline-health,ops-check-pipeline,trace2}.spec.js`.

Modified (`git diff --stat --ignore-all-space HEAD`, EOL-safe):
```
package.json                               |  1 +   (ops:check-pipeline script)
src/config/env.js                          | 13 +   (PIPELINE_* thresholds)
src/controllers/pixel.controller.js        | 53 +   (web heartbeat + disk-persist logs)
src/routes/health.route.js                 | 76 +-  (/debug/pipeline + /health?queue=1 full report)
src/services/bigquery-worker.service.js    | 38 +   (worker heartbeat + bigquery-insert-attempt)
src/services/redis-queue.service.js        | 12 +   (sendCommandSafe export)
src/services/request-dispatcher.service.js | 48 +   (dispatcher heartbeat + status/backlog logs)
scripts/load-pipeline.js                   |  4 +-  (qa: disk-persisted column)
tests/helpers/fake-redis.js                | 120 +  (qa: heartbeat-key surface)
tests/helpers/pipeline-harness.js          | 21 +   (qa: release-on-error drain)
```
Scopes respected: backend src/** + script + ops wrapper; qa tests/** + harness. No `git add -A` used.

---

## 3. Tests added (from `investigation/TEST2.md`)

All drive the real services against an in-memory fake-Redis (now with heartbeat keys + real TTL expiry) +
mock BigQuery. New specs (all pass):
- `tests/pipeline-health.spec.js` — `computePipelineStatus` rule table (every CONTRACT2 reason → correct
  status, asserted on the real snapshot shape); `/debug/pipeline` unhealthy on stuck-dispatcher+backlog
  (disk_queue.total visible, no secrets) → healthy after full drain.
- `tests/ops-check-pipeline.spec.js` — `check-pipeline.js` exit codes healthy=0 / unhealthy=1 (HTTP mode,
  async spawn against in-process server).
- `tests/stuck-state.spec.js` — dispatcher stopped → backlog visible → restart drains 0-loss; worker stopped
  → XLEN grows → restart drains 0-loss; redis down → enqueue throws (not silent) → recovery drains 0-loss.
- `tests/trace2.spec.js` — one `event_hash` across HTTP→disk→redis→worker→mock-BQ incl. new logs; real
  dispatcher loop writes `pixel:health:dispatcher` + `dispatcher-backlog-summary`.

---

## 4. Commands run + results (architect, independent)

- `npm test` → **78 tests, 76 pass, 0 fail, 2 skipped** (gated docker/BQ). Matches qa.
- `node scripts/load-pipeline.js --events=5000 --mix=start` → PASS, LOST=0.
- `node scripts/load-pipeline.js --events=5000 --mix=mixed` → PASS, LOST=0.
- `PORT=8099 node src/server.js &` then `curl /debug/pipeline` → captured (§5).
- `PORT=8099 node scripts/check-pipeline.js` → printed `UNHEALTHY: redis unreachable`, **exit 1** (correct).
- `git diff --stat --ignore-all-space HEAD` → only the intended files (§2); no EOL-only noise.
- Source reads — all §0/§1 file:line citations confirmed.
- Gated/not runnable here (Docker Desktop WSL integration off, redis-cli absent): `RUN_REDIS_IT=1` live-Redis
  integration and `check-pipeline.js --direct` against a live Redis — both runnable on the VPS.

---

## 5. Health/debug sample output (captured live)

`GET /debug/pipeline` from a locally-started server with no Redis reachable — shows the full field set,
credential redaction (`redis_url:null`), and correct `unhealthy` verdict when the shared store is down:

```json
{
  "ok": true,
  "pipeline_status": "unhealthy",
  "degraded_reasons": [],
  "unhealthy_reasons": ["redis unreachable"],
  "queue_backend": "redis-stream",
  "queue_type": "stream",
  "queue_key": null,
  "group": null,
  "redis_reachable": false,
  "redis_url": null,
  "stream_length": null,
  "pending": null,
  "rejected_length": null,
  "disk_queue": { "pending": 0, "ready": 0, "processing": 0, "total": 0, "approxItems": 0 },
  "dispatcher": { "running": false, "lastSuccessAt": null, "lastErrorAt": null,
                  "dispatcher_id": null, "heartbeatAgeMs": null, "diskBacklog": null },
  "web": null,
  "workers": [],
  "bigquery": { "configured": false, "lastInsertAt": null, "failureCount": 0 }
}
```
`ops:check-pipeline` (HTTP mode) on the same state → `UNHEALTHY: redis unreachable`, **exit 1**.
(Equivalently, `node scripts/check-pipeline.js --direct --json` prints this exact report object with no infra.)

### Evidence 1 — one event_hash traced across all tiers (`node scripts/trace-event.js`, qa)

```
event_hash = bd5f56799ac671e6cb3625e2f62b11ec4b190b1ed7a1959c7a8e99cb526a4bc7
[HTTP/disk]  disk-persist-success     event_hash=bd5f…4bc7 session_id=trace-session-1 env=test
[dispatcher] redis-enqueue-success    event_hash=bd5f…4bc7 queue_key=pixel:events message_id=1-0 stream_len=1
[worker]     bigquery-insert-attempt  event_hashes=[bd5f…4bc7]
[worker]     bigquery-insert-success  event_hashes=[bd5f…4bc7]
mock BigQuery sink contains row with this hash: true   |   SAME event_hash at every stage: true
```

### Evidence 2 — stuck states flip pipeline_status (the heart of phase 2)

```
DISPATCHER STOPPED (events accepted, bridge off):
  pipeline_status = unhealthy
  unhealthy_reasons = ["dispatcher stuck; disk backlog stranded"]   ← THE ORIGINAL INCIDENT, now visible
  disk_queue.total = 4 (backlog visible), stream_length = 0

WORKER STOPPED (entries in stream, no consumer):
  pipeline_status = unhealthy
  unhealthy_reasons = ["events accepted but nothing inserted to BigQuery", "no workers consuming"]
  degraded_reasons = ["worker consume stale while stream non-empty"]
  stream_length = 20 (depth visible)
```

These (and healthy-after-drain) are asserted in `tests/pipeline-health.spec.js` / `tests/stuck-state.spec.js`
against the real `buildPipelineReport`/`computePipelineStatus` code path.

---

## 6. Load result table (from `investigation/LOAD2.md`, re-run by architect)

Invariant: **accepted == disk persisted == enqueued == consumed == inserted + failed + dedup**, **LOST=0**.

| metric | 5k start | 5k mixed |
|---|---|---|
| accepted HTTP (200) | 5000 | 5000 |
| disk persisted (NDJSON) | 5000 | 5000 |
| redis enqueued (XADD) | 5000 | 5000 |
| worker consumed | 5000 | 5000 |
| dedup skipped | 0 | 0 |
| inserted (BigQuery) | 5000 | 5000 |
| failed (rejected) | 0 | 0 |
| **LOST** | **0** | **0** |

Stuck-state runs (`tests/stuck-state.spec.js`, all zero-loss):

| Scenario | Observable symptom | Recovery |
|---|---|---|
| Dispatcher stopped (50) | HTTP 200; disk backlog>0; XADD=0; 0 inserted | restart bridges all 50, inserted 50, disk=0 |
| Worker stopped (40) | `XLEN`=40 (depth visible); 0 inserted | restart drains 40, stream=0 |
| Redis down→recovery (35) | HTTP 200 (disk); enqueue throws (not silent) | recovery drains+inserts 35 |

Health verdicts: stuck-dispatcher+backlog → unhealthy (exit 1); idle/empty → healthy (exit 0);
backlog/stream over-warn or rejected>0 → degraded (exit 2).

---

## 7. Production verification checklist (read-only; no destructive Redis)

Run redis-cli inside the container: `docker exec -it pixel-redis redis-cli <cmd>`.

- [ ] Tiers up incl. dispatcher: `docker ps --format '{{.Names}}\t{{.Status}}' | grep pixel-`
      (1 redis, 1 nginx, APP_REPLICAS servers, WORKER_COUNT workers, ≥1 dispatcher).
- [ ] **Cross-process heartbeats present:** `redis-cli --scan --pattern 'pixel:health:*'` — expect
      `pixel:health:web`, `pixel:health:dispatcher`, `pixel:health:worker:*`. A MISSING key = that tier dead.
      `redis-cli TTL pixel:health:dispatcher` (should be > 0 and refreshing).
- [ ] **Authoritative status:** `curl -s 'http://127.0.0.1:9000/debug/pipeline'` → `pipeline_status` +
      `unhealthy_reasons`/`degraded_reasons`; check `disk_queue.total`, `stream_length`, `workers[]`,
      `dispatcher.heartbeatAgeMs`, `bigquery.lastInsertAt`. Confirm `redis_url` is null/redacted.
- [ ] **Ops gate / cron:** `npm run ops:check-pipeline` (HTTP) or `node scripts/check-pipeline.js --direct`
      on the dispatcher/worker host → exit 0/1/2 for alerting; `ops/bin/pipeline-check-loop.sh` for a loop.
- [ ] Stream/group health: `redis-cli TYPE pixel:events` (=stream), `XLEN pixel:events`,
      `XINFO GROUPS pixel:events`, `XPENDING pixel:events pixel-workers`, `XLEN pixel:rejected`.
- [ ] New log types: `grep -h '"type":"disk-persist-success"' logs/server/*.ndjson`;
      `'"type":"dispatcher-backlog-summary"'` + `'"dispatcher-status"'` in `logs/dispatcher/*.ndjson`;
      `'"type":"bigquery-insert-attempt"'` in `logs/worker/*.ndjson`.
- [ ] Trace one event end-to-end: `grep -Rh '<event_hash>' logs/server logs/dispatcher logs/worker`.

---

## 8. Rollback plan

**Current state: uncommitted** (per team-lead; commit/deploy is the user's call).
- **Pre-commit:** discard phase-2 by removing the 3 new src/script/ops files and the 4 new specs, and
  restoring the modified files: `git checkout HEAD -- package.json src/config/env.js
  src/controllers/pixel.controller.js src/routes/health.route.js src/services/bigquery-worker.service.js
  src/services/redis-queue.service.js src/services/request-dispatcher.service.js scripts/load-pipeline.js
  tests/helpers/fake-redis.js tests/helpers/pipeline-harness.js`, then `rm` the new untracked files
  (`pipeline-health.service.js`, `check-pipeline.js`, `ops/bin/pipeline-check-loop.sh`, the 4 new specs).
- **Post-commit:** `git revert <SHA>` of the phase-2 commit, then re-run `scripts/deploy-prod.sh`.
- Phase 2 is **purely additive observability** (a new module, new logs, extra read-only endpoint fields, an
  ops script) — reverting it changes no data path and cannot reintroduce loss; it only removes visibility.
- **No destructive Redis commands.** Heartbeat keys (`pixel:health:*`) self-expire via TTL — no cleanup needed.

---

## 9. FINAL VERDICT

**FIXED** (for the silent-failure objective), with documented operational caveats.

**"Can the previous silent missing-data issue still happen?" — No, not silently.** The original incident was:
events accepted (HTTP 200, disk-persisted) but never reaching BigQuery because the dispatcher/worker was
down, with **no visibility** (phase-1 `/debug/pipeline` could only see the web process's own in-memory state).
Evidence it is now surfaced AND non-lossy:

1. **Cross-process detection** — every tier writes a TTL'd Redis heartbeat (`pixel:health:*`); a dead tier =
   a missing key. `computePipelineStatus` flips to **unhealthy** on exactly the original incident ("dispatcher
   stuck; disk backlog stranded"), plus "events accepted but nothing inserted", "no workers consuming", and
   "redis unreachable". Asserted in `tests/pipeline-health.spec.js`; the dead-store case captured live in §5.
2. **Alertable** — `npm run ops:check-pipeline` exits 1 (unhealthy) / 2 (degraded) for cron; verified exit 1
   live in §4, exit-code test in `tests/ops-check-pipeline.spec.js`.
3. **No loss, only delay** (carried from phase 1, re-proven) — accepted events sit durably on disk / in the
   Redis stream and **fully drain on recovery**: dispatcher-stopped 50/50, worker-stopped 40/40, redis-down
   35/35, both 5k loads LOST=0 (§6).

**Residual OPERATIONAL requirements (not code gaps; they do not change the verdict).** Phase 2 makes the
failure *detectable and non-lossy*; turning detection into prevention is an ops wiring step:
- **Wire `ops:check-pipeline` into cron + an alert channel.** The script and `ops/bin/pipeline-check-loop.sh`
  loop wrapper are provided (exit 1/2 for unhealthy/degraded), but a deploy must schedule it and route the
  non-zero exit to a human/pager. Until then the status is visible on `/debug/pipeline` but not pushed.
- **Dispatcher/worker restart policy.** `deploy-prod.sh` runs containers with `--restart always`, so a crashed
  dispatcher/worker auto-restarts and the disk/stream buffer drains with zero loss (§6). A *hung* (not
  crashed) process is caught by the stale-heartbeat → unhealthy signal, which the cron alert must act on.
- **Heartbeats live in Redis**, so if Redis itself is down no tier can write them — but that case is itself
  reported as `redis unreachable` → unhealthy (verified live, §5), so it is still not silent.
- The live-Redis integration (`RUN_REDIS_IT=1`) and `check-pipeline.js --direct` against a live Redis were
  validated by logic + the fake-redis path here and are runnable on the VPS (Docker Desktop WSL integration is
  off on this host; redis-cli not installed).

### Acceptance-criteria map (CONTRACT2)

| Requirement | Status | Evidence |
|---|---|---|
| 1. Verify flow + same disk/redis/table config + trace one event_hash | ✅ | §0 |
| 2. Redis-backed heartbeats (recordHeartbeat/readHeartbeats/computePipelineStatus) | ✅ | §1; `pipeline-health.service.js` |
| 3. Writers in web/dispatcher/worker | ✅ | §1 (controller:23, dispatcher:112, worker:53) |
| pipeline_status rules (unhealthy/degraded/healthy) | ✅ | `computePipelineStatus`; `pipeline-health.spec.js` |
| /debug/pipeline + /health?queue=1 full field set, no secrets | ✅ | §5 live sample; `health.route.js` |
| 4. New logs (disk-persist-*, dispatcher-status, dispatcher-backlog-summary, bigquery-insert-attempt) | ✅ | grep in src |
| 4. ops:check-pipeline exit 0/1/2 + --direct + ops wrapper | ✅ | §4 exit 1 live; `check-pipeline.js`; `pipeline-check-loop.sh` |
| 5. Tests: e2e trace, stuck-dispatcher, restart drains, redis-down, worker-stopped, bq-temp, dedup, 5k loads, status table, ops exit-code | ✅ | §3/§6; `npm test` 78/76/0/2 |
| Original silent missing-data issue prevented | ✅ FIXED | §9 (1)(2)(3) |
