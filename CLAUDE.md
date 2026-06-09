# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

- Install deps: `npm install`
- Run web server (HTTP/pixel endpoint): `npm start` (alias for `node src/server.js`) — listens on `PORT` (default `8080`); cluster-forks when `WEB_CONCURRENCY > 1`.
- Run BigQuery worker (drains Redis stream into BigQuery): `npm run start:worker`
- Run dispatcher (rotates disk queue → Redis stream): `npm run start:dispatcher`
- Run full test suite: `npm test` (Node's built-in `node --test`, sequential via `--test-concurrency=1`)
- Run a single test file: `node --test tests/<name>.spec.js`
- Synthetic traffic generator against a running server: `npm run test:tracking -- --baseUrl=http://127.0.0.1:8080/p.gif --sessions=10`
- Docker image: `docker build -t pixel-server .`
- Full prod-style deploy (Docker + Redis + nginx + N workers + N dispatchers): `./scripts/deploy-prod.sh` — requires `app/credentials/pixel-writer-key.json` to exist.

## Architecture

The server is a high-throughput tracking-pixel endpoint. The critical design constraint is that `/p.gif` must respond with a 1×1 GIF immediately — actual delivery to BigQuery happens asynchronously through a three-stage pipeline split across three Node processes.

### Request pipeline (one event)

1. **Web (`src/server.js` → `app.js` → `routes/pixel.route.js` → `controllers/pixel.controller.js`)**
   - `buildEvent(req)` (`services/event.service.js`) extracts reserved query params (`e`, `pid`, `sid`, `playableId`, `platform`, `camp`/`campaign_raw`, `env`) and lumps the rest into `params`.
   - `resolveTableName(event)` picks the BigQuery table based on `env`: `production` → `pixel_events_production`, anything else → `pixel_events_ver_2` (see `TABLE_BY_ENVIRONMENT` in `bigquery.service.js`).
   - `buildRow(event)` produces the BigQuery row shape and computes a deterministic `event_hash` (SHA-256 over normalized fields) used as the BigQuery `insertId` for dedupe.
   - `persistRequest({ tableName, row })` writes one NDJSON line to a sharded pending file under `data/bigquery-queue/pending/pending-<shard>.ndjson`. Writes are coalesced per file via `setImmediate` (`getAppendState` / `flushPendingAppends`). On any failure the pixel is still served, but with status 503.

2. **Dispatcher (`src/dispatcher.js` → `services/request-dispatcher.service.js`)**
   - Loops: `rotatePendingFiles()` renames hot pending shards into `data/bigquery-queue/ready/` based on size (`REQUEST_QUEUE_ROTATE_MIN_BYTES`) or age (`REQUEST_QUEUE_ROTATE_MAX_AGE_MS`).
   - `claimReadyFiles()` atomically renames ready files into `data/bigquery-queue/processing/processing--<worker>-<hash>.ndjson` (the rename is the lock).
   - Parses each file and pushes batches into the Redis Stream (`REDIS_QUEUE_STREAM`, default `pixel:events`) via `XADD … MAXLEN ~ N` (capped log).
   - On success: `unlink` the processing file. On failure: rename it back to `ready/` (`releaseProcessingFile`) so another dispatcher can retry.
   - **Important:** the web server can also run the dispatcher in-process when `REQUEST_QUEUE_BRIDGE_ENABLED=true`, but a warning is logged — production deploys a dedicated `pixel-dispatcher` container instead.

3. **BigQuery Worker (`src/worker.js` → `services/bigquery-worker.service.js`)**
   - `XREADGROUP GROUP pixel-workers <consumer> COUNT N BLOCK ms STREAMS pixel:events >` to consume new entries; `XAUTOCLAIM` to reclaim stale pending entries from dead consumers (`BIGQUERY_WORKER_LEASE_MS`).
   - Groups items by `tableName`, chunks by `BIGQUERY_BATCH_SIZE`, calls `bigquery.service.insertBatch`.
   - On partial row failures: classifies each row error by reason; retryable reasons (`backendError`, `internalError`, `rateLimitExceeded`, `quotaExceeded`, `timeout`, `stopped`) are re-`XADD`'d to the main stream with `attempts++`; non-retryable rows or rows past `BIGQUERY_MAX_RETRIES` are written to `REDIS_REJECTED_STREAM` (default `pixel:rejected`) and dropped, plus logged via `logInsertError`. The original stream chunk is always `XACK + XDEL`'d so it doesn't get reprocessed.

### BigQuery schema mapping (`services/bigquery.service.js`)

- `getTableSchema(tableName)` fetches and caches the table's `fields[]` from BigQuery metadata; rows are then normalized to match each field's type. JSON-typed columns are serialized via `JSON.stringify` by default (`jsonMode: "string"`), but the helper supports `jsonMode: "native"`. Fields not present in the live schema are dropped at insert time.
- Rows are validated locally before `table.insert` so misformed payloads surface as `BIGQUERY_ROW_VALIDATION` rather than opaque BigQuery errors.
- `event_hash` is reused as BigQuery's `insertId` for streaming-insert dedupe.

### Configuration

All configuration funnels through `src/config/env.js` (loaded via `dotenv` from a `.env` that is never committed). Notable env vars beyond the ones in `docs/bigquery-setup.md`:

- `WEB_CONCURRENCY` — number of cluster workers in `server.js`.
- `BIGQUERY_QUEUE_DIR` / `BIGQUERY_QUEUE_SHARDS` — disk-queue layout.
- `REQUEST_QUEUE_ROTATE_MIN_BYTES` / `REQUEST_QUEUE_ROTATE_MAX_AGE_MS` — when the dispatcher rotates pending shards.
- `REDIS_URL`, `REDIS_QUEUE_STREAM`, `REDIS_QUEUE_GROUP`, `REDIS_REJECTED_STREAM`, `REDIS_QUEUE_MAXLEN`.
- `BIGQUERY_WORKER_LEASE_MS` — `XAUTOCLAIM` idle threshold; tune in concert with worker poll interval.
- `BIGQUERY_ENABLED` is the master switch — if `false`, the worker startup fails fast (`isBigQueryConfigured()`).

### Production topology (`scripts/deploy-prod.sh`)

The script tears down and re-launches the full stack on a single Docker host:
- 1 Redis (`redis:7-alpine`, in-memory only — `--save "" --appendonly no`)
- `APP_REPLICAS` pixel-server containers (default 8) behind 1 nginx container that listens on host port 9000
- `WORKER_COUNT` BigQuery worker containers (default 4)
- `DISPATCHER_COUNT` dispatcher containers (default 1)

All non-web containers share the `pixel-server-net` Docker network and mount the host `./data` dir into `/app/data` — that mount is how the dispatcher (running in its own container) sees the NDJSON files that the pixel-server containers wrote.

Because Redis runs without persistence, the only durable buffer for in-flight events is the on-disk queue under `./data/bigquery-queue`. Treat that directory as production state.

### Logging

`services/log.service.js` writes JSON lines to `logs/pixel-tracking.txt` and daily NDJSON files such as `logs/server/<date>.ndjson`, `logs/dispatcher/<date>.ndjson`, `logs/worker/<date>.ndjson`, and `logs/redis-queue/<date>.ndjson`. All structured logs across the codebase use `console.log/error(JSON.stringify({...}))` with a `ts` field — preserve that pattern instead of emitting plain strings.

## Conventions

- CommonJS (`require`/`module.exports`), 4-space indent, double quotes, semicolons.
- Filenames are kebab-case with a domain suffix: `*.controller.js`, `*.service.js`, `*.route.js`, `*.spec.js`.
- Tests are Node's built-in test runner (`node:test` + `node:assert/strict`); HTTP tests use `supertest`. Run sequentially because several specs mutate the on-disk queue.
- `tests/<area>.spec.js` mirrors `src/services/<area>.js` (and the queue tests `resetQueueState()` in `beforeEach`/`after` — keep that pattern when adding queue tests so parallel state doesn't leak).
- Never commit anything under `app/credentials/` (gitignored except `.gitkeep`); the BigQuery service account JSON is mounted into containers at deploy time.
