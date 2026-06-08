# ops/ — pixel-server monitoring & load-capacity toolkit

Self-contained ops layer for the playable-tracking pixel server. Four roles run as
background loops and feed a live 4-pane tmux dashboard. Built by the `pixel-ops`
agent team; safe to run before the server is deployed (every probe degrades
gracefully when the target is down).

## Roles

| Role | Script | Purpose | Output |
|------|--------|---------|--------|
| **monitor** | `bin/monitor.sh` + `bin/monitor-loop.sh` | Daily server-status snapshots; raises alerts on overload / timeout / missing-data | `logs/<date>/status.ndjson`, `status/monitor-latest.json`, `status/alerts.ndjson` |
| **logcollector** | `bin/logcollector.sh` + `bin/logcollector-loop.sh` | Records all backend logs (app + docker + pm2) into a daily archive, rolls up errors | `logs/<date>/{app.log,docker/,errors-rollup.txt}`, `status/logcollector-latest.json` |
| **loadtester** | `bin/stress.sh`, `bin/hammer.js`, `bin/capacity-loop.sh`, `bin/capacity-trend.sh` | Ramping stress test to find the capacity knee; scheduled daily re-test + trend | `status/last-stress-verdict.json`, `reports/capacity-history.ndjson` |
| **planner** | `bin/plan.sh` | Generates the optimal load-bearing plan (sizing, overload, timeout, durability) from measured capacity | `reports/capacity-plan.md`, `status/capacity-plan.json` |

`lib/common.sh` holds shared helpers (`jlog`, `heartbeat`, timestamps, paths,
`PIXEL_BASE`/`HEALTH_URL`/`PIXEL_URL`). Source it; never edit it inline.

## Local usage

```bash
# 1. (optional) point at a non-default target
export PIXEL_BASE=http://127.0.0.1:9000

# 2. start the background loops (monitor + logcollector + capacity)
bash ops/bin/ops-start.sh        # idempotent — re-run any time, never duplicates

# 3. check daemon health
bash ops/bin/ops-status.sh       # table; also `json` or `gate` (CI exit code)

# 4. open the live 4-pane dashboard
bash ops/bin/dashboard.sh        # detach with Ctrl-b d

# 5. generate / refresh the load-bearing plan on demand
bash ops/bin/plan.sh

# stop everything
bash ops/bin/ops-stop.sh
```

## VPS usage

Run the toolkit on the host where the server runs (it needs to see
`data/bigquery-queue/`, docker logs, and the server processes).

```bash
ssh user@vps && cd /path/to/playable_tracking
git pull origin main

# deps the toolkit uses (redis-tools gives redis-cli; tmux for the dashboard)
sudo apt install -y tmux python3 curl redis-tools
chmod +x ops/bin/*.sh ops/tests/*.sh

export PIXEL_BASE=http://127.0.0.1:9000     # deploy-prod nginx host port
bash ops/bin/ops-start.sh
bash ops/bin/ops-status.sh
```

Keep the loops alive across crashes/reboots with cron (each entry re-runs the
idempotent reconcile, so no duplicate daemons are ever created):

```cron
*/5 * * * * cd /path/to/playable_tracking && PIXEL_BASE=http://127.0.0.1:9000 bash ops/bin/ops-start.sh >> ops/logs/cron-reconcile.out 2>&1
@reboot      cd /path/to/playable_tracking && PIXEL_BASE=http://127.0.0.1:9000 bash ops/bin/ops-start.sh >> ops/logs/cron-reconcile.out 2>&1
```

Notes: prod redis runs in a container (not on host `localhost`), so host
`redis-cli` can't reach it — use the Docker-exec fallback below instead. Add the
deploy user to the `docker` group so the monitor/logcollector can read containers.

Copy `ops/.env.example` to set the Docker/Redis/pixel-probe vars in one place:

```bash
cp ops/.env.example ops/.env && $EDITOR ops/.env
set -a; . ops/.env; set +a      # export everything
bash ops/bin/ops-start.sh
```

## Minimal config after deploy

After the first `git pull` + `./scripts/deploy-prod.sh`, create `ops/.env` **once**
on the VPS.  The file is gitignored and survives every redeploy.  `ops-start.sh`
sources it automatically; CI never overwrites it.

**Setup A — same VPS, one Docker Compose project (most common)**

```bash
cat > ops/.env <<'EOF'
PIXEL_BASE=http://127.0.0.1:9000
OPS_DOCKER_COMPOSE_PROJECT=playable_tracking   # compose project name
OPS_REDIS_QUEUE_KEY=pixel:events               # stream key used by XLEN → LLEN fallback
EOF
set -a; . ops/.env; set +a
bash ops/bin/ops-start.sh
```

The monitor and logcollector discover all containers whose name starts with the
compose project (e.g. `playable_tracking-nginx-1`, `playable_tracking-app-1`, …)
without you having to enumerate them explicitly.  Redis depth is read via
`docker exec <OPS_REDIS_CONTAINER> redis-cli` — set `OPS_REDIS_CONTAINER` to the
Redis container name if needed (see `ops/.env.example`).

**Setup B — auto-detect (only PIXEL_BASE, no explicit container list)**

```bash
echo "PIXEL_BASE=http://127.0.0.1:9000" > ops/.env
set -a; . ops/.env; set +a
bash ops/bin/ops-start.sh
```

Discovery mode: the monitor lists every running container without raising alerts
for missing names.  Redis depth is skipped unless `OPS_REDIS_CONTAINER` is set.

**Setup C — remote observation node (pixel probe only, no docker/redis)**

```bash
echo "PIXEL_BASE=https://pixel.example.com" > ops/.env
# Optionally, provide compose project for informational discovery only:
# echo "OPS_DOCKER_COMPOSE_PROJECT=playable_tracking" >> ops/.env
set -a; . ops/.env; set +a
bash ops/bin/ops-start.sh
```

HTTP, `/health`, and pixel probes work from any host.  Docker and Redis checks
only work when `ops/` runs on the **same host** as the Docker daemon — omit those
vars on a remote box.  View the dashboard by SSHing in; do not expose it publicly.

### Docker permission setup (same-VPS only)

If Docker is installed but `docker ps` fails without `sudo`:

```bash
sudo usermod -aG docker $USER
# log out and back in (or: newgrp docker)
docker ps    # must succeed without sudo
```

The monitor and logcollector run as the deploy user and degrade silently to
`permission_denied` if the user is not in the `docker` group.
*Note: Docker CLI is unavailable or non-functional in some WSL2 distros — this
is expected; the monitor records `no_cli` / `permission_denied` and continues.*

### Dashboard (local-over-SSH only)

`ops/bin/dashboard.sh` opens a tmux 4-pane window meant to be viewed on the VPS
console or over SSH — it is **not** exposed over HTTP.  Open it with:

```bash
ssh user@vps
cd /path/to/playable_tracking
bash ops/bin/dashboard.sh   # detach: Ctrl-b d
```

### Loadtest / capacity

`LOADTEST_ENABLED=0` and `CAPACITY_ENABLED=0` are the defaults and are **never
changed by CI**.  The capacity daemon heartbeats and polls health, but does not
stress the target unless an operator explicitly sets `CAPACITY_ENABLED=1` in
the shell or `ops/.env`.  This keeps every deploy and post-deploy CI step load-free.

### `ops/` is observer-only

`ops/` reads from the server and Docker: `docker ps`, `docker inspect`,
`docker logs`, `docker exec … redis-cli` (read-only), and HTTP probes.  It
**never** restarts, recreates, kills, or otherwise mutates application containers.

## Production visibility (Docker, Redis, pixel)

The pixel server is Docker-deployed, so proving `/health` responds is not enough.
These env vars make `ops/` observe the real deployment. **`ops/` is observer-only —
it runs `docker ps/inspect/logs` and `docker exec … redis-cli` (read commands)
and NEVER restarts, recreates, or otherwise mutates application containers.**

### Docker container monitoring

Declare the containers you expect:

```bash
export OPS_DOCKER_CONTAINERS="pixel-nginx pixel-server pixel-redis pixel-worker"
```

The monitor then records each container's state/health/restarts/ports under
`docker` in `ops/status/monitor-latest.json`, and the dashboard MONITOR pane
renders a `== DOCKER HEALTH ==` table. With `OPS_DOCKER_CONTAINERS` set, a
**missing / exited / restarting / healthcheck-unhealthy** container raises a
deduped alert in `ops/status/alerts.ndjson` (one key per container, with
recovery). Unset → the monitor only lists discovered containers and never fails
health on a missing name. logcollector also collects
`docker logs --since <window> --tail <N>` per declared container into
`ops/logs/<date>/docker/<container>.log` and folds error/warn/fatal/exception
lines into `errors-rollup.txt`.

### Docker permission setup

If Docker is installed but you see `permission_ok=false` / `permission_denied`
(monitor) or `sources=0` with a `docker permission denied` warning
(logcollector / dashboard):

```bash
sudo usermod -aG docker $USER
# then log out and back in (or: newgrp docker)
docker ps        # must succeed without sudo
```

### Redis via Docker exec

Prod redis has no published host port, so set the container + queue key and the
monitor reads depth through `docker exec`:

```bash
export OPS_REDIS_CONTAINER=pixel-redis
export OPS_REDIS_QUEUE_KEY=pixel:events    # stream (XLEN) or list (LLEN) — both supported
```

Reported as `{"status":"ok","method":"docker_exec","container":...,"queue_key":...,"depth":N}`.
Host `redis-cli` is tried first (`method:"host_cli"`). If neither is configured
the section reads `not_configured` (no alert); if configured but unreadable it
reads `error` and raises a `redis-unreadable` alert.

### Pixel endpoint probe

A bare `/p.gif` has no query params and returns **HTTP 400 by design** — the
monitor labels this with a `note` so the dashboard never presents it as a real
failure. Point the probe at a valid synthetic event instead:

```bash
export OPS_PIXEL_EXPECT_CODE=200
export OPS_PIXEL_PROBE_PATH='/p.gif?e=interaction&sid=ops_healthcheck&env=ops&event_params=%7B%22name%22%3A%22ops_healthcheck%22%7D'
# or an absolute URL (overrides the path):
# export OPS_PIXEL_PROBE_URL='http://127.0.0.1:9000/p.gif?...'
```

A 200 needs `e=<start|interaction|store_trigger|end>`, `sid`, and the per-event
fields inside the `event_params` JSON blob (URL-encoded `{"name":"ops_healthcheck"}`
above). `env=ops` (≠ `production`) keeps the synthetic row in the `pixel_events_ver_2`
table, not real production analytics, and `ops_healthcheck` marks it. Only a
**configured** probe that misses `OPS_PIXEL_EXPECT_CODE` raises a `pixel-probe-bad`
alert — the default bare 400 never alerts.

### Where to point `PIXEL_BASE`

`http://127.0.0.1:9000` is correct **only when `ops/` runs on the same VPS** as
the server (deploy-prod's nginx publishes host port 9000). If `ops/` runs
remotely, point it at the public IP / domain instead — and note Docker/Redis
checks need to run **on the host**, since `docker exec` and `docker logs` are
local to the Docker daemon:

```bash
export PIXEL_BASE=https://pixel.example.com    # remote HTTP probes only
```

### Capacity / plan

Loadtesting is **off by default** and never auto-runs on deploy
(`LOADTEST_ENABLED=0`, `CAPACITY_ENABLED=0`); the capacity pane explains the
empty state instead of looking broken. `plan.sh` always produces a baseline —
**no loadtest required** — and flags `measured_capacity_available=false` with a
placeholder banner until a real stress run exists:

```bash
bash ops/bin/plan.sh        # writes reports/capacity-plan.md + status/capacity-plan.json
```

See **Manual capacity test** below to deliberately measure capacity.

## CI/CD

**ops validation** — `.github/workflows/ops-validate.yml` runs on PRs/pushes that
touch `ops/**`: `bash -n` syntax checks for `ops/bin|lib|tests/*.sh`, a node
syntax check for `hammer.js`, and the full `ops/tests/reconcile.test.sh` suite.
Any syntax error or test failure fails the workflow. It never runs the loadtester.

**post-deploy reconcile** — `.github/workflows/deploy.yml` has a final step that
SSHes to the VPS, `cd`s into the repo, exports `PIXEL_BASE`, `chmod +x`es the ops
scripts, runs `ops-start.sh`, then `ops-status.sh gate`. The gate exits non-zero
if `monitor` / `logcollector` / `capacity` are not healthy, **failing the deploy**.
`ops-start.sh` is idempotent (reconciles pidfile + `/proc` cmdline + heartbeat),
so this step never spawns duplicate daemons. All VPS connection details come from
GitHub Secrets (`VPS_HOST`/`VPS_USER`/`VPS_SSH_KEY`/`VPS_PORT`/`VPS_REPO_PATH`) —
nothing is hardcoded.

## Manual capacity test (⚠ generates real load)

The loadtester (`stress.sh` / `hammer.js`) floods the target and is **never** run
automatically on deploy. It is gated so it cannot fire by accident:

- `LOADTEST_ENABLED=1` is required or `stress.sh` refuses (exit 2).
- The ramp is capped at `MAX_CONCURRENCY` and stops once a stage reaches `MAX_RPS`.
- A production target (`LOADTEST_ENV=production`) additionally requires
  `ALLOW_PROD_LOADTEST=1`.

```bash
# manual one-off against a NON-production target
LOADTEST_ENABLED=1 MAX_RPS=3000 MAX_CONCURRENCY=400 \
  PIXEL_BASE=http://127.0.0.1:9000 bash ops/bin/stress.sh
```

Scheduled capacity testing is handled by `capacity-loop.sh` (started by
`ops-start.sh`). It is **off by default**: the daemon heartbeats and polls health
but never runs a stress test until an operator sets `CAPACITY_ENABLED=1`. This is
what keeps a deploy from triggering load. When enabled it self-skips a dead
target, runs at most once per `CAPACITY_INTERVAL`, flips the `stress.sh` master
switch on for its own deliberate run, and still honours the `MAX_RPS` / production
guards. To schedule real capacity tests:

```bash
# start the daemon with scheduled testing enabled (non-production target)
CAPACITY_ENABLED=1 PIXEL_BASE=http://127.0.0.1:9000 bash ops/bin/ops-start.sh
# against production, additionally and explicitly:
CAPACITY_ENABLED=1 LOADTEST_ENV=production ALLOW_PROD_LOADTEST=1 bash ops/bin/ops-start.sh
```

## Layout

```
ops/
  bin/        role scripts + loop wrappers + dashboard/start/stop
  lib/        common.sh (shared shell helpers)
  logs/       <UTC-date>/ daily archives + *-loop.out streams
  reports/    stress-*.ndjson, capacity-history.ndjson, capacity-plan.md
  status/     *.pid, *.heartbeat, *-latest.json, alerts.ndjson, verdicts
```

## Key tunables (env)

| Var | Default | Role |
|-----|---------|------|
| `PIXEL_BASE` | `http://127.0.0.1:9000` | all |
| `MONITOR_INTERVAL` | `30` | monitor loop period (s) |
| `HIGH_LATENCY_MS` / `QUEUE_BACKLOG_FILES` / `STUCK_PROCESSING_S` | `500` / `50` / `300` | monitor alert thresholds |
| `OPS_DOCKER_CONTAINERS` | _(unset)_ | expected containers; set → missing/unhealthy alerts (monitor + logcollector) |
| `OPS_REDIS_CONTAINER` / `OPS_REDIS_QUEUE_KEY` | _(unset)_ / `pixel:events` | redis depth via `docker exec` fallback (monitor) |
| `OPS_PIXEL_PROBE_PATH` / `OPS_PIXEL_PROBE_URL` / `OPS_PIXEL_EXPECT_CODE` | bare `/p.gif` / _(unset)_ / `200` | configurable pixel probe (monitor) |
| `LOGCOLLECT_INTERVAL` | `60` | logcollector loop period (s) |
| `LOGCOLLECT_DOCKER_SINCE` / `LOGCOLLECT_DOCKER_TAIL` | `10m` / `500` | docker-log window per container (logcollector) |
| `ERROR_SPIKE` | `20` | error-spike alert threshold |
| `CAPACITY_INTERVAL` / `CAPACITY_POLL` | `86400` / `300` | capacity re-test cadence / health poll (s) |
| `STAGES` / `STAGE_SECONDS` / `FAIL_RATE` / `FAIL_P95_MS` | `10..1600` / `15` / `0.01` / `500` | stress ramp + degrade thresholds |
| `LOADTEST_ENABLED` | `0` | master switch — `stress.sh` refuses unless `1` |
| `CAPACITY_ENABLED` | `0` | `capacity-loop` only schedules stress runs when `1` |
| `MAX_CONCURRENCY` / `MAX_RPS` | `800` / `5000` | hard ceilings — ramp stops at either |
| `LOADTEST_ENV` / `ALLOW_PROD_LOADTEST` | `test` / `0` | production target requires both set |
| `DASH_REFRESH` | `5` | dashboard pane refresh (s) |

## BigQuery log export

The ops layer can export nginx request logs and Redis queue-depth metrics to
BigQuery for long-term analysis. The feature is **off by default**
(`OPS_BQ_EXPORT_ENABLED=0`) and does nothing unless explicitly enabled on the
VPS. The existing three daemons (monitor, logcollector, capacity) continue to
run normally with or without it.

### What each export does

**nginx request log export** (`OPS_BQ_NGINX_TABLE`, default `nginx_requests`)

Tails the nginx access log inside the nginx container (via `docker exec cat
<path>`) on every `OPS_BQ_EXPORT_INTERVAL_SECONDS` cycle. Each line is parsed
into structured fields (`timestamp`, `method`, `uri`, `status`, `bytes_sent`,
`request_time`, `request_id`, `ip_hash`, `query_params`), cursor-tracked so
lines are never re-exported, and batch-inserted into BigQuery. Combined with the
custom `log_format` described at the end of this section, you get
`request_time` (latency) and a correlation `request_id` per row.

**Redis metrics export** (`OPS_BQ_REDIS_TABLE`, default `redis_metrics`)

On each cycle, reads the Redis stream depth (`XLEN pixel:events`) via
`docker exec` (same path the monitor uses) and writes one row per sample:
`{timestamp, queue_key, depth, worker_count}`. Use this to correlate queue
backlog spikes with error-rate spikes in the nginx table.

### BigQuery setup

#### 1. Create the dataset

```sql
CREATE SCHEMA `<OPS_BQ_PROJECT_ID>.<OPS_BQ_DATASET>`
  OPTIONS (location = 'US');   -- match the region of your pixel_events tables
```

#### 2. Create partitioned tables

**nginx_requests** (partitioned by event date, clustered for common WHERE clauses):

```sql
CREATE TABLE `<project>.<dataset>.nginx_requests`
(
  event_date     DATE         NOT NULL OPTIONS (description='Partition column'),
  timestamp      TIMESTAMP    NOT NULL,
  method         STRING,
  uri            STRING,
  status         INT64,
  bytes_sent     INT64,
  request_time   FLOAT64,     -- seconds; requires custom log_format (see below)
  request_id     STRING,      -- requires $request_id in log_format
  ip_hash        STRING,      -- SHA-256 of client IP (OPS_LOG_HASH_IP=1)
  query_params   JSON         -- filtered by OPS_LOG_QUERY_ALLOWLIST
)
PARTITION BY event_date
CLUSTER BY status, uri
OPTIONS (
  require_partition_filter = false,
  partition_expiration_days = 90
);
```

**redis_metrics**:

```sql
CREATE TABLE `<project>.<dataset>.redis_metrics`
(
  event_date    DATE      NOT NULL OPTIONS (description='Partition column'),
  timestamp     TIMESTAMP NOT NULL,
  queue_key     STRING,
  depth         INT64,
  worker_count  INT64
)
PARTITION BY event_date
OPTIONS (partition_expiration_days = 90);
```

Set `OPS_BQ_CREATE_TABLES=1` to let the exporter create these tables
automatically on first run (uses the schemas above). Leave it at `0` if you
want manual control over schema options.

#### 3. IAM

The service-account that runs the exporter needs two roles on the BigQuery
**dataset** (not the project root — least-privilege):

| Role | Why |
|------|-----|
| `roles/bigquery.dataEditor` | Insert rows, create tables when `OPS_BQ_CREATE_TABLES=1` |
| `roles/bigquery.jobUser`    | Run insert jobs (project-level role) |

```bash
# grant dataEditor on the dataset
gcloud projects add-iam-policy-binding <project> \
  --member="serviceAccount:<sa>@<project>.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor" \
  --condition="expression=resource.name.startsWith('projects/<project>/datasets/<dataset>'),title=ops-export-dataset"

# grant jobUser at project level
gcloud projects add-iam-policy-binding <project> \
  --member="serviceAccount:<sa>@<project>.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

Download the key JSON, copy it to the VPS (never commit it), and set the path in
`ops/.env`:

```bash
scp pixel-ops-sa.json user@vps:/path/to/playable_tracking/app/credentials/pixel-ops-sa.json
# then in ops/.env:
GOOGLE_APPLICATION_CREDENTIALS=/path/to/playable_tracking/app/credentials/pixel-ops-sa.json
```

### Enabling on the VPS

After completing the BigQuery setup above, add these lines to `ops/.env` on the
VPS and restart:

```bash
OPS_BQ_EXPORT_ENABLED=1
OPS_BQ_PROJECT_ID=my-gcp-project
OPS_BQ_DATASET=pixel_ops_logs          # or your chosen name
OPS_BQ_NGINX_TABLE=nginx_requests
OPS_BQ_REDIS_TABLE=redis_metrics
OPS_BQ_EXPORT_INTERVAL_SECONDS=300     # 5-minute buckets
OPS_BQ_EXPORT_BATCH_SIZE=5000
GOOGLE_APPLICATION_CREDENTIALS=/path/to/playable_tracking/app/credentials/pixel-ops-sa.json
OPS_NGINX_CONTAINER=playable_tracking-nginx-1
OPS_NGINX_ACCESS_LOG_PATH=/var/log/nginx/access.log
OPS_REDIS_CONTAINER=playable_tracking-redis-1
OPS_REDIS_QUEUE_KEY=pixel:events
```

Then restart ops:

```bash
set -a; . ops/.env; set +a
bash ops/bin/ops-start.sh
bash ops/bin/ops-status.sh
```

`ops-status.sh` (and `ops-status.sh json`) will show a `bq_export` block once
`OPS_BQ_EXPORT_ENABLED=1`. The deploy-gate (`ops-status.sh gate`) includes
exporter health in its exit code only when the feature is enabled.

### Keeping it disabled safely

`OPS_BQ_EXPORT_ENABLED=0` (the default) is a hard off-switch:

- The `bq-export-loop.sh` daemon is never started by `ops-start.sh`.
- `ops-status.sh` shows `bq_export: disabled` — no missing-daemon alert.
- The deploy gate (`ops-status.sh gate`) ignores the exporter entirely.
- No BigQuery API calls are ever made; no credentials are required.
- The other three daemons (monitor, logcollector, capacity) are unaffected.

If you enable the exporter and later want to turn it off, set
`OPS_BQ_EXPORT_ENABLED=0` in `ops/.env` and run `bash ops/bin/ops-stop.sh` to
kill the exporter loop process.

### Privacy controls

All four privacy vars default to the safest setting:

| Var | Default | Effect |
|-----|---------|--------|
| `OPS_LOG_HASH_IP` | `1` | Replace client IP with `SHA-256(<ip>)` before export |
| `OPS_LOG_INCLUDE_QUERY` | `1` | Include query-string (needed for campaign/platform columns) |
| `OPS_LOG_QUERY_ALLOWLIST` | `"event game source campaign platform"` | Only these param names are kept; all others are dropped |
| `OPS_LOG_DROP_HEADERS` | `1` | Strip `User-Agent`, `Referer`, `Cookie` before upload |

To maximise privacy (at the cost of less analytical detail):

```bash
OPS_LOG_HASH_IP=1
OPS_LOG_INCLUDE_QUERY=0     # drop entire query-string
OPS_LOG_DROP_HEADERS=1
```

To maximise analytics (full query + unhashed IP — only appropriate if legally
permitted in your jurisdiction):

```bash
OPS_LOG_HASH_IP=0
OPS_LOG_INCLUDE_QUERY=1
OPS_LOG_QUERY_ALLOWLIST=""  # keep all params
OPS_LOG_DROP_HEADERS=0
```

### Example BigQuery queries

```sql
-- Requests per day and hour
SELECT
  event_date,
  EXTRACT(HOUR FROM timestamp) AS hour,
  COUNT(*)                      AS requests
FROM `<project>.<dataset>.nginx_requests`
WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
GROUP BY 1, 2
ORDER BY 1, 2;

-- Error rate by HTTP status (last 24 h)
SELECT
  status,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM `<project>.<dataset>.nginx_requests`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY status
ORDER BY n DESC;

-- p.gif pixel-event volume by day
SELECT
  event_date,
  COUNT(*) AS pixel_hits
FROM `<project>.<dataset>.nginx_requests`
WHERE uri LIKE '/p.gif%'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

-- Top campaign and platform (last 7 days)
SELECT
  JSON_VALUE(query_params, '$.campaign') AS campaign,
  JSON_VALUE(query_params, '$.platform') AS platform,
  COUNT(*) AS hits
FROM `<project>.<dataset>.nginx_requests`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND uri LIKE '/p.gif%'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;

-- Redis queue depth over time (last 48 h)
SELECT
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS minute,
  AVG(depth)                          AS avg_depth,
  MAX(depth)                          AS max_depth
FROM `<project>.<dataset>.redis_metrics`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY 1
ORDER BY 1;

-- 5xx rate vs Redis backlog correlation (15-minute buckets)
WITH
  nginx AS (
    SELECT
      TIMESTAMP_TRUNC(timestamp, MINUTE) AS t,
      COUNTIF(status >= 500)             AS errors_5xx,
      COUNT(*)                           AS total
    FROM `<project>.<dataset>.nginx_requests`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    GROUP BY 1
  ),
  redis AS (
    SELECT
      TIMESTAMP_TRUNC(timestamp, MINUTE) AS t,
      AVG(depth)                          AS avg_depth
    FROM `<project>.<dataset>.redis_metrics`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    GROUP BY 1
  )
SELECT
  n.t,
  n.errors_5xx,
  n.total,
  ROUND(100.0 * n.errors_5xx / NULLIF(n.total, 0), 2) AS error_rate_pct,
  r.avg_depth AS redis_depth
FROM nginx n
JOIN redis r USING (t)
ORDER BY t;
```

### Custom nginx `log_format` for `request_time` and `request_id`

By default, nginx's `combined` format does not include `$request_time` (latency
in seconds) or a per-request correlation ID. Add this `log_format` block to your
nginx config before the `server {}` block:

```nginx
log_format ops_json escape=json
  '{'
    '"time":"$time_iso8601",'
    '"method":"$request_method",'
    '"uri":"$request_uri",'
    '"status":$status,'
    '"bytes":$body_bytes_sent,'
    '"request_time":$request_time,'
    '"request_id":"$request_id",'
    '"remote_addr":"$remote_addr"'
  '}';

access_log /var/log/nginx/access.log ops_json;
```

Then set `OPS_NGINX_ACCESS_LOG_PATH=/var/log/nginx/access.log` (or whatever path
you mount) and `OPS_BQ_EXPORT_ENABLED=1`. The exporter detects the JSON format
automatically and maps `request_time` → `request_time` and `request_id` →
`request_id` in the BigQuery row. Without this format the fields are empty but
all other columns still populate normally.

## Notes

- Redis runs without persistence in prod, so the on-disk NDJSON queue under
  `data/bigquery-queue/` is the only durable buffer. The monitor watches
  `processing/` for stuck files — that is the data-loss / missing-data signal.
- The capacity loop will not hammer a dead target; it self-skips until
  `HEALTH_URL` returns 200, then runs at most once per `CAPACITY_INTERVAL`.
- Runtime state (`status/`, `logs/`, `reports/`, pidfiles, heartbeats) is
  gitignored via `ops/.gitignore`; only the code under `bin/ lib/ tests/` and
  this README are tracked.
