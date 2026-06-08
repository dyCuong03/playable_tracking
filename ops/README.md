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

## Notes

- Redis runs without persistence in prod, so the on-disk NDJSON queue under
  `data/bigquery-queue/` is the only durable buffer. The monitor watches
  `processing/` for stuck files — that is the data-loss / missing-data signal.
- The capacity loop will not hammer a dead target; it self-skips until
  `HEALTH_URL` returns 200, then runs at most once per `CAPACITY_INTERVAL`.
- Runtime state (`status/`, `logs/`, `reports/`, pidfiles, heartbeats) is
  gitignored via `ops/.gitignore`; only the code under `bin/ lib/ tests/` and
  this README are tracked.
