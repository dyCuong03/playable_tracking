# ops/ — pixel-server monitoring & load-capacity toolkit

Self-contained ops layer for the playable-tracking pixel server. Four roles run as
background loops and feed a live 4-pane tmux dashboard. Built by the `pixel-ops`
agent team; safe to run before the server is deployed (every probe degrades
gracefully when the target is down).

## Roles

| Role | Script | Purpose | Output |
|------|--------|---------|--------|
| **monitor** | `bin/monitor.sh` + `bin/monitor-loop.sh` | Daily server-status snapshots; raises alerts on overload / timeout / missing-data | `logs/<date>/status.ndjson`, `status/monitor-latest.json`, `status/alerts.ndjson` |
| **logcollector** | `bin/logcollector.sh` + `bin/logcollector-loop.sh` | Records all backend logs (app + docker + pm2) into a daily archive, rolls up errors | `logs/<date>/{app.log,containers/,errors-rollup.txt}`, `status/logcollector-latest.json` |
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

Notes: prod redis runs in a container (not on host `localhost`), so `redis-cli`
depth shows `cli_missing` unless you publish its port; add the deploy user to the
`docker` group so logcollector can read container logs.

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
| `LOGCOLLECT_INTERVAL` | `60` | logcollector loop period (s) |
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
