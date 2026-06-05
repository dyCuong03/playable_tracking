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

## Quick start

```bash
# 1. (optional) point at a non-default target
export PIXEL_BASE=http://127.0.0.1:9000

# 2. start the background loops (monitor + logcollector + capacity)
bash ops/bin/ops-start.sh

# 3. open the live 4-pane dashboard
bash ops/bin/dashboard.sh        # detach with Ctrl-b d

# 4. generate / refresh the load-bearing plan on demand
bash ops/bin/plan.sh

# stop everything
bash ops/bin/ops-stop.sh
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
| `DASH_REFRESH` | `5` | dashboard pane refresh (s) |

## Notes

- Redis runs without persistence in prod, so the on-disk NDJSON queue under
  `data/bigquery-queue/` is the only durable buffer. The monitor watches
  `processing/` for stuck files — that is the data-loss / missing-data signal.
- The capacity loop will not hammer a dead target; it self-skips until
  `HEALTH_URL` returns 200, then runs at most once per `CAPACITY_INTERVAL`.
- Nothing here is committed automatically; `ops/` is currently untracked.
