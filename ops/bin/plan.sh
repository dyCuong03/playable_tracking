#!/usr/bin/env bash
# ops/bin/plan.sh — re-runnable generator for the OPTIMAL LOAD-BEARING PLAN.
#
# Reads the latest measured inputs (stress verdict, capacity history, alerts) and
# regenerates two artifacts from scratch on every run:
#   - ops/reports/capacity-plan.md      (human-readable strategy)
#   - ops/status/capacity-plan.json     (machine-readable sizing table for the dashboard)
#
# Owned by: planner. Inputs may be absent → degrade gracefully and state assumptions.
#
# Tunable via env (all optional):
#   SAFETY_FACTOR          headroom multiplier on target peak rps        (default 1.5)
#   REPLICAS_AT_TEST       how many web replicas the stress test hit     (default 1; see CONFLICT note)
#   WORKER_THROUGHPUT_RPS  sustained BigQuery rows/sec per worker         (default 3000, assumed)
#   TARGET_PEAKS           space-separated target peak rps list           (default "1000 5000 10000 25000 50000")
set -euo pipefail

# shellcheck source=../lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/common.sh"

ROLE="planner"
heartbeat "$ROLE"

VERDICT_FILE="$STATUS_DIR/last-stress-verdict.json"
HISTORY_FILE="$REPORTS_DIR/capacity-history.ndjson"
ALERTS_FILE="$STATUS_DIR/alerts.ndjson"
PLAN_MD="$REPORTS_DIR/capacity-plan.md"
PLAN_JSON="$STATUS_DIR/capacity-plan.json"

SAFETY_FACTOR="${SAFETY_FACTOR:-1.5}"
REPLICAS_AT_TEST="${REPLICAS_AT_TEST:-1}"
WORKER_THROUGHPUT_RPS="${WORKER_THROUGHPUT_RPS:-3000}"
TARGET_PEAKS="${TARGET_PEAKS:-1000 5000 10000 25000 50000}"

jlog INFO "$ROLE" "Regenerating load-bearing plan" \
    "{\"verdict\":$( [ -f "$VERDICT_FILE" ] && echo true || echo false ),\"safety_factor\":$SAFETY_FACTOR,\"replicas_at_test\":$REPLICAS_AT_TEST}"

GENERATED_AT="$(ts_now)"

VERDICT_FILE="$VERDICT_FILE" \
HISTORY_FILE="$HISTORY_FILE" \
ALERTS_FILE="$ALERTS_FILE" \
PLAN_MD="$PLAN_MD" \
PLAN_JSON="$PLAN_JSON" \
SAFETY_FACTOR="$SAFETY_FACTOR" \
REPLICAS_AT_TEST="$REPLICAS_AT_TEST" \
WORKER_THROUGHPUT_RPS="$WORKER_THROUGHPUT_RPS" \
TARGET_PEAKS="$TARGET_PEAKS" \
GENERATED_AT="$GENERATED_AT" \
python3 <<'PY'
import json, math, os

def env(k, d=None): return os.environ.get(k, d)

verdict_file = env("VERDICT_FILE")
history_file = env("HISTORY_FILE")
alerts_file  = env("ALERTS_FILE")
plan_md      = env("PLAN_MD")
plan_json    = env("PLAN_JSON")
safety       = float(env("SAFETY_FACTOR", "1.5"))
replicas_at_test = int(float(env("REPLICAS_AT_TEST", "1")))
worker_tput  = float(env("WORKER_THROUGHPUT_RPS", "3000"))
targets      = [int(float(x)) for x in env("TARGET_PEAKS", "").split()]
generated_at = env("GENERATED_AT")

assumptions = []

# ---- Load measured baseline (degrade gracefully) ----
verdict = None
if verdict_file and os.path.exists(verdict_file):
    try:
        with open(verdict_file) as f:
            verdict = json.load(f)
    except Exception as e:
        assumptions.append(f"Could not parse {verdict_file} ({e}); fell back to documented baseline.")

measured_capacity_available = bool(verdict and verdict.get("sustainable_rps"))
if measured_capacity_available:
    sustainable_rps = float(verdict["sustainable_rps"])
    knee = verdict.get("knee_concurrency")
    last_good = verdict.get("last_good_concurrency")
    baseline_src = os.path.basename(verdict_file)
    baseline_run = verdict.get("run_id", "n/a")
else:
    # Documented placeholder from ops/reports/loadtest-2026-06-05.md — NOT measured this run.
    sustainable_rps = 1178.8
    knee = 400
    last_good = 200
    baseline_src = "PLACEHOLDER (no measured capacity)"
    baseline_run = "n/a"
    assumptions.append("MEASURED CAPACITY UNAVAILABLE — no stress verdict found. "
                       "Using placeholder baseline sustainable_rps=1178.8, knee=400, last_good=200. "
                       "Loadtesting is OFF by default and must be enabled manually: "
                       "LOADTEST_ENABLED=1 CAPACITY_ENABLED=1 bash ops/bin/stress.sh (NON-production target). "
                       "Re-run plan.sh after a real stress run to replace this placeholder.")

knee = knee if knee is not None else 400
last_good = last_good if last_good is not None else 200

# ---- Per-replica throughput ----
# CONFLICT: team-lead briefed "divide by 8" (assumes the stress hit the 8-replica
# deploy-prod stack). But ops/status/to-architect.md states the loadtester could NOT
# run docker, so the test was a SINGLE local `node src/server.js` process. Therefore
# the measured sustainable_rps is already PER-REPLICA. We default REPLICAS_AT_TEST=1
# (evidence-based). Re-run with REPLICAS_AT_TEST=8 to adopt the briefed interpretation.
per_replica_rps = sustainable_rps / max(1, replicas_at_test)
assumptions.append(
    f"per_replica_rps = sustainable_rps({sustainable_rps:.1f}) / REPLICAS_AT_TEST({replicas_at_test}) "
    f"= {per_replica_rps:.1f} rps. CONFLICT w/ briefing: lead assumed the test hit the 8-replica stack "
    f"(=> /8), but to-architect.md proves it was a single local node process (=> /1). Default = /1. "
    f"Override: REPLICAS_AT_TEST=8 ./ops/bin/plan.sh."
)
assumptions.append(
    f"Worker BigQuery throughput assumed {worker_tput:.0f} sustained rows/sec/worker (streaming insert, "
    f"BIGQUERY_BATCH_SIZE pipelined). Not yet load-tested — docker/BigQuery path unverified."
)
assumptions.append(
    f"SAFETY_FACTOR={safety} headroom applied to every target peak before sizing."
)

# ---- Sizing math ----
def ceil_div(a, b): return int(math.ceil(a / b))

rows = []
for t in targets:
    demand = t * safety
    app_replicas = max(2, ceil_div(demand, per_replica_rps))         # min 2 for HA
    # worker:web ~1:2, but never under BigQuery insert demand
    workers_by_ratio = max(1, ceil_div(app_replicas, 2))
    workers_by_tput  = max(1, ceil_div(demand, worker_tput))
    worker_count = max(workers_by_ratio, workers_by_tput)
    dispatcher_count = max(1, ceil_div(app_replicas, 8))             # 1 dispatcher per ~8 web replicas
    rows.append({
        "target_rps": t,
        "app_replicas": app_replicas,
        "worker_count": worker_count,
        "dispatcher_count": dispatcher_count,
    })

# ---- Recommended env per tier ----
# Higher tiers => larger redis MAXLEN buffer, larger BQ batches, faster rotation.
def env_tier(target):
    if target <= 1000:
        return dict(WEB_CONCURRENCY=1, BIGQUERY_BATCH_SIZE=100, REQUEST_QUEUE_ROTATE_MIN_BYTES=262144,
                    REQUEST_QUEUE_ROTATE_MAX_AGE_MS=1000, REDIS_QUEUE_MAXLEN=500000, BIGQUERY_WORKER_LEASE_MS=120000)
    if target <= 5000:
        return dict(WEB_CONCURRENCY=1, BIGQUERY_BATCH_SIZE=200, REQUEST_QUEUE_ROTATE_MIN_BYTES=524288,
                    REQUEST_QUEUE_ROTATE_MAX_AGE_MS=750, REDIS_QUEUE_MAXLEN=1000000, BIGQUERY_WORKER_LEASE_MS=90000)
    if target <= 10000:
        return dict(WEB_CONCURRENCY=1, BIGQUERY_BATCH_SIZE=300, REQUEST_QUEUE_ROTATE_MIN_BYTES=524288,
                    REQUEST_QUEUE_ROTATE_MAX_AGE_MS=500, REDIS_QUEUE_MAXLEN=2000000, BIGQUERY_WORKER_LEASE_MS=60000)
    if target <= 25000:
        return dict(WEB_CONCURRENCY=2, BIGQUERY_BATCH_SIZE=500, REQUEST_QUEUE_ROTATE_MIN_BYTES=1048576,
                    REQUEST_QUEUE_ROTATE_MAX_AGE_MS=400, REDIS_QUEUE_MAXLEN=5000000, BIGQUERY_WORKER_LEASE_MS=45000)
    return dict(WEB_CONCURRENCY=2, BIGQUERY_BATCH_SIZE=500, REQUEST_QUEUE_ROTATE_MIN_BYTES=2097152,
                REQUEST_QUEUE_ROTATE_MAX_AGE_MS=300, REDIS_QUEUE_MAXLEN=10000000, BIGQUERY_WORKER_LEASE_MS=30000)

# Per-replica connection caps tied to measured knee.
# Stay under last_good concurrency per replica; nginx limit_conn is per-server.
limit_conn_per_replica = max(50, int(last_good))   # keep each replica at/below its measured safe concurrency

# ---- Recent alerts summary ----
alert_lines = []
if alerts_file and os.path.exists(alerts_file):
    try:
        with open(alerts_file) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    a = json.loads(line)
                    alert_lines.append(a)
                except Exception:
                    pass
    except Exception:
        pass

# ---- Capacity history trend ----
history_lines = []
if history_file and os.path.exists(history_file):
    try:
        with open(history_file) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    history_lines.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass

# ============ Emit JSON sizing table ============
with open(plan_json, "w") as f:
    json.dump({
        "generated_at": generated_at,
        "measured_capacity_available": measured_capacity_available,
        "baseline": {
            "sustainable_rps": sustainable_rps,
            "per_replica_rps": round(per_replica_rps, 1),
            "knee_concurrency": knee,
            "last_good_concurrency": last_good,
            "replicas_at_test": replicas_at_test,
            "source": baseline_src,
            "run_id": baseline_run,
        },
        "safety_factor": safety,
        "worker_throughput_rps": worker_tput,
        "sizing": rows,
    }, f, indent=2)
    f.write("\n")

# ============ Emit Markdown plan ============
def tbl(headers, body_rows):
    out = "| " + " | ".join(headers) + " |\n"
    out += "| " + " | ".join("---" for _ in headers) + " |\n"
    for r in body_rows:
        out += "| " + " | ".join(str(c) for c in r) + " |\n"
    return out

md = []
md.append(f"# Optimal Load-Bearing Plan — pixel-tracking server\n")
md.append(f"_Generated: {generated_at} by `ops/bin/plan.sh` (planner). "
          f"Re-run to regenerate from latest inputs._\n")

if not measured_capacity_available:
    md.append("\n> 🟡 **MEASURED CAPACITY UNAVAILABLE — this is a baseline/placeholder plan.**\n>\n"
              "> No stress verdict exists yet, so the sizing below uses a documented placeholder, not a\n"
              "> real measurement. Loadtesting is OFF by default and must be enabled manually:\n>\n"
              "> ```\n> LOADTEST_ENABLED=1 CAPACITY_ENABLED=1 bash ops/bin/stress.sh   # NON-production target\n> ```\n>\n"
              "> Then re-run `bash ops/bin/plan.sh` to replace the placeholder with measured numbers.\n")

md.append("\n> ⚠️ **ASSUMPTION CONFLICT — read before trusting the sizing table.**\n>\n"
          f"> The briefing said to derive per-replica rps as `sustainable_rps / 8` (assuming the stress\n"
          f"> test hit the 8-replica `deploy-prod` stack). But `ops/status/to-architect.md` states the\n"
          f"> loadtester **could not run docker** and instead tested a **single local `node src/server.js`\n"
          f"> process**. So the measured `sustainable_rps` is already **per-replica**, not a /8 aggregate.\n>\n"
          f"> This plan defaults to the **evidence-based** interpretation (`REPLICAS_AT_TEST=1`,\n"
          f"> per_replica = {per_replica_rps:.0f} rps). To adopt the briefed /8 view, re-run:\n>\n"
          f"> ```\n> REPLICAS_AT_TEST=8 ./ops/bin/plan.sh\n> ```\n>\n"
          f"> Sensitivity: under /8 the replica counts below grow ~8×. Confirm which topology the next\n"
          f"> stress run actually targets before committing hardware.\n")

# 1. Baseline
md.append("\n## 1. Measured baseline\n")
md.append(tbl(
    ["Metric", "Value", "Notes"],
    [
        ["Total sustainable RPS", f"{sustainable_rps:.1f}", f"from `{baseline_src}` (run {baseline_run})"],
        ["Replicas under test", replicas_at_test, "1 = single local node process (per to-architect.md)"],
        ["Derived per-replica RPS", f"{per_replica_rps:.1f}", "sustainable_rps / replicas_at_test"],
        ["Knee concurrency", knee, "throughput declines / p95 spikes beyond this (per process)"],
        ["Last-good concurrency", last_good, "highest concurrency with healthy p95"],
        ["Failure mode", "graceful latency growth", "zero errors / timeouts / data loss observed at 2× knee"],
    ],
))

# 2. Sizing table
md.append("\n## 2. Sizing table\n")
md.append(f"Formula: `APP_REPLICAS = ceil(target_peak_rps × {safety} / {per_replica_rps:.1f})` (min 2 for HA). "
          f"`WORKER_COUNT = max(ceil(replicas/2), ceil(demand/{worker_tput:.0f}))` (worker:web ≈ 1:2, "
          f"floored by BigQuery insert demand). `DISPATCHER_COUNT = max(1, ceil(replicas/8))`.\n\n")
md.append(tbl(
    ["Target peak RPS", "APP_REPLICAS", "WORKER_COUNT", "DISPATCHER_COUNT"],
    [[r["target_rps"], r["app_replicas"], r["worker_count"], r["dispatcher_count"]] for r in rows],
))
md.append(f"\n> `deploy-prod.sh` defaults today: APP_REPLICAS=8, WORKER_COUNT=4, DISPATCHER_COUNT=1 "
          f"→ matches roughly the ~{int(per_replica_rps*8/safety)} rps tier under the current interpretation.\n")

# 3. Overload handling
md.append("\n## 3. Overload handling\n")
md.append(f"""**nginx edge caps** (add to `scripts/nginx-pixel.conf.template` `http{{}}` + `location /p.gif`):
- `limit_req_zone $binary_remote_addr zone=pgif:50m rate=50r/s;` then `limit_req zone=pgif burst=100 nodelay;`
  in `location /p.gif` — caps abusive single-IP floods without throttling normal pixel bursts.
- `limit_conn_zone $binary_remote_addr zone=perip:20m;` + `limit_conn perip 100;` — per-client connection ceiling.
- Per-replica safety: keep each upstream at/below **{limit_conn_per_replica} concurrent** (measured last-good).
  With N replicas the cluster ceiling is N × {limit_conn_per_replica}; size `worker_connections`/`keepalive` to match.

**Disk-queue backpressure** (`data/bigquery-queue`, the only durable buffer):
- WARN when `ready/` + `pending/` byte total > **2 GB**, CRITICAL > **8 GB** → dispatcher/worker not draining.
- WARN when any `processing/` file age > **5 min** → likely a dead/stuck dispatcher (data-loss risk; see §5).
- If queue keeps growing under sustained load: that is the worker→BigQuery stage saturating, **not** the web tier
  — scale `WORKER_COUNT` first, then `DISPATCHER_COUNT`.

**Autoscale triggers** (wire to monitor alerts; scale the named tier up one step):
| Trigger | Signal | Scale |
| --- | --- | --- |
| Web saturation | loadavg > cores OR nginx p95 > 500ms (sustained 60s) | +APP_REPLICAS |
| Ingest backlog | `ready/` byte total rising 3 intervals straight | +DISPATCHER_COUNT |
| BQ backlog | redis stream `XLEN pixel:events` > 50% of REDIS_QUEUE_MAXLEN | +WORKER_COUNT |
| Stuck processing | `processing/` file age > lease (BIGQUERY_WORKER_LEASE_MS) | page on-call, +WORKER_COUNT |
""")

# 4. Timeout strategy
md.append("\n## 4. Timeout strategy\n")
md.append(f"""`/p.gif` returns the 1×1 GIF immediately; the NDJSON append is coalesced via `setImmediate`,
so client-facing timeouts should be **short and aggressive** — a slow response means the box is sick, not busy.

| Layer | Knob | Recommended | Rationale |
| --- | --- | --- | --- |
| nginx → upstream | `proxy_read_timeout` | **3s** (down from 10s) | pixel write never legitimately takes >1s |
| nginx → upstream | `proxy_connect_timeout` | **1s** | fail fast to next upstream (`max_fails=3 fail_timeout=5s`) |
| nginx client | `client_header_timeout` / `client_body_timeout` | **5s** / **5s** | already set; keep |
| nginx upstream | `keepalive` | **256+** per worker | already set; raise with replica count |
| load/client | `REQ_TIMEOUT_MS` | **2000ms** | callers give up well before nginx, avoids socket pileup |
| worker | `BIGQUERY_WORKER_LEASE_MS` | **30–120s** by tier (table §6) | shorter at high load so dead-worker reclaim is fast |
| worker | `BIGQUERY_MAX_RETRIES` | **5** | retryable BQ reasons re-XADD'd with attempts++; then → rejected stream |
| dispatcher | `REQUEST_QUEUE_ROTATE_MIN_BYTES` | **256KB–2MB** by tier | bigger at high load = fewer, fatter redis batches |
| dispatcher | `REQUEST_QUEUE_ROTATE_MAX_AGE_MS` | **300–1000ms** by tier | caps worst-case event latency to redis at low load |

Net event-to-BigQuery latency budget (high tier): ≤300ms rotate + redis hop + ≤worker poll(1s) + BQ insert ≈ **2–3s**.
""")

# 5. Durability
md.append("\n## 5. Missing-data / durability\n")
md.append(f"""Redis runs with **`--save "" --appendonly no`** (no persistence). If redis dies, every in-flight
stream entry is lost. The **on-disk NDJSON queue under `data/bigquery-queue` is the only durable buffer** —
treat it as production state.

**Data-loss watchpoints & procedures:**
1. **Stuck `processing/` files** — a dispatcher renames `ready/ → processing/--<worker>-<hash>.ndjson` as a lock.
   If that dispatcher dies mid-push, the file is orphaned (not yet in redis, not in `ready/`). Monitor
   `processing/` for files older than the rotate/lease window and **`mv` them back to `ready/`** to replay.
2. **Dead BigQuery workers** — handled in-band by `XAUTOCLAIM` after `BIGQUERY_WORKER_LEASE_MS`; another worker
   reclaims pending stream entries. Lower the lease at high tiers so reclaim is fast.
3. **Rejected stream** — non-retryable rows or rows past `BIGQUERY_MAX_RETRIES` go to `pixel:rejected` and are
   dropped from the main flow. **Alert on `XLEN pixel:rejected` > 0**; inspect with `XRANGE`, fix schema/payload,
   re-`XADD` to `pixel:events` to replay.
4. **Redis loss** — because there is no AOF/RDB, on a redis restart, **drain `ready/` + replay `processing/`**
   before resuming ingest; in-flight stream entries that were already XACK'd-but-not-inserted are gone (accepted risk).
5. **Replay procedure (manual):**
   ```
   # 1. Stop dispatchers so nothing claims while you work.
   # 2. Recover orphaned locks:
   mv data/bigquery-queue/processing/* data/bigquery-queue/ready/ 2>/dev/null || true
   # 3. Restart dispatchers → they re-claim ready/ → re-push to redis → workers insert.
   # 4. Audit rejected:
   redis-cli XLEN pixel:rejected
   ```
""")

# 6. Env table per tier
md.append("\n## 6. Recommended env by load tier\n")
env_headers = ["Target RPS", "WEB_CONCURRENCY", "BIGQUERY_BATCH_SIZE", "REQUEST_QUEUE_ROTATE_MIN_BYTES",
               "REQUEST_QUEUE_ROTATE_MAX_AGE_MS", "REDIS_QUEUE_MAXLEN", "BIGQUERY_WORKER_LEASE_MS"]
env_body = []
for t in targets:
    e = env_tier(t)
    env_body.append([t, e["WEB_CONCURRENCY"], e["BIGQUERY_BATCH_SIZE"], e["REQUEST_QUEUE_ROTATE_MIN_BYTES"],
                     e["REQUEST_QUEUE_ROTATE_MAX_AGE_MS"], e["REDIS_QUEUE_MAXLEN"], e["BIGQUERY_WORKER_LEASE_MS"]])
md.append(tbl(env_headers, env_body))
md.append(f"\n> `WEB_CONCURRENCY` stays low (1–2) and we scale **horizontally** (more replicas) instead, because the "
          f"per-process bottleneck is event-loop + disk-append coalescing, and all replicas share one `./data` "
          f"mount — adding processes per box just contends harder on the same disk. Keep `BIGQUERY_QUEUE_SHARDS` "
          f"≥ replica count so shards don't collide.\n")

# 7. Inputs / assumptions
md.append("\n## 7. Inputs & assumptions\n")
md.append(f"- Stress verdict: `{baseline_src}`" + (f" (run `{baseline_run}`)\n" if baseline_run != "n/a" else "\n"))
if alert_lines:
    md.append(f"- Recent alerts ({len(alert_lines)}):\n")
    for a in alert_lines[-5:]:
        md.append(f"  - `{a.get('ts','?')}` {a.get('severity','?')}/{a.get('event','?')}: {a.get('detail','')[:160]}\n")
else:
    md.append("- Recent alerts: none found (`alerts.ndjson` absent or empty).\n")
if history_lines:
    md.append(f"- Capacity history: {len(history_lines)} datapoint(s) in `capacity-history.ndjson`.\n")
else:
    md.append("- Capacity history: none yet (`capacity-history.ndjson` absent) — no trend available.\n")
md.append("\n**Assumptions made:**\n")
for a in assumptions:
    md.append(f"- {a}\n")

with open(plan_md, "w") as f:
    f.write("".join(md))

print(f"WROTE {plan_md}")
print(f"WROTE {plan_json}")
PY

jlog INFO "$ROLE" "Plan regenerated" "{\"md\":$(json_str "$PLAN_MD"),\"json\":$(json_str "$PLAN_JSON")}"
echo "=== capacity-plan.json ==="
cat "$PLAN_JSON"
