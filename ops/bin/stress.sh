#!/usr/bin/env bash
# Ramping load test. Runs hammer.js at increasing concurrency until the server
# degrades (error_rate > FAIL_RATE or p95 > FAIL_P95_MS), then stops and records
# the knee. Each stage's JSON summary is appended to a run report; a final
# verdict line marks the sustainable capacity.
set -u
. "$(dirname "$0")/../lib/common.sh"

ROLE="loadtester"
heartbeat "$ROLE"

STAGES="${STAGES:-10 25 50 100 200 400 800 1600}"   # concurrency levels
STAGE_SECONDS="${STAGE_SECONDS:-15}"
REQ_TIMEOUT_MS="${REQ_TIMEOUT_MS:-2000}"
FAIL_RATE="${FAIL_RATE:-0.01}"        # 1% errors = degraded
FAIL_P95_MS="${FAIL_P95_MS:-500}"     # p95 over 500ms = degraded
COOLDOWN="${COOLDOWN:-5}"

# ---- safety guards -----------------------------------------------------------
# stress.sh generates REAL traffic. It must never fire by accident (CI, deploy)
# and must stay bounded. It refuses unless explicitly enabled, caps the ramp at
# MAX_CONCURRENCY, stops once a stage reaches MAX_RPS, and refuses a target the
# operator declared production unless ALLOW_PROD_LOADTEST=1.
LOADTEST_ENABLED="${LOADTEST_ENABLED:-0}"        # master switch — must be 1 to run at all
LOADTEST_ENV="${LOADTEST_ENV:-test}"             # operator-declared target env: test | production
ALLOW_PROD_LOADTEST="${ALLOW_PROD_LOADTEST:-0}"  # must be 1 to load a production target
MAX_CONCURRENCY="${MAX_CONCURRENCY:-800}"        # hard ceiling — ramp never exceeds this
MAX_RPS="${MAX_RPS:-5000}"                       # stop the ramp once a stage reaches this rps

if [ "$LOADTEST_ENABLED" != "1" ]; then
    jlog "error" "$ROLE" "loadtest refused - LOADTEST_ENABLED != 1" "{\"hint\":\"manual or scheduled only; never wired into deploy\"}"
    echo "REFUSED: set LOADTEST_ENABLED=1 to run a load test"
    exit 2
fi
if [ "$LOADTEST_ENV" = "production" ] && [ "$ALLOW_PROD_LOADTEST" != "1" ]; then
    jlog "error" "$ROLE" "loadtest refused - production target needs ALLOW_PROD_LOADTEST=1" "{\"env\":\"production\"}"
    echo "REFUSED: production target requires ALLOW_PROD_LOADTEST=1"
    exit 2
fi

RUN_ID="$(ts_file)"
REPORT="$REPORTS_DIR/stress-$RUN_ID.ndjson"
jlog "info" "$ROLE" "stress run started" "{\"target\":$(json_str "$PIXEL_URL"),\"env\":$(json_str "$LOADTEST_ENV"),\"max_concurrency\":$MAX_CONCURRENCY,\"max_rps\":$MAX_RPS,\"stages\":$(json_str "$STAGES")}" | tee -a "$REPORT"

# Preflight: target must answer before we hammer it.
PRE="$(curl -s -m 3 -o /dev/null -w '%{http_code}' "$HEALTH_URL" 2>/dev/null || echo 000)"
if [ "$PRE" != "200" ]; then
    jlog "error" "$ROLE" "target not healthy - aborting stress" "{\"health_url\":$(json_str "$HEALTH_URL"),\"http_code\":$(json_str "$PRE")}" | tee -a "$REPORT"
    exit 1
fi

KNEE=""
LAST_GOOD=""
LAST_GOOD_RPS="0"
for c in $STAGES; do
    # Max-concurrency guard: never ramp past the configured ceiling.
    if [ "$c" -gt "$MAX_CONCURRENCY" ]; then
        jlog "warn" "$ROLE" "max-concurrency guard reached - stopping ramp" "{\"concurrency\":$c,\"max_concurrency\":$MAX_CONCURRENCY}" | tee -a "$REPORT"
        break
    fi

    heartbeat "$ROLE"
    jlog "info" "$ROLE" "stage start" "{\"concurrency\":$c,\"seconds\":$STAGE_SECONDS}" | tee -a "$REPORT"
    SUMMARY="$(node "$OPS_DIR/bin/hammer.js" --url="$PIXEL_URL" --concurrency="$c" --duration="$STAGE_SECONDS" --timeout="$REQ_TIMEOUT_MS")"
    echo "$SUMMARY" >> "$REPORT"
    echo "$SUMMARY"

    ER="$(echo "$SUMMARY" | grep -o '"error_rate":[0-9.]*' | cut -d: -f2)"
    P95="$(echo "$SUMMARY" | grep -o '"p95":[0-9]*' | cut -d: -f2)"
    RPS="$(echo "$SUMMARY" | grep -o '"rps":[0-9.]*' | cut -d: -f2)"
    ER="${ER:-1}"; P95="${P95:-999999}"; RPS="${RPS:-0}"

    # Degraded if error_rate > FAIL_RATE or p95 > FAIL_P95_MS (awk for float compare).
    DEGRADED="$(awk -v er="$ER" -v fr="$FAIL_RATE" -v p="$P95" -v fp="$FAIL_P95_MS" 'BEGIN{print (er>fr || p>fp)?1:0}')"
    if [ "$DEGRADED" = "1" ]; then
        KNEE="$c"
        jlog "warn" "$ROLE" "knee reached - stopping ramp" "{\"concurrency\":$c,\"error_rate\":$ER,\"p95\":$P95}" | tee -a "$REPORT"
        break
    fi
    LAST_GOOD="$c"
    LAST_GOOD_RPS="$RPS"

    # Max-RPS guard: this stage is healthy but already at/over the rps ceiling —
    # record it as sustainable and stop before driving more load.
    OVER_RPS="$(awk -v r="$RPS" -v m="$MAX_RPS" 'BEGIN{print (r>=m)?1:0}')"
    if [ "$OVER_RPS" = "1" ]; then
        jlog "warn" "$ROLE" "max-rps guard reached - stopping ramp" "{\"concurrency\":$c,\"rps\":$RPS,\"max_rps\":$MAX_RPS}" | tee -a "$REPORT"
        break
    fi

    sleep "$COOLDOWN"
done

# Sustainable RPS = best RPS observed at the last non-degraded stage.
VERDICT="{\"ts\":\"$(ts_now)\",\"role\":\"$ROLE\",\"run_id\":\"$RUN_ID\",\"target\":$(json_str "$PIXEL_URL"),\"knee_concurrency\":${KNEE:-null},\"last_good_concurrency\":${LAST_GOOD:-null},\"sustainable_rps\":${LAST_GOOD_RPS},\"report\":$(json_str "$REPORT")}"
echo "$VERDICT" > "$STATUS_DIR/last-stress-verdict.json"
jlog "info" "$ROLE" "stress run verdict" "$VERDICT" | tee -a "$REPORT"
echo "VERDICT: $VERDICT"
