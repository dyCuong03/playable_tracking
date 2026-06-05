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

RUN_ID="$(ts_file)"
REPORT="$REPORTS_DIR/stress-$RUN_ID.ndjson"
jlog "info" "$ROLE" "stress run started" "{\"target\":$(json_str "$PIXEL_URL"),\"stages\":$(json_str "$STAGES")}" | tee -a "$REPORT"

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
    sleep "$COOLDOWN"
done

# Sustainable RPS = best RPS observed at the last non-degraded stage.
VERDICT="{\"ts\":\"$(ts_now)\",\"role\":\"$ROLE\",\"run_id\":\"$RUN_ID\",\"target\":$(json_str "$PIXEL_URL"),\"knee_concurrency\":${KNEE:-null},\"last_good_concurrency\":${LAST_GOOD:-null},\"sustainable_rps\":${LAST_GOOD_RPS},\"report\":$(json_str "$REPORT")}"
echo "$VERDICT" > "$STATUS_DIR/last-stress-verdict.json"
jlog "info" "$ROLE" "stress run verdict" "$VERDICT" | tee -a "$REPORT"
echo "VERDICT: $VERDICT"
