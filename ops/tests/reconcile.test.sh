#!/usr/bin/env bash
# ops/tests/reconcile.test.sh — verifies daemon reconciliation (no false-positive
# health, no duplicate daemons, exact-pid kills only). Self-contained: uses
# throwaway fixtures in a temp dir, never touches real ops daemons.
#
# Covers: 1 missing pidfile, 2 dead pid, 3 alive-but-wrong-cmd ghost,
# 4 deleted/old script ghost, 5 cmd-ok-but-stale-heartbeat, 6 healthy skip.
set -u
. "$(dirname "$0")/../lib/common.sh"

TMP="$(mktemp -d)"
LOGS_DIR="$TMP"                                   # redirect nohup out away from real logs
# Guard: only the top-level shell cleans up. Command-substitution subshells
# ($(...)) also fire EXIT — without this guard the first $(cat) would rm $TMP.
# EXIT trap kills leftover fixture jobs only. It deliberately does NOT rm $TMP:
# the trap is inherited by forked/background contexts whose early exit would
# otherwise wipe $TMP mid-test. $TMP is removed once, explicitly, at the end.
trap 'for p in $(jobs -p); do kill "$p" 2>/dev/null; done' EXIT

EXPECTED="fake-loop"                              # substring expected in cmdline
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }
chk() { if eval "$2"; then ok "$1"; else bad "$1   [$2]"; fi; }

# ---- fixtures -------------------------------------------------------------
cat > "$TMP/fake-loop-good.sh" <<'EOF'
#!/usr/bin/env bash
PIDFILE="$1"; HB="$2"
echo $$ > "$PIDFILE"; trap 'rm -f "$PIDFILE"' EXIT
while true; do date -u +%Y-%m-%dT%H:%M:%SZ > "$HB"; sleep 1; done
EOF
cat > "$TMP/fake-loop-stale.sh" <<'EOF'
#!/usr/bin/env bash
PIDFILE="$1"; HB="$2"
echo $$ > "$PIDFILE"; trap 'rm -f "$PIDFILE"' EXIT
date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ > "$HB"   # write once, never refresh
while true; do sleep 1; done
EOF
chmod +x "$TMP"/fake-loop-*.sh

reconcile() { reconcile_daemon "$1" "$2" "$EXPECTED" "$3" 10 bash "$TMP/$4" "$2" "$3"; }
wait_healthy() {  # pidfile hbfile
    # Up to ~15s: tolerate process-launch + first-heartbeat latency on a loaded
    # box / CI runner so this never false-fails the gate.
    local p i
    for i in $(seq 1 50); do
        p="$(cat "$1" 2>/dev/null)"
        if [ -n "$p" ] && is_pid_alive "$p" && is_expected_daemon "$p" "$EXPECTED" && is_heartbeat_fresh "$2" 30; then return 0; fi
        sleep 0.3
    done
    return 1
}

# Wait until a fixture's pidfile holds a live pid running the expected script.
# Heartbeat-agnostic — for the stale-heartbeat fixture whose hb stays old by design.
wait_cmd() {  # pidfile
    local p i
    for i in $(seq 1 50); do
        p="$(cat "$1" 2>/dev/null)"
        if [ -n "$p" ] && is_pid_alive "$p" && is_expected_daemon "$p" "$EXPECTED"; then return 0; fi
        sleep 0.3
    done
    return 1
}

echo "== reconcile.test.sh =="

# ---- Case 1: missing pidfile -> start -------------------------------------
echo "[case 1] missing pidfile -> start"
PF="$TMP/c1.pid"; HB="$TMP/c1.hb"; rm -f "$PF"
reconcile c1 "$PF" "$HB" fake-loop-good.sh >/dev/null
wait_healthy "$PF" "$HB" && ok "case1 started healthy from no pidfile" || bad "case1 not healthy"
kill "$(cat "$PF" 2>/dev/null)" 2>/dev/null

# ---- Case 2: dead pid in pidfile -> clear + start -------------------------
echo "[case 2] dead pid -> clear + start"
PF="$TMP/c2.pid"; HB="$TMP/c2.hb"
sleep 30 & D=$!; kill "$D" 2>/dev/null; wait "$D" 2>/dev/null; echo "$D" > "$PF"
chk "case2 pre: pid $D dead" "! is_pid_alive $D"
reconcile c2 "$PF" "$HB" fake-loop-good.sh >/dev/null
wait_healthy "$PF" "$HB" && chk "case2 new pid != dead pid" "[ \"$(cat "$PF")\" != \"$D\" ]" || bad "case2 not healthy"
kill "$(cat "$PF" 2>/dev/null)" 2>/dev/null

# ---- Case 3: alive but wrong cmd (ghost) -> kill exact pid + restart -------
echo "[case 3] ghost (alive, wrong cmd) -> restart, NOT skip"
PF="$TMP/c3.pid"; HB="$TMP/c3.hb"
sleep 300 & G=$!; echo "$G" > "$PF"        # cmdline "sleep 300" — no fake-loop
chk "case3 pre: ghost alive"        "is_pid_alive $G"
chk "case3 pre: ghost not expected" "! is_expected_daemon $G $EXPECTED"
reconcile c3 "$PF" "$HB" fake-loop-good.sh >/dev/null; sleep 1
chk "case3 exact ghost pid killed"  "! is_pid_alive $G"
wait_healthy "$PF" "$HB" && chk "case3 restarted, pid changed" "[ \"$(cat "$PF")\" != \"$G\" ]" || bad "case3 not healthy"
kill "$(cat "$PF" 2>/dev/null)" 2>/dev/null

# ---- Case 4: process whose cmdline points to a DELETED/old script ----------
echo "[case 4] deleted/old script ghost -> detected"
PF="$TMP/c4.pid"; HB="$TMP/c4.hb"
cat > "$TMP/health-snapshot.sh" <<'EOF'
#!/usr/bin/env bash
while true; do sleep 1; done
EOF
nohup bash "$TMP/health-snapshot.sh" >/dev/null 2>&1 & O=$!; sleep 1
rm -f "$TMP/health-snapshot.sh"            # delete the script; process lingers (the real bug)
echo "$O" > "$PF"
CMD="$(read_cmdline "$O")"
case "$CMD" in *health-snapshot.sh*) ok "case4 cmdline still shows deleted health-snapshot.sh";; *) bad "case4 cmdline missing old script ($CMD)";; esac
chk "case4 not expected daemon" "! is_expected_daemon $O $EXPECTED"
J="$(daemon_status_json c4 "$PF" "$EXPECTED" "$HB" 10)"
echo "$J" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d["alive"] and not d["cmd_ok"] and not d["healthy"], d' \
  && ok "case4 status: alive=true cmd_ok=false healthy=false" || bad "case4 status wrong: $J"
reconcile c4 "$PF" "$HB" fake-loop-good.sh >/dev/null; sleep 1
chk "case4 old ghost killed" "! is_pid_alive $O"
wait_healthy "$PF" "$HB" && ok "case4 restarted healthy" || bad "case4 not healthy"
kill "$(cat "$PF" 2>/dev/null)" 2>/dev/null

# ---- Case 5: cmd correct but heartbeat stale -> restart -------------------
echo "[case 5] cmd ok, heartbeat stale -> restart"
PF="$TMP/c5.pid"; HB="$TMP/c5.hb"
nohup bash "$TMP/fake-loop-stale.sh" "$PF" "$HB" >/dev/null 2>&1 &
wait_cmd "$PF" || true
S="$(cat "$PF")"
chk "case5 pre: cmd ok"   "is_expected_daemon $S $EXPECTED"
chk "case5 pre: hb stale" "! is_heartbeat_fresh $HB 10"
J="$(daemon_status_json c5 "$PF" "$EXPECTED" "$HB" 10)"
echo "$J" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d["cmd_ok"] and not d["heartbeat_fresh"] and not d["healthy"], d' \
  && ok "case5 status: cmd_ok=true hb_fresh=false healthy=false" || bad "case5 status wrong: $J"
reconcile c5 "$PF" "$HB" fake-loop-good.sh >/dev/null; sleep 1
chk "case5 stale pid killed" "! is_pid_alive $S"
wait_healthy "$PF" "$HB" && chk "case5 restarted, pid changed" "[ \"$(cat "$PF")\" != \"$S\" ]" || bad "case5 not healthy"
kill "$(cat "$PF" 2>/dev/null)" 2>/dev/null

# ---- Case 6: healthy -> skip, no duplicate -------------------------------
echo "[case 6] healthy -> skip, no duplicate"
PF="$TMP/c6.pid"; HB="$TMP/c6.hb"
nohup bash "$TMP/fake-loop-good.sh" "$PF" "$HB" >/dev/null 2>&1 &
wait_healthy "$PF" "$HB" || true
H="$(cat "$PF")"
chk "case6 pre: healthy" "is_expected_daemon $H $EXPECTED && is_heartbeat_fresh $HB 15"
OUT="$(reconcile c6 "$PF" "$HB" fake-loop-good.sh)"
echo "$OUT" | grep -q 'healthy.*skip' && ok "case6 logged skip" || bad "case6 did not skip: $OUT"
sleep 1
chk "case6 same pid, no duplicate" "[ \"$(cat "$PF")\" = \"$H\" ] && is_pid_alive $H"
kill "$H" 2>/dev/null

echo
echo "== RESULT: PASS=$PASS FAIL=$FAIL =="
rm -rf "$TMP"
[ "$FAIL" -eq 0 ]
