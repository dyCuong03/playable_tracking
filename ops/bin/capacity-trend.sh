#!/usr/bin/env bash
# Capacity trend view. Reads ops/reports/capacity-history.ndjson (one verdict
# JSON object per line, appended by capacity-loop.sh) and prints a compact table
# of capacity over time, then the latest verdict from last-stress-verdict.json.
# Consumed by the dashboard + planner. Empty/missing history is handled cleanly.
set -u
. "$(dirname "$0")/../lib/common.sh"

HISTORY="$REPORTS_DIR/capacity-history.ndjson"
VERDICT_FILE="$STATUS_DIR/last-stress-verdict.json"

python3 - "$HISTORY" "$VERDICT_FILE" <<'PY'
import json, os, sys

history_path, verdict_path = sys.argv[1], sys.argv[2]

def load_lines(path):
    rows = []
    if not path or not os.path.exists(path):
        return rows
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    return rows

def load_one(path):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path) as fh:
            return json.load(fh)
    except ValueError:
        return None

def fmt(v):
    return "-" if v is None else str(v)

def row(r):
    return (
        fmt(r.get("ts")),
        fmt(r.get("run_id")),
        fmt(r.get("sustainable_rps")),
        fmt(r.get("knee_concurrency")),
        fmt(r.get("last_good_concurrency")),
    )

cols = ("ts", "run_id", "sustainable_rps", "knee_conc", "last_good_conc")

def print_table(rows):
    data = [cols] + [row(r) for r in rows]
    widths = [max(len(d[i]) for d in data) for i in range(len(cols))]
    for i, r in enumerate(data):
        print("  ".join(c.ljust(widths[j]) for j, c in enumerate(r)))
        if i == 0:
            print("  ".join("-" * widths[j] for j in range(len(cols))))

history = load_lines(history_path)
latest = load_one(verdict_path)

print("== Capacity trend ==")
if history:
    print_table(history)
else:
    print("no capacity history yet")

print()
print("== Latest verdict (last-stress-verdict.json) ==")
if latest:
    print_table([latest])
else:
    print("no verdict recorded yet")
PY
