#!/bin/bash
set -euo pipefail

export PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin:$PATH"

trap 'echo ""; echo "Error on line $LINENO — aborting." >&2' ERR

cd /Users/geraldyeung/code/tbth-analytics

echo "==> Fetching episode metadata..."
python3 fetch_episodes.py

echo ""
echo "==> Fetching video list..."
python3 fetch_videos.py

echo ""
echo "==> Fetching YouTube analytics..."
python3 fetch_analytics.py

echo ""
echo "==> Fetching Instagram analytics..."
python3 fetch_instagram.py

echo ""
echo "==> Fetching Threads analytics..."
python3 fetch_threads.py

echo ""
echo "==> Verifying data freshness..."
python3 - <<'PY'
import json, os, sys, time
from datetime import date

today = date.today().isoformat()
errors = []

# episodes.json carries fetched_at — must be today
ep = json.load(open("data/episodes.json"))
if not ep.get("fetched_at", "").startswith(today):
    errors.append(f"data/episodes.json fetched_at={ep.get('fetched_at')!r}, expected {today}")

# Files without an internal timestamp: mtime must be within last 30 min
for path in ("data/videos.json", "data/analytics.json", "data/instagram.json", "data/threads.json"):
    age = time.time() - os.path.getmtime(path)
    if age > 1800:
        errors.append(f"{path} mtime is {int(age)}s old (>30min) — fetcher likely skipped")

if errors:
    print("FRESHNESS CHECK FAILED:", file=sys.stderr)
    for e in errors:
        print(f"  - {e}", file=sys.stderr)
    sys.exit(1)
print("All data files are fresh.")
PY

echo ""
echo "==> Pushing to GitHub..."
git add -A
git commit -m "Refresh data $(date '+%Y-%m-%d')" || echo "No changes to commit"
git push

echo ""
echo "Done! Data refreshed and pushed at $(date)"
