#!/bin/bash
set -euo pipefail

export PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin:$PATH"

trap 'echo ""; echo "Error on line $LINENO — aborting." >&2' ERR

cd /Users/geraldyeung/code/tbth-analytics

echo "==> Fetching video list..."
python3 fetch_videos.py

echo ""
echo "==> Fetching analytics..."
python3 fetch_analytics.py

echo ""
echo "==> Pushing to GitHub..."
git add -A
git commit -m "Refresh data $(date '+%Y-%m-%d')" || echo "No changes to commit"
git push

echo ""
echo "Done! Data refreshed and pushed at $(date)"
