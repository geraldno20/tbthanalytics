#!/usr/bin/env python3
"""
Fetch Google Ads data from a published Google Sheet and output data/ads.json.

Setup:
1. In your Google Sheet, go to File → Share → Publish to web
2. Select the "AdsSummary" sheet, format: CSV
3. Copy the published URL and paste it below as SHEET_CSV_URL

Usage:
  python3 fetch_ads_from_sheet.py
"""

import csv
import io
import json
import urllib.request

# Replace with your published CSV URL for the AdsSummary sheet
SHEET_CSV_URL = "YOUR_PUBLISHED_CSV_URL_HERE"

OUTPUT_PATH = "data/ads.json"


def main():
    print(f"Fetching ads data from Google Sheet...")
    req = urllib.request.Request(SHEET_CSV_URL)
    with urllib.request.urlopen(req) as resp:
        content = resp.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    videos = []
    total_spend = 0
    total_views = 0

    for row in reader:
        cost = float(row.get("Total Cost", 0) or 0)
        impressions = int(row.get("Total Impressions", 0) or 0)
        views = int(row.get("Total Views", 0) or 0)

        videos.append({
            "video_id": row.get("Video ID", ""),
            "video_title": row.get("Video Title", ""),
            "total_cost": round(cost, 2),
            "total_impressions": impressions,
            "total_views": views,
        })
        total_spend += cost
        total_views += views

    output = {
        "total_spend": round(total_spend, 2),
        "total_views": total_views,
        "videos": videos,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Written {len(videos)} videos to {OUTPUT_PATH}")
    print(f"Total spend: ${total_spend:.2f}, Total views: {total_views:,}")


if __name__ == "__main__":
    main()
