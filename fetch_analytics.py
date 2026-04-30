"""
Fetches per-video analytics using the YouTube Analytics API.
Metrics: views (24h/72h/1wk/2wk), lifetime ad views,
avg view duration %, avg view time, new subscribers.
Also fetches duration and detects Shorts via the Data API.
Saves results to data/analytics.json.
"""

import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

from auth import get_credentials
from googleapiclient.discovery import build

DATA_DIR = Path(__file__).parent / "data"
CHANNEL_ID = "UC-obSTyigrLPiN-kiW1bgoA"


def parse_duration(iso_duration):
    """Convert ISO 8601 duration (PT1H2M3S) to seconds."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration or "")
    if not match:
        return 0
    h, m, s = (int(g) if g else 0 for g in match.groups())
    return h * 3600 + m * 60 + s


def check_is_short(video_id):
    """Check if a video is a Short by testing if /shorts/ URL redirects away."""
    try:
        url = f"https://www.youtube.com/shorts/{video_id}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req, timeout=5)
        return "/shorts/" in resp.url
    except Exception:
        return False


def fetch_video_details(youtube, video_ids):
    """Fetch duration and current stats for a batch of video IDs."""
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        response = youtube.videos().list(
            part="statistics,contentDetails,snippet,status",
            id=",".join(batch),
        ).execute()
        for item in response.get("items", []):
            vid = item["id"]
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            thumbs = snippet.get("thumbnails", {})
            thumb_url = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
            duration_sec = parse_duration(item.get("contentDetails", {}).get("duration"))
            privacy = item.get("status", {}).get("privacyStatus", "public")
            details[vid] = {
                "total_views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "duration_sec": duration_sec,
                "thumbnail": thumb_url,
                "privacy_status": privacy,
            }
        time.sleep(0.2)

    # Detect Shorts via /shorts/ URL check
    print("Detecting Shorts...")
    for vid in details:
        is_short = check_is_short(vid)
        details[vid]["is_short"] = is_short
        time.sleep(0.1)

    shorts_count = sum(1 for d in details.values() if d["is_short"])
    print(f"  {shorts_count} Shorts, {len(details) - shorts_count} Videos")
    return details


def fetch_period_metrics(video_id, start_date, end_date, yt_analytics, retries=3):
    """Fetch aggregated metrics for a video over a date range using day dimension."""
    for attempt in range(retries):
        try:
            response = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start_date,
                endDate=end_date,
                metrics="views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained",
                dimensions="day",
                filters=f"video=={video_id}",
                sort="day",
            ).execute()

            rows = response.get("rows", [])
            if not rows:
                return {"views": 0, "avg_view_duration_sec": 0, "avg_view_duration_pct": 0, "new_subscribers": 0}

            total_views = sum(r[1] for r in rows)
            avg_duration = sum(r[3] * r[1] for r in rows) / total_views if total_views else 0
            avg_pct = sum(r[4] * r[1] for r in rows) / total_views if total_views else 0
            total_subs = sum(r[5] for r in rows)

            return {
                "views": total_views,
                "avg_view_duration_sec": round(avg_duration, 1),
                "avg_view_duration_pct": round(avg_pct, 1),
                "new_subscribers": total_subs,
            }
        except Exception as e:
            print(f"    Error ({start_date} to {end_date}), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_ad_views(video_id, published_at, yt_analytics, retries=3):
    """Fetch lifetime advertising views using traffic source dimension."""
    today = datetime.now().strftime("%Y-%m-%d")
    for attempt in range(retries):
        try:
            response = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=published_at,
                endDate=today,
                metrics="views",
                dimensions="insightTrafficSourceType",
                filters=f"video=={video_id}",
            ).execute()

            for row in response.get("rows", []):
                if row[0] == "ADVERTISING":
                    return row[1]
            return 0
        except Exception as e:
            print(f"    Error (ad views), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return 0


def fetch_video_analytics(video_id, published_at, yt_analytics):
    """Fetch all metrics across the 4 time windows for a single video."""
    pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    start = pub.strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    # Analytics API date ranges are inclusive on both ends, so
    # "first 24h" = publish day only (1 calendar day),
    # "first 72h" = publish day + 2 more days (3 calendar days), etc.
    periods = {
        "24h": (pub + timedelta(days=0)).strftime("%Y-%m-%d"),
        "72h": (pub + timedelta(days=2)).strftime("%Y-%m-%d"),
        "1wk": (pub + timedelta(days=6)).strftime("%Y-%m-%d"),
        "2wk": (pub + timedelta(days=13)).strftime("%Y-%m-%d"),
    }

    result = {}

    for label, end in periods.items():
        if end > today:
            result[f"views_{label}"] = None
            if label == "2wk":
                result["avg_view_duration_pct"] = None
                result["avg_view_time_sec"] = None
                result["new_subscribers"] = None
            continue

        data = fetch_period_metrics(video_id, start, end, yt_analytics)
        time.sleep(0.1)

        if data:
            result[f"views_{label}"] = data["views"]
            if label == "2wk":
                result["avg_view_duration_pct"] = data["avg_view_duration_pct"]
                result["avg_view_time_sec"] = data["avg_view_duration_sec"]
                result["new_subscribers"] = data["new_subscribers"]
        else:
            result[f"views_{label}"] = 0
            if label == "2wk":
                result["avg_view_duration_pct"] = None
                result["avg_view_time_sec"] = None
                result["new_subscribers"] = None

    # Fetch lifetime ad views (single call per video)
    result["ad_views"] = fetch_ad_views(video_id, start, yt_analytics)

    return result


def fetch_last_24h_views(video_id, yt_analytics, retries=3):
    """Fetch recent daily views (2-3 days ago, accounting for API processing delay)."""
    today = datetime.now()
    start = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    end = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    for attempt in range(retries):
        try:
            response = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start,
                endDate=end,
                metrics="views",
                dimensions="day",
                filters=f"video=={video_id}",
                sort="day",
            ).execute()
            rows = response.get("rows", [])
            return sum(r[1] for r in rows) if rows else 0
        except Exception as e:
            print(f"    Error (last 24h), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_daily_watch_hours(yt_analytics, retries=3):
    """Fetch channel-level daily watch hours for the past year."""
    today = datetime.now()
    start = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    end = (today - timedelta(days=2)).strftime("%Y-%m-%d")  # API has ~2-day delay
    for attempt in range(retries):
        try:
            response = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start,
                endDate=end,
                metrics="estimatedMinutesWatched,views",
                dimensions="day",
                sort="day",
            ).execute()
            rows = response.get("rows", [])
            return [{"date": r[0], "watch_hours": round(r[1] / 60, 2), "views": r[2]} for r in rows]
        except Exception as e:
            print(f"    Error (daily watch hours), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return []


def run():
    videos_file = DATA_DIR / "videos.json"
    if not videos_file.exists():
        print("No videos.json found. Run fetch_videos.py first.")
        return

    videos = json.loads(videos_file.read_text())
    credentials = get_credentials()
    youtube = build("youtube", "v3", credentials=credentials)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=credentials)

    # Fetch video details (duration, current stats) via Data API
    print("Fetching video details...")
    video_ids = [v["video_id"] for v in videos]
    all_details = fetch_video_details(youtube, video_ids)

    # Fetch per-video analytics via Analytics API
    results = []
    skipped = 0
    for i, video in enumerate(videos, 1):
        vid = video["video_id"]
        detail = all_details.get(vid)
        if not detail:
            print(f"[{i}/{len(videos)}] SKIPPED (deleted): {video['title'][:60]}")
            skipped += 1
            continue
        print(f"[{i}/{len(videos)}] {video['title'][:60]}")
        analytics = fetch_video_analytics(vid, video["published_at"], yt_analytics)
        last_24h = fetch_last_24h_views(vid, yt_analytics)
        results.append({
            **video,
            **detail,
            **analytics,
            "last_24h_views": last_24h,
        })

    # Fetch channel-level daily watch hours
    print("\nFetching daily watch hours...")
    daily_watch = fetch_daily_watch_hours(yt_analytics)
    print(f"  Got {len(daily_watch)} days of data")

    output = DATA_DIR / "analytics.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "videos": results,
        "daily_watch_hours": daily_watch,
    }
    output.write_text(json.dumps(output_data, indent=2))

    shorts = sum(1 for r in results if r.get("is_short"))
    skip_msg = f", {skipped} deleted/skipped" if skipped else ""
    print(f"\nSaved {len(results)} videos ({shorts} Shorts, {len(results) - shorts} Videos{skip_msg}) to {output}")


if __name__ == "__main__":
    run()
