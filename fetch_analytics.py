"""
Fetches per-video analytics using the YouTube Analytics API.
Metrics: views (24h/72h/1wk/2wk/1mo/3mo/lifetime), lifetime ad views,
avg view duration %, avg view time, new subscribers.
Also fetches duration and detects Shorts via the Data API.
Saves results to data/analytics.json.

Optimized: 1 daily-metrics call + 1 ad-views call per video (2 total),
instead of separate calls per time period.
"""

import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request

from auth import get_credentials
from googleapiclient.discovery import build

DATA_DIR = Path(__file__).parent / "data"
CHANNEL_ID = "UC-obSTyigrLPiN-kiW1bgoA"

# Period definitions: label -> number of extra calendar days from publish date
# (API date ranges are inclusive, so 0 = publish day only = ~24h)
PERIODS = {
    "24h": 0,
    "72h": 2,
    "1wk": 6,
    "2wk": 13,
    "1mo": 29,
    "3mo": 89,
}


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
        details[vid]["is_short"] = check_is_short(vid)
        time.sleep(0.1)

    shorts_count = sum(1 for d in details.values() if d["is_short"])
    print(f"  {shorts_count} Shorts, {len(details) - shorts_count} Videos")
    return details


def api_call_with_retry(query_fn, label="", retries=3):
    """Execute an API query function with retry and exponential backoff."""
    for attempt in range(retries):
        try:
            return query_fn()
        except Exception as e:
            print(f"    Error ({label}), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_lifetime_daily(video_id, start_date, yt_analytics):
    """Fetch daily metrics from publish date to today in a single API call."""
    today = datetime.now().strftime("%Y-%m-%d")

    def query():
        return yt_analytics.reports().query(
            ids=f"channel=={CHANNEL_ID}",
            startDate=start_date,
            endDate=today,
            metrics="views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained",
            dimensions="day",
            filters=f"video=={video_id}",
            sort="day",
        ).execute().get("rows", [])

    return api_call_with_retry(query, "daily metrics")


def compute_period_metrics(rows, start_date, end_date):
    """Compute aggregated metrics from daily rows within a date range (inclusive)."""
    filtered = [r for r in rows if start_date <= r[0] <= end_date]
    if not filtered:
        return {"views": 0, "avg_view_duration_sec": 0, "avg_view_duration_pct": 0, "new_subscribers": 0}

    total_views = sum(r[1] for r in filtered)
    avg_duration = sum(r[3] * r[1] for r in filtered) / total_views if total_views else 0
    avg_pct = sum(r[4] * r[1] for r in filtered) / total_views if total_views else 0
    total_subs = sum(r[5] for r in filtered)

    return {
        "views": total_views,
        "avg_view_duration_sec": round(avg_duration, 1),
        "avg_view_duration_pct": round(avg_pct, 1),
        "new_subscribers": total_subs,
    }


def fetch_ad_views(video_id, start_date, yt_analytics):
    """Fetch lifetime advertising views using traffic source dimension."""
    today = datetime.now().strftime("%Y-%m-%d")

    def query():
        response = yt_analytics.reports().query(
            ids=f"channel=={CHANNEL_ID}",
            startDate=start_date,
            endDate=today,
            metrics="views",
            dimensions="insightTrafficSourceType",
            filters=f"video=={video_id}",
        ).execute()
        for row in response.get("rows", []):
            if row[0] == "ADVERTISING":
                return row[1]
        return 0

    result = api_call_with_retry(query, "ad views")
    return result if result is not None else 0


def fetch_video_analytics(video_id, published_at, yt_analytics):
    """Fetch all metrics for a video: 1 daily call + 1 ad call, then slice by period."""
    pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    start = pub.strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    # Single API call: get all daily rows from publish to today
    rows = fetch_lifetime_daily(video_id, start, yt_analytics)
    if rows is None:
        result = {f"views_{label}": None for label in PERIODS}
        result.update({
            "views_lifetime": None, "avg_view_duration_pct": None,
            "avg_view_time_sec": None, "new_subscribers": None,
            "avd_over_time": {}, "last_24h_views": None,
            "ad_views": fetch_ad_views(video_id, start, yt_analytics),
        })
        return result

    result = {}
    avd_over_time = {}

    # Compute metrics for each period by slicing the daily rows
    for label, days_offset in PERIODS.items():
        end = (pub + timedelta(days=days_offset)).strftime("%Y-%m-%d")
        if end > today:
            result[f"views_{label}"] = None
            continue

        data = compute_period_metrics(rows, start, end)
        result[f"views_{label}"] = data["views"]
        avd_over_time[label] = data["avg_view_duration_pct"]

        if label == "2wk":
            result["avg_view_duration_pct"] = data["avg_view_duration_pct"]
            result["avg_view_time_sec"] = data["avg_view_duration_sec"]
            result["new_subscribers"] = data["new_subscribers"]

    # Lifetime metrics
    lifetime = compute_period_metrics(rows, start, today)
    result["views_lifetime"] = lifetime["views"]
    avd_over_time["lifetime"] = lifetime["avg_view_duration_pct"]

    result["avd_over_time"] = avd_over_time

    # Last 24h views: 2-3 days ago (API processing delay)
    recent_start = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    recent_end = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    result["last_24h_views"] = compute_period_metrics(rows, recent_start, recent_end)["views"]

    # Ad views (separate call — different dimension, can't combine with daily)
    result["ad_views"] = fetch_ad_views(video_id, start, yt_analytics)

    return result


def fetch_daily_channel_stats(yt_analytics):
    """Fetch channel-level daily watch hours and views for the past year."""
    today = datetime.now()
    start = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    end = (today - timedelta(days=2)).strftime("%Y-%m-%d")

    def query_daily():
        response = yt_analytics.reports().query(
            ids=f"channel=={CHANNEL_ID}",
            startDate=start,
            endDate=end,
            metrics="estimatedMinutesWatched,views",
            dimensions="day",
            sort="day",
        ).execute()
        return [{"date": r[0], "watch_hours": round(r[1] / 60, 2), "views": r[2]}
                for r in response.get("rows", [])]

    daily = api_call_with_retry(query_daily, "daily channel stats")
    return daily if daily is not None else []


def fetch_channel_summary(youtube, yt_analytics):
    """Fetch channel subscriber count and unique viewers (lifetime + last 30 days)."""
    today = datetime.now()
    end = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    start_30 = (today - timedelta(days=32)).strftime("%Y-%m-%d")
    start_60 = (today - timedelta(days=62)).strftime("%Y-%m-%d")
    start_all = "2020-01-01"

    # Subscribers gained over entire channel lifetime via Analytics API
    def query_lifetime_subs():
        def q():
            resp = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start_all,
                endDate=end,
                metrics="subscribersGained,subscribersLost",
            ).execute()
            rows = resp.get("rows", [])
            if rows:
                return {"gained": rows[0][0], "lost": rows[0][1]}
            return {"gained": 0, "lost": 0}
        return api_call_with_retry(q, "lifetime subs") or {"gained": 0, "lost": 0}

    lifetime_subs = query_lifetime_subs()

    # Subscribers gained in last 30 days vs previous 30 days
    def query_subs_gained(start, end_date):
        def q():
            resp = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start,
                endDate=end_date,
                metrics="subscribersGained,subscribersLost",
            ).execute()
            rows = resp.get("rows", [])
            if rows:
                return rows[0][0] - rows[0][1]  # net subscribers
            return 0
        return api_call_with_retry(q, f"subs gained {start}") or 0

    subs_30 = query_subs_gained(start_30, end)
    subs_prev_30 = query_subs_gained(start_60, start_30)

    # Current subscriber count from Data API (rounded by YouTube)
    def query_subs():
        resp = youtube.channels().list(
            part="statistics",
            id=CHANNEL_ID,
        ).execute()
        items = resp.get("items", [])
        if items:
            return int(items[0]["statistics"].get("subscriberCount", 0))
        return 0

    subs_rounded = api_call_with_retry(query_subs, "subscriber count") or 0

    # Try to get a more accurate count: use the rounded count as a reference,
    # but also compute from lifetime gained-lost to get a better number
    net_lifetime = lifetime_subs["gained"] - lifetime_subs["lost"]
    # Use rounded count as the base since net_lifetime might not account for
    # pre-Analytics subscribers. But we can use the relationship to estimate:
    # If net_lifetime is close to subs_rounded, use net_lifetime as more precise
    subs = subs_rounded
    if net_lifetime > 0 and abs(net_lifetime - subs_rounded) <= 50:
        subs = net_lifetime

    # Unique viewers via Analytics API
    # Try viewerPercentage first, fall back to summing daily views
    def query_unique_viewers(start, end_date):
        def q():
            resp = yt_analytics.reports().query(
                ids=f"channel=={CHANNEL_ID}",
                startDate=start,
                endDate=end_date,
                metrics="uniqueViewers",
            ).execute()
            print(f"    uniqueViewers response ({start}): {resp.get('rows', [])}")
            rows = resp.get("rows", [])
            if rows and rows[0][0] > 0:
                return rows[0][0]
            return None
        return api_call_with_retry(q, f"unique viewers {start}")

    uv_total = query_unique_viewers(start_all, end)
    uv_30 = query_unique_viewers(start_30, end)
    uv_prev_30 = query_unique_viewers(start_60, start_30)

    # If uniqueViewers returned None/0, fall back to summing views from daily data
    # (not unique but better than showing 0)
    if not uv_total:
        print("    uniqueViewers metric unavailable, falling back to views")

        def query_views(start, end_date):
            def q():
                resp = yt_analytics.reports().query(
                    ids=f"channel=={CHANNEL_ID}",
                    startDate=start,
                    endDate=end_date,
                    metrics="views",
                ).execute()
                rows = resp.get("rows", [])
                return rows[0][0] if rows else 0
            return api_call_with_retry(q, f"views {start}") or 0

        uv_total = query_views(start_all, end)
        uv_30 = query_views(start_30, end)
        uv_prev_30 = query_views(start_60, start_30)
        viewer_metric = "views"
    else:
        viewer_metric = "unique_viewers"

    return {
        "subscribers": subs,
        "subs_30d_change": subs_30,
        "subs_prev_30d_change": subs_prev_30,
        "unique_viewers_total": uv_total,
        "unique_viewers_30d": uv_30,
        "unique_viewers_prev_30d": uv_prev_30,
        "viewer_metric": viewer_metric,
    }


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

    # Fetch per-video analytics (2 API calls per video: 1 daily + 1 ad views)
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
        results.append({**video, **detail, **analytics})

    # Fetch channel-level daily watch hours (1 API call)
    print("\nFetching daily channel stats...")
    daily_watch = fetch_daily_channel_stats(yt_analytics)
    print(f"  Got {len(daily_watch)} days of data")

    # Fetch channel summary (subscribers, unique viewers)
    print("\nFetching channel summary...")
    channel_summary = fetch_channel_summary(youtube, yt_analytics)
    print(f"  Subscribers: {channel_summary['subscribers']:,}")
    print(f"  Unique viewers (total): {channel_summary['unique_viewers_total']:,}")

    output = DATA_DIR / "analytics.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "videos": results,
        "daily_watch_hours": daily_watch,
        "channel_summary": channel_summary,
    }
    output.write_text(json.dumps(output_data, indent=2))

    shorts = sum(1 for r in results if r.get("is_short"))
    skip_msg = f", {skipped} deleted/skipped" if skipped else ""
    print(f"\nSaved {len(results)} videos ({shorts} Shorts, {len(results) - shorts} Videos{skip_msg}) to {output}")


if __name__ == "__main__":
    run()
