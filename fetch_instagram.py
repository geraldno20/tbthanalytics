"""
Fetches Instagram analytics using the Instagram Graph API.
Metrics: account-level (followers, reach, impressions), post-level, stories.
Saves results to data/instagram.json.

Setup:
1. Create a Meta Developer app at https://developers.facebook.com/
2. Add Instagram Graph API product
3. Generate a long-lived access token
4. Save it to ig_token.json: {"access_token": "YOUR_TOKEN", "ig_user_id": "YOUR_IG_USER_ID"}
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError

DATA_DIR = Path(__file__).parent / "data"
TOKEN_FILE = Path(__file__).parent / "ig_token.json"
API_BASE = "https://graph.facebook.com/v21.0"


def load_token():
    """Load Instagram access token and user ID."""
    if not TOKEN_FILE.exists():
        print(f"No {TOKEN_FILE} found. See setup instructions in this file.")
        return None, None
    creds = json.loads(TOKEN_FILE.read_text())
    return creds.get("access_token"), creds.get("ig_user_id")


def api_get(endpoint, params=None, retries=3):
    """Make a GET request to the Instagram Graph API."""
    if params is None:
        params = {}
    url = f"{API_BASE}{endpoint}?{urlencode(params)}"
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urlopen(req, timeout=15)
            return json.loads(resp.read().decode())
        except HTTPError as e:
            body = e.read().decode() if e.fp else ""
            print(f"  API error ({endpoint}), attempt {attempt + 1}/{retries}: {e.code} {body[:200]}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error ({endpoint}), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_account_info(token, ig_user_id):
    """Fetch basic account info: followers, media count, etc."""
    data = api_get(f"/{ig_user_id}", {
        "fields": "username,name,followers_count,follows_count,media_count,biography,profile_picture_url",
        "access_token": token,
    })
    if not data:
        return {}
    return {
        "username": data.get("username"),
        "name": data.get("name"),
        "followers": data.get("followers_count"),
        "following": data.get("follows_count"),
        "media_count": data.get("media_count"),
        "profile_picture": data.get("profile_picture_url"),
    }


def fetch_account_insights(token, ig_user_id):
    """Fetch account-level insights (reach, impressions, follower count) for last 30 + prev 30 days."""
    results = {}

    # Last 30 days
    end = datetime.now() - timedelta(days=1)
    start_30 = end - timedelta(days=29)
    start_60 = start_30 - timedelta(days=30)

    for label, since, until in [
        ("30d", start_30, end),
        ("prev_30d", start_60, start_30 - timedelta(days=1)),
    ]:
        data = api_get(f"/{ig_user_id}/insights", {
            "metric": "reach,impressions,follower_count",
            "period": "day",
            "since": int(since.timestamp()),
            "until": int((until + timedelta(days=1)).timestamp()),
            "access_token": token,
        })
        if not data or "data" not in data:
            continue

        for metric_data in data["data"]:
            metric_name = metric_data["name"]
            values = metric_data.get("values", [])
            total = sum(v.get("value", 0) for v in values)

            if metric_name == "follower_count":
                # follower_count is cumulative, take the last value
                if values:
                    results[f"followers_{label}"] = values[-1].get("value", 0)
                    if label == "30d":
                        results["follower_history"] = [
                            {"date": v["end_time"][:10], "followers": v["value"]}
                            for v in values if "value" in v
                        ]
            else:
                results[f"{metric_name}_{label}"] = total

    # Compute follower change
    if "followers_30d" in results and "followers_prev_30d" in results:
        results["followers_30d_change"] = results["followers_30d"] - results["followers_prev_30d"]

    return results


def fetch_daily_insights(token, ig_user_id):
    """Fetch daily reach and impressions for the last 30 days."""
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=29)

    data = api_get(f"/{ig_user_id}/insights", {
        "metric": "reach,impressions",
        "period": "day",
        "since": int(start.timestamp()),
        "until": int((end + timedelta(days=1)).timestamp()),
        "access_token": token,
    })
    if not data or "data" not in data:
        return []

    # Merge reach and impressions by date
    daily = {}
    for metric_data in data["data"]:
        metric_name = metric_data["name"]
        for v in metric_data.get("values", []):
            date = v["end_time"][:10]
            if date not in daily:
                daily[date] = {"date": date}
            daily[date][metric_name] = v.get("value", 0)

    return sorted(daily.values(), key=lambda d: d["date"])


def fetch_media(token, ig_user_id, limit=100):
    """Fetch recent media (posts, reels, carousels) with insights."""
    # Get media list
    data = api_get(f"/{ig_user_id}/media", {
        "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count",
        "limit": min(limit, 100),
        "access_token": token,
    })
    if not data or "data" not in data:
        return []

    posts = []
    for item in data["data"]:
        media_id = item["id"]
        media_type = item.get("media_type", "")

        post = {
            "id": media_id,
            "caption": (item.get("caption") or "")[:200],
            "media_type": media_type,
            "timestamp": item.get("timestamp"),
            "permalink": item.get("permalink"),
            "thumbnail": item.get("thumbnail_url") or item.get("media_url", ""),
            "likes": item.get("like_count", 0),
            "comments": item.get("comments_count", 0),
        }

        # Fetch per-post insights (reach, impressions, saves, shares)
        metrics = "reach,impressions,saved,shares"
        if media_type == "VIDEO" or media_type == "REEL":
            metrics += ",plays,video_views"

        insights = api_get(f"/{media_id}/insights", {
            "metric": metrics,
            "access_token": token,
        })
        if insights and "data" in insights:
            for m in insights["data"]:
                post[m["name"]] = m.get("values", [{}])[0].get("value", 0)

        posts.append(post)
        time.sleep(0.2)

    return posts


def fetch_stories(token, ig_user_id):
    """Fetch current active stories with insights."""
    data = api_get(f"/{ig_user_id}/stories", {
        "fields": "id,media_type,media_url,timestamp",
        "access_token": token,
    })
    if not data or "data" not in data:
        return []

    stories = []
    for item in data["data"]:
        story_id = item["id"]
        story = {
            "id": story_id,
            "media_type": item.get("media_type"),
            "timestamp": item.get("timestamp"),
        }

        insights = api_get(f"/{story_id}/insights", {
            "metric": "reach,impressions,replies,exits",
            "access_token": token,
        })
        if insights and "data" in insights:
            for m in insights["data"]:
                story[m["name"]] = m.get("values", [{}])[0].get("value", 0)

        stories.append(story)
        time.sleep(0.2)

    return stories


def run():
    token, ig_user_id = load_token()
    if not token or not ig_user_id:
        return

    print("Fetching account info...")
    account = fetch_account_info(token, ig_user_id)
    print(f"  @{account.get('username', '?')} — {account.get('followers', 0):,} followers")

    print("Fetching account insights...")
    insights = fetch_account_insights(token, ig_user_id)
    follower_history = insights.pop("follower_history", [])
    account.update(insights)
    print(f"  Reach (30d): {account.get('reach_30d', 0):,}")
    print(f"  Impressions (30d): {account.get('impressions_30d', 0):,}")

    print("Fetching daily insights...")
    daily = fetch_daily_insights(token, ig_user_id)
    print(f"  Got {len(daily)} days of data")

    print("Fetching posts...")
    posts = fetch_media(token, ig_user_id)
    print(f"  Got {len(posts)} posts")

    print("Fetching stories...")
    stories = fetch_stories(token, ig_user_id)
    print(f"  Got {len(stories)} active stories")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "instagram.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "account": account,
        "follower_history": follower_history,
        "daily": daily,
        "posts": posts,
        "stories": stories,
    }
    output.write_text(json.dumps(output_data, indent=2))
    print(f"\nSaved to {output}")


if __name__ == "__main__":
    run()
