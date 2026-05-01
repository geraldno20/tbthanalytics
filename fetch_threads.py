"""
Fetches Threads analytics using the Threads API (graph.threads.net).
Metrics: account-level (followers, views), post-level (views, likes, replies, reposts, quotes).
Saves results to data/threads.json.

Setup:
1. In your Meta Developer app at https://developers.facebook.com/
2. Add "Threads API" use case
3. Under Threads API > Settings, add your Threads account
4. Generate an access token from the Threads API use case
5. Save it to threads_token.json: {"access_token": "YOUR_TOKEN"}
   The script will auto-detect your Threads user ID via GET /me
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError

DATA_DIR = Path(__file__).parent / "data"
TOKEN_FILE = Path(__file__).parent / "threads_token.json"
API_BASE = "https://graph.threads.net/v1.0"


def load_token():
    """Load Threads access token."""
    if not TOKEN_FILE.exists():
        print(f"No {TOKEN_FILE} found. See setup instructions in this file.")
        return None
    creds = json.loads(TOKEN_FILE.read_text())
    return creds.get("access_token")


def api_get(endpoint, params=None, retries=3):
    """Make a GET request to the Threads API."""
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
            if "Not enough" in body:
                print(f"  Skipped ({endpoint}): not enough data for this metric")
                return None
            print(f"  API error ({endpoint}), attempt {attempt + 1}/{retries}: {e.code} {body[:200]}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error ({endpoint}), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_user_info(token):
    """Fetch basic Threads profile info and user ID."""
    data = api_get("/me", {
        "fields": "id,username,name,threads_profile_picture_url,threads_biography",
        "access_token": token,
    })
    if not data:
        return None, {}
    return data.get("id"), {
        "user_id": data.get("id"),
        "username": data.get("username"),
        "name": data.get("name"),
        "profile_picture": data.get("threads_profile_picture_url"),
        "biography": data.get("threads_biography"),
    }


def fetch_user_insights(token, user_id):
    """Fetch user-level insights: views and follower count over last 30 + prev 30 days."""
    results = {}
    end = datetime.now() - timedelta(days=1)
    start_30 = end - timedelta(days=29)
    start_60 = start_30 - timedelta(days=30)

    for label, since, until in [
        ("30d", start_30, end),
        ("prev_30d", start_60, start_30 - timedelta(days=1)),
    ]:
        # Views
        data = api_get(f"/{user_id}/threads_insights", {
            "metric": "views",
            "period": "day",
            "since": int(since.timestamp()),
            "until": int((until + timedelta(days=1)).timestamp()),
            "access_token": token,
        })
        if data and "data" in data:
            for metric_data in data["data"]:
                if metric_data["name"] == "views":
                    values = metric_data.get("values", [])
                    total = sum(v.get("value", 0) for v in values)
                    results[f"views_{label}"] = total
                    if label == "30d":
                        results["daily_views"] = [
                            {"date": v["end_time"][:10], "views": v["value"]}
                            for v in values if "value" in v
                        ]

        # Follower count (daily snapshots)
        data = api_get(f"/{user_id}/threads_insights", {
            "metric": "followers_count",
            "period": "day",
            "since": int(since.timestamp()),
            "until": int((until + timedelta(days=1)).timestamp()),
            "access_token": token,
        })
        if data and "data" in data:
            for metric_data in data["data"]:
                if metric_data["name"] == "followers_count":
                    values = metric_data.get("values", [])
                    if values:
                        results[f"followers_{label}"] = values[-1].get("value", 0)
                    if label == "30d" and values:
                        results["follower_history"] = [
                            {"date": v["end_time"][:10], "followers": v["value"]}
                            for v in values if "value" in v
                        ]

        time.sleep(0.3)

    if "followers_30d" in results and "followers_prev_30d" in results:
        results["followers_30d_change"] = results["followers_30d"] - results["followers_prev_30d"]

    return results


def fetch_threads_posts(token, user_id, limit=100):
    """Fetch recent threads with per-post insights."""
    data = api_get(f"/{user_id}/threads", {
        "fields": "id,media_type,media_url,text,timestamp,permalink,shortcode,thumbnail_url,is_quote_status",
        "limit": min(limit, 100),
        "access_token": token,
    })
    if not data or "data" not in data:
        return []

    posts = []
    for item in data["data"]:
        thread_id = item["id"]
        post = {
            "id": thread_id,
            "text": (item.get("text") or "")[:200],
            "media_type": item.get("media_type", ""),
            "timestamp": item.get("timestamp"),
            "permalink": item.get("permalink"),
            "thumbnail": item.get("thumbnail_url") or item.get("media_url", ""),
            "is_quote": item.get("is_quote_status", False),
        }

        # Fetch per-post insights
        insights = api_get(f"/{thread_id}/insights", {
            "metric": "views,likes,replies,reposts,quotes",
            "access_token": token,
        })
        if insights and "data" in insights:
            for m in insights["data"]:
                val = m.get("values", [{}])[0].get("value")
                if val is None:
                    val = m.get("total_value", {}).get("value", 0)
                post[m["name"]] = val

        posts.append(post)
        time.sleep(0.2)

    return posts


def fetch_demographics(token, user_id):
    """Fetch follower demographics (if enough followers)."""
    demographics = {}
    for breakdown in ["age", "gender", "country", "city"]:
        data = api_get(f"/{user_id}/threads_insights", {
            "metric": "follower_demographics",
            "period": "lifetime",
            "breakdown": breakdown,
            "access_token": token,
        })
        if not data or not data.get("data"):
            continue
        for metric_data in data["data"]:
            breakdowns = metric_data.get("total_value", {}).get("breakdowns", [])
            for bd in breakdowns:
                for result in bd.get("results", []):
                    key = result.get("dimension_values", [""])[0]
                    value = result.get("value", 0)
                    if key and value:
                        if breakdown not in demographics:
                            demographics[breakdown] = {}
                        demographics[breakdown][key] = demographics[breakdown].get(key, 0) + value
        time.sleep(0.2)

    if "city" in demographics:
        demographics["city"] = dict(sorted(demographics["city"].items(), key=lambda x: -x[1])[:10])
    if "country" in demographics:
        demographics["country"] = dict(sorted(demographics["country"].items(), key=lambda x: -x[1])[:10])

    return demographics


def run():
    token = load_token()
    if not token:
        return

    print("Fetching Threads user info...")
    user_id, account = fetch_user_info(token)
    if not user_id:
        print("  Failed to fetch user info. Check your access token.")
        return
    print(f"  @{account.get('username', '?')}")

    print("Fetching user insights...")
    insights = fetch_user_insights(token, user_id)
    follower_history = insights.pop("follower_history", [])
    daily_views = insights.pop("daily_views", [])
    account.update(insights)
    print(f"  Views (30d): {account.get('views_30d', 0):,}")
    print(f"  Followers: {account.get('followers_30d', 0):,}")

    print("Fetching threads...")
    posts = fetch_threads_posts(token, user_id)
    print(f"  Got {len(posts)} threads")

    print("Fetching demographics...")
    demographics = fetch_demographics(token, user_id)
    if demographics:
        for k, v in demographics.items():
            print(f"    {k}: {len(v)} entries")
    else:
        print("    No demographic data (may need more followers)")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "threads.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "account": account,
        "follower_history": follower_history,
        "daily_views": daily_views,
        "posts": posts,
        "demographics": demographics,
    }
    output.write_text(json.dumps(output_data, indent=2))
    print(f"\nSaved to {output}")


if __name__ == "__main__":
    run()
