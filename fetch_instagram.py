"""
Fetches Instagram analytics using the new Instagram API (graph.instagram.com).
Metrics: account-level (followers, reach, impressions), post-level, stories.
Saves results to data/instagram.json.

Setup:
1. Create a Meta Developer app at https://developers.facebook.com/
2. Add "Instagram API" use case
3. Generate an access token (starts with IGAA...)
4. Save it to ig_token.json: {"access_token": "YOUR_TOKEN", "ig_user_id": "YOUR_IG_USER_ID"}
   ig_user_id is numeric, find it via GET /me in Graph API Explorer
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
API_BASE = "https://graph.instagram.com/v21.0"


def load_token():
    """Load Instagram access token and user ID."""
    if not TOKEN_FILE.exists():
        print(f"No {TOKEN_FILE} found. See setup instructions in this file.")
        return None, None
    creds = json.loads(TOKEN_FILE.read_text())
    return creds.get("access_token"), creds.get("ig_user_id")


def api_get(endpoint, params=None, retries=3):
    """Make a GET request to the Instagram API."""
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
            # Don't retry "Not enough users" — it won't change
            if "Not enough users" in body:
                print(f"  Skipped ({endpoint}): not enough users for demographics")
                return None
            print(f"  API error ({endpoint}), attempt {attempt + 1}/{retries}: {e.code} {body[:200]}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error ({endpoint}), attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_account_info(token, ig_user_id):
    """Fetch basic account info."""
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
    """Fetch account-level insights for last 30 + prev 30 days."""
    results = {}

    end = datetime.now() - timedelta(days=1)
    start_30 = end - timedelta(days=29)
    start_60 = start_30 - timedelta(days=30)

    for label, since, until in [
        ("30d", start_30, end),
        ("prev_30d", start_60, start_30 - timedelta(days=1)),
    ]:
        data = api_get(f"/{ig_user_id}/insights", {
            "metric": "reach,follower_count,accounts_engaged,total_interactions,profile_views",
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

            # Check for total_value format (new API)
            total_value = metric_data.get("total_value", {}).get("value")
            if total_value is not None:
                results[f"{metric_name}_{label}"] = total_value
                continue

            total = sum(v.get("value", 0) for v in values)

            if metric_name == "follower_count":
                if values:
                    results[f"followers_{label}"] = values[-1].get("value", 0)
                    if label == "30d":
                        results["follower_history"] = [
                            {"date": v["end_time"][:10], "followers": v["value"]}
                            for v in values if "value" in v
                        ]
            else:
                results[f"{metric_name}_{label}"] = total

    if "followers_30d" in results and "followers_prev_30d" in results:
        results["followers_30d_change"] = results["followers_30d"] - results["followers_prev_30d"]

    return results


def fetch_daily_insights(token, ig_user_id):
    """Fetch daily reach and impressions for the last 30 days."""
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=29)

    data = api_get(f"/{ig_user_id}/insights", {
        "metric": "reach,accounts_engaged,total_interactions",
        "period": "day",
        "since": int(start.timestamp()),
        "until": int((end + timedelta(days=1)).timestamp()),
        "access_token": token,
    })
    if not data or "data" not in data:
        return []

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
    """Fetch recent media with insights."""
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

        # Fetch per-post insights
        metrics = "reach,likes,comments,shares,saved,total_interactions"

        insights = api_get(f"/{media_id}/insights", {
            "metric": metrics,
            "access_token": token,
        })
        # If that fails (unsupported metric for this type), try without shares
        if not insights or "data" not in (insights or {}):
            insights = api_get(f"/{media_id}/insights", {
                "metric": "reach,likes,comments,saved,total_interactions",
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
            "metric": "reach,replies,total_interactions",
            "access_token": token,
        })
        if insights and "data" in insights:
            for m in insights["data"]:
                val = m.get("values", [{}])[0].get("value")
                if val is None:
                    val = m.get("total_value", {}).get("value", 0)
                story[m["name"]] = val

        stories.append(story)
        time.sleep(0.2)

    return stories


def fetch_demographics(token, ig_user_id):
    """Fetch demographic breakdowns for followers, reached, and engaged audiences."""
    demographics = {}
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=29)

    metrics_to_try = [
        ("reached_audience_demographics", "reached"),
        ("engaged_audience_demographics", "engaged"),
        ("follower_demographics", "followers"),
    ]

    for metric, label in metrics_to_try:
        demo = {"age": {}, "gender": {}, "city": {}, "country": {}}
        has_data = False

        for breakdown in ["age", "gender", "city", "country"]:
            params = {
                "metric": metric,
                "period": "lifetime",
                "breakdown": breakdown,
                "metric_type": "total_value",
                "access_token": token,
            }
            if metric != "follower_demographics":
                params["since"] = int(start.timestamp())
                params["until"] = int((end + timedelta(days=1)).timestamp())

            data = api_get(f"/{ig_user_id}/insights", params)
            if not data or not data.get("data"):
                continue

            for metric_data in data["data"]:
                breakdowns = metric_data.get("total_value", {}).get("breakdowns", [])
                for bd in breakdowns:
                    for result in bd.get("results", []):
                        key = result.get("dimension_values", [""])[0]
                        value = result.get("value", 0)
                        if key and value:
                            demo[breakdown][key] = demo[breakdown].get(key, 0) + value
                            has_data = True

            time.sleep(0.2)

        if has_data:
            # Sort and keep top entries for city/country
            demo["city"] = dict(sorted(demo["city"].items(), key=lambda x: -x[1])[:10])
            demo["country"] = dict(sorted(demo["country"].items(), key=lambda x: -x[1])[:10])
            demographics[label] = demo
            print(f"    {label}: {len(demo['age'])} age groups, {len(demo['gender'])} genders, {len(demo['city'])} cities")
        else:
            print(f"    {label}: no data (not enough users)")

    return demographics


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

    print("Fetching demographics...")
    demographics = fetch_demographics(token, ig_user_id)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "instagram.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "account": account,
        "follower_history": follower_history,
        "daily": daily,
        "posts": posts,
        "stories": stories,
        "demographics": demographics,
    }
    output.write_text(json.dumps(output_data, indent=2))
    print(f"\nSaved to {output}")


if __name__ == "__main__":
    run()
