"""
Fetches Google Ads spend data per YouTube video.
Uses the Google Ads API to pull campaign metrics (cost, views, impressions)
broken down by video (via asset/ad group video creative).

Requires:
- google-ads pip package
- client_secret.json (same OAuth client as YouTube)
- Google Ads API enabled in Google Cloud Console
- Developer token from Google Ads API Center

Saves results to data/ads.json.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

DATA_DIR = Path(__file__).parent / "data"
ROOT = Path(__file__).parent
CLIENT_SECRET = ROOT / "client_secret.json"
ADS_TOKEN_FILE = ROOT / "ads_token.json"

DEVELOPER_TOKEN = "7Khkr5d0KEpCfZjHFHRENQ"
MANAGER_CUSTOMER_ID = "8402387772"  # MCC (no dashes)
CUSTOMER_ID = "6006333695"  # Child account (no dashes)

SCOPES = ["https://www.googleapis.com/auth/adwords"]


def get_ads_credentials():
    """Get OAuth credentials for Google Ads API."""
    creds = None
    if ADS_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(ADS_TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRET.exists():
                raise FileNotFoundError(f"Missing {CLIENT_SECRET}.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
            creds = flow.run_local_server(port=0)
        ADS_TOKEN_FILE.write_text(creds.to_json())
    return creds


def get_google_ads_client(credentials):
    """Create a GoogleAdsClient using OAuth credentials."""
    return GoogleAdsClient(
        credentials=credentials,
        developer_token=DEVELOPER_TOKEN,
        login_customer_id=MANAGER_CUSTOMER_ID,
    )


def fetch_video_ad_spend(client):
    """
    Fetch ad spend per YouTube video ID using video campaign metrics.
    Uses the video segment to get per-video cost data.
    """
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    # Query video-level metrics from video campaigns
    # This gets cost, views, impressions for each YouTube video ad
    query = f"""
        SELECT
            campaign.name,
            campaign.id,
            ad_group.name,
            ad_group.id,
            video.id,
            video.title,
            metrics.cost_micros,
            metrics.video_views,
            metrics.impressions,
            segments.date
        FROM video
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND metrics.cost_micros > 0
        ORDER BY segments.date DESC
    """

    print("Fetching video ad spend data...")
    results = []
    try:
        response = ga_service.search_stream(
            customer_id=CUSTOMER_ID, query=query
        )
        for batch in response:
            for row in batch.results:
                results.append({
                    "campaign_name": row.campaign.name,
                    "campaign_id": str(row.campaign.id),
                    "ad_group_name": row.ad_group.name,
                    "video_id": row.video.id,
                    "video_title": row.video.title,
                    "cost": row.metrics.cost_micros / 1_000_000,  # Convert micros to dollars
                    "views": row.metrics.video_views,
                    "impressions": row.metrics.impressions,
                    "date": row.segments.date,
                })
    except Exception as e:
        print(f"  Error fetching video data: {e}")
        # Try alternative query using ad_group_ad with video asset
        print("  Trying alternative query...")
        return fetch_video_ad_spend_alt(client)

    print(f"  Got {len(results)} daily video rows")
    return results


def fetch_video_ad_spend_alt(client):
    """Alternative query using ad_group_ad resource for video ads."""
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    query = f"""
        SELECT
            campaign.name,
            campaign.id,
            ad_group_ad.ad.video_ad.video.asset,
            ad_group_ad.ad.name,
            metrics.cost_micros,
            metrics.video_views,
            metrics.impressions,
            segments.date
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND metrics.cost_micros > 0
            AND ad_group_ad.ad.type = VIDEO_RESPONSIVE_AD
        ORDER BY segments.date DESC
    """

    results = []
    try:
        response = ga_service.search_stream(
            customer_id=CUSTOMER_ID, query=query
        )
        for batch in response:
            for row in batch.results:
                results.append({
                    "campaign_name": row.campaign.name,
                    "campaign_id": str(row.campaign.id),
                    "ad_name": row.ad_group_ad.ad.name,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "views": row.metrics.video_views,
                    "impressions": row.metrics.impressions,
                    "date": row.segments.date,
                })
    except Exception as e:
        print(f"  Error with alternative query: {e}")

    print(f"  Got {len(results)} daily ad rows")
    return results


def aggregate_by_video(results):
    """Aggregate daily data into per-video totals."""
    videos = {}
    for row in results:
        vid = row.get("video_id") or row.get("ad_name", "unknown")
        if vid not in videos:
            videos[vid] = {
                "video_id": row.get("video_id", ""),
                "video_title": row.get("video_title", row.get("ad_name", "")),
                "campaign_name": row.get("campaign_name", ""),
                "total_cost": 0,
                "total_views": 0,
                "total_impressions": 0,
                "daily": [],
            }
        videos[vid]["total_cost"] += row["cost"]
        videos[vid]["total_views"] += row["views"]
        videos[vid]["total_impressions"] += row["impressions"]
        videos[vid]["daily"].append({
            "date": row["date"],
            "cost": row["cost"],
            "views": row["views"],
            "impressions": row["impressions"],
        })

    # Round costs
    for v in videos.values():
        v["total_cost"] = round(v["total_cost"], 2)
        v["cost_per_view"] = round(v["total_cost"] / v["total_views"], 4) if v["total_views"] else 0
        for d in v["daily"]:
            d["cost"] = round(d["cost"], 4)

    return list(videos.values())


def run():
    credentials = get_ads_credentials()
    client = get_google_ads_client(credentials)

    results = fetch_video_ad_spend(client)

    if not results:
        print("No ad spend data found.")
        return

    videos = aggregate_by_video(results)

    # Summary
    total_spend = sum(v["total_cost"] for v in videos)
    total_views = sum(v["total_views"] for v in videos)
    print(f"\n  Total spend: ${total_spend:.2f}")
    print(f"  Total ad views: {total_views:,}")
    print(f"  Videos with ads: {len(videos)}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "ads.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_spend": round(total_spend, 2),
        "total_views": total_views,
        "videos": sorted(videos, key=lambda v: v["total_cost"], reverse=True),
    }
    output.write_text(json.dumps(output_data, indent=2))
    print(f"\nSaved to {output}")


if __name__ == "__main__":
    run()
