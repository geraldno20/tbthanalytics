"""Check ad views using traffic source dimension (no day combination)."""
from auth import get_credentials
from googleapiclient.discovery import build

CHANNEL_ID = "UC-obSTyigrLPiN-kiW1bgoA"
VIDEO_ID = "SGqZIlp7u9I"

credentials = get_credentials()
yt_analytics = build("youtubeAnalytics", "v2", credentials=credentials)

# Get total views by traffic source
response = yt_analytics.reports().query(
    ids=f"channel=={CHANNEL_ID}",
    startDate="2025-07-17",
    endDate="2026-04-29",
    metrics="views",
    dimensions="insightTrafficSourceType",
    filters=f"video=={VIDEO_ID}",
    sort="-views",
).execute()

total = 0
ad_views = 0
for row in response.get("rows", []):
    total += row[1]
    if row[0] == "ADVERTISING":
        ad_views = row[1]
    print(f"  {row[0]}: {row[1]}")

print(f"\nTotal: {total}")
print(f"Ad views: {ad_views}")
print(f"Organic: {total - ad_views}")
