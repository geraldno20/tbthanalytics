"""Check traffic source breakdown for a specific video."""
from auth import get_credentials
from googleapiclient.discovery import build

CHANNEL_ID = "UC-obSTyigrLPiN-kiW1bgoA"
VIDEO_ID = "SGqZIlp7u9I"

credentials = get_credentials()
yt_analytics = build("youtubeAnalytics", "v2", credentials=credentials)

response = yt_analytics.reports().query(
    ids=f"channel=={CHANNEL_ID}",
    startDate="2025-07-17",
    endDate="2025-12-31",
    metrics="views",
    dimensions="insightTrafficSourceType",
    filters=f"video=={VIDEO_ID}",
    sort="-views",
).execute()

print("Traffic sources for 同班同學 - 黃凱寧:")
for row in response.get("rows", []):
    print(f"  {row[0]}: {row[1]} views")
