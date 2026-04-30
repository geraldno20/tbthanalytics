"""
Diagnostic script to test YouTube Analytics API access.
Run this to check which channel the API sees and if it can pull data.
"""

from auth import get_credentials
from googleapiclient.discovery import build

credentials = get_credentials()

# Check which channels this account has access to
youtube = build("youtube", "v3", credentials=credentials)

print("=== Channels accessible via mine=True ===")
res = youtube.channels().list(part="snippet,contentDetails", mine=True).execute()
for ch in res.get("items", []):
    print(f"  Channel: {ch['snippet']['title']}")
    print(f"  ID: {ch['id']}")

print("\n=== Channels accessible via managedByMe ===")
try:
    res2 = youtube.channels().list(part="snippet", managedByMe=True, maxResults=50).execute()
    for ch in res2.get("items", []):
        print(f"  Channel: {ch['snippet']['title']}")
        print(f"  ID: {ch['id']}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== Testing Analytics API with channel==MINE ===")
yt_analytics = build("youtubeAnalytics", "v2", credentials=credentials)
try:
    response = yt_analytics.reports().query(
        ids="channel==MINE",
        startDate="2025-01-01",
        endDate="2025-12-31",
        metrics="views",
        dimensions="month",
    ).execute()
    print(f"  Rows: {response.get('rows', [])}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== Testing Analytics API with explicit channel ID ===")
try:
    response = yt_analytics.reports().query(
        ids="channel==UC-obSTyigrLPiN-kiW1bgoA",
        startDate="2025-01-01",
        endDate="2025-12-31",
        metrics="views",
        dimensions="month",
    ).execute()
    print(f"  Rows: {response.get('rows', [])}")
except Exception as e:
    print(f"  Error: {e}")
