"""
Fetches all videos from the authenticated YouTube channel using the Data API v3.
Uses uploads playlist for public videos, then search API to find unlisted ones.
Saves video metadata (id, title, publish date) to data/videos.json.
"""

import json
import time
from pathlib import Path

from auth import get_credentials
from googleapiclient.discovery import build

DATA_DIR = Path(__file__).parent / "data"


CHANNEL_ID = "UC-obSTyigrLPiN-kiW1bgoA"


def fetch_all_videos():
    credentials = get_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    # Step 1: Get videos from uploads playlist (public + some unlisted)
    channels = youtube.channels().list(part="contentDetails", id=CHANNEL_ID).execute()
    uploads_playlist = channels["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    seen_ids = set()
    videos = []
    next_page = None

    print("Fetching from uploads playlist...")
    while True:
        request = youtube.playlistItems().list(
            part="snippet,status",
            playlistId=uploads_playlist,
            maxResults=50,
            pageToken=next_page,
        )
        response = request.execute()

        for item in response["items"]:
            snippet = item["snippet"]
            vid = snippet["resourceId"]["videoId"]
            if vid not in seen_ids:
                seen_ids.add(vid)
                videos.append({
                    "video_id": vid,
                    "title": snippet["title"],
                    "published_at": snippet["publishedAt"],
                })

        next_page = response.get("nextPageToken")
        if not next_page:
            break

    print(f"  Found {len(videos)} from uploads playlist")

    # Step 2: Use search API with forMine=True to find unlisted videos
    print("Searching for unlisted videos...")
    next_page = None
    unlisted_count = 0

    while True:
        request = youtube.search().list(
            part="snippet",
            forMine=True,
            type="video",
            maxResults=50,
            pageToken=next_page,
        )
        response = request.execute()

        for item in response.get("items", []):
            vid = item["id"]["videoId"]
            if vid not in seen_ids:
                seen_ids.add(vid)
                unlisted_count += 1
                videos.append({
                    "video_id": vid,
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                })

        next_page = response.get("nextPageToken")
        if not next_page:
            break
        time.sleep(0.2)

    if unlisted_count:
        print(f"  Found {unlisted_count} additional videos via search")

    print(f"Total: {len(videos)} videos")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "videos.json"
    output.write_text(json.dumps(videos, indent=2))
    print(f"Saved to {output}")
    return videos


if __name__ == "__main__":
    fetch_all_videos()
