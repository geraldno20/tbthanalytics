from auth import get_credentials
from googleapiclient.discovery import build

yt = build("youtube", "v3", credentials=get_credentials())
res = yt.channels().list(part="snippet", mine=True).execute()
for ch in res["items"]:
    print("Channel:", ch["snippet"]["title"])
    print("ID:", ch["id"])
