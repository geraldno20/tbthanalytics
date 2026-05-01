"""
Fetches episode metadata from Google Sheets.
Reads season, episode, guest, interviewer, recording date, release date.
Saves results to data/episodes.json.

Uses separate OAuth credentials from YouTube since the Sheet may be
owned by a different Google account. Stores token in sheets_token.json.
"""

import json
from datetime import datetime
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

DATA_DIR = Path(__file__).parent / "data"
ROOT = Path(__file__).parent
CLIENT_SECRET = ROOT / "client_secret.json"
SHEETS_TOKEN = ROOT / "sheets_token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SPREADSHEET_ID = "1PoUfMdia4D78XlpmOfQUogsGAfLVslvxokcKXD5Ylts"
RANGE = "Scheduling!A:H"


def get_sheets_credentials():
    creds = None
    if SHEETS_TOKEN.exists():
        creds = Credentials.from_authorized_user_file(str(SHEETS_TOKEN), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRET.exists():
                raise FileNotFoundError(f"Missing {CLIENT_SECRET}.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
            creds = flow.run_local_server(port=0)
        SHEETS_TOKEN.write_text(creds.to_json())
    return creds


def run():
    credentials = get_sheets_credentials()
    sheets = build("sheets", "v4", credentials=credentials)

    print("Fetching episode metadata from Google Sheets...")
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE,
    ).execute()

    rows = result.get("values", [])
    if not rows:
        print("  No data found.")
        return

    # First row is headers
    headers = rows[0]
    print(f"  Columns: {headers}")
    print(f"  {len(rows) - 1} episodes found")

    # Normalize headers to snake_case keys
    def to_key(h):
        return h.strip().lower().replace(" ", "_").replace("#", "num")

    keys = [to_key(h) for h in headers]
    print(f"  Keys: {keys}")

    episodes = []
    for row in rows[1:]:
        # Pad row to ensure we have all columns
        while len(row) < len(headers):
            row.append("")

        episode = {keys[i]: row[i] for i in range(len(keys))}
        episodes.append(episode)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / "episodes.json"
    output_data = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "episodes": episodes,
    }
    output.write_text(json.dumps(output_data, indent=2, ensure_ascii=False))
    print(f"\nSaved {len(episodes)} episodes to {output}")


if __name__ == "__main__":
    run()
