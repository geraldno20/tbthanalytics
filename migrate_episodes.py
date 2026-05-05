"""
Migrate episodes from data/episodes.json into Supabase projects + project_tasks tables.

- air_date is set from the 'episode_release' field in the Google Sheet
- If air_date is in the past, ALL tasks are marked 'complete'
- If air_date is in the future or missing, tasks are 'not_started'

Run: python3 migrate_episodes.py
"""
import json
import requests
from datetime import datetime

SUPABASE_URL = 'https://avpizwlpceuuawxaiwbv.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cGl6d2xwY2V1dWF3eGFpd2J2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc5NzM1NzAsImV4cCI6MjA5MzU0OTU3MH0.TnfoJEpSHpqOxtk70SVPj3nXGD_7aFIXRPKygF4yD6M'

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation',
}

WORKFLOW_TASKS = [
    'outreach', 'acceptance', 'schedule', 'research_topics',
    'filming', 'edit', 'thumbnail', 'intro', 'title',
    'teaser_1', 'teaser_2', 'pre_ig_post', 'post_ig_post',
    'publish', 'marketing_plan',
]

SKIP_SEASONS = ('Agreed', 'Declined', 'No', '??', 'Done', 'Check back', 'TBD', '0', '-', '', '#REF!')


def parse_date(s):
    """Parse M/D/YY or M/D/YYYY format to YYYY-MM-DD"""
    if not s or s in ('-', ''):
        return None
    for fmt in ('%m/%d/%Y', '%m/%d/%y'):
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def sb_post(endpoint, body):
    resp = requests.post(
        f'{SUPABASE_URL}/rest/v1/{endpoint}',
        headers=HEADERS,
        json=body,
    )
    if not resp.ok:
        print(f'  ERROR {resp.status_code}: {resp.text}')
        return None
    return resp.json()


def main():
    with open('data/episodes.json') as f:
        data = json.load(f)

    eps = data['episodes']
    today = datetime.now()
    count = 0

    for e in eps:
        season = e['season']
        if season in SKIP_SEASONS:
            continue
        if season == 'Nonsense':
            season_val = 'NoS'
        else:
            try:
                int(season)
            except ValueError:
                continue
            season_val = season

        guest = e.get('guest', '').strip()
        if not guest:
            continue

        episode_num = e.get('episode', '')
        air_date = parse_date(e.get('episode_release', ''))
        name = guest

        # Determine if aired: air_date is in the past
        is_aired = False
        if air_date:
            rd = datetime.strptime(air_date, '%Y-%m-%d')
            is_aired = rd <= today

        # Create project
        project_body = {
            'name': name,
            'season': season_val,
            'episode': episode_num if episode_num else None,
            'air_date': air_date,
        }
        result = sb_post('projects', project_body)
        if not result or not len(result):
            print(f'  Failed to create project: {name}')
            continue

        project_id = result[0]['id']
        count += 1

        # Create task rows
        # If aired: ALL tasks = complete
        # If not aired: ALL tasks = not_started
        status = 'complete' if is_aired else 'not_started'
        tasks = [{'project_id': project_id, 'task_key': tk, 'status': status} for tk in WORKFLOW_TASKS]

        resp = requests.post(
            f'{SUPABASE_URL}/rest/v1/project_tasks',
            headers=HEADERS,
            json=tasks,
        )
        if not resp.ok:
            print(f'  ERROR inserting tasks for {name}: {resp.status_code} {resp.text}')
        else:
            label = 'AIRED - all complete' if is_aired else 'not aired'
            print(f'  S{season_val}E{episode_num} {name} | air_date={air_date} [{label}]')

    print(f'\nDone! Migrated {count} episodes.')


if __name__ == '__main__':
    main()
