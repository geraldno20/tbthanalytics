import json
from datetime import datetime

with open('data/episodes.json') as f:
    data = json.load(f)
eps = data['episodes']

seasons = set(e['season'] for e in eps)
print('Seasons:', sorted(seasons))

for e in eps[:3]:
    print(f"S{e['season']}E{e['episode']} - {e['guest']} | release: {e['episode_release']}")

today = datetime.now()
aired = 0
not_aired = 0
skip_seasons = ('Agreed', 'Declined', 'No', 'Nonsense', '??', 'Done', '0', '-', '', '#REF!')

for e in eps:
    s = e['season']
    if s in skip_seasons:
        continue
    try:
        int(s)
    except ValueError:
        continue
    rd = e.get('episode_release', '')
    if rd and rd not in ('-', ''):
        try:
            d = datetime.strptime(rd, '%m/%d/%y')
            if d <= today:
                aired += 1
            else:
                not_aired += 1
        except ValueError:
            not_aired += 1
    else:
        not_aired += 1

print(f'\nAired (release date passed): {aired}')
print(f'Not yet aired / no date: {not_aired}')
print(f'Total numeric-season episodes: {aired + not_aired}')
