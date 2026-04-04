#!/usr/bin/env python3
"""
Login, trigger one-shot paper agent run via API, then fetch and print the activity log.
Run locally: python3 scripts/api_trigger_agent.py
"""
import urllib.request, urllib.error, json, sys, time

BASE = 'https://api.trading-galaxy.uk'
EMAIL = 'albertjemmettwaite@gmail.com'
PASSWORD = 'ScoobyDoo2016!'
USER_ID = 'albertjemmettwaite_uggwq'

def post(path, body=None, token=None):
    data = json.dumps(body or {}).encode()
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(f'{BASE}{path}', data=data, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {'error': e.read().decode(), 'status': e.code}

def get(path, token=None):
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(f'{BASE}{path}', headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {'error': e.read().decode(), 'status': e.code}

# 1. Login
print('Logging in...')
auth = post('/auth/token', {'email': EMAIL, 'password': PASSWORD})
if 'access_token' not in auth:
    print('Login failed:', auth)
    sys.exit(1)
token = auth['access_token']
print(f'Token obtained: {token[:20]}...')

# 2. Trigger one-shot agent run
print('\nTriggering one-shot paper agent run...')
result = post(f'/users/{USER_ID}/paper/agent/run', token=token)
print('Agent run result:', json.dumps(result, indent=2))

# 3. Fetch activity log
print('\nFetching activity log...')
log = get(f'/users/{USER_ID}/paper/activity?limit=20', token=token)
if isinstance(log, list):
    entries = log
elif isinstance(log, dict):
    entries = log.get('entries', log.get('activity', log.get('log', [])))
else:
    entries = []

print(f'\n--- Last {len(entries)} activity entries ---')
for e in entries:
    ts = str(e.get('created_at', e.get('timestamp', '')))[:19]
    etype = e.get('event_type', e.get('type', ''))
    ticker = e.get('ticker', '') or ''
    detail = e.get('detail', e.get('message', e.get('reasoning', '')))
    print(f"[{ts}] {etype:12s} {ticker:8s} {detail}")
