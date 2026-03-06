"""Fire a single macro query against the local server and print the response + snippet."""
import requests, os, json

BASE = 'http://127.0.0.1:5050'
PASSWORD = os.environ.get('BETA_PASSWORD', 'eval-local-only')
JWT_SECRET = os.environ.get('JWT_SECRET_KEY', 'eval-local-dev-key')

import uuid
uid = f"debug_{uuid.uuid4().hex[:8]}@eval.local"
user_id = f"debug_{uuid.uuid4().hex[:8]}"

# Register
r = requests.post(f'{BASE}/auth/register', json={
    'user_id': user_id, 'email': uid, 'password': 'Debug1234!',
    'beta_password': PASSWORD,
}, timeout=15)
print(f"register: {r.status_code}")

# Get token
r2 = requests.post(f'{BASE}/auth/token', json={
    'email': uid, 'password': 'Debug1234!'
}, timeout=15)
token = r2.json().get('access_token', '')
print(f"token: {'yes' if token else 'NONE'}")

headers = {'Authorization': f'Bearer {token}'} if token else {}

queries = [
    "What's the current market regime?",
    "What's the Fed stance right now?",
    "What does the yield curve say about the market?",
]

for q in queries:
    print(f"\n{'='*60}")
    print(f"QUERY: {q}")
    r3 = requests.post(f'{BASE}/chat', json={'message': q}, headers=headers, timeout=60)
    data = r3.json()
    answer = data.get('answer', data.get('response', ''))
    snippet = data.get('snippet', '')
    atom_count = data.get('atom_count', '?')
    print(f"atom_count: {atom_count}")
    print(f"snippet (first 400 chars):\n{snippet[:400]}")
    print(f"answer:\n{answer[:400]}")
