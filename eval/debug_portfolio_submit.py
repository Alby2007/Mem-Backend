"""eval/debug_portfolio_submit.py — verify portfolio submission and retrieval end-to-end."""
import json, os, urllib.request, urllib.error, uuid

BASE = "http://127.0.0.1:5050"
BETA = os.environ.get("BETA_PASSWORD", "")
DEV_KEY = os.environ.get("DEV_UPGRADE_KEY", "")

run_id = uuid.uuid4().hex[:8]
user_id = f"debug_{run_id}"
email = f"{user_id}@eval.local"
password = "Ev@lH4rness!"

def post(path, body, headers=None):
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=h)
    try:
        r = urllib.request.urlopen(req, timeout=15)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def get(path, headers=None):
    h = {}
    if headers:
        h.update(headers)
    req = urllib.request.Request(BASE + path, headers=h)
    try:
        r = urllib.request.urlopen(req, timeout=15)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

print(f"[1] Register {user_id}")
s, d = post("/auth/register", {"user_id": user_id, "email": email, "password": password, "beta_password": BETA})
print(f"  {s}: {d}")

print("[2] Login")
s, d = post("/auth/token", {"email": email, "password": password})
token = d.get("access_token", "")
print(f"  {s}: token_ok={bool(token)}")
auth = {"Authorization": f"Bearer {token}"}

print("[3] Upgrade to premium")
upg_h = dict(auth)
if DEV_KEY:
    upg_h["X-Dev-Key"] = DEV_KEY
s, d = post("/dev/upgrade-premium", {"user_id": user_id}, headers=upg_h)
print(f"  {s}: {d}")

print("[4] Submit portfolio")
holdings = [
    {"ticker": "AAPL", "quantity": 10, "avg_cost": 150.0},
    {"ticker": "TSLA", "quantity": 5, "avg_cost": 200.0},
    {"ticker": "BP.L", "quantity": 100, "avg_cost": 420.0},
]
s, d = post(f"/users/{user_id}/portfolio", {"holdings": holdings, "cash": 1000.0, "currency": "USD"}, headers=auth)
print(f"  {s}: {d}")

print("[5] Retrieve portfolio")
s, d = get(f"/users/{user_id}/portfolio", headers=auth)
print(f"  {s}: count={d.get('count')} holdings={[h.get('ticker') for h in d.get('holdings', [])]}")

print("[6] Send chat query that requires portfolio context")
s, d = post("/chat", {"message": "Give me an overview of my holdings", "session_id": f"debug_{run_id}"}, headers=auth)
answer = d.get("answer", "") or ""
print(f"  {s}: atoms_used={d.get('atoms_used')} answer_len={len(answer)}")
print(f"  answer preview: {answer[:400]}")

# Check coverage
tickers_lower = [h["ticker"].lower().replace(".l", "") for h in holdings]
covered = [t for t in tickers_lower if t in answer.lower()]
print(f"\nCoverage: {len(covered)}/{len(tickers_lower)} tickers mentioned: {covered}")
missing = [t for t in tickers_lower if t not in answer.lower()]
if missing:
    print(f"Missing tickers: {missing}")
