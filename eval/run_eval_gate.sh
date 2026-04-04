#!/usr/bin/env bash
# eval/run_eval_gate.sh — runs on OCI, tests registration then full eval
set -euo pipefail

BASE="http://127.0.0.1:5050"
export DEV_UPGRADE_KEY="eval-gate-key"
export BETA_PASSWORD="ScoobyDoo2016!"
export EVAL_MODE=1

echo "[1/3] Health check..."
curl -sf "$BASE/health" | python3 -m json.tool
echo ""

echo "[2/3] Smoke-test registration (should not be rate-limited from localhost)..."
python3 - <<'PYEOF'
import urllib.request, json, sys

base = "http://127.0.0.1:5050"

def post(path, body):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        r = urllib.request.urlopen(req, timeout=15)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

import os
beta_pw = os.environ.get("BETA_PASSWORD", "")
status, data = post("/auth/register", {
    "user_id": "eval_smoke_test_001",
    "email": "eval_smoke_test_001@eval.local",
    "password": "Ev@lH4rness!",
    "beta_password": beta_pw,
})
print(f"  register: status={status} data={data}")
if status not in (200, 201, 409):
    print(f"  FAIL: unexpected status {status}")
    sys.exit(1)

status2, data2 = post("/auth/token", {
    "email": "eval_smoke_test_001@eval.local",
    "password": "Ev@lH4rness!",
})
token = data2.get("access_token", "")
print(f"  token: status={status2} token_ok={bool(token)}")
if not token:
    print("  FAIL: no token")
    sys.exit(1)

# Test upgrade
import os
dev_key = os.environ.get("DEV_UPGRADE_KEY", "")
req3 = urllib.request.Request(
    base + "/dev/upgrade-premium",
    data=json.dumps({"user_id": "eval_smoke_test_001"}).encode(),
    headers={"Content-Type": "application/json",
             "Authorization": f"Bearer {token}",
             "X-Dev-Key": dev_key},
)
try:
    r3 = urllib.request.urlopen(req3, timeout=10)
    print(f"  upgrade: status={r3.status} ok")
except urllib.error.HTTPError as e:
    print(f"  upgrade: status={e.code} body={e.read()[:200]}")

print("  Smoke test PASSED")
PYEOF

echo ""
echo "[3/3] Running full eval harness (50 portfolios x 1 query/intent = 350 requests)..."
cd /home/ubuntu/trading-galaxy
DEV_UPGRADE_KEY="eval-gate-key" .venv/bin/python eval/eval_harness.py --workers 4 --n 50 --qpi 1 --beta-password "$BETA_PASSWORD"
