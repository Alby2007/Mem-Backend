#!/bin/bash
# Run on OCI: bash scripts/get_token_and_run.sh
set -e

BASE="https://api.trading-galaxy.uk"
USER_ID="albertjemmettwaite_uggwq"
EMAIL="albertjemmettwaite@gmail.com"
PASS="ScoobyDoo2016!"

echo "=== Getting token ==="
TOKEN=$(curl -s -X POST "$BASE/auth/token" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token','ERROR: '+str(d)))")

echo "Token: ${TOKEN:0:30}..."

if [[ "$TOKEN" == ERROR* ]]; then
  echo "Auth failed — trying beta password"
  PASS="ScoobyDoo2016!"
  exit 1
fi

echo ""
echo "=== Triggering one-shot agent run ==="
RESULT=$(curl -s -X POST "$BASE/users/$USER_ID/paper/agent/run" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"

echo ""
echo "=== Agent log (last 15 entries) ==="
curl -s "$BASE/users/$USER_ID/paper/agent/log" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
entries = d.get('log', [])
for e in entries[:15]:
    ts = str(e.get('created_at',''))[:19]
    etype = e.get('event_type','')
    ticker = (e.get('ticker') or '').ljust(8)
    detail = str(e.get('detail',''))[:120]
    print(f'[{ts}] {etype:12s} {ticker} {detail}')
"
