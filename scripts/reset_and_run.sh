#!/bin/bash
# Reset password for test user and trigger one-shot agent run
# Run on OCI: bash /home/ubuntu/trading-galaxy/scripts/reset_and_run.sh

BASE="https://api.trading-galaxy.uk"
USER_ID="albertjemmettwaite_uggwq"
DB="/opt/trading-galaxy/data/trading_knowledge.db"

# Generate bcrypt hash for test password "Test1234!"
NEW_PASS="Test1234!"
echo "=== Setting password to: $NEW_PASS ==="
HASH=$(python3 -c "import bcrypt; print(bcrypt.hashpw('Test1234!'.encode(), bcrypt.gensalt()).decode())")
echo "Hash: ${HASH:0:20}..."

sqlite3 "$DB" "UPDATE user_auth SET password_hash='$HASH', failed_attempts=0, locked_until=NULL WHERE user_id='$USER_ID';"
echo "Password updated in DB"

# Now login
echo ""
echo "=== Getting token ==="
TOKEN=$(curl -s -X POST "$BASE/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"email":"albertjemmettwaite@gmail.com","password":"Test1234!"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token','FAIL: '+str(d)))")
echo "Token: ${TOKEN:0:40}..."

if [[ "$TOKEN" == FAIL* ]] || [[ -z "$TOKEN" ]]; then
  echo "Login failed"
  exit 1
fi

echo ""
echo "=== Triggering one-shot agent run (may take up to 60s for Groq) ==="
RESULT=$(curl -s --max-time 120 -X POST "$BASE/users/$USER_ID/paper/agent/run" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"

echo ""
echo "=== Agent log (last 10 non-scan_start entries) ==="
curl -s "$BASE/users/$USER_ID/paper/agent/log" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
entries = [e for e in d.get('log', []) if e.get('event_type') != 'scan_start']
for e in entries[:10]:
    ts = str(e.get('created_at',''))[:19]
    etype = e.get('event_type','')
    ticker = (e.get('ticker') or '').ljust(8)
    detail = str(e.get('detail',''))[:130]
    print(f'[{ts}] {etype:12s} {ticker} {detail}')
"
