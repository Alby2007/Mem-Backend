# Onboarding Smoke Test — Full End-to-End Runbook

Validates every security layer and the complete new-user flow on a clean machine.
Run this before any production deployment and after any change to `middleware/` or `api.py`.

---

## Prerequisites

- Docker and Docker Compose installed
- `jq` installed (`apt install jq` / `brew install jq`)
- No existing container using ports 5000 or 4040
- A Telegram bot token + a chat ID you can receive messages on (for step 7)

---

## 1 — Build and start from scratch

```bash
# From the repo root — forces a clean image rebuild
docker-compose down -v          # wipe any previous volumes
docker-compose up --build -d

# Wait for the API to be healthy
until curl -sf http://localhost:5000/health > /dev/null; do
  echo "waiting for API…"; sleep 2
done
echo "API is up"
```

Expected: `{"status":"ok"}` from `/health`.

---

## 2 — Register a new user

```bash
REGISTER=$(curl -sf -X POST http://localhost:5000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"smoketest","email":"smoke@example.com","password":"Sm0keT3st!"}')

echo "$REGISTER" | jq .
```

Expected HTTP 201, body:
```json
{
  "user_id":    "smoketest",
  "email":      "smoke@example.com",
  "created_at": "..."
}
```

**Security check:** repeat the same call — must return `409` (duplicate email).

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"smoketest","email":"smoke@example.com","password":"Sm0keT3st!"}' \
  | grep -q 409 && echo "PASS duplicate rejected" || echo "FAIL"
```

---

## 3 — Obtain tokens

```bash
TOKEN_RESP=$(curl -sf -X POST http://localhost:5000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"email":"smoke@example.com","password":"Sm0keT3st!"}')

echo "$TOKEN_RESP" | jq .

ACCESS=$(echo  "$TOKEN_RESP" | jq -r .access_token)
REFRESH=$(echo "$TOKEN_RESP" | jq -r .refresh_token)
USER_ID=$(echo "$TOKEN_RESP" | jq -r .user_id)
```

Expected: `access_token`, `refresh_token`, `token_type: "Bearer"`, `expires_in: 86400`.

**Security check — wrong password must return 401, not 200:**
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"email":"smoke@example.com","password":"wrongpassword"}' \
  | grep -q 401 && echo "PASS wrong password rejected" || echo "FAIL"
```

**Security check — unauthenticated request must return 401:**
```bash
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:5000/users/smoketest/portfolio \
  | grep -q 401 && echo "PASS unauthenticated rejected" || echo "FAIL"
```

---

## 4 — Verify /auth/me

```bash
curl -sf http://localhost:5000/auth/me \
  -H "Authorization: Bearer $ACCESS" | jq .
```

Expected: object with `"user_id": "smoketest"`.

---

## 5 — Submit portfolio

```bash
curl -sf -X POST http://localhost:5000/users/smoketest/portfolio \
  -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' \
  -d '{
    "holdings": [
      {"ticker":"AAPL","quantity":10,"avg_cost":175.00,"sector":"Technology"},
      {"ticker":"NVDA","quantity":5, "avg_cost":800.00,"sector":"Technology"},
      {"ticker":"JPM", "quantity":8, "avg_cost":195.00,"sector":"Financials"}
    ]
  }' | jq .
```

Expected HTTP 201, body includes `"count": 3` and a `model` object.

**Security check — another user cannot read this portfolio:**
```bash
# Register a second user
curl -sf -X POST http://localhost:5000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"attacker","email":"attacker@example.com","password":"Att4ck3r!"}' > /dev/null

ATTACKER_TOKEN=$(curl -sf -X POST http://localhost:5000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"email":"attacker@example.com","password":"Att4ck3r!"}' | jq -r .access_token)

curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:5000/users/smoketest/portfolio \
  -H "Authorization: Bearer $ATTACKER_TOKEN" \
  | grep -q 403 && echo "PASS horizontal escalation blocked" || echo "FAIL"
```

---

## 6 — Configure tip delivery

```bash
curl -sf -X POST http://localhost:5000/users/smoketest/tip-config \
  -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' \
  -d '{
    "tip_delivery_time":     "08:30",
    "tip_delivery_timezone": "Europe/London",
    "account_size":          10000.0,
    "max_risk_per_trade_pct": 1.5,
    "account_currency":      "GBP",
    "tier":                  "basic"
  }' | jq .
```

Expected: updated preferences row.

**Validation check — bad time format must return 400:**
```bash
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:5000/users/smoketest/tip-config \
  -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' \
  -d '{"tip_delivery_time":"not-a-time"}' \
  | grep -q 400 && echo "PASS invalid input rejected" || echo "FAIL"
```

---

## 7 — Complete onboarding preferences

```bash
# Replace 123456789 with your real Telegram chat ID
curl -sf -X POST http://localhost:5000/users/smoketest/onboarding \
  -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' \
  -d '{
    "selected_sectors":  ["technology","financials"],
    "risk_tolerance":    "moderate",
    "delivery_time":     "08:30",
    "timezone":          "Europe/London",
    "telegram_chat_id":  "123456789"
  }' | jq .
```

---

## 8 — Verify Telegram connection

```bash
curl -sf -X POST http://localhost:5000/users/smoketest/telegram/verify \
  -H "Authorization: Bearer $ACCESS" | jq .
```

Expected: `{"sent": true}`.
Check your Telegram — you should receive a test message.
If `sent` is `false`, verify `TELEGRAM_BOT_TOKEN` is set in your `.env`.

---

## 9 — Check onboarding status

```bash
curl -sf http://localhost:5000/users/smoketest/onboarding-status \
  -H "Authorization: Bearer $ACCESS" | jq .
```

Expected — all five flags `true` after completing steps 5–7:
```json
{
  "onboarding_complete": true,
  "portfolio_submitted":  true,
  "telegram_connected":   true,
  "tip_config_set":       true,
  "account_size_set":     true,
  "preferences_set":      true,
  "complete":             true
}
```

---

## 10 — Refresh token rotation

```bash
REFRESH_RESP=$(curl -sf -X POST http://localhost:5000/auth/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\":\"$REFRESH\"}")

echo "$REFRESH_RESP" | jq .

NEW_ACCESS=$(echo  "$REFRESH_RESP" | jq -r .access_token)
NEW_REFRESH=$(echo "$REFRESH_RESP" | jq -r .refresh_token)
```

Expected: fresh `access_token` and `refresh_token` (different values from before).

**Security check — old refresh token must be revoked:**
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5000/auth/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\":\"$REFRESH\"}" \
  | grep -q 401 && echo "PASS old refresh token revoked" || echo "FAIL"
```

---

## 11 — Rate limit check

```bash
# Auth endpoints are limited to 10 req/min — fire 12 rapid requests
for i in $(seq 1 12); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5000/auth/token \
    -H 'Content-Type: application/json' \
    -d '{"email":"x@x.com","password":"wrong"}')
  echo "request $i → $CODE"
done
# At least one of the later requests should return 429
```

---

## 12 — Logout

```bash
curl -sf -X POST http://localhost:5000/auth/logout \
  -H "Authorization: Bearer $NEW_ACCESS" \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\":\"$NEW_REFRESH\"}" | jq .
```

Expected: `{"logged_out": true}`.

**Security check — new refresh token must be revoked after logout:**
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5000/auth/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\":\"$NEW_REFRESH\"}" \
  | grep -q 401 && echo "PASS logged-out refresh token rejected" || echo "FAIL"
```

---

## 13 — Security headers

```bash
curl -sI http://localhost:5000/health | grep -E \
  'X-Content-Type-Options|X-Frame-Options|X-XSS-Protection|Referrer-Policy|Content-Security-Policy'
```

Expected — all five headers present:
```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Referrer-Policy: strict-origin-when-cross-origin
Content-Security-Policy: default-src 'none'
```

---

## 14 — Chat with KB retrieval

Verifies that the chat endpoint is reachable, calls the KB retrieval pipeline, and returns a grounded response. **Without this step, a broken KB retrieval path would not be caught by steps 1–13.**

```bash
# Uses NEW_ACCESS from step 10 (post-rotation token)
CHAT_RESP=$(curl -sf -X POST http://localhost:5050/chat \
  -H "Authorization: Bearer $NEW_ACCESS" \
  -H 'Content-Type: application/json' \
  -d '{"message": "What is the signal on NVDA?", "session_id": "smoketest-chat-1"}')

echo "$CHAT_RESP" | jq .
```

**Check 1 — KB atoms were retrieved (not a zero-atom response):**
```bash
ATOMS_USED=$(echo "$CHAT_RESP" | jq '.kb_atoms_used // .atoms | length')
[ "$ATOMS_USED" -gt 0 ] \
  && echo "PASS KB retrieval working ($ATOMS_USED atoms)" \
  || echo "FAIL — zero atoms returned, KB retrieval may be broken"
```

**Check 2 — Response contains a snippet (KB context was built):**
```bash
echo "$CHAT_RESP" | jq -e '.snippet // .response' > /dev/null \
  && echo "PASS snippet/response field present" \
  || echo "FAIL — no snippet or response field"
```

**Check 3 — Stress score present (epistemic pipeline ran):**
```bash
echo "$CHAT_RESP" | jq -e '.stress.composite_stress' > /dev/null \
  && echo "PASS epistemic stress computed" \
  || echo "FAIL — stress field missing"
```

**Check 4 — Unauthenticated chat is rejected:**
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:5050/chat \
  -H 'Content-Type: application/json' \
  -d '{"message": "What is NVDA?"}' \
  | grep -q 401 && echo "PASS unauthenticated chat rejected" || echo "FAIL"
```

**Check 5 — Ingest status shows adapters running:**
```bash
STATUS=$(curl -sf http://localhost:5050/ingest/status \
  -H "Authorization: Bearer $NEW_ACCESS")
echo "$STATUS" | jq '.scheduler'
echo "$STATUS" | jq '.adapters | keys'
# Expect scheduler="running", adapters list includes yfinance, fred, edgar, rss_news etc.
```

> **Note on port:** The smoke test uses port `5050` throughout (gunicorn production port).  
> If testing locally without Docker + Caddy, Flask dev server uses `5050` by default.  
> The Caddy reverse proxy forwards `443` → `5050` in production — test against the  
> HTTPS URL (`https://api.tradinggalaxy.dev/chat`) after the Hetzner deployment is live.

---

## Pass criteria

All checks marked **PASS** in steps 2–14.  
`/health` returns `{"status": "ok", "facts": <non-zero>}`.  
No `500` responses anywhere in the flow.  
Telegram test message received (step 8).  
Step 14 KB retrieval returns ≥ 1 atom and a stress score.

---

## Teardown

```bash
docker compose down -v
```
