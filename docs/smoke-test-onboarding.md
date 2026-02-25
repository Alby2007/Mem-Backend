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

## Pass criteria

All checks marked **PASS** in steps 2–13.  
`/health/detailed` returns `"status": "ok"`.  
No `500` responses anywhere in the flow.  
Telegram test message received (step 8).

---

## Teardown

```bash
docker-compose down -v
```
