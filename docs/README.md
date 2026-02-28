# Documentation

Reference documentation for the Trading Galaxy knowledge base.

---

## Documents

| File | Description |
|---|---|
| [architecture.md](architecture.md) | System architecture, component diagram, data flow, persistence model, deployment |
| [codemap.md](codemap.md) | File-by-file code reference — every module, class, function, and constant |
| [api.md](api.md) | REST API reference — all endpoints with request/response examples |
| [auth.md](auth.md) | JWT authentication — token lifecycle, endpoints, storage recommendations |
| [data-model.md](data-model.md) | Atom schema, SQL schema, predicate vocabulary, source naming, confidence guidelines |
| [ingest.md](ingest.md) | Ingest pipeline deep-dive — all adapters, atoms produced, rate limiting, extending |
| [retrieval.md](retrieval.md) | Retrieval engine strategies, keyword boost map, output formatting, extending |
| [epistemic.md](epistemic.md) | Authority weights, decay half-lives, contradiction detection, stress signals, working state |
| [smoke-test-onboarding.md](smoke-test-onboarding.md) | End-to-end security and flow verification runbook |
| [frontend.md](frontend.md) | Frontend SPA — design system, screens, state, API layer, auth, CSP, local storage |

---

## Quick Reference

### Production URLs

| Service | URL |
|---|---|
| API | `https://api.trading-galaxy.uk` |
| App | `https://app.trading-galaxy.uk` |
| Landing | `https://trading-galaxy.uk` |

### Start the server locally
```powershell
$env:FRED_API_KEY="your_key_here"
python api.py
```

### Check API health
```bash
# Local
curl http://localhost:5050/health

# Production
curl https://api.trading-galaxy.uk/health
```

### Check ingest health
```
GET https://api.trading-galaxy.uk/ingest/status
```

### Query the KB
```bash
# Smart retrieval (main endpoint)
curl -X POST https://api.trading-galaxy.uk/retrieve \
  -H "Content-Type: application/json" \
  -d '{"message": "NVDA META GOOGL upside price target"}'

# Direct triple query
curl "https://api.trading-galaxy.uk/query?subject=NVDA&predicate=price_target"

# All facts for a ticker
curl https://api.trading-galaxy.uk/context/NVDA

# KB statistics
curl https://api.trading-galaxy.uk/stats
```

### Push a custom atom
```bash
curl -X POST https://api.trading-galaxy.uk/ingest \
  -H "Content-Type: application/json" \
  -d '{"subject":"AAPL","predicate":"signal_direction","object":"long","confidence":0.85,"source":"model_signal_my_model"}'
```

### Deploy to production
```bash
# Backend (OCI)
ssh -i <key> ubuntu@132.145.33.75 "bash ~/trading-galaxy/deploy/oci-update.sh"

# App frontend (Netlify)
netlify deploy --dir static --prod

# Landing page (Netlify)
cd landing && netlify deploy --dir . --prod
```

---

## Environment Variables

| Variable | Default | Required for |
|---|---|---|
| `FRED_API_KEY` | — | FRED macro data adapter |
| `EDGAR_USER_AGENT` | `trading-galaxy-kb research@example.com` | SEC EDGAR fair-use |
| `TRADING_KB_DB` | `trading_knowledge.db` | Custom DB path |
| `PORT` | `5050` | Custom port |
| `JWT_SECRET_KEY` | insecure dev default | **Required in production** — sign JWTs |
| `TELEGRAM_BOT_TOKEN` | — | Daily briefing delivery + waitlist Telegram pings |
| `WAITLIST_TELEGRAM_CHAT_ID` | — | Personal chat ID for waitlist signup notifications |

---

## Ingest Schedule

| Adapter | Interval | Data |
|---|---|---|
| `YFinanceAdapter` | 5 min | Prices, fundamentals, analyst targets, ETF data (50 tickers) |
| `RSSAdapter` | 15 min | Financial news headlines (FT, CNBC, BBC, Investing.com, MarketWatch) |
| `EDGARAdapter` | 6 hours | SEC filings: 8-K, 10-Q, 10-K, Form 4 insider transactions |
| `FREDAdapter` | 24 hours | Fed funds rate, CPI, GDP, unemployment, yield curve, HY spread |

---

## Stress Score Interpretation

Returned on every `POST /retrieve` response.

| `composite_stress` | Meaning |
|---|---|
| < 0.15 | Low — context is fresh, coherent, authoritative |
| 0.15–0.35 | Moderate — minor age or authority mix |
| 0.35–0.60 | Elevated — notable conflicts or decay; flag to user |
| > 0.60 | High — stale or conflicted; refresh recommended |
