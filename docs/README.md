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
| API | `https://api.trading-galaxy.uk` (OCI, `132.145.33.75`) |
| App | `https://trading-galaxy.uk` (Cloudflare Pages `mem-backend2`) |

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

# App frontend (Cloudflare Pages)
npx wrangler pages deploy static --project-name mem-backend2 --branch master --commit-dirty=true
```

```powershell
# One-shot (backend + frontend)
.\deploy\deploy.ps1
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
| `GROQ_API_KEY` | — | Groq LLM API (preferred over Ollama when set; faster, free tier) |

---

## Ingest Schedule

| Adapter | Interval | Data |
|---|---|---|
| `YFinanceAdapter` | 5 min | Prices, fundamentals, analyst targets, ETF data |
| `OptionsAdapter` | 15 min | Options chains, IV rank, put/call ratio, smart money signals |
| `RSSAdapter` | 15 min | Financial news headlines (FT, CNBC, BBC, Investing.com, MarketWatch) |
| `SignalEnrichmentAdapter` | 30 min | Derived signals: conviction tier, momentum, position size |
| `EDGARRealtimeAdapter` | 30 min | 8-K real-time filings |
| `PatternAdapter` | 60 min | Chart pattern detection |
| `LLMExtractionAdapter` | 60 min | LLM-extracted signals from RSS headlines |
| `LSEFlowAdapter` | 60 min | Institutional order flow for LSE equities |
| `EarningsCalendarAdapter` | 60 min | Earnings proximity risk, implied move |
| `GDELTAdapter` | 12 hours | Geopolitical tension tone scores (country pairs) |
| `UCDPAdapter` | 12 hours | Country conflict intensity (GDELT artlist proxy) |
| `EDGARAdapter` | 6 hours | SEC filings: 8-K, 10-Q, 10-K, Form 4 insider transactions |
| `BoEAdapter` | 24 hours | Bank of England macro indicators |
| `FREDAdapter` | 24 hours | Fed funds rate, CPI, GDP, unemployment, yield curve, HY spread |
| `FCAShortInterestAdapter` | 24 hours | FCA short position disclosures for UK equities |

---

## Stress Score Interpretation

Returned on every `POST /retrieve` response.

| `composite_stress` | Meaning |
|---|---|
| < 0.15 | Low — context is fresh, coherent, authoritative |
| 0.15–0.35 | Moderate — minor age or authority mix |
| 0.35–0.60 | Elevated — notable conflicts or decay; flag to user |
| > 0.60 | High — stale or conflicted; refresh recommended |
