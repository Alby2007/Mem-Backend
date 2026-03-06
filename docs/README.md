# Documentation

Reference documentation for the Trading Galaxy platform.

---

## Documents

| File | Description |
|---|---|
| [architecture.md](architecture.md) | System architecture, component diagram, data flow, paper trader, deployment |
| [codemap.md](codemap.md) | File-by-file code reference — every module, class, function, and constant |
| [api.md](api.md) | REST API reference — all endpoints with request/response examples |
| [auth.md](auth.md) | Authentication — HttpOnly cookie flow, token lifecycle, session restore |
| [data-model.md](data-model.md) | Atom schema, SQL schema, paper trading tables, predicate vocabulary |
| [ingest.md](ingest.md) | Ingest pipeline deep-dive — all adapters, atoms produced, rate limiting, extending |
| [retrieval.md](retrieval.md) | Retrieval engine strategies, keyword boost map, output formatting, extending |
| [epistemic.md](epistemic.md) | Authority weights, decay half-lives, contradiction detection, stress signals, working state |
| [smoke-test-onboarding.md](smoke-test-onboarding.md) | End-to-end security and flow verification runbook |
| [frontend.md](frontend.md) | Frontend SPAs — design system, screens, state, API layer, auth, session restore |

---

## Quick Reference

### Production URLs

| Service | URL |
|---|---|
| API | `https://api.trading-galaxy.uk` (OCI, `132.145.33.75`) |
| App | `https://trading-galaxy.uk` (Cloudflare Pages `mem-backend2`, publish dir `static/`) |

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

> **GitHub auto-deploy is broken** — always deploy manually.

```powershell
# One-shot: push to GitHub + deploy backend to OCI + deploy frontend to Cloudflare Pages
.\deploy\deploy.ps1
```

```bash
# Backend only (OCI)
ssh -i <key> ubuntu@132.145.33.75 \
  "cd /home/ubuntu/trading-galaxy && git pull origin master && sudo systemctl restart trading-galaxy"

# Frontend only (Cloudflare Pages)
npx wrangler pages deploy static --project-name mem-backend2 --branch master --commit-dirty=true
```

---

## Environment Variables

| Variable | Default | Required for |
|---|---|---|
| `FRED_API_KEY` | — | FRED macro data adapter |
| `EDGAR_USER_AGENT` | `trading-galaxy-kb research@example.com` | SEC EDGAR fair-use header |
| `TRADING_KB_DB` | `trading_knowledge.db` | Custom DB path (production: `/opt/trading-galaxy/data/trading_knowledge.db`) |
| `PORT` | `5050` | Custom port |
| `JWT_SECRET_KEY` | insecure dev default | **Required in production** — signs JWTs |
| `TELEGRAM_BOT_TOKEN` | — | Daily briefing delivery + waitlist Telegram pings |
| `WAITLIST_TELEGRAM_CHAT_ID` | — | Personal chat ID for waitlist signup notifications |
| `GROQ_API_KEY` | — | Groq LLM API — preferred over Ollama when set (faster, free tier) |
| `POLYGON_API_KEY` | — | Polygon.io — enables `PolygonOptionsAdapter` (Greeks/GEX) and `YieldCurveAdapter` |

---

## Ingest Schedule

| Adapter | Interval | Data |
|---|---|---|
| `EDGARRealtimeAdapter` | 3 min | 8-K real-time filings via EDGAR full-text search |
| `YFinanceAdapter` | 5 min | Prices, fundamentals, analyst targets, ETF data |
| `SignalEnrichmentAdapter` | 5 min | Derived signals: conviction tier, momentum, position size |
| `LLMExtractionAdapter` | 5 min | LLM-extracted signals from RSS headlines |
| `RSSAdapter` | 15 min | Financial news headlines (FT, CNBC, BBC, Investing.com, MarketWatch) |
| `OptionsAdapter` | 30 min | Yahoo Finance options chains, IV rank, put/call ratio |
| `PolygonOptionsAdapter` | 30 min | Real Greeks, IV, GEX from Polygon (requires `POLYGON_API_KEY`) |
| `PaperAgentAdapter` | **30 min** | Autonomous paper trading agent — scans patterns, opens/monitors positions |
| `GDELTAdapter` | 60 min | Geopolitical tension tone scores (country pairs) |
| `USGSAdapter` | 60 min | Significant earthquakes near key regions |
| `InsiderAdapter` | 60 min | SEC Form 4 insider transactions |
| `SectorRotationAdapter` | 60 min | Sector ETF relative performance regime |
| `PatternAdapter` | 60 min | Chart pattern detection over rolling windows |
| `LSEFlowAdapter` | 60 min | Institutional order flow for LSE equities |
| `EarningsCalendarAdapter` | 60 min | Earnings proximity risk, implied move |
| `ACLEDAdapter` | 6 hours | Protest/unrest intensity (GDELT artlist proxy) |
| `EDGARAdapter` | 6 hours | SEC filings: 8-K, 10-Q, 10-K, Form 4 |
| `FREDAdapter` | 24 hours | Fed funds rate, CPI, GDP, unemployment, yield curve, HY spread |
| `BoEAdapter` | 24 hours | Bank of England macro indicators |
| `YieldCurveAdapter` | 24 hours | Yield curve regime from TLT/IEF/SHY ETFs (requires `POLYGON_API_KEY`) |
| `FINRAShortInterestAdapter` | 24 hours | US short interest from FINRA CDN (free, biweekly) |
| `FCAShortInterestAdapter` | 24 hours | FCA short position disclosures for UK equities |
| `EconomicCalendarAdapter` | 24 hours | FOMC/CPI/NFP upcoming event risk flags |
| `EIAAdapter` | 24 hours | EIA crude oil inventory and production |
| `UCDPAdapter` | 24 hours | Country conflict intensity (GDELT artlist proxy) |

---

## Paper Trader Quick Reference

The autonomous paper trading agent runs every 30 minutes for all `pro`/`premium` users via `PaperAgentAdapter`. Starting balance: £500,000 virtual GBP.

```bash
# One-shot scan (run immediately, returns result synchronously)
POST /users/{id}/paper/agent/run

# Start continuous 30-min scanner
POST /users/{id}/paper/agent/start

# Stop scanner
POST /users/{id}/paper/agent/stop

# Scanner status
GET /users/{id}/paper/agent/status

# Open paper positions
GET /users/{id}/paper/positions

# Activity log (last N entries)
GET /users/{id}/paper/log?limit=50
```

**Sizing rules (per scan):**
- Risk per trade: `1% of virtual_balance` (capped at 2% regardless of preferences)
- Max position value: `10% of virtual_balance`
- Max open positions: 12
- Max new entries per scan: 3
- 24-hour cooldown on stopped-out tickers

---

## Stress Score Interpretation

Returned on every `POST /retrieve` response.

| `composite_stress` | Meaning |
|---|---|
| < 0.15 | Low — context is fresh, coherent, authoritative |
| 0.15–0.35 | Moderate — minor age or authority mix |
| 0.35–0.60 | Elevated — notable conflicts or decay; flag to user |
| > 0.60 | High — stale or conflicted; refresh recommended |
