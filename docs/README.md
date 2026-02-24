# Documentation

Reference documentation for the Trading Galaxy knowledge base.

---

## Documents

| File | Description |
|---|---|
| [architecture.md](architecture.md) | System architecture, component diagram, data flow, persistence model |
| [codemap.md](codemap.md) | File-by-file code reference — every module, class, function, and constant |
| [api.md](api.md) | REST API reference — all endpoints with request/response examples |
| [data-model.md](data-model.md) | Atom schema, SQL schema, predicate vocabulary, source naming, confidence guidelines |
| [ingest.md](ingest.md) | Ingest pipeline deep-dive — all four adapters, atoms produced, rate limiting, extending |
| [retrieval.md](retrieval.md) | Retrieval engine strategies, keyword boost map, output formatting, extending |
| [epistemic.md](epistemic.md) | Authority weights, decay half-lives, contradiction detection, stress signals, working state |

---

## Quick Reference

### Start the server
```powershell
$env:FRED_API_KEY="your_key_here"
python api.py
```

### Check ingest health
```
GET http://localhost:5050/ingest/status
```

### Query the KB
```bash
# Smart retrieval (main endpoint)
curl -X POST http://localhost:5050/retrieve \
  -H "Content-Type: application/json" \
  -d '{"message": "NVDA META GOOGL upside price target"}'

# Direct triple query
curl "http://localhost:5050/query?subject=NVDA&predicate=price_target"

# All facts for a ticker
curl http://localhost:5050/context/NVDA

# KB statistics
curl http://localhost:5050/stats
```

### Push a custom atom
```bash
curl -X POST http://localhost:5050/ingest \
  -H "Content-Type: application/json" \
  -d '{"subject":"AAPL","predicate":"signal_direction","object":"long","confidence":0.85,"source":"model_signal_my_model"}'
```

---

## Environment Variables

| Variable | Default | Required for |
|---|---|---|
| `FRED_API_KEY` | — | FRED macro data adapter |
| `EDGAR_USER_AGENT` | `trading-galaxy-kb research@example.com` | SEC EDGAR fair-use |
| `TRADING_KB_DB` | `trading_knowledge.db` | Custom DB path |
| `PORT` | `5050` | Custom port |

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
