# Session Handoff — Trading Galaxy Build Session
## 9-10 March 2026

---

## Quick Start for Next Session

Tell Claude: "Read `docs/SESSION_HANDOFF_20260310.md` in the trading-galaxy repo to get context."

### Available Tools (confirmed working)
- `filesystem:` — 9 tools, full read/write access to LLTM, trading-galaxy, pltm-mcp
- `Claude in Chrome:` — browser automation for live site testing
- `codemap:` and `pltm-memory:` — **NOT YET WORKING** in conversations (servers run but tools not loaded by Claude Desktop — see MCP Debug section below)

---

## Production System State

| Metric | Value |
|---|---|
| API | `api.trading-galaxy.uk` |
| Frontend | `trading-galaxy.uk` (Cloudflare Pages, project: `mem-backend2`, publish dir: `static/`) |
| Server | OCI `132.145.33.75` |
| Deploy | `deploy.ps1` or manual `git pull + systemctl restart` (GitHub auto-deploy broken) |
| Production DB | `/opt/trading-galaxy/data/trading_knowledge.db` (NOT `trading_galaxy.db`) |
| KB facts | 14,926 |
| Subjects | 1,848 |
| Predicates | 637 |
| Open patterns | 93,489 across 167 tickers |
| Adapters | 22 registered, cycling autonomously |
| Tip scheduler | Running |
| Delivery scheduler | Running |
| Position monitor | Running |
| KB stress | 0.085 (healthy) |
| LLM | Groq `meta-llama/llama-4-scout-17b-16e-instruct` (primary), Ollama `llama3.2` (fallback) |
| Extraction queue | ~15k pending, draining at 600/hr |
| Backtester | Snapshot 1 stored (68 tickers, no high conviction yet). Next forward-looking test in 4 weeks. |

---

## Codebase Map (key files)

### Core
| File | Purpose |
|---|---|
| `api_v2.py` | FastAPI app factory, lifespan with all 22 adapter registrations + scheduler startup |
| `extensions.py` | Shared globals, feature flags, LLM wiring. **BUG: imports Flask g/jsonify but app is FastAPI** |
| `retrieval.py` | 6-strategy retrieval engine (FTS, ticker match, signal atoms, graph traversal, keyword boost, historical state match) |
| `services/chat_pipeline.py` | KB-grounded chat: retrieval → prompt → Groq LLM → response with grounding + precedent + epistemic footer |

### Analytics
| File | Purpose |
|---|---|
| `analytics/state_matcher.py` | Historical State Matching: fuzzy 6-feature similarity + temporal decay vs signal_calibration |
| `analytics/signal_calibration.py` | Collective hit rates per (ticker, pattern_type, timeframe, regime). **BUG: T1 hit rate undercounts — hit_t2/t3 not counted toward T1** |
| `analytics/historical_calibration.py` | Sliding-window backtester. **NOTE: uses yf.download() which is rate-limited on OCI** |
| `analytics/position_monitor.py` | Background position watcher, auto-resolve abandoned positions |
| `analytics/position_calculator.py` | Target/stop/position sizing computation |
| `analytics/pattern_detector.py` | SMC pattern detection (FVG, IFVG, OB, breaker, etc.) |

### Ingest (22 adapters)
| File | Purpose |
|---|---|
| `ingest/signal_enrichment_adapter.py` | Conviction tiers with Bayesian calibration (log-ratio, dynamic baseline, prior smoothing) |
| `ingest/pattern_adapter.py` | SMC pattern scanner across 167 tickers |
| `ingest/yfinance_adapter.py` | Price + fundamentals + OHLCV cache via Yahoo chart API |
| `ingest/rss_adapter.py` | RSS feeds. **BUG: Reuters feed DNS dead from OCI, wastes thread slot** |
| `ingest/llm_extraction_adapter.py` | Drains extraction queue via Groq |
| `ingest/gpr_adapter.py` | Fed geopolitical risk index (no key) |
| `ingest/alpha_vantage_adapter.py` | AI news sentiment (needs ALPHA_VANTAGE_API_KEY) |
| `ingest/polymarket_adapter.py` | Prediction market odds (no key) |
| `ingest/gdelt_adapter.py` | Geopolitical tension + 120-term conflict lexicon patch |
| `ingest/acled_adapter.py` | Conflict events. **WARNING: free tier is non-commercial only** |

### Notifications
| File | Purpose |
|---|---|
| `notifications/premarket_briefing.py` | Monday briefing with YOUR WEEK + KB PERFORMANCE sections |
| `notifications/tip_scheduler.py` | User delivery-time-aware tip delivery via Telegram |
| `notifications/delivery_scheduler.py` | Timezone-aware snapshot delivery |
| `notifications/telegram_notifier.py` | Telegram Bot API wrapper with retry |

### Frontend
| File | Purpose |
|---|---|
| `static/login/js/screens/chat.js` | Chat UI: KB grounding card, precedent card, feedback widget, starburst spinner |
| `static/login/js/screens/journal.js` | Trade journal: open/closed/stats tabs, partial close modal |
| `static/login/js/screens/patterns.js` | Pattern browser + modal with TradingView iframe, zone bar, KB evidence |
| `static/login/js/screens/dashboard.js` | Dashboard with top conviction, regime, active signals, briefing countdown |
| `static/login/js/screens/visualiser.js` | D3 bubble map, sector heatmap, signal radar |
| `static/login/js/screens/tips.js` | Tip delivery config + preview |
| `static/login/css/app.css` | All styles |

---

## Audit Findings (30 items)

### CRITICAL
1. **`extensions.py` Flask imports** — `from flask import g, jsonify` used in `require_feature()` but app is FastAPI. Will crash if called from v2 routes.
2. **`extensions.py` double-imports** — Module-level adapter imports include dead code (`short_interest_adapter`, `options_adapter`). If any fail, `HAS_INGEST=False` breaks everything.
3. **ACLED commercial license** — Free tier is research-only. Need commercial license before paid launch.
4. **GPR idempotency** — `_last_period` resets on restart (instance state), re-writes same month's data.

### MEDIUM (incorrect behaviour)
5. GROQ_MODEL default mismatch (code: `llama-3.3-70b-versatile`, env: `llama-4-scout`)
6. INGEST_BATCH_SIZE vs LLM_EXTRACTION_BATCH env var name mismatch
7. Reuters RSS DNS dead — still in feed list
8. TradingView `.L` → `LSE:` mapping doesn't handle other exchanges
9. `extractKbGrounding()` regex doesn't match markdown-wrapped `**[KB_GROUNDING]**`
10. `state_matcher._ensure_table()` runs on every chat query
11. **`update_calibration()` T1 hit rate undercounting** — `hit_t2`/`hit_t3` not counted as T1 hits
12. `_fetch_chart_candles` returns string `'429'` (type violation)
13. `historical_calibration.py` uses rate-limited `yf.download()` on OCI

### LOW
14-22. tmp files cleanup, page title stuck, chat-enhanced.css not linked, adapters missing db_path, Polymarket dates hardcoded, dead imports, stub adapters in base.py
23. T1 hit rate undercounting (duplicate of #11 — found in second pass)
24-30. CSP header gaps, ALTER TABLE overhead, missing DB index, no calibrate endpoint rate limit

---

## Features Shipped This Session

1. Fixed `/tmp/inspect.py` killing 15 adapters
2. 6 cascading pattern adapter bugs
3. LLM extraction phi3 → Groq Llama 4 Scout
4. SQLite WAL + busy_timeout across all adapters
5. 5 new data adapters (GPR, Alpha Vantage, Polymarket, GDELT lexicon, ACLED fix)
6. Chat feedback widget (Taking it / More / Pass)
7. Chat UI redesign (starburst spinner, arrow send button)
8. P0-P6: full backtester + journal + calibration-adjusted conviction with Bayesian smoothing
9. Historical State Matching (fuzzy similarity + temporal decay + precedent cards)
10. All 3 notification schedulers started for first time
11. Backtester snapshot 1 stored (68 tickers)
12. Signal enrichment `kg_db_path` scoping bug fixed

---

## Outstanding Backlog (priority order)

| Priority | Item | Effort |
|---|---|---|
| HIGH | Fix #11: T1 hit rate undercounting in `update_calibration()` | 10 min |
| HIGH | Fix #1: Replace Flask `g`/`jsonify` in `extensions.py` with FastAPI | 30 min |
| HIGH | Remove Reuters RSS from feed list | 5 min |
| MED | Build Pending Tips Queue (specced in chat, not yet built) | 3-4 hours |
| MED | Link `chat-enhanced.css` in `index.html` | 5 min |
| MED | Fix `extractKbGrounding()` regex for markdown-wrapped blocks | 15 min |
| MED | Onboard interns as real users | — |
| MED | Link Telegram + submit portfolio to test full briefing loop | — |
| LOW | Clean 27 `tmp_*.py` + 100+ `scripts/_*.py` files | 30 min |
| LOW | Fix page title (router.js `document.title`) | 10 min |
| LOW | Second backtest in 4 weeks | — |
| LOW | Sector momentum adapter | 2 hours |
| LOW | FRED API key renewal | 5 min |

---

## Specs Produced

- `TradingGalaxy_Backtester_Journal_Spec.docx` — 12-page spec for P0-P6
- `historical-state-matching-plan.md` — full implementation spec for market memory
- Pending Tips Queue — specced in conversation (not saved as document)

---

## MCP Debug Notes

The `codemap:` and `pltm-memory:` Python MCP servers are configured correctly in `%APPDATA%\Claude\claude_desktop_config.json` and import successfully, but their tools do not appear in Claude Desktop conversations. The `filesystem:` Node.js server works fine.

### Config (confirmed correct)
```json
{
  "mcpServers": {
    "pltm-memory": {
      "command": "C:/Python314/python.exe",
      "args": ["-m", "mcp_server.pltm_server"],
      "env": {"PYTHONPATH": "C:/Users/alber/CascadeProjects/pltm-mcp"}
    },
    "codemap": {
      "command": "C:/Python314/python.exe",
      "args": ["-m", "mcp_server.codemap_server"],
      "env": {"PYTHONPATH": "C:/Users/alber/CascadeProjects/pltm-mcp"}
    },
    "filesystem": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem",
               "C:/Users/alber/CascadeProjects/LLTM",
               "C:/Users/alber/CascadeProjects/trading-galaxy",
               "C:/Users/alber/CascadeProjects/pltm-mcp"]
    }
  }
}
```

### What was tried
- Server imports OK: `python -c "import mcp_server.codemap_server; print('OK')"` succeeds
- Logs show servers connecting and serving tools/list with all 12 tools
- Added `NotificationOptions(tools_changed=True)` to both Python servers
- Removed `enabledMcpTools` from preferences
- Multiple Claude Desktop restarts + new conversations
- Tools appear in logs under `[codemap]` namespace but not in conversation tool lists

### Likely cause
Claude Desktop may require a specific MCP SDK version or capability handshake that the Python servers aren't providing correctly. The Node.js filesystem server works because it uses the official `@modelcontextprotocol/server-filesystem` package which handles the handshake natively.

### Workaround
The filesystem tools provide full read/write access to the codebase. The codemap functionality can be replicated by reading files directly — slower but functional.
