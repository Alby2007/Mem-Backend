"""routes_v2/markets.py — Phase 6: markets endpoints."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

import extensions as ext

router = APIRouter()


@router.get("/markets/chart", response_class=HTMLResponse)
async def markets_chart(sym: str = "AAPL", symbol: str = None):
    tvSym = sym if sym != "AAPL" or symbol is None else (symbol or sym)
    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{tvSym} – Trading Galaxy Chart</title>
<style>body{{margin:0;overflow:hidden}}#tv{{width:100vw;height:100vh}}</style>
</head><body>
<div id="tv"></div>
<script src="https://s3.tradingview.com/tv.js"></script>
<script>
new TradingView.widget({{
  container_id: 'tv',
  autosize: true,
  symbol: '{tvSym}',
  interval: 'D',
  timezone: 'Etc/UTC',
  theme: 'dark',
  style: '1',
  locale: 'en',
  toolbar_bg: '#0a0e17',
  enable_publishing: false,
  hide_side_toolbar: false,
  allow_symbol_change: true,
  save_image: false,
}});
</script>
</body></html>"""
    from fastapi.responses import HTMLResponse as _HR
    return _HR(
        content=html,
        headers={
            "X-Frame-Options": "ALLOWALL",
            "Content-Security-Policy": (
                "default-src 'self' https://s3.tradingview.com https://*.tradingview.com; "
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://s3.tradingview.com https://*.tradingview.com; "
                "style-src 'self' 'unsafe-inline'; "
                "img-src 'self' data: https://*.tradingview.com; "
                "frame-src 'self' https://*.tradingview.com; "
                "connect-src 'self' https://*.tradingview.com wss://*.tradingview.com"
            ),
        },
    )


@router.get("/market/snapshot")
async def market_snapshot():
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        rows = conn.execute(
            """SELECT subject, predicate, object, confidence, source, timestamp
               FROM facts
               WHERE predicate IN (
                   'conviction_tier','signal_quality','upside_pct',
                   'invalidation_distance','position_size_pct','options_regime',
                   'macro_confirmation','thesis_risk_level','market_regime',
                   'last_price','price_regime','signal_direction',
                   'volatility_regime','return_1m','return_1y',
                   'price_target','sector','put_call_ratio'
               )
               ORDER BY subject, predicate"""
        ).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    by_subject: dict = {}
    for subj, pred, obj, conf, src, ts in rows:
        if subj not in by_subject:
            by_subject[subj] = {}
        if pred not in by_subject[subj]:
            by_subject[subj][pred] = obj

    tickers = []
    macro = {}
    for subj, atoms in by_subject.items():
        if atoms.get("conviction_tier") or atoms.get("signal_quality"):
            atoms["ticker"] = subj
            tickers.append(atoms)
        elif subj in ("SPY", "TLT", "HYG", "VIX", "DXY", "GLD", "USO", "MACRO"):
            macro[subj] = atoms

    tickers.sort(key=lambda t: t.get("conviction_tier", "z"))
    return {
        "tickers": tickers,
        "count": len(tickers),
        "macro": macro,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/markets/tickers")
async def markets_tickers():
    _SECTORS = [
        {"group": "Mega-cap Tech",    "tickers": ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","AVGO"]},
        {"group": "Financials",       "tickers": ["JPM","V","MA","BAC","GS","MS","BRK-B","AXP","BLK","SCHW"]},
        {"group": "Healthcare",       "tickers": ["UNH","JNJ","LLY","ABBV","PFE","CVS","MRK","BMY","GILD"]},
        {"group": "Energy",           "tickers": ["XOM","CVX","COP"]},
        {"group": "Consumer",         "tickers": ["WMT","PG","KO","MCD","COST"]},
        {"group": "Industrials",      "tickers": ["CAT","HON","RTX"]},
        {"group": "Comms / Media",    "tickers": ["DIS","NFLX","CMCSA"]},
        {"group": "Semis / Software", "tickers": ["AMD","INTC","QCOM","MU","CRM","ADBE","NOW","SNOW"]},
        {"group": "Fintech",          "tickers": ["PYPL","COIN"]},
        {"group": "REITs",            "tickers": ["AMT","PLD","EQIX"]},
        {"group": "Utilities",        "tickers": ["NEE","DUK","SO"]},
        {"group": "ETFs — Broad",     "tickers": ["SPY","QQQ","IWM","DIA","VTI"]},
        {"group": "ETFs — Sector",    "tickers": ["XLF","XLE","XLK","XLV","XLI","XLC","XLY","XLP"]},
        {"group": "ETFs — Macro",     "tickers": ["GLD","SLV","TLT","HYG","LQD","UUP"]},
    ]
    all_default = [t for s in _SECTORS for t in s["tickers"]]
    extra = []
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        try:
            rows = conn.execute(
                "SELECT ticker FROM universe_tickers WHERE added_to_ingest=1"
            ).fetchall()
            for (t,) in rows:
                if t.upper() not in (x.upper() for x in all_default):
                    extra.append(t.upper())
        finally:
            conn.close()
    except Exception:
        pass
    result = list(_SECTORS)
    if extra:
        result.append({"group": "User-added", "tickers": extra})
    return {"sectors": result}


@router.get("/markets/overview")
async def markets_overview():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")

    result: dict = {"as_of": datetime.now(timezone.utc).isoformat()}

    try:
        summary = ext.build_portfolio_summary(ext.DB_PATH)
        result["top_conviction"] = [
            {
                "ticker":            t.get("ticker"),
                "conviction_tier":   t.get("conviction_tier"),
                "upside_pct":        t.get("upside_pct"),
                "position_size_pct": t.get("position_size_pct"),
            }
            for t in summary.get("top_conviction", [])[:3]
        ]
        result["regime"]        = summary.get("macro_regime")
        result["macro_summary"] = summary.get("macro_summary")
    except Exception:
        result["top_conviction"] = []
        result["regime"]         = None
        result["macro_summary"]  = None

    if ext.HAS_STRESS:
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=5)
            sample = conn.execute(
                "SELECT subject,predicate,object,confidence,source,timestamp "
                "FROM facts ORDER BY confidence DESC LIMIT 50"
            ).fetchall()
            conn.close()
            cols = ["subject","predicate","object","confidence","source","timestamp"]
            atoms = [dict(zip(cols, r)) for r in sample]
            result["kb_stress"] = ext.compute_stress(atoms, [], None).composite_stress
        except Exception:
            result["kb_stress"] = None

    try:
        result["unread_alerts"] = len(ext.get_alerts(ext.DB_PATH, unseen_only=True, limit=500))
    except Exception:
        result["unread_alerts"] = 0

    return result


@router.get("/tickers/{ticker}/summary")
async def ticker_summary(ticker: str):
    ticker = ticker.upper()
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        rows = conn.execute(
            "SELECT predicate, object, confidence, source, timestamp "
            "FROM facts WHERE UPPER(subject) = ? ORDER BY confidence DESC",
            (ticker,),
        ).fetchall()
    finally:
        conn.close()

    atoms: dict = {}
    for pred, obj, conf, src, ts in rows:
        if pred not in atoms:
            atoms[pred] = obj

    signal_preds = [
        "conviction_tier","signal_quality","upside_pct","invalidation_distance",
        "position_size_pct","options_regime","macro_confirmation","thesis_risk_level",
        "signal_direction","volatility_regime","price_target","last_price",
    ]
    profile = {p: atoms.get(p) for p in signal_preds}
    profile["ticker"] = ticker

    if ext.HAS_STRESS:
        try:
            conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            ticker_atoms = [
                dict(zip(["subject","predicate","object","confidence","source","timestamp"], r))
                for r in conn2.execute(
                    "SELECT subject,predicate,object,confidence,source,timestamp "
                    "FROM facts WHERE UPPER(subject) = ? ORDER BY confidence DESC LIMIT 30",
                    (ticker,),
                ).fetchall()
            ]
            conn2.close()
            profile["kb_stress"] = ext.compute_stress(ticker_atoms, [ticker.lower()], None).composite_stress
        except Exception:
            profile["kb_stress"] = None

    if ext.HAS_PATTERN_LAYER:
        try:
            patterns = ext.get_open_patterns(ext.DB_PATH, ticker=ticker, limit=5)
            profile["open_patterns"] = [
                {k: p[k] for k in ("pattern_type","direction","quality_score","timeframe","status")}
                for p in patterns
            ]
        except Exception:
            profile["open_patterns"] = []
    else:
        profile["open_patterns"] = []

    if ext.HAS_ANALYTICS:
        try:
            recent_alerts = ext.get_alerts(ext.DB_PATH, unseen_only=False, limit=200)
            profile["recent_alerts"] = [a for a in recent_alerts if a.get("ticker") == ticker][:5]
        except Exception:
            profile["recent_alerts"] = []
    else:
        profile["recent_alerts"] = []

    profile["as_of"] = datetime.now(timezone.utc).isoformat()
    return profile


@router.get("/opportunities")
async def opportunities():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        summary = ext.build_portfolio_summary(ext.DB_PATH)
        return {
            "opportunities": summary.get("top_conviction", []),
            "count":         len(summary.get("top_conviction", [])),
            "regime":        summary.get("macro_regime"),
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))
