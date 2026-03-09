"""routes_v2/markets.py — Phase 6: markets endpoints."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse

from middleware.fastapi_auth import get_current_user

import extensions as ext

router = APIRouter()


@router.get("/markets/chart", response_class=HTMLResponse)
async def markets_chart(
    sym: str = "AAPL", symbol: str = None, interval: str = "D",
    zone_high: str = None, zone_low: str = None,
    pattern_type: str = None, direction: str = None,
):
    tvSym = sym if sym != "AAPL" or symbol is None else (symbol or sym)
    tvInt = interval if interval in ("1","5","15","30","60","120","240","D","W","M") else "D"

    # Zone overlay — only if both prices provided
    try:
        zh = float(zone_high) if zone_high else None
        zl = float(zone_low)  if zone_low  else None
    except (TypeError, ValueError):
        zh = zl = None

    has_zone   = zh is not None and zl is not None
    zone_js    = f"const ZONE_HIGH={zh}, ZONE_LOW={zl};" if has_zone else "const ZONE_HIGH=null, ZONE_LOW=null;"
    pat_label  = (pattern_type or "").replace("_", " ").upper() or "ZONE"
    dir_colour = "#22c55e" if (direction or "").lower().startswith("bull") else ("#ef4444" if (direction or "").lower().startswith("bear") else "#f59e0b")

    zone_overlay_html = ""
    if has_zone:
        zone_overlay_html = f"""
<canvas id="ov" style="position:fixed;top:0;left:0;pointer-events:none;z-index:10;"></canvas>
<button id="ov-toggle" title="Toggle zone overlay" style="
  position:fixed;bottom:12px;right:12px;z-index:20;
  background:#1a1a2a;border:1px solid #f59e0b44;color:#f59e0b;
  font-size:10px;font-family:monospace;padding:4px 10px;border-radius:4px;
  cursor:pointer;opacity:0.85;letter-spacing:0.05em;">
  ◈ ZONE ON
</button>"""

    zone_script = f"""
{zone_js}
const PAT_LABEL  = {repr(pat_label)};
const DIR_COLOUR = {repr(dir_colour)};

let zoneVisible = true;
let tvFrame     = null;   // TradingView iframe element
let priceMin    = null, priceMax = null, chartTop = 0, chartBottom = 0;

const canvas   = document.getElementById('ov');
const toggleBtn = document.getElementById('ov-toggle');

function drawOverlay() {{
  if (!canvas || ZONE_HIGH === null || !zoneVisible) {{
    if (canvas) canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height);
    return;
  }}
  if (priceMin === null || priceMax === null) return;

  const W = window.innerWidth, H = window.innerHeight;
  canvas.width  = W;
  canvas.height = H;

  const priceRange = priceMax - priceMin;
  if (priceRange <= 0) return;

  // Map price to Y pixel (price axis: top=high, bottom=low)
  const pxRange = chartBottom - chartTop;
  function priceToY(p) {{
    return chartTop + (1 - (p - priceMin) / priceRange) * pxRange;
  }}

  const yHigh = priceToY(ZONE_HIGH);
  const yLow  = priceToY(ZONE_LOW);
  const ctx   = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  // Amber zone band
  ctx.fillStyle = 'rgba(245,158,11,0.13)';
  ctx.fillRect(0, yHigh, W, yLow - yHigh);

  // Top/bottom borders
  ctx.strokeStyle = DIR_COLOUR + 'aa';
  ctx.lineWidth   = 1;
  ctx.setLineDash([6, 4]);
  ctx.beginPath(); ctx.moveTo(0, yHigh); ctx.lineTo(W, yHigh); ctx.stroke();
  ctx.beginPath(); ctx.moveTo(0, yLow);  ctx.lineTo(W, yLow);  ctx.stroke();
  ctx.setLineDash([]);

  // Label
  ctx.fillStyle    = DIR_COLOUR;
  ctx.font         = '9px monospace';
  ctx.letterSpacing = '0.06em';
  ctx.fillText(PAT_LABEL, 8, yHigh - 4);
}}

if (toggleBtn) {{
  toggleBtn.addEventListener('click', () => {{
    zoneVisible = !zoneVisible;
    toggleBtn.textContent = zoneVisible ? '◈ ZONE ON' : '◈ ZONE OFF';
    toggleBtn.style.opacity = zoneVisible ? '0.85' : '0.45';
    drawOverlay();
  }});
}}

// Poll TradingView's iframe for its visible price range via postMessage
function requestPriceRange() {{
  const frames = document.querySelectorAll('iframe');
  frames.forEach(f => {{
    try {{ f.contentWindow.postMessage({{name: 'tv-widget-range'}}, '*'); }} catch(e) {{}}
  }});
}}

window.addEventListener('message', e => {{
  if (!e.data) return;
  // TradingView emits range info as {{ priceRange: {{high, low}}, timeRange: ... }}
  const d = e.data;
  if (d.name === 'quoteData' || d.name === 'price-scale-changed') return; // ignore noise
  if (d.priceRange && d.priceRange.high != null) {{
    priceMax = d.priceRange.high;
    priceMin = d.priceRange.low;
    const r = document.getElementById('tv')?.getBoundingClientRect();
    if (r) {{ chartTop = r.top; chartBottom = r.bottom; }}
    drawOverlay();
  }}
}});

// Fallback: estimate price range from the zone itself (±40% padding)
// so the overlay shows immediately even without TV postMessage
setTimeout(() => {{
  if (priceMin === null && ZONE_HIGH !== null) {{
    const mid  = (ZONE_HIGH + ZONE_LOW) / 2;
    const span = Math.max(ZONE_HIGH - ZONE_LOW, mid * 0.05);
    priceMin   = ZONE_LOW  - span * 3;
    priceMax   = ZONE_HIGH + span * 3;
    const el   = document.getElementById('tv');
    if (el) {{
      const r = el.getBoundingClientRect();
      chartTop    = r.top    + 40;   // approx toolbar height
      chartBottom = r.bottom - 20;   // approx scale padding
    }}
    drawOverlay();
  }}
}}, 2500);

window.addEventListener('resize', drawOverlay);
"""

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{tvSym} – Trading Galaxy Chart</title>
<style>body{{margin:0;overflow:hidden}}#tv{{width:100vw;height:100vh}}</style>
</head><body>
<div id="tv"></div>
{zone_overlay_html}
<script src="https://s3.tradingview.com/tv.js"></script>
<script>
new TradingView.widget({{
  container_id: 'tv',
  autosize: true,
  symbol: '{tvSym}',
  interval: '{tvInt}',
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
{"<script>" + zone_script + "</script>" if has_zone else ""}
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

    # Build symbols dict for dashboard market snapshot widget
    # Keys are yFinance-style tickers; values have price + return_1m
    _SNAPSHOT_TICKERS = ['^GSPC', '^FTSE', '^FTMC', 'GLD', 'GBPUSD=X']
    # Case-insensitive lookup: KB may store tickers in any case (e.g. 'gld' vs 'GLD')
    _by_subject_lower = {k.lower(): v for k, v in by_subject.items()}
    symbols: dict = {}
    for sym in _SNAPSHOT_TICKERS:
        atoms = _by_subject_lower.get(sym.lower(), {})
        if atoms.get('last_price') is not None:
            try:
                symbols[sym] = {
                    'price':     float(atoms['last_price']),
                    'return_1m': float(atoms['return_1m']) if atoms.get('return_1m') is not None else None,
                }
            except (TypeError, ValueError):
                pass

    return {
        "tickers": tickers,
        "count": len(tickers),
        "macro": macro,
        "symbols": symbols,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


_VIS_SECTOR_NORM: dict[str, str] = {
    'financial services': 'Financial Services', 'financial_services': 'Financial Services',
    'financials': 'Financial Services', 'financial': 'Financial Services',
    'technology': 'Technology', 'information technology': 'Technology', 'tech': 'Technology',
    'healthcare': 'Healthcare', 'health care': 'Healthcare',
    'consumer cyclical': 'Consumer', 'consumer discretionary': 'Consumer',
    'consumer defensive': 'Consumer', 'consumer staples': 'Consumer', 'consumer': 'Consumer',
    'energy': 'Energy',
    'industrials': 'Industrials', 'industrial': 'Industrials',
    'communication services': 'Communication', 'communications': 'Communication',
    'communication': 'Communication',
    'real estate': 'Real Estate', 'reits': 'Real Estate',
    'utilities': 'Utilities',
    'basic materials': 'Materials', 'materials': 'Materials',
}

def _vis_norm_sector(raw: str | None) -> str:
    if not raw:
        return 'Other'
    return _VIS_SECTOR_NORM.get(raw.lower(), raw)


@router.get("/kb/visualiser")
async def kb_visualiser(_user: str = Depends(get_current_user)):
    """Pre-aggregated KB data for the Visualiser screen."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        rows = conn.execute(
            """SELECT subject, predicate, object
               FROM facts
               WHERE predicate IN (
                   'conviction_tier','signal_direction','upside_pct',
                   'macro_confirmation','volatility_regime','return_1m','return_1y',
                   'price_target','last_price','sector'
               )
               ORDER BY subject, predicate"""
        ).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    by_subject: dict = {}
    for subj, pred, obj in rows:
        if subj not in by_subject:
            by_subject[subj] = {}
        if pred not in by_subject[subj]:
            by_subject[subj][pred] = obj

    tickers_out = []
    for subj, atoms in by_subject.items():
        if not (atoms.get('conviction_tier') or atoms.get('signal_direction')):
            continue
        try:
            upside = float(atoms['upside_pct']) if atoms.get('upside_pct') else None
        except (TypeError, ValueError):
            upside = None
        tickers_out.append({
            'ticker':            subj,
            'sector':            _vis_norm_sector(atoms.get('sector')),
            'signal_direction':  atoms.get('signal_direction', 'neutral'),
            'upside_pct':        upside,
            'conviction_tier':   atoms.get('conviction_tier'),
            'macro_confirmation':atoms.get('macro_confirmation'),
            'volatility_regime': atoms.get('volatility_regime'),
            'return_1m':         atoms.get('return_1m'),
            'return_1y':         atoms.get('return_1y'),
            'price_target':      atoms.get('price_target'),
            'last_price':        atoms.get('last_price'),
        })

    # Sector stats
    sector_stats: dict = {}
    for t in tickers_out:
        s = t['sector']
        if s not in sector_stats:
            sector_stats[s] = {'count': 0, 'bull': 0, 'upsides': [], 'tickers': []}
        sector_stats[s]['count'] += 1
        if (t['signal_direction'] or '').lower().startswith('bull'):
            sector_stats[s]['bull'] += 1
        if t['upside_pct'] is not None:
            sector_stats[s]['upsides'].append(t['upside_pct'])
        sector_stats[s]['tickers'].append((t['ticker'], t['upside_pct'] or 0))

    sector_out = {}
    for s, d in sector_stats.items():
        top3 = sorted(d['tickers'], key=lambda x: x[1], reverse=True)[:3]
        avg_up = (sum(d['upsides']) / len(d['upsides'])) if d['upsides'] else None
        sector_out[s] = {
            'count':       d['count'],
            'avg_upside':  round(avg_up, 1) if avg_up is not None else None,
            'bullish_pct': round(d['bull'] / d['count'] * 100) if d['count'] else 0,
            'top':         [t[0] for t in top3],
        }

    # Signal counts
    bull = sum(1 for t in tickers_out if (t['signal_direction'] or '').lower().startswith('bull'))
    bear = sum(1 for t in tickers_out if (t['signal_direction'] or '').lower().startswith('bear'))
    neut = len(tickers_out) - bull - bear

    # Top 20 by upside
    top_upside = sorted(
        [t for t in tickers_out if t['upside_pct'] is not None],
        key=lambda x: x['upside_pct'], reverse=True
    )[:20]

    return {
        'tickers':      tickers_out,
        'sector_stats': sector_out,
        'signal_counts': {'bullish': bull, 'bearish': bear, 'neutral': neut, 'total': len(tickers_out)},
        'top_upside':   top_upside,
        'as_of':        datetime.now(timezone.utc).isoformat(),
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
