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


def _get_market_regime(db_path: str):
    """Read the current market_regime atom directly from DB (subject='market')."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject='market' AND predicate='market_regime' "
            "ORDER BY confidence DESC, timestamp DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


@router.get("/markets/chart", response_class=HTMLResponse)
async def markets_chart(
    sym: str = "AAPL", symbol: str = None, interval: str = "D",
    zone_high: str = None, zone_low: str = None,
    pattern_type: str = None, direction: str = None,
    fill_color: str = None,
    entry: str = None, stop: str = None, t1: str = None, t2: str = None,
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

    # Level lines — entry, stop, t1, t2
    def _parse_level(v):
        try: return float(v) if v else None
        except (TypeError, ValueError): return None
    lv_entry = _parse_level(entry)
    lv_stop  = _parse_level(stop)
    lv_t1    = _parse_level(t1)
    lv_t2    = _parse_level(t2)
    levels_js = (
        f"const LV_ENTRY={lv_entry if lv_entry is not None else 'null'};"
        f"const LV_STOP={lv_stop   if lv_stop  is not None else 'null'};"
        f"const LV_T1={lv_t1       if lv_t1    is not None else 'null'};"
        f"const LV_T2={lv_t2       if lv_t2    is not None else 'null'};"
    )
    has_levels = any(v is not None for v in [lv_entry, lv_stop, lv_t1, lv_t2])
    pat_label  = (pattern_type or "").replace("_", " ").upper() or "ZONE"
    dir_colour = "#22c55e" if (direction or "").lower().startswith("bull") else ("#ef4444" if (direction or "").lower().startswith("bear") else "#f59e0b")
    zone_fill = fill_color if fill_color else "rgba(245,158,11,0.13)"
    # Border always uses the direction colour (or a solid version of fill_color)
    zone_border_colour = dir_colour

    zone_overlay_html = ""
    if has_zone or has_levels:
        toggle_label = "◈ LEVELS ON" if (has_levels and not has_zone) else "◈ ZONE ON"
        zone_overlay_html = f"""
<canvas id="ov" style="position:fixed;top:0;left:0;width:100vw;height:100vh;pointer-events:none;z-index:9999;"></canvas>
<button id="ov-toggle" title="Toggle overlay" style="
  position:fixed;bottom:12px;right:12px;z-index:20;
  background:#1a1a2a;border:1px solid #f59e0b44;color:#f59e0b;
  font-size:10px;font-family:monospace;padding:4px 10px;border-radius:4px;
  cursor:pointer;opacity:0.85;letter-spacing:0.05em;">
  {toggle_label}
</button>"""

    zone_script = f"""
{zone_js}
{levels_js}
const PAT_LABEL  = {repr(pat_label)};
const DIR_COLOUR = {repr(dir_colour)};
const ZONE_FILL = {repr(zone_fill)};
const ZONE_BORDER_COLOUR = {repr(zone_border_colour)};

let zoneVisible = true;
let tvFrame     = null;   // TradingView iframe element
let priceMin    = null, priceMax = null, chartTop = 0, chartBottom = 0;
// Safe default — full viewport height until TV iframe bounds are known
function _safeChartBottom() {{ return chartBottom > chartTop + 50 ? chartBottom : window.innerHeight; }}

const canvas   = document.getElementById('ov');
const toggleBtn = document.getElementById('ov-toggle');

function drawOverlay() {{
  if (!canvas || !zoneVisible) {{
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
  const effectiveBottom = _safeChartBottom();
  const pxRange = effectiveBottom - chartTop;
  if (pxRange <= 0) return;
  function priceToY(p) {{
    return chartTop + (1 - (p - priceMin) / priceRange) * pxRange;
  }}

  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  // Zone band (only if zone data available)
  if (ZONE_HIGH !== null && ZONE_LOW !== null) {{
    const yHigh = priceToY(ZONE_HIGH);
    const yLow  = priceToY(ZONE_LOW);
    const yHeight = yLow - yHigh;
    ctx.fillStyle = 'rgba(38,63,226,0.15)';
    ctx.fillRect(0, yHigh, W, yHeight);

    // Zone borders
    ctx.strokeStyle = ZONE_BORDER_COLOUR;
    ctx.lineWidth   = 1;
    ctx.setLineDash([6, 4]);
    ctx.beginPath(); ctx.moveTo(0, yHigh); ctx.lineTo(W, yHigh); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(0, yLow);  ctx.lineTo(W, yLow);  ctx.stroke();
    ctx.setLineDash([]);

    // Zone label
    ctx.fillStyle     = DIR_COLOUR;
    ctx.font          = '9px monospace';
    ctx.letterSpacing = '0.06em';
    ctx.fillText(PAT_LABEL, 8, yHigh - 4);
  }}

  // Level lines
  const LEVELS = [
    {{ price: LV_ENTRY, color: '#3b82f6', dash: [],     label: 'ENTRY', width: 1.5 }},
    {{ price: LV_STOP,  color: '#ef4444', dash: [6, 4], label: 'STOP',  width: 1   }},
    {{ price: LV_T1,    color: '#22c55e', dash: [6, 4], label: 'T1',    width: 1   }},
    {{ price: LV_T2,    color: '#10b981', dash: [4, 6], label: 'T2',    width: 1   }},
  ];
  ctx.font = 'bold 9px monospace';
  ctx.letterSpacing = '0.06em';
  LEVELS.forEach(lv => {{
    if (lv.price === null || lv.price === undefined) return;
    const y = priceToY(lv.price);
    if (y < chartTop - 4 || y > effectiveBottom + 4) return; // off screen
    ctx.strokeStyle = lv.color;
    ctx.lineWidth   = lv.width;
    ctx.setLineDash(lv.dash);
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    ctx.setLineDash([]);
    // Right-edge label
    const label = `${{lv.label}} ${{lv.price.toFixed(2)}}`;
    const tw    = ctx.measureText(label).width;
    ctx.fillStyle = lv.color;
    ctx.fillRect(W - tw - 14, y - 9, tw + 10, 13);
    ctx.fillStyle = '#0a0e17';
    ctx.fillText(label, W - tw - 9, y + 1);
  }});
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

// Fallback: estimate price range from the zone itself (±300% padding)
// so the overlay shows immediately even without TV postMessage.
// Retries every second for up to 8s to wait for TradingView to fully render.
let _fallbackAttempts = 0;
function _fallbackDraw() {{
  if (priceMin !== null) return;  // postMessage already provided range
  if (ZONE_HIGH === null) return;
  _fallbackAttempts++;
  const mid  = (ZONE_HIGH + ZONE_LOW) / 2;
  const span = Math.max(ZONE_HIGH - ZONE_LOW, mid * 0.04);
  priceMin   = ZONE_LOW  - span * 1.5;
  priceMax   = ZONE_HIGH + span * 1.5;
  // TradingView widget renders an <iframe> inside #tv; use that for bounds
  const tvDiv = document.getElementById('tv');
  const tvIframe = tvDiv ? tvDiv.querySelector('iframe') : null;
  const el = tvIframe || tvDiv;
  if (el) {{
    const r = el.getBoundingClientRect();
    if (r.bottom > r.top + 50) {{
      // TV toolbar ~50px top, bottom scale ~30px
      chartTop    = r.top    + 50;
      chartBottom = r.bottom - 30;
    }} else if (_fallbackAttempts < 8) {{
      // iframe not sized yet — reset and retry
      priceMin = null;
      setTimeout(_fallbackDraw, 1000);
      return;
    }}
  }} else if (_fallbackAttempts < 8) {{
    priceMin = null;
    setTimeout(_fallbackDraw, 1000);
    return;
  }}
  drawOverlay();
}}
setTimeout(_fallbackDraw, 1500);
setTimeout(() => {{ if (priceMin === null) _fallbackDraw(); }}, 3500);

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
{"<script>" + zone_script + "</script>" if (has_zone or has_levels) else ""}
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
        result["regime"]        = _get_market_regime(ext.DB_PATH)
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
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        r_atoms = dict(conn.execute(
            "SELECT predicate, object FROM facts WHERE subject='market' "
            "AND predicate IN ('volatility_regime','sector_lead','regime_confidence') "
            "ORDER BY confidence DESC"
        ).fetchall())
        conn.close()
        result["regime_volatility"]    = r_atoms.get("volatility_regime")
        result["regime_sector_lead"]   = r_atoms.get("sector_lead")
        result["regime_kb_confidence"] = r_atoms.get("regime_confidence")
    except Exception:
        result["regime_volatility"] = result["regime_sector_lead"] = result["regime_kb_confidence"] = None

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


_CT_SCORE: dict = {'high': 0.85, 'medium': 0.65, 'low': 0.40, 'avoid': 0.10}


@router.get("/opportunities")
async def opportunities():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        summary   = ext.build_portfolio_summary(ext.DB_PATH)
        regime    = _get_market_regime(ext.DB_PATH)
        top_items = summary.get("top_conviction", [])

        enriched: dict = {}
        if top_items:
            tickers = [item['ticker'].lower() for item in top_items]
            conn = sqlite3.connect(ext.DB_PATH, timeout=5)
            ph = ','.join('?' * len(tickers))
            rows = conn.execute(
                f"""SELECT subject, predicate, object FROM facts
                    WHERE subject IN ({ph})
                    AND predicate IN ('conviction_score','best_regime','thesis',
                                      'macro_confirmation','signal_quality')
                    ORDER BY confidence DESC""",
                tickers,
            ).fetchall()
            conn.close()
            for subj, pred, obj in rows:
                enriched.setdefault(subj, {})
                if pred not in enriched[subj]:
                    enriched[subj][pred] = obj

        opps = []
        for item in top_items:
            tk    = item['ticker'].lower()
            atoms = enriched.get(tk, {})

            raw_cs = atoms.get('conviction_score')
            try:
                conviction_score = round(float(raw_cs), 3) if raw_cs else None
            except (TypeError, ValueError):
                conviction_score = None
            if conviction_score is None:
                conviction_score = _CT_SCORE.get(item.get('conviction_tier'))

            best_regime     = atoms.get('best_regime', '')
            best_regime_key = best_regime.split(' ')[0] if best_regime else ''
            regime_aligned  = bool(
                best_regime_key and regime and regime != 'no_data'
                and best_regime_key == regime
            )

            thesis = atoms.get('thesis')
            if thesis:
                thesis_preview = thesis[:120]
            else:
                mac = item.get('macro_confirmation') or atoms.get('macro_confirmation')
                sig = item.get('signal_quality')     or atoms.get('signal_quality')
                if mac and mac != 'no_data' and sig:
                    thesis_preview = f"Macro: {mac} · Signal: {sig}"
                elif sig:
                    thesis_preview = f"Signal: {sig}"
                else:
                    thesis_preview = None

            opps.append({
                **item,
                'conviction_score': conviction_score,
                'regime_aligned':   regime_aligned,
                'thesis_preview':   thesis_preview,
            })

        return {
            "opportunities": opps,
            "count":         len(opps),
            "regime":        regime,
            "summary_text":  summary.get("summary_text"),
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


def _prob_label(p: float) -> str:
    if p >= 0.70: return 'high'
    if p >= 0.50: return 'likely'
    if p >= 0.30: return 'unlikely'
    return 'low'


@router.get("/markets/prediction-odds")
async def prediction_odds(_: str = Depends(get_current_user)):
    """
    Return all live Polymarket prediction market probabilities from the KB.
    Groups by category. Used by the Scenario screen and Dispatch card.
    """
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        rows = conn.execute(
            """SELECT predicate, object, timestamp,
                      JSON_EXTRACT(metadata, '$.question') as question,
                      JSON_EXTRACT(metadata, '$.category') as category
               FROM facts
               WHERE source LIKE '%polymarket%'
                 AND predicate LIKE '%_yes_prob'
               ORDER BY timestamp DESC""",
        ).fetchall()
    finally:
        conn.close()

    markets = {}
    for predicate, prob_str, ts, question, category in rows:
        slug = predicate.replace('_yes_prob', '')
        if slug not in markets:
            try:
                prob = float(prob_str)
            except (ValueError, TypeError):
                continue
            markets[slug] = {
                'slug':     slug,
                'question': question or slug.replace('_', ' ').title(),
                'category': category or 'macro',
                'prob':     round(prob, 3),
                'label':    _prob_label(prob),
                'updated':  ts,
            }

    grouped = {}
    for m in markets.values():
        cat = m['category']
        grouped.setdefault(cat, []).append(m)

    return {
        'markets':      list(markets.values()),
        'by_category':  grouped,
        'total':        len(markets),
        'as_of':        max((m['updated'] for m in markets.values()), default=None),
    }
