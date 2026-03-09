"""routes_v2/users.py — Phase 7: user management endpoints."""

from __future__ import annotations

import base64
import hashlib
import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user, user_path_auth
from middleware.fastapi_rate_limiter import RATE_LIMITS, limiter

router = APIRouter()


# ── Pydantic models ────────────────────────────────────────────────────────────

class OnboardingRequest(BaseModel):
    portfolio: Optional[list] = None
    tip_delivery_time: Optional[str] = None
    tip_delivery_timezone: Optional[str] = None
    account_size: Optional[float] = None
    selected_sectors: Optional[list] = None


class PortfolioRequest(BaseModel):
    portfolio: Optional[list] = None
    holdings: Optional[list] = None


class TipConfigRequest(BaseModel):
    tip_delivery_time: Optional[str] = None
    tip_delivery_timezone: Optional[str] = None
    tip_markets: Optional[list] = None
    tip_timeframes: Optional[list] = None
    tip_pattern_types: Optional[list] = None
    account_size: Optional[float] = None
    max_risk_per_trade_pct: Optional[float] = None
    account_currency: Optional[str] = None
    tier: Optional[str] = None


class NotificationPrefsRequest(BaseModel):
    monday_briefing: Optional[bool] = None
    wednesday_update: Optional[bool] = None
    zone_alerts: Optional[bool] = None
    thesis_alerts: Optional[bool] = None
    profit_lock_alerts: Optional[bool] = None
    trailing_alerts: Optional[bool] = None


class TradingPrefsRequest(BaseModel):
    max_risk_per_trade_pct: Optional[float] = None
    preferred_broker: Optional[str] = None
    experience_level: Optional[str] = None
    trading_bio: Optional[str] = None


class StylePrefsRequest(BaseModel):
    style_risk_tolerance: Optional[str] = None  # conservative | moderate | aggressive
    style_timeframe: Optional[str] = None       # scalp | intraday | swing | position
    style_sector_focus: Optional[list] = None   # list of sector strings


class ExpandUniverseRequest(BaseModel):
    description: str
    market_type: str = "equities"


class SetFocusRequest(BaseModel):
    preferred_upside_min: Optional[float] = None
    preferred_pattern: Optional[str] = None


class EngagementRequest(BaseModel):
    event_type: str
    ticker: Optional[str] = None
    pattern_type: Optional[str] = None
    sector: Optional[str] = None


class CashRequest(BaseModel):
    available_cash: Optional[float] = None
    cash_currency: str = "GBP"


class ProfileRequest(BaseModel):
    first_name: str = ""
    last_name: str = ""
    phone: str = ""


class NotifyTestRequest(BaseModel):
    chat_id: str


class TraderLevelRequest(BaseModel):
    level: str


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_prefs_confirmation(new: dict, old: dict) -> str:
    try:
        from notifications.tip_formatter import _escape_mdv2, _PATTERN_LABELS, _TF_LABELS
    except ImportError:
        return ""
    lines = []
    new_markets = new.get("tip_markets"); old_markets = old.get("tip_markets")
    if new_markets != old_markets and "tip_markets" in new:
        if not new_markets:
            lines.append("🌐 You'll now receive tips from *all available markets*\\.")
        else:
            tickers_str = ", ".join(_escape_mdv2(t) for t in new_markets[:10])
            suffix = f" \\+{len(new_markets)-10} more" if len(new_markets) > 10 else ""
            if old_markets:
                added   = [t for t in new_markets if t not in old_markets]
                removed = [t for t in old_markets if t not in new_markets]
                if added and removed:
                    lines.append(f"📊 More *{', '.join(_escape_mdv2(t) for t in added[:5])}* tips, fewer *{', '.join(_escape_mdv2(t) for t in removed[:5])}* tips\\.")
                elif added:
                    lines.append(f"📊 Added to your watchlist: *{', '.join(_escape_mdv2(t) for t in added[:5])}*\\.")
                elif removed:
                    lines.append(f"📊 Removed from your watchlist: *{', '.join(_escape_mdv2(t) for t in removed[:5])}*\\.")
            else:
                lines.append(f"🎯 Your tips will now focus on: *{tickers_str}{suffix}*\\.")
    new_patterns = new.get("tip_pattern_types"); old_patterns = old.get("tip_pattern_types")
    if new_patterns != old_patterns and "tip_pattern_types" in new:
        def _plabel(p): return _escape_mdv2(_PATTERN_LABELS.get(p, p.replace("_"," ").title()))
        if not new_patterns:
            lines.append("📐 Pattern filter cleared — you'll see *all pattern types*\\.")
        else:
            added_p   = [p for p in new_patterns if not old_patterns or p not in old_patterns]
            removed_p = [p for p in (old_patterns or []) if p not in new_patterns]
            if added_p and removed_p:
                lines.append(f"📐 More *{', '.join(_plabel(p) for p in added_p)}*, fewer *{', '.join(_plabel(p) for p in removed_p)}* patterns\\.")
            elif added_p:
                lines.append(f"📐 Added pattern types: *{', '.join(_plabel(p) for p in added_p)}*\\.")
            elif removed_p:
                lines.append(f"📐 Removed pattern types: *{', '.join(_plabel(p) for p in removed_p)}*\\.")
    new_tfs = new.get("tip_timeframes"); old_tfs = old.get("tip_timeframes")
    if new_tfs != old_tfs and "tip_timeframes" in new:
        def _tflabel(tf): return _escape_mdv2(_TF_LABELS.get(tf, tf.upper()))
        if not new_tfs:
            lines.append("⏱ Timeframe filter cleared — you'll see *all timeframes*\\.")
        else:
            added_tf   = [tf for tf in new_tfs if not old_tfs or tf not in old_tfs]
            removed_tf = [tf for tf in (old_tfs or []) if tf not in new_tfs]
            if added_tf and removed_tf:
                lines.append(f"⏱ More *{', '.join(_tflabel(t) for t in added_tf)}* tips, fewer *{', '.join(_tflabel(t) for t in removed_tf)}* tips\\.")
            elif added_tf:
                lines.append(f"⏱ Added timeframes: *{', '.join(_tflabel(t) for t in added_tf)}*\\.")
            elif removed_tf:
                lines.append(f"⏱ Removed timeframes: *{', '.join(_tflabel(t) for t in removed_tf)}*\\.")
    new_time = new.get("tip_delivery_time"); new_tz = new.get("tip_delivery_timezone")
    old_time = old.get("tip_delivery_time"); old_tz  = old.get("tip_delivery_timezone")
    if (new_time and new_time != old_time) or (new_tz and new_tz != old_tz):
        t  = _escape_mdv2(new_time or old_time or "?")
        tz = _escape_mdv2(new_tz or old_tz or "UTC")
        lines.append(f"🕐 Tips will now arrive at *{t}* \\({tz}\\)\\.")
    new_tier = new.get("tier"); old_tier = old.get("tier")
    if new_tier and new_tier != old_tier:
        _TIER_DISPLAY = {"basic":"Basic \\(Mon weekly batch\\)","pro":"Pro \\(Mon \\+ Wed batch\\)","premium":"Premium \\(daily tips\\)"}
        lines.append(f"⭐ Tier updated to *{_TIER_DISPLAY.get(new_tier, _escape_mdv2(new_tier.title()))}*\\.")
    if not lines:
        return ""
    return "✅ *Tip preferences updated\\!*\n" + "\n".join(lines) + "\n_Changes take effect from your next scheduled tip\\._"


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.post("/users/{user_id}/onboarding")
async def user_onboarding(user_id: str, data: OnboardingRequest, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    if ext.HAS_VALIDATORS:
        result = ext.validate_onboarding(data.model_dump(exclude_none=True))
        if not result.valid:
            raise HTTPException(400, detail={"error": "validation_failed", "details": result.errors})
    try:
        ext.update_preferences(ext.DB_PATH, user_id, data.model_dump(exclude_none=True))
        if data.portfolio:
            ext.upsert_portfolio(ext.DB_PATH, user_id, data.portfolio)
        return {"ok": True, "user_id": user_id}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/portfolio")
async def user_portfolio_get(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        from users.user_store import get_portfolio_with_signals
        holdings = get_portfolio_with_signals(ext.DB_PATH, user_id)
        return {"holdings": holdings or [], "count": len(holdings or [])}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/portfolio")
async def user_portfolio_update(user_id: str, data: PortfolioRequest, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    holdings = data.portfolio or data.holdings or []
    if ext.HAS_VALIDATORS:
        result = ext.validate_portfolio_submission(holdings)
        if not result.valid:
            raise HTTPException(400, detail={"error": "validation_failed", "details": result.errors})
    try:
        ext.upsert_portfolio(ext.DB_PATH, user_id, holdings)
        if ext.HAS_HYBRID:
            try:
                ext.infer_and_write_from_portfolio(user_id, ext.DB_PATH)
            except Exception:
                pass
        return {"ok": True, "count": len(holdings)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


class SingleHoldingRequest(BaseModel):
    ticker: str
    quantity: Optional[float] = None
    avg_cost: Optional[float] = None
    sector: Optional[str] = None


@router.post("/users/{user_id}/portfolio/holding")
async def user_portfolio_add_holding(user_id: str, data: SingleHoldingRequest, _: str = Depends(user_path_auth)):
    """Add or update a single holding without replacing the whole portfolio."""
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    ticker = data.ticker.strip().upper()
    if not ticker:
        raise HTTPException(400, detail="ticker is required")
    try:
        from users.user_store import upsert_single_holding
        result = upsert_single_holding(
            ext.DB_PATH, user_id, ticker,
            quantity=data.quantity, avg_cost=data.avg_cost, sector=data.sector,
        )
        if ext.HAS_HYBRID:
            try:
                ext.infer_and_write_from_portfolio(user_id, ext.DB_PATH)
            except Exception:
                pass
        return {"ok": True, "holding": result}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/tip-config")
async def user_tip_config_get(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT user_id, tier, tip_delivery_time, tip_delivery_timezone, "
            "tip_markets, tip_timeframes, tip_pattern_types, account_size, "
            "max_risk_per_trade_pct, account_currency, available_cash "
            "FROM user_preferences WHERE user_id = ?", (user_id,)
        ).fetchone()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    if row is None:
        raise HTTPException(404, detail="user not found")
    cols = ["user_id","tier","tip_delivery_time","tip_delivery_timezone","tip_markets",
            "tip_timeframes","tip_pattern_types","account_size","max_risk_per_trade_pct",
            "account_currency","available_cash"]
    d = dict(zip(cols, row))
    for jcol in ("tip_markets","tip_timeframes","tip_pattern_types"):
        try:
            d[jcol] = json.loads(d[jcol]) if d[jcol] else None
        except Exception:
            d[jcol] = None
    try:
        from users.user_store import get_available_cash as _gc
        _cd = _gc(ext.DB_PATH, user_id)
        d["cash_currency"] = _cd.get("cash_currency","GBP")
    except Exception:
        d["cash_currency"] = "GBP"
    # Sanitise implausibly large available_cash values (legacy defaults / data errors)
    if d.get("available_cash") is not None and d["available_cash"] > 1_000_000:
        d["available_cash"] = None
    return d


@router.post("/users/{user_id}/tip-config")
async def user_tip_config_post(user_id: str, data: TipConfigRequest, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    d = data.model_dump(exclude_none=True)
    if ext.HAS_VALIDATORS:
        result = ext.validate_tip_config(d)
        if not result.valid:
            raise HTTPException(400, detail={"error": "validation_failed", "details": result.errors})
    try:
        _old_prefs: dict = {}
        try:
            oc = sqlite3.connect(ext.DB_PATH, timeout=5)
            or_ = oc.execute(
                "SELECT telegram_chat_id, tip_markets, tip_timeframes, tip_pattern_types, "
                "tip_delivery_time, tip_delivery_timezone, tier FROM user_preferences WHERE user_id=?",
                (user_id,)
            ).fetchone()
            oc.close()
            if or_:
                _cols = ["telegram_chat_id","tip_markets","tip_timeframes","tip_pattern_types",
                         "tip_delivery_time","tip_delivery_timezone","tier"]
                _old_prefs = dict(zip(_cols, or_))
                for jc in ("tip_markets","tip_timeframes","tip_pattern_types"):
                    try:
                        _old_prefs[jc] = json.loads(_old_prefs[jc]) if _old_prefs[jc] else None
                    except Exception:
                        _old_prefs[jc] = None
        except Exception:
            pass

        updated = ext.update_tip_config(
            ext.DB_PATH, user_id,
            tip_delivery_time=data.tip_delivery_time,
            tip_delivery_timezone=data.tip_delivery_timezone,
            tip_markets=data.tip_markets,
            tip_timeframes=data.tip_timeframes,
            tip_pattern_types=data.tip_pattern_types,
            account_size=data.account_size,
            max_risk_per_trade_pct=data.max_risk_per_trade_pct,
            account_currency=data.account_currency,
            tier=data.tier,
        )
        try:
            _chat_id = (_old_prefs.get("telegram_chat_id") or "").strip()
            if _chat_id:
                _msg = _build_prefs_confirmation(d, _old_prefs)
                if _msg:
                    from notifications.telegram_notifier import TelegramNotifier as _TGN
                    _TGN().send(_chat_id, _msg, parse_mode="MarkdownV2")
        except Exception:
            pass
        return updated
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/tips")
async def user_tips(user_id: str, limit: int = 50, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        tips = ext.get_tip_history(ext.DB_PATH, user_id, limit=limit)
        return {"tips": tips, "count": len(tips)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/notification-prefs")
async def update_notification_prefs(
    user_id: str, data: NotificationPrefsRequest,
    current_user: str = Depends(get_current_user),
):
    if current_user != user_id:
        raise HTTPException(403, detail="forbidden")
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")

    try:
        user = ext.get_user(ext.DB_PATH, user_id)
        tier = (user.get("tier") or "basic").lower() if user else "basic"
    except Exception:
        tier = "basic"

    PRO_ONLY = {"profit_lock_alerts","trailing_alerts"}
    ALLOWED  = {"monday_briefing","wednesday_update","zone_alerts",
                "thesis_alerts","profit_lock_alerts","trailing_alerts"}

    prefs = {}
    for key in ALLOWED:
        val = getattr(data, key, None)
        if val is None:
            continue
        if key in PRO_ONLY and tier == "basic":
            raise HTTPException(403, detail=f"{key} requires Pro or Premium tier")
        prefs[key] = bool(val)

    existing: dict = {}
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            row = conn.execute(
                "SELECT notification_prefs FROM user_preferences WHERE user_id=?", (user_id,)
            ).fetchone()
            if row and row[0]:
                try:
                    existing = json.loads(row[0])
                except Exception:
                    pass
            existing.update(prefs)
            conn.execute("UPDATE user_preferences SET notification_prefs=? WHERE user_id=?",
                         (json.dumps(existing), user_id))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return {"ok": True, "prefs": existing}


@router.patch("/users/{user_id}/trading-prefs")
async def update_trading_prefs(
    user_id: str, data: TradingPrefsRequest,
    current_user: str = Depends(get_current_user),
):
    if current_user != user_id:
        raise HTTPException(403, detail="forbidden")
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")

    risk_pct = data.max_risk_per_trade_pct
    if risk_pct is not None and not (0 < risk_pct <= 100):
        raise HTTPException(400, detail="max_risk_per_trade_pct must be between 0 and 100")

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        for col, default in [("preferred_broker","TEXT DEFAULT ''"),
                              ("experience_level","TEXT DEFAULT ''"),
                              ("trading_bio","TEXT DEFAULT ''")]:
            try:
                conn.execute(f"ALTER TABLE user_preferences ADD COLUMN {col} {default}")
            except Exception:
                pass
        updates, params = [], []
        if risk_pct is not None:
            updates.append("max_risk_per_trade_pct=?"); params.append(risk_pct)
        if data.preferred_broker:
            updates.append("preferred_broker=?"); params.append(data.preferred_broker[:100])
        if data.experience_level:
            updates.append("experience_level=?"); params.append(data.experience_level[:100])
        if data.trading_bio is not None:
            updates.append("trading_bio=?"); params.append(data.trading_bio[:1000])
        if updates:
            params.append(user_id)
            conn.execute(f"UPDATE user_preferences SET {', '.join(updates)} WHERE user_id=?", params)
            conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return {"ok": True}


@router.get("/users/{user_id}/style-prefs")
async def get_style_prefs_route(user_id: str, _: str = Depends(user_path_auth)):
    try:
        from users.user_store import get_style_prefs
        return get_style_prefs(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/style-prefs")
async def update_style_prefs_route(
    user_id: str, data: StylePrefsRequest,
    current_user: str = Depends(get_current_user),
):
    if current_user != user_id:
        raise HTTPException(403, detail="forbidden")
    try:
        from users.user_store import update_style_prefs
        result = update_style_prefs(
            ext.DB_PATH, user_id,
            style_risk_tolerance=data.style_risk_tolerance,
            style_timeframe=data.style_timeframe,
            style_sector_focus=data.style_sector_focus,
        )
        return {"ok": True, **result}
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.delete("/users/{user_id}")
async def delete_account(
    request: Request, user_id: str,
    current_user: str = Depends(get_current_user),
):
    if current_user != user_id:
        raise HTTPException(403, detail="forbidden")
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        conn.execute("DELETE FROM user_auth WHERE user_id=?", (user_id,))
        for tbl in ("user_preferences","refresh_tokens"):
            try:
                conn.execute(f"DELETE FROM {tbl} WHERE user_id=?", (user_id,))
            except Exception:
                pass
        conn.commit()
        conn.close()
        ext.log_audit_event(ext.DB_PATH, action="account_deleted", user_id=user_id,
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="success")
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return {"deleted": True}


@router.get("/users/{user_id}/watchlist/signals")
async def watchlist_signals(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        tickers = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    if not tickers:
        return {"signals": [], "count": 0}

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"SELECT UPPER(subject) as ticker, predicate, object FROM facts "
            f"WHERE UPPER(subject) IN ({placeholders}) AND predicate IN "
            f"('conviction_tier','signal_quality','upside_pct','position_size_pct') "
            f"ORDER BY UPPER(subject), predicate",
            tickers,
        ).fetchall()
    finally:
        conn.close()

    by_ticker: dict = {t: {} for t in tickers}
    for ticker, pred, obj in rows:
        if ticker in by_ticker and pred not in by_ticker[ticker]:
            by_ticker[ticker][pred] = obj

    if ext.HAS_PATTERN_LAYER:
        for ticker in tickers:
            try:
                by_ticker[ticker]["pattern_count"] = len(
                    ext.get_open_patterns(ext.DB_PATH, ticker=ticker, limit=100)
                )
            except Exception:
                by_ticker[ticker]["pattern_count"] = 0

    if ext.HAS_ANALYTICS:
        try:
            tip_logs = {}
            c = sqlite3.connect(ext.DB_PATH, timeout=5)
            log_rows = c.execute(
                "SELECT ps.ticker, MAX(t.delivered_at) FROM tip_delivery_log t "
                "JOIN pattern_signals ps ON ps.id = t.pattern_signal_id "
                "WHERE t.user_id = ? AND t.success = 1 GROUP BY ps.ticker",
                (user_id,),
            ).fetchall()
            c.close()
            for t, dt in log_rows:
                tip_logs[t.upper()] = dt
        except Exception:
            tip_logs = {}
        for ticker in tickers:
            by_ticker[ticker]["last_tip_date"] = tip_logs.get(ticker)

    signals = [{"ticker": t, **v} for t, v in by_ticker.items()]
    return {"signals": signals, "count": len(signals)}


@router.get("/users/{user_id}/alerts/unread-count")
async def user_alerts_unread_count(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        tickers = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id) if ext.HAS_PRODUCT_LAYER else []
        unseen  = ext.get_alerts(ext.DB_PATH, unseen_only=True, limit=10000)
        if tickers:
            unseen = [a for a in unseen if a.get("ticker") in tickers]
        return {"count": len(unseen)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/alerts")
async def user_alerts(user_id: str, all: str = "false", limit: int = 50,
                      _: str = Depends(user_path_auth)):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        tickers     = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id) if ext.HAS_PRODUCT_LAYER else []
        unseen_only = all.lower() != "true"
        rows        = ext.get_alerts(ext.DB_PATH, unseen_only=unseen_only, limit=10000)
        if tickers:
            rows = [a for a in rows if a.get("ticker") in tickers]
        return {"alerts": rows[:limit], "count": len(rows[:limit])}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/onboarding-status")
async def user_onboarding_status(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT onboarding_complete, telegram_chat_id, tip_delivery_time, "
            "tip_delivery_timezone, account_size, selected_sectors "
            "FROM user_preferences WHERE user_id = ?", (user_id,)
        ).fetchone()
        portfolio_count = conn.execute(
            "SELECT COUNT(*) FROM user_portfolios WHERE user_id = ?", (user_id,)
        ).fetchone()[0]
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    if row is None:
        raise HTTPException(404, detail="user not found")
    onboarding_complete, chat_id, tip_time, tip_tz, account_size, sectors = row
    try:
        sector_list = json.loads(sectors or "[]")
    except Exception:
        sector_list = []
    portfolio_submitted = portfolio_count > 0
    telegram_connected  = bool(chat_id and str(chat_id).strip())
    tip_config_set      = bool((tip_time and tip_time != "07:30") or (tip_tz and tip_tz != "Europe/London"))
    account_size_set    = account_size is not None and float(account_size or 0) > 0
    return {
        "portfolio_submitted": portfolio_submitted,
        "telegram_connected":  telegram_connected,
        "tip_config_set":      tip_config_set,
        "account_size_set":    account_size_set,
        "preferences_set":     len(sector_list) > 0,
        "complete":            all([portfolio_submitted, telegram_connected, tip_config_set, account_size_set]),
    }


@router.post("/users/{user_id}/telegram/verify")
async def user_telegram_verify(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        user = ext.get_user(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    if user is None:
        raise HTTPException(404, detail="user not found")
    chat_id = (user.get("telegram_chat_id") or "").strip()
    if not chat_id:
        raise HTTPException(400, detail="no telegram_chat_id on record")
    try:
        notifier = ext.TelegramNotifier()
        sent = notifier.send_test(chat_id)
        return {"sent": sent}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/trader-level")
async def user_set_trader_level(user_id: str, data: TraderLevelRequest,
                                _: str = Depends(user_path_auth)):
    level = data.level.strip().lower()
    _valid = {"beginner","developing","experienced","quant"}
    if level not in _valid:
        raise HTTPException(400, detail=f"Invalid level '{level}'. Must be one of: {sorted(_valid)}")
    try:
        from users.user_store import set_trader_level as _set_level
        _set_level(ext.DB_PATH, user_id, level)
        return {"trader_level": level}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.delete("/users/{user_id}/telegram")
async def user_telegram_delink(request: Request, user_id: str, _: str = Depends(user_path_auth)):
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            conn.execute("UPDATE user_preferences SET telegram_chat_id = NULL WHERE user_id = ?", (user_id,))
            conn.commit()
        finally:
            conn.close()
        ext.log_audit_event(ext.DB_PATH, action="telegram_delink", user_id=user_id,
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"), outcome="success")
        return {"delinked": True}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/performance")
async def user_performance(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        return ext.get_tip_performance(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/expand-universe")
async def expand_universe(user_id: str, data: ExpandUniverseRequest,
                          _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    if len(data.description) < 3:
        raise HTTPException(400, detail="description must be at least 3 characters")
    tier = "basic"
    try:
        tier = ext.get_user_tier(ext.DB_PATH, user_id)
    except Exception:
        pass
    max_universe = 100 if tier == "pro" else 20
    current_count = len(ext.DynamicWatchlistManager.get_user_tickers(user_id, ext.DB_PATH))
    if current_count >= max_universe:
        raise HTTPException(400, detail=f"universe limit reached ({max_universe} tickers for {tier} tier)")

    try:
        expansion = ext.resolve_interest(data.description, data.market_type, user_id, ext.DB_PATH)
        if expansion.error == "llm_unavailable":
            return {"resolved_tickers":[],"rejected_tickers":[],"staging_tickers":[],
                    "causal_edges_seeded":0,"estimated_bootstrap_seconds":0,"error":"llm_unavailable"}

        validation = ext.validate_tickers(expansion.tickers[:20], market_region=data.market_type)
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            ext.ensure_hybrid_tables(conn)
            cur = conn.execute(
                "INSERT INTO user_universe_expansions "
                "(user_id, description, sector_label, tickers, etfs, keywords, causal_edges, status, requested_at, activated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user_id, data.description, expansion.sector_label,
                 json.dumps(validation.valid), json.dumps(expansion.etfs),
                 json.dumps(expansion.keywords), json.dumps(expansion.causal_relationships),
                 "active", now, now),
            )
            expansion_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()

        result   = ext.DynamicWatchlistManager.add_tickers(validation.valid, user_id, ext.DB_PATH,
                                                             sector_label=expansion.sector_label)
        promoted = result["promoted"]; staged = result["staged"]
        edges_seeded = ext.seed_causal_edges(expansion.causal_relationships, ext.DB_PATH)
        for t in promoted:
            ext.bootstrap_ticker_async(t, ext.DB_PATH)
        ext.write_universe_atoms(user_id, validation.valid, data.description, ext.DB_PATH)
        return {
            "expansion_id":                expansion_id,
            "resolved_tickers":            validation.valid,
            "rejected_tickers":            validation.rejected,
            "staging_tickers":             staged,
            "causal_edges_seeded":         edges_seeded,
            "estimated_bootstrap_seconds": ext.estimate_bootstrap_seconds(len(promoted), ext.DB_PATH),
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/universe")
async def get_user_universe(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        tickers = ext.DynamicWatchlistManager.get_user_tickers(user_id, ext.DB_PATH)
        result  = []
        for t in tickers:
            ct = ext.compute_coverage_tier(t, ext.DB_PATH)
            result.append({"ticker": t,
                           "coverage_tier":  ct.tier if ct else "unknown",
                           "coverage_count": ct.coverage_count if ct else 0})
        return {"tickers": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.delete("/users/{user_id}/universe/{ticker}")
async def remove_universe_ticker(user_id: str, ticker: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        removed = ext.DynamicWatchlistManager.remove_ticker(ticker, user_id, ext.DB_PATH)
        if not removed:
            raise HTTPException(404, detail="ticker not found or not owned by this user")
        return {"removed": ticker.upper(), "ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/universe/bootstrap-status")
async def universe_bootstrap_status(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        return ext.DynamicWatchlistManager.get_bootstrap_status(user_id, ext.DB_PATH)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/universe/staging")
async def user_universe_staging(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        rows = ext.get_staged_tickers(ext.DB_PATH, user_id=user_id)
        return {"staging": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/preferences/focus")
async def set_user_focus(user_id: str, data: SetFocusRequest, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        from users.personal_kb import write_atom as pkb_write
        written = []
        if data.preferred_upside_min is not None:
            pkb_write(user_id, user_id, "preferred_upside_min",
                      str(float(data.preferred_upside_min)), 0.9, "user_override", ext.DB_PATH)
            written.append("preferred_upside_min")
        if data.preferred_pattern is not None:
            pkb_write(user_id, user_id, "preferred_pattern",
                      str(data.preferred_pattern), 0.9, "user_override", ext.DB_PATH)
            written.append("preferred_pattern")
        return {"updated": written, "ok": True}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/engagement")
async def log_user_engagement(user_id: str, data: EngagementRequest,
                              _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    if not data.event_type:
        raise HTTPException(400, detail="event_type is required")
    try:
        ext.log_engagement_event(ext.DB_PATH, user_id, data.event_type,
                                 ticker=data.ticker, pattern_type=data.pattern_type,
                                 sector=data.sector)
        ext.update_from_engagement(user_id, ext.DB_PATH)
        return {"logged": True}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/kb-context")
async def user_kb_context(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        from users.personal_kb import read_atoms as pkb_read
        atoms = pkb_read(user_id, ext.DB_PATH)
        return {"atoms": atoms, "count": len(atoms)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/preferences/inferred")
async def user_inferred_preferences(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        ctx = ext.get_context_document(user_id, ext.DB_PATH)
        return {
            "sector_affinity":        ctx.sector_affinity,
            "risk_tolerance":         ctx.risk_tolerance,
            "holding_style":          ctx.holding_style,
            "portfolio_beta":         ctx.portfolio_beta,
            "preferred_pattern":      ctx.preferred_pattern,
            "avg_win_rate":           ctx.avg_win_rate,
            "high_engagement_sector": ctx.high_engagement_sector,
            "low_engagement_sector":  ctx.low_engagement_sector,
            "preferred_upside_min":   ctx.preferred_upside_min,
            "active_universe":        ctx.active_universe,
            "pattern_hit_rates":      ctx.pattern_hit_rates,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/model")
async def user_model_get(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        model = ext.get_user_model(ext.DB_PATH, user_id)
        if model is None:
            raise HTTPException(404, detail="no model found — submit portfolio first")
        return model
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/cash")
async def user_cash_get(user_id: str, _: str = Depends(user_path_auth)):
    try:
        from users.user_store import get_available_cash
        return get_available_cash(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/cash")
async def user_cash_post(user_id: str, data: CashRequest, _: str = Depends(user_path_auth)):
    cash_currency = (data.cash_currency or "GBP").upper().strip() or "GBP"
    if data.available_cash is None:
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=5)
            conn.execute("UPDATE user_preferences SET available_cash = NULL WHERE user_id = ?", (user_id,))
            conn.commit()
            conn.close()
            return {"user_id": user_id, "available_cash": None, "cash_currency": cash_currency}
        except Exception as e:
            raise HTTPException(500, detail=str(e))
    try:
        from users.user_store import update_available_cash
        return update_available_cash(ext.DB_PATH, user_id, data.available_cash,
                                     cash_currency=cash_currency)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/positions/open")
async def user_positions_open(user_id: str, _: str = Depends(user_path_auth)):
    try:
        from users.user_store import get_user_open_positions
        positions = get_user_open_positions(ext.DB_PATH, user_id)
        return {"user_id": user_id, "positions": positions, "count": len(positions)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/positions/closed")
async def user_positions_closed(user_id: str, since: str = "", _: str = Depends(user_path_auth)):
    try:
        from users.user_store import get_recently_closed_positions
        if not since:
            since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        positions = get_recently_closed_positions(ext.DB_PATH, user_id, since)
        return {"user_id": user_id, "positions": positions, "count": len(positions), "since": since}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/history/screenshot")
async def user_portfolio_screenshot(
    user_id: str,
    file: UploadFile = File(...),
    _: str = Depends(user_path_auth),
):
    from llm.ollama_client import chat_vision, list_models, VISION_MODEL
    available_models = list_models()
    vision_available = any(VISION_MODEL.split(":")[0] in m for m in available_models)
    if not vision_available:
        return {"holdings": [], "vision_available": False,
                "reason": "vision_model_unavailable", "available_models": available_models}

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(400, detail="file must be an image (image/png or image/jpeg)")

    image_bytes = await file.read()
    if len(image_bytes) > 10 * 1024 * 1024:
        raise HTTPException(400, detail="image too large (max 10 MB)")

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    prompt = (
        "This is a screenshot of a stock brokerage portfolio page. "
        "Extract all stock holdings visible in the image. "
        "For each holding, identify: the ticker symbol, the quantity held, and the average cost/price per share if visible. "
        "LSE-listed UK stocks use a .L suffix (e.g. SHEL.L, BARC.L). "
        "Respond with ONLY valid JSON — no markdown, no explanation. "
        'Format: [{"ticker": "SHEL.L", "quantity": 10, "avg_cost": 27.50}, ...] '
        "If avg_cost is not visible, set it to null. "
        "If no holdings are visible, return []."
    )
    try:
        raw = chat_vision(image_b64, prompt, timeout=90)
        if not raw:
            return {"holdings": [], "vision_available": True, "reason": "model_returned_empty"}
        raw = raw.strip()
        if raw.startswith("```"):
            raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
        holdings = json.loads(raw)
        if not isinstance(holdings, list):
            holdings = []
        clean = []
        for h in holdings:
            ticker = str(h.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            try:
                qty = float(h.get("quantity") or 0)
            except (TypeError, ValueError):
                qty = 0.0
            avg_cost = h.get("avg_cost")
            try:
                avg_cost = float(avg_cost) if avg_cost is not None else None
            except (TypeError, ValueError):
                avg_cost = None
            clean.append({"ticker": ticker, "quantity": qty, "avg_cost": avg_cost})
        return {"holdings": clean, "vision_available": True, "count": len(clean)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/snapshot/preview")
async def user_snapshot_preview(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        snapshot = ext.curate_snapshot(user_id, ext.DB_PATH)
        return ext.snapshot_to_dict(snapshot)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/snapshot/send-now")
@limiter.limit(RATE_LIMITS["snapshot"])
async def user_snapshot_send_now(request: Request, user_id: str,
                                 _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        from users.user_store import log_delivery
        user    = ext.get_user(ext.DB_PATH, user_id)
        chat_id = (user or {}).get("telegram_chat_id")
        if not chat_id:
            raise HTTPException(400, detail="no telegram_chat_id — complete onboarding first")
        snapshot   = ext.curate_snapshot(user_id, ext.DB_PATH)
        message    = ext.format_snapshot(snapshot)
        notifier   = ext.TelegramNotifier()
        sent       = notifier.send(chat_id, message)
        local_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_delivery(ext.DB_PATH, user_id, success=sent, message_length=len(message),
                     regime_at_delivery=snapshot.market_regime,
                     opportunities_count=len(snapshot.top_opportunities),
                     local_date=local_date)
        return {"sent": sent, "opportunities": len(snapshot.top_opportunities),
                "regime": snapshot.market_regime}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/delivery-history")
async def user_delivery_history(user_id: str, limit: int = 30, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        history = ext.get_delivery_history(ext.DB_PATH, user_id, limit=limit)
        return {"user_id": user_id, "history": history, "count": len(history)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/tip/preview")
async def tip_preview(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT tier, tip_timeframes, tip_pattern_types, tip_markets, "
            "account_size, max_risk_per_trade_pct, account_currency "
            "FROM user_preferences WHERE user_id = ?", (user_id,)
        ).fetchone()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    if row is None:
        raise HTTPException(404, detail="user not found")
    cols  = ["tier","tip_timeframes","tip_pattern_types","tip_markets",
             "account_size","max_risk_per_trade_pct","account_currency"]
    prefs = dict(zip(cols, row))
    tier  = prefs.get("tier") or "basic"
    for jcol in ("tip_timeframes","tip_pattern_types","tip_markets"):
        try:
            prefs[jcol] = json.loads(prefs[jcol]) if prefs[jcol] else None
        except Exception:
            prefs[jcol] = None
    from core.tiers import TIER_CONFIG as TIER_LIMITS
    limits         = TIER_LIMITS.get(tier, TIER_LIMITS["basic"])
    tip_timeframes = prefs.get("tip_timeframes") or limits["timeframes"]
    tip_pattern_tys = prefs.get("tip_pattern_types")
    tip_markets    = prefs.get("tip_markets")
    delivery_days  = limits.get("delivery_days","daily")
    is_weekly      = delivery_days != "daily"
    from analytics.pattern_detector import PatternSignal
    if is_weekly:
        from notifications.tip_scheduler import _pick_batch
        batch_size = limits.get("batch_size",3)
        batch, tip_source = _pick_batch(ext.DB_PATH, user_id, tier, tip_timeframes,
                                        tip_pattern_tys, tip_markets, batch_size)
        if not batch:
            return {"tip":None,"tips":[],"reason":"no eligible patterns","cadence":"weekly","tip_source":None}
        tips = []
        for r in batch:
            sig = PatternSignal(
                pattern_type=r["pattern_type"], ticker=r["ticker"], direction=r["direction"],
                zone_high=r["zone_high"], zone_low=r["zone_low"], zone_size_pct=r["zone_size_pct"],
                timeframe=r["timeframe"], formed_at=r["formed_at"],
                quality_score=r["quality_score"] or 0.0, status=r["status"],
                kb_conviction=r.get("kb_conviction",""), kb_regime=r.get("kb_regime",""),
                kb_signal_dir=r.get("kb_signal_dir",""),
            )
            tips.append(ext.tip_to_dict(sig, ext.calculate_position(sig, prefs), tier=tier))
        return {"tip": tips[0] if tips else None, "tips": tips,
                "tip_source": tip_source, "cadence": "weekly", "delivery_days": delivery_days}

    from notifications.tip_scheduler import _pick_best_pattern
    pr = _pick_best_pattern(ext.DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys, tip_markets)
    if pr is None:
        return {"tip":None,"tips":[],"reason":"no eligible patterns","cadence":"daily","tip_source":None}
    sig = PatternSignal(
        pattern_type=pr["pattern_type"], ticker=pr["ticker"], direction=pr["direction"],
        zone_high=pr["zone_high"], zone_low=pr["zone_low"], zone_size_pct=pr["zone_size_pct"],
        timeframe=pr["timeframe"], formed_at=pr["formed_at"],
        quality_score=pr["quality_score"] or 0.0, status=pr["status"],
        kb_conviction=pr.get("kb_conviction",""), kb_regime=pr.get("kb_regime",""),
        kb_signal_dir=pr.get("kb_signal_dir",""),
    )
    position = ext.calculate_position(sig, prefs)
    tip_dict = ext.tip_to_dict(sig, position, tier=tier)
    return {"tip": tip_dict, "tips": [tip_dict], "tip_source": pr.get("tip_source"), "cadence": "daily"}


@router.get("/users/{user_id}/tip/history")
async def tip_history(user_id: str, limit: int = 30, _: str = Depends(user_path_auth)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        history = ext.get_tip_history(ext.DB_PATH, user_id, limit=limit)
        return {"history": history, "count": len(history)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/profile")
async def update_user_profile(
    user_id: str, data: ProfileRequest,
    current_user: str = Depends(get_current_user),
):
    if current_user != user_id:
        raise HTTPException(403, detail="forbidden")
    first_name = data.first_name.strip()[:100]
    last_name  = data.last_name.strip()[:100]
    phone      = data.phone.strip()[:30]
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        for col in ("first_name","last_name","phone"):
            try:
                conn.execute(f"ALTER TABLE user_auth ADD COLUMN {col} TEXT DEFAULT ''")
            except Exception:
                pass
        conn.execute("UPDATE user_auth SET first_name=?, last_name=?, phone=? WHERE user_id=?",
                     (first_name, last_name, phone, user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return {"ok": True, "first_name": first_name, "last_name": last_name, "phone": phone}


@router.post("/users/{user_id}/portfolio/generate-sim", status_code=201)
async def user_portfolio_generate_sim(user_id: str, _: str = Depends(user_path_auth)):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")

    _ARCHETYPES = [
        {"key":"conservative_income","title":"Conservative Income Trader",
         "description":"Focuses on FTSE defensive names and dividend payers.",
         "tips_alignment":"Tips favour low-risk setups: mitigation blocks and IFVG patterns.",
         "risk_tolerance":"conservative","holding_style":"value",
         "sectors":["utilities","consumer_staples","healthcare","financials"],
         "holdings":[{"ticker":"ULVR.L","quantity":120,"avg_cost":3820.0,"sector":"consumer_staples"},
                     {"ticker":"NG.L","quantity":400,"avg_cost":1042.0,"sector":"utilities"},
                     {"ticker":"TSCO.L","quantity":350,"avg_cost":295.0,"sector":"consumer_staples"},
                     {"ticker":"GSK.L","quantity":180,"avg_cost":1685.0,"sector":"healthcare"},
                     {"ticker":"BATS.L","quantity":160,"avg_cost":2460.0,"sector":"consumer_staples"},
                     {"ticker":"NWG.L","quantity":900,"avg_cost":285.0,"sector":"financials"}]},
        {"key":"ftse_momentum","title":"FTSE Momentum Trader",
         "description":"Chases high-conviction breakouts in FTSE growth names.",
         "tips_alignment":"Tips favour momentum breakouts: FVG and order block patterns.",
         "risk_tolerance":"moderate","holding_style":"momentum",
         "sectors":["technology","industrials","healthcare","financials"],
         "holdings":[{"ticker":"AZN.L","quantity":80,"avg_cost":11200.0,"sector":"healthcare"},
                     {"ticker":"LSEG.L","quantity":100,"avg_cost":9850.0,"sector":"financials"},
                     {"ticker":"RR.L","quantity":600,"avg_cost":415.0,"sector":"industrials"},
                     {"ticker":"BA.L","quantity":250,"avg_cost":1295.0,"sector":"industrials"},
                     {"ticker":"AUTO.L","quantity":200,"avg_cost":630.0,"sector":"technology"},
                     {"ticker":"SAGE.L","quantity":220,"avg_cost":1105.0,"sector":"technology"}]},
        {"key":"energy_commodities","title":"Commodities & Energy Trader",
         "description":"Concentrated in FTSE energy and mining.",
         "tips_alignment":"Tips favour commodity cycle plays.",
         "risk_tolerance":"aggressive","holding_style":"mixed",
         "sectors":["energy","materials","mining"],
         "holdings":[{"ticker":"SHEL.L","quantity":200,"avg_cost":2680.0,"sector":"energy"},
                     {"ticker":"BP.L","quantity":500,"avg_cost":445.0,"sector":"energy"},
                     {"ticker":"RIO.L","quantity":120,"avg_cost":4950.0,"sector":"materials"},
                     {"ticker":"GLEN.L","quantity":800,"avg_cost":420.0,"sector":"materials"},
                     {"ticker":"AAL.L","quantity":450,"avg_cost":225.0,"sector":"materials"},
                     {"ticker":"BHP.L","quantity":150,"avg_cost":2150.0,"sector":"materials"}]},
    ]

    seed_int = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
    archetype = _ARCHETYPES[seed_int % len(_ARCHETYPES)]
    rng = random.Random(seed_int)
    holdings = [{"ticker": h["ticker"], "quantity": h["quantity"],
                 "avg_cost": round(h["avg_cost"] * (1.0 + rng.uniform(-0.10,0.10)), 2),
                 "sector": h["sector"]} for h in archetype["holdings"]]
    try:
        result = ext.upsert_portfolio(ext.DB_PATH, user_id, holdings)
        model  = ext.build_user_model(user_id, ext.DB_PATH)
        result["model"] = model
        if ext.HAS_HYBRID:
            try:
                ext.infer_and_write_from_portfolio(user_id, ext.DB_PATH)
            except Exception:
                pass
        return {"simulated": True, "archetype": archetype["key"], "title": archetype["title"],
                "description": archetype["description"], "tips_alignment": archetype["tips_alignment"],
                "risk_tolerance": archetype["risk_tolerance"], "holding_style": archetype["holding_style"],
                "sectors": archetype["sectors"], "holdings": holdings,
                "count": len(holdings), "model": model}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/notify/test")
async def notify_test(data: NotifyTestRequest):
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    if not data.chat_id:
        raise HTTPException(400, detail="chat_id is required")
    try:
        notifier = ext.TelegramNotifier()
        sent = notifier.send_test(data.chat_id)
        return {"sent": sent}
    except Exception as e:
        raise HTTPException(500, detail=str(e))
