"""
services/execution_gateway.py — Execution payload delivery on user confirmation.

When a user taps YES on a Telegram entry alert, this module:
1. Builds a signed JSON payload from the tip_followup row
2. POSTs it to the user's registered broker_webhook_url
3. Logs the attempt in execution_log

The user's webhook receiver places the actual order.
Trading Galaxy never holds broker credentials.

PAYLOAD FORMAT
==============
{
  "signal_id":    "tf_42",
  "ticker":       "DPLM.L",
  "direction":    "bullish",
  "action":       "entry",
  "entry":        4.82,
  "stop":         4.61,
  "target_1":     5.14,
  "target_2":     5.47,
  "size_pct":     1.0,
  "size_gbp":     2400.0,
  "pattern_type": "liquidity_void",
  "timeframe":    "4h",
  "conviction":   "medium",
  "timestamp":    "2026-03-18T18:44:00Z",
  "source":       "trading_galaxy_meridian"
}

SIGNING
=======
Header: X-TG-Signature: sha256=<hex>
HMAC-SHA256 of body bytes using broker_webhook_secret.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)


def _build_payload(row: dict) -> dict:
    return {
        "signal_id":    f"tf_{row['id']}",
        "ticker":       row.get("ticker", ""),
        "direction":    (row.get("direction") or "bullish").lower(),
        "action":       "entry",
        "entry":        float(row.get("target_entry") or row.get("entry_price") or 0),
        "stop":         float(row.get("stop_loss") or 0) or None,
        "target_1":     float(row.get("target_1") or 0) or None,
        "target_2":     float(row.get("target_2") or 0) or None,
        "size_pct":     float(row.get("max_risk_per_trade_pct") or 1.0),
        "size_gbp":     float(row.get("position_size") or 0) or None,
        "pattern_type": row.get("pattern_type", ""),
        "timeframe":    row.get("timeframe", ""),
        "conviction":   row.get("conviction_at_entry", ""),
        "timestamp":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":       "trading_galaxy_meridian",
    }


def _sign_payload(body_bytes: bytes, secret: str) -> str:
    sig = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def deliver_execution_signal(db_path: str, followup_id: int, user_id: str) -> bool:
    """
    Build and POST the execution payload to the user's webhook URL.
    Returns True on success or if no webhook configured (graceful no-op).
    """
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        tf = conn.execute(
            "SELECT * FROM tip_followups WHERE id=? AND user_id=?",
            (followup_id, user_id),
        ).fetchone()
        if not tf:
            conn.close()
            return False
        tf = dict(tf)

        prefs = conn.execute(
            "SELECT broker_webhook_url, broker_webhook_secret, max_risk_per_trade_pct FROM user_preferences WHERE user_id=?",
            (user_id,),
        ).fetchone()
        conn.close()

        webhook_url    = prefs["broker_webhook_url"] if prefs else None
        webhook_secret = prefs["broker_webhook_secret"] if prefs else None
        if prefs and prefs["max_risk_per_trade_pct"]:
            tf["max_risk_per_trade_pct"] = prefs["max_risk_per_trade_pct"]
    except Exception as e:
        _log.error("ExecutionGateway: DB error %d: %s", followup_id, e)
        return False

    if not webhook_url:
        _log.info("ExecutionGateway: no webhook for %s — intent logged", user_id)
        _log_execution(db_path, followup_id, user_id, "no_webhook", None)
        return True

    payload = _build_payload(tf)
    body    = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent":   "TradingGalaxy-Meridian/1.0",
    }
    if webhook_secret:
        headers["X-TG-Signature"] = _sign_payload(body, webhook_secret)

    try:
        import requests as _rq
        resp    = _rq.post(webhook_url, data=body, headers=headers, timeout=8)
        success = resp.status_code < 300
        _log.info("ExecutionGateway: %s HTTP %d (followup=%d)",
                  webhook_url[:40], resp.status_code, followup_id)
        _log_execution(db_path, followup_id, user_id,
                       "delivered" if success else "delivery_failed",
                       resp.status_code)
        return success
    except Exception as e:
        _log.error("ExecutionGateway: POST failed %d: %s", followup_id, e)
        _log_execution(db_path, followup_id, user_id, "delivery_error", None)
        return False


def _log_execution(db_path: str, followup_id: int, user_id: str,
                   outcome: str, http_status: Optional[int]) -> None:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS execution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                followup_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                http_status INTEGER,
                logged_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT INTO execution_log (followup_id, user_id, outcome, http_status, logged_at) VALUES (?,?,?,?,?)",
            (followup_id, user_id, outcome, http_status, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        _log.warning("ExecutionGateway: log write failed: %s", e)
