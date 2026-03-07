"""routes_v2/waitlist.py — Phase 6: waitlist endpoints."""

from __future__ import annotations

import logging
import os
import sqlite3

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_rate_limiter import limiter

router = APIRouter()
_logger = logging.getLogger(__name__)


def _ensure_waitlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            email     TEXT PRIMARY KEY,
            joined_at TEXT NOT NULL DEFAULT (datetime('now')),
            source    TEXT DEFAULT 'landing'
        )
    """)
    conn.commit()


def _notify_waitlist_telegram(email: str) -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id   = os.environ.get("WAITLIST_TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        return
    try:
        import json
        import urllib.request
        payload = json.dumps({"chat_id": chat_id, "text": f"\U0001f680 New waitlist signup: {email}"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


class WaitlistRequest(BaseModel):
    email: str
    source: str = "landing"


@router.post("/waitlist")
@limiter.limit("3/hour")
async def waitlist_join(request: Request, data: WaitlistRequest):
    try:
        email = data.email.strip().lower()
        if not email or "@" not in email or len(email) > 254:
            raise HTTPException(400, detail="Invalid email")
        source = data.source[:64]
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        cur = conn.execute(
            "INSERT OR IGNORE INTO waitlist (email, joined_at, source) VALUES (?, datetime('now'), ?)",
            (email, source),
        )
        conn.commit()
        already = cur.rowcount == 0
        conn.close()
        if not already:
            _notify_waitlist_telegram(email)
        msg = "You're already on the list" if already else "You're on the list"
        return {"message": msg, "already": already}
    except HTTPException:
        raise
    except Exception as e:
        _logger.error("waitlist_join error: %s", e)
        raise HTTPException(500, detail="Something went wrong")


@router.get("/waitlist/count")
async def waitlist_count():
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        row = conn.execute("SELECT COUNT(*) FROM waitlist").fetchone()
        conn.close()
        return {"count": row[0] if row else 0}
    except Exception:
        return {"count": 0}
