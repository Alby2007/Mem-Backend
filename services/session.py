"""
services/session.py — Thread-safe session state manager.

Replaces the three global dicts (_session_streaks, _session_tickers,
_session_portfolio_tickers) that were shared unsafely across Gunicorn
threads.

Each dict is guarded by its own threading.Lock so concurrent /chat
requests on different threads cannot corrupt each other's state.

Includes TTL-based cleanup to prevent unbounded memory growth on
long-running processes.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional


# Default TTL: 2 hours — sessions idle longer than this are evicted.
_DEFAULT_TTL_SEC = 7200

# Cleanup runs at most once per this interval (seconds).
_CLEANUP_INTERVAL_SEC = 300


class SessionManager:
    """Thread-safe manager for per-session state with TTL eviction."""

    def __init__(self, ttl_sec: int = _DEFAULT_TTL_SEC):
        self._ttl_sec = ttl_sec

        # { session_id: { 'streak': int, 'last_stress': float } }
        self._streaks: Dict[str, dict] = {}
        self._streaks_lock = threading.Lock()

        # { session_id: [list of canonical ticker strings] }
        self._tickers: Dict[str, List[str]] = {}
        self._tickers_lock = threading.Lock()

        # { session_id: [list of portfolio ticker strings] }
        self._portfolio_tickers: Dict[str, List[str]] = {}
        self._portfolio_tickers_lock = threading.Lock()

        # { session_id: float(timestamp) } — last-access time
        self._last_access: Dict[str, float] = {}
        self._access_lock = threading.Lock()

        self._last_cleanup = time.monotonic()

    # ── Internal helpers ──────────────────────────────────────────────────

    def _touch(self, session_id: str) -> None:
        """Update last-access timestamp for a session."""
        with self._access_lock:
            self._last_access[session_id] = time.monotonic()

    def _maybe_cleanup(self) -> None:
        """Evict expired sessions if enough time has passed since last cleanup."""
        now = time.monotonic()
        if now - self._last_cleanup < _CLEANUP_INTERVAL_SEC:
            return
        self._last_cleanup = now
        # Run cleanup in a daemon thread to avoid blocking request threads
        threading.Thread(target=self._cleanup, daemon=True).start()

    def _cleanup(self) -> None:
        """Remove sessions that haven't been accessed within TTL."""
        now = time.monotonic()
        cutoff = now - self._ttl_sec

        with self._access_lock:
            expired = [
                sid for sid, ts in self._last_access.items()
                if ts < cutoff
            ]
            for sid in expired:
                del self._last_access[sid]

        if not expired:
            return

        with self._streaks_lock:
            for sid in expired:
                self._streaks.pop(sid, None)
        with self._tickers_lock:
            for sid in expired:
                self._tickers.pop(sid, None)
        with self._portfolio_tickers_lock:
            for sid in expired:
                self._portfolio_tickers.pop(sid, None)

    # ── Streaks ───────────────────────────────────────────────────────────

    def get_streak(self, session_id: str) -> dict:
        """Get or create the streak dict for a session.

        Returns a *copy* — callers must use set_streak() to persist changes.
        """
        self._touch(session_id)
        self._maybe_cleanup()
        with self._streaks_lock:
            entry = self._streaks.setdefault(
                session_id, {'streak': 0, 'last_stress': 0.0}
            )
            return dict(entry)

    def set_streak(self, session_id: str, streak: dict) -> None:
        """Persist updated streak dict for a session."""
        self._touch(session_id)
        with self._streaks_lock:
            self._streaks[session_id] = dict(streak)

    def reset_streak(self, session_id: str) -> None:
        """Reset streak to zero for a session."""
        self._touch(session_id)
        with self._streaks_lock:
            self._streaks[session_id] = {'streak': 0, 'last_stress': 0.0}

    def all_streaks(self) -> Dict[str, dict]:
        """Return a snapshot of all streak dicts (for /adapt/status)."""
        with self._streaks_lock:
            return {sid: dict(s) for sid, s in self._streaks.items()}

    def active_streak_count(self) -> int:
        """Count sessions with streak > 0."""
        with self._streaks_lock:
            return sum(1 for s in self._streaks.values() if s.get('streak', 0) > 0)

    def total_streak_count(self) -> int:
        """Total number of tracked sessions."""
        with self._streaks_lock:
            return len(self._streaks)

    def has_streak(self, session_id: str) -> bool:
        """Check if a streak entry exists for a session."""
        with self._streaks_lock:
            return session_id in self._streaks

    # ── Tickers ───────────────────────────────────────────────────────────

    def get_tickers(self, session_id: str) -> Optional[List[str]]:
        """Get the ticker list for a session, or None if not set."""
        self._touch(session_id)
        self._maybe_cleanup()
        with self._tickers_lock:
            val = self._tickers.get(session_id)
            return list(val) if val is not None else None

    def set_tickers(self, session_id: str, tickers: List[str]) -> None:
        """Set the ticker list for a session."""
        self._touch(session_id)
        with self._tickers_lock:
            self._tickers[session_id] = list(tickers)

    def has_tickers(self, session_id: str) -> bool:
        """Check if tickers are set for a session."""
        with self._tickers_lock:
            return session_id in self._tickers

    def pop_tickers(self, session_id: str) -> Optional[List[str]]:
        """Remove and return tickers for a session."""
        with self._tickers_lock:
            return self._tickers.pop(session_id, None)

    # ── Portfolio tickers ─────────────────────────────────────────────────

    def get_portfolio_tickers(self, session_id: str) -> Optional[List[str]]:
        """Get portfolio tickers for a session, or None if not set."""
        self._touch(session_id)
        self._maybe_cleanup()
        with self._portfolio_tickers_lock:
            val = self._portfolio_tickers.get(session_id)
            return list(val) if val is not None else None

    def set_portfolio_tickers(self, session_id: str, tickers: List[str]) -> None:
        """Set portfolio tickers for a session."""
        self._touch(session_id)
        with self._portfolio_tickers_lock:
            self._portfolio_tickers[session_id] = list(tickers)

    def pop_portfolio_tickers(self, session_id: str) -> Optional[List[str]]:
        """Remove and return portfolio tickers for a session."""
        with self._portfolio_tickers_lock:
            return self._portfolio_tickers.pop(session_id, None)

    # ── Bulk clear ────────────────────────────────────────────────────────

    def clear_session(self, session_id: str) -> None:
        """Remove all state for a session."""
        with self._streaks_lock:
            self._streaks.pop(session_id, None)
        with self._tickers_lock:
            self._tickers.pop(session_id, None)
        with self._portfolio_tickers_lock:
            self._portfolio_tickers.pop(session_id, None)
        with self._access_lock:
            self._last_access.pop(session_id, None)
