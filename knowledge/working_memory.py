"""
knowledge/working_memory.py — On-Demand Fetch → Working Memory → KB Commit Loop

When the retrieval layer finds no atoms for a ticker, the chat endpoint calls
fetch_on_demand() to pull live data for that ticker into a per-session working
memory store.  After the LLM response is built, commit_session() decides which
atoms are worth writing back to the persistent KB (confidence ≥ 0.70).

Design:
  - Sessions are in-memory dicts — ephemeral by design, no DB overhead
  - fetch_on_demand caps at 2 tickers per request to bound latency
  - yf.fast_info for price (~300ms), yf.info for direction/regime (~500ms)
  - Recent headlines come from existing KB catalyst atoms (no new HTTP call)
  - Commit writes via kg.add_fact() — same path as all scheduled ingest
  - Staleness: atom is "missing" if KB has 0 last_price rows for ticker

Zero-LLM, pure Python.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

_logger = logging.getLogger(__name__)

# Only commit atoms at or above this confidence level back to the persistent KB
_COMMIT_THRESHOLD = 0.70

# Max on-demand fetches per chat request (latency guard)
MAX_ON_DEMAND_TICKERS = 2

# A ticker is considered "missing" from the KB if it has fewer than this many
# last_price atoms (catches both brand-new and recently-cleared tickers)
_MISSING_THRESHOLD = 1


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class CommitResult:
    committed: int = 0
    discarded: int = 0
    tickers:   List[str] = field(default_factory=list)


@dataclass
class _Session:
    session_id: str
    atoms:      List[dict] = field(default_factory=list)
    fetch_log:  List[str]  = field(default_factory=list)


# ── Staleness check ────────────────────────────────────────────────────────────

def kb_has_atoms(ticker: str, db_path: str) -> bool:
    """Return True if the KB has at least one last_price atom for this ticker."""
    try:
        conn = sqlite3.connect(db_path, timeout=3)
        row = conn.execute(
            "SELECT COUNT(*) FROM facts WHERE LOWER(subject) = ? AND predicate = 'last_price'",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        return (row[0] if row else 0) >= _MISSING_THRESHOLD
    except Exception:
        return True  # on error assume present — don't add latency


# ── Helpers ────────────────────────────────────────────────────────────────────

def _price_regime_from_ratio(ratio: float) -> str:
    """Map 52-week position ratio to a price_regime label."""
    if ratio >= 0.85:
        return 'near_52w_high'
    if ratio <= 0.20:
        return 'near_52w_low'
    return 'mid_range'


def _direction_from_target(current: float, target: float) -> str:
    if target <= 0 or current <= 0:
        return 'neutral'
    pct = (target - current) / current
    if pct > 0.10:
        return 'long'
    if pct < -0.10:
        return 'short'
    return 'neutral'


def _should_commit(atom: dict) -> bool:
    pred = atom.get('predicate', '')
    if pred in ('last_price', 'price_regime', 'signal_direction', 'market_cap_tier'):
        return True
    return atom.get('confidence', 0.0) >= _COMMIT_THRESHOLD


# ── WorkingMemory ──────────────────────────────────────────────────────────────

class WorkingMemory:
    """
    Per-process singleton (created once in api.py).
    Thread-safe for reads; GIL protects the dict writes on CPython.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}

    # ── Session lifecycle ──────────────────────────────────────────────────────

    def open_session(self, session_id: str) -> None:
        if session_id not in self._sessions:
            self._sessions[session_id] = _Session(session_id=session_id)

    def close_without_commit(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    # ── On-demand fetch ────────────────────────────────────────────────────────

    def fetch_on_demand(self, ticker: str, session_id: str, db_path: str) -> List[dict]:
        """
        Fetch live price + fundamentals for a ticker absent from the KB.
        Atoms are stored in the session only — not written to KB yet.
        Returns the list of new atoms fetched.
        """
        try:
            import yfinance as yf
        except ImportError:
            _logger.warning('yfinance not installed — on-demand fetch skipped')
            return []

        session = self._sessions.get(session_id)
        if session is None:
            self.open_session(session_id)
            session = self._sessions[session_id]

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[dict] = []

        try:
            t = yf.Ticker(ticker)
            fi = t.fast_info

            # last_price
            price = getattr(fi, 'last_price', None) or getattr(fi, 'regularMarketPrice', None)
            if price:
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'last_price',
                    'object':     f'{price:.4f}',
                    'confidence': 0.95,
                    'source':     'exchange_feed_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })

            # price_regime from 52w position
            high_52w = getattr(fi, 'year_high', None) or getattr(fi, 'fiftyTwoWeekHigh', None)
            low_52w  = getattr(fi, 'year_low',  None) or getattr(fi, 'fiftyTwoWeekLow',  None)
            if price and high_52w and low_52w and high_52w > low_52w:
                ratio = (price - low_52w) / (high_52w - low_52w)
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'price_regime',
                    'object':     _price_regime_from_ratio(ratio),
                    'confidence': 0.85,
                    'source':     'exchange_feed_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })

        except Exception as e:
            _logger.debug('fast_info fetch failed for %s: %s', ticker, e)

        # Analyst target → signal_direction (slower path, best-effort)
        try:
            t = yf.Ticker(ticker)
            info = t.info
            target = info.get('targetMeanPrice') or info.get('targetMedianPrice')
            price_now = info.get('regularMarketPrice') or info.get('currentPrice')
            if target and price_now:
                direction = _direction_from_target(float(price_now), float(target))
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'signal_direction',
                    'object':     direction,
                    'confidence': 0.75,
                    'source':     'broker_research_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })
                upside = round((float(target) - float(price_now)) / float(price_now) * 100, 1)
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'upside_pct',
                    'object':     str(upside),
                    'confidence': 0.75,
                    'source':     'broker_research_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })
        except Exception as e:
            _logger.debug('info fetch failed for %s: %s', ticker, e)

        # Recent headlines — query existing KB catalyst atoms (no new HTTP call)
        try:
            conn = sqlite3.connect(db_path, timeout=3)
            rows = conn.execute(
                """SELECT object, confidence, source FROM facts
                   WHERE LOWER(subject) = ? AND predicate IN ('catalyst','risk_factor')
                   ORDER BY confidence DESC LIMIT 3""",
                (ticker.lower(),),
            ).fetchall()
            conn.close()
            for obj, conf, src in rows:
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'catalyst',
                    'object':     obj,
                    'confidence': float(conf),
                    'source':     src,
                    'fetched_at': now_iso,
                    'upsert':     False,
                })
        except Exception as e:
            _logger.debug('KB catalyst lookup failed for %s: %s', ticker, e)

        session.atoms.extend(atoms)
        session.fetch_log.append(f'{ticker} fetched at {now_iso} ({len(atoms)} atoms)')
        _logger.info('on-demand fetch: %s → %d atoms', ticker, len(atoms))
        return atoms

    # ── Session context for prompt injection ──────────────────────────────────

    def get_session_snippet(self, session_id: str) -> str:
        """
        Return session atoms as a formatted context string for LLM injection.
        Returns empty string if no session atoms.
        """
        session = self._sessions.get(session_id)
        if not session or not session.atoms:
            return ''

        lines = ['=== LIVE DATA (fetched this session) ===']
        for a in session.atoms:
            ts = a.get('fetched_at', '')[:16]  # trim to minute
            lines.append(
                f"{a['subject']} | {a['predicate']} | {a['object']}"
                f"  [conf:{a['confidence']:.2f}, live:{ts}]"
            )
        if session.fetch_log:
            lines.append(f'Fetch log: {"; ".join(session.fetch_log)}')
        return '\n'.join(lines)

    def get_fetched_tickers(self, session_id: str) -> List[str]:
        session = self._sessions.get(session_id)
        if not session:
            return []
        return list({
            a['subject'] for a in session.atoms
            if 'fetched_at' in a
        })

    # ── Commit ────────────────────────────────────────────────────────────────

    def commit_session(self, session_id: str, kg) -> CommitResult:
        """
        Write high-confidence session atoms back to the persistent KB via kg.add_fact().
        Cleans up the session afterwards.
        """
        session = self._sessions.get(session_id)
        if not session:
            return CommitResult()

        result = CommitResult()
        committed_tickers: set = set()

        for atom in session.atoms:
            if not _should_commit(atom):
                result.discarded += 1
                continue
            try:
                ok = kg.add_fact(
                    subject=atom['subject'],
                    predicate=atom['predicate'],
                    object=atom['object'],
                    confidence=atom['confidence'],
                    source=atom['source'],
                    metadata={'fetched_at': atom.get('fetched_at', ''), 'on_demand': True},
                    upsert=atom.get('upsert', False),
                )
                if ok:
                    result.committed += 1
                    committed_tickers.add(atom['subject'])
                else:
                    result.discarded += 1
            except Exception as e:
                _logger.debug('commit failed for atom %s|%s: %s',
                              atom['subject'], atom['predicate'], e)
                result.discarded += 1

        result.tickers = list(committed_tickers)
        self._sessions.pop(session_id, None)
        if result.committed:
            _logger.info('commit_session %s: committed=%d discarded=%d tickers=%s',
                         session_id, result.committed, result.discarded, result.tickers)
        return result
