"""
notifications/premarket_briefing.py — Pre-Market Narrative Briefing Generator

Generates a personalised 3-paragraph narrative briefing for Pro/Premium users
at their configured delivery_time. The output is hybrid:

  [3-paragraph narrative — KB-grounded, portfolio-first]
  ─────────────────────
  [structured position list — entry, stop, T1, T2]

The structured list is omitted when the user has zero open positions.

ATOM RANKING
============
Before building the KB snippet, atoms are ranked by:
  1. Subject overlap with open position tickers (+2.0 pts)
  2. Confidence score
  3. Freshness — atoms written within the last 24h (+1.0 pt)

The top 30 ranked atoms form the grounding context so the LLM always leads
with the most portfolio-relevant finding.

NARRATIVE LENGTH GUARD
======================
After the LLM returns the narrative, _truncate_at_paragraph_boundary() ensures
the text stays within 220 words by trimming at the last paragraph boundary.
This prevents essay-length responses on small phone screens.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional

_log = logging.getLogger(__name__)

_PREMARKET_QUERY_TEMPLATE = (
    "Pre-market briefing for portfolio: {tickers}. "
    "Focus on macro regime, yield curve, sector rotation, and any geo-risk or "
    "supply disruption atoms for these tickers."
)

_MAX_NARRATIVE_WORDS = 220
_ATOM_RANK_LIMIT = 30
_DIVIDER = "─────────────────────"


# ── Atom ranking ──────────────────────────────────────────────────────────────

def _rank_atoms(atoms: list, portfolio_tickers: set) -> list:
    """
    Score and sort atoms by portfolio relevance + confidence + freshness.
    Returns sorted list (highest score first), capped at _ATOM_RANK_LIMIT.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    tickers_lower = {t.lower() for t in portfolio_tickers}

    def _score(atom: dict) -> float:
        s = 0.0
        subj = (atom.get('subject') or '').lower()
        if subj in tickers_lower:
            s += 2.0
        s += float(atom.get('confidence') or 0.5)
        ts_raw = atom.get('timestamp') or atom.get('created_at') or atom.get('updated_at') or ''
        if ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                if ts > cutoff_24h:
                    s += 1.0
            except Exception:
                pass
        return s

    ranked = sorted(atoms, key=_score, reverse=True)
    return ranked[:_ATOM_RANK_LIMIT]


# ── Narrative truncation guard ────────────────────────────────────────────────

def _truncate_at_paragraph_boundary(text: str, max_words: int = _MAX_NARRATIVE_WORDS) -> str:
    """
    Truncate text to at most max_words words, cutting at the last paragraph
    boundary (double newline) that fits within the word budget.
    Returns the trimmed text.
    """
    words = text.split()
    if len(words) <= max_words:
        return text

    # Find the last paragraph boundary within the word limit
    truncated = ' '.join(words[:max_words])
    last_para_break = truncated.rfind('\n\n')
    if last_para_break > 0:
        return truncated[:last_para_break].rstrip()

    # No paragraph break found — cut at last sentence boundary
    last_period = max(truncated.rfind('. '), truncated.rfind('.\n'))
    if last_period > len(truncated) // 2:
        return truncated[:last_period + 1]

    return truncated


# ── Portfolio context formatter ───────────────────────────────────────────────

def _format_portfolio_context(open_positions: list, db_path: str) -> str:
    """
    Build a compact portfolio context string for the LLM prompt.
    Includes ticker, direction, entry, stop, targets, and current KB price if available.
    """
    if not open_positions:
        return ''

    lines = ['USER PORTFOLIO (open positions):']
    for pos in open_positions:
        ticker    = pos.get('ticker', '?')
        direction = pos.get('direction', '?')
        entry     = pos.get('entry_price')
        stop      = pos.get('stop_loss')
        t1        = pos.get('target_1')
        t2        = pos.get('target_2')

        # Try to pull current KB price
        cur_price = None
        try:
            conn = sqlite3.connect(db_path, timeout=5)
            row = conn.execute(
                "SELECT object FROM facts WHERE subject=? AND predicate='last_price' "
                "ORDER BY confidence DESC, timestamp DESC LIMIT 1",
                (ticker.lower(),),
            ).fetchone()
            conn.close()
            if row:
                cur_price = row[0]
        except Exception:
            pass

        parts = [f"  {ticker} ({direction.upper()})"]
        if entry:
            parts.append(f"entry={entry}")
        if cur_price:
            parts.append(f"current={cur_price}")
        if stop:
            parts.append(f"stop={stop}")
        if t1:
            parts.append(f"T1={t1}")
        if t2:
            parts.append(f"T2={t2}")
        lines.append(' | '.join(parts))

    return '\n'.join(lines)


# ── Structured position list formatter ───────────────────────────────────────

def _format_position_list(open_positions: list, db_path: str) -> str:
    """
    Render the Telegram MarkdownV2 structured position list.
    Mirrors the style of _format_open_position_line() in tip_formatter.py
    but keeps it self-contained to avoid circular imports.
    """
    if not open_positions:
        return ''

    def _e(s: str) -> str:
        """Escape MarkdownV2 special characters."""
        special = r'\_*[]()~`>#+-=|{}.!'
        return ''.join('\\' + c if c in special else c for c in str(s))

    lines = [_DIVIDER]
    for pos in open_positions:
        ticker    = pos.get('ticker', '?')
        direction = (pos.get('direction') or 'long').lower()
        entry     = pos.get('entry_price')
        stop      = pos.get('stop_loss')
        t1        = pos.get('target_1')
        t2        = pos.get('target_2')
        status    = (pos.get('status') or 'watching').upper()

        # Direction emoji + status
        dir_emoji = '📈' if direction in ('long', 'buy', 'bullish') else '📉'
        header = f"*{_e(ticker)}* {dir_emoji} _{_e(status)}_"
        lines.append(header)

        # Levels row
        level_parts = []
        if entry:
            level_parts.append(f"Entry {_e(str(entry))}")
        if stop:
            level_parts.append(f"Stop {_e(str(stop))}")
        if t1:
            level_parts.append(f"T1 {_e(str(t1))}")
        if t2:
            level_parts.append(f"T2 {_e(str(t2))}")
        if level_parts:
            lines.append(' · '.join(level_parts))
        lines.append('')

    return '\n'.join(lines)


# ── KB retrieval + snippet builder ────────────────────────────────────────────

def _build_ranked_snippet(
    portfolio_tickers: list,
    db_path: str,
    limit: int = 50,
) -> tuple:
    """
    Retrieve atoms relevant to the portfolio tickers + macro subjects,
    rank them, and return (ranked_snippet_str, ranked_atoms_list).
    """
    try:
        import extensions as ext
        import sqlite3 as _sq

        # Build a composite query: portfolio tickers + macro subjects
        macro_subjects = ['market', 'uk_macro', 'us_macro', 'oil_market', 'gas_market',
                          'gdelt_tension', 'macro_regime']
        query_terms = list(portfolio_tickers) + macro_subjects
        query = ' '.join(query_terms[:12])  # retrieval handles natural language

        conn = ext.kg.thread_local_conn()
        snippet, atoms = ext.retrieve(query, conn, limit=limit)

        # Rank atoms by portfolio relevance
        ranked_atoms = _rank_atoms(atoms, set(portfolio_tickers))

        # Rebuild snippet from ranked atoms using the formatter
        # Fall back to the original snippet if the formatter isn't available
        try:
            from knowledge.snippet_formatter import format_snippet
            ranked_snippet = format_snippet(ranked_atoms)
        except Exception:
            ranked_snippet = snippet  # use original if formatter unavailable

        return ranked_snippet, ranked_atoms

    except Exception as exc:
        _log.warning('premarket_briefing: snippet build failed: %s', exc)
        return '', []


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_premarket_narrative(
    user_id: str,
    db_path: str,
    open_positions: list,
    tier: str,
    trader_level: str,
) -> str:
    """
    Generate the hybrid pre-market briefing:
      [3-paragraph narrative]
      ─────────────────────
      [structured position list]   ← omitted if no open positions

    Parameters
    ----------
    user_id        : User identifier (for logging)
    db_path        : Path to KB SQLite DB
    open_positions : List of tip_followup dicts from get_user_open_positions()
    tier           : User tier string ('pro', 'premium', …)
    trader_level   : User trader level string ('beginner', 'developing', …)

    Returns
    -------
    Telegram MarkdownV2-formatted string ready to send.
    """
    import extensions as ext

    portfolio_tickers = [p['ticker'] for p in open_positions if p.get('ticker')]
    has_positions = bool(portfolio_tickers)

    # ── Build KB snippet ranked by portfolio relevance ────────────────────────
    ranked_snippet, ranked_atoms = _build_ranked_snippet(portfolio_tickers, db_path)

    # ── Portfolio context string for the prompt ───────────────────────────────
    portfolio_context = _format_portfolio_context(open_positions, db_path) if has_positions else None

    # ── Build the LLM prompt ──────────────────────────────────────────────────
    tickers_str = ', '.join(portfolio_tickers[:8]) if portfolio_tickers else 'no open positions'
    premarket_query = _PREMARKET_QUERY_TEMPLATE.format(tickers=tickers_str)

    try:
        messages = ext.build_prompt(
            user_message=premarket_query,
            snippet=ranked_snippet,
            portfolio_context=portfolio_context,
            briefing_mode='premarket',
            telegram_mode=True,
            trader_level=trader_level,
            atom_count=len(ranked_atoms),
        )
    except Exception as exc:
        _log.error('premarket_briefing: prompt build failed for user %s: %s', user_id, exc)
        return ''

    # ── LLM call ─────────────────────────────────────────────────────────────
    try:
        narrative_raw = ext.llm_chat(messages)
    except Exception as exc:
        _log.error('premarket_briefing: LLM call failed for user %s: %s', user_id, exc)
        return ''

    if not narrative_raw:
        _log.warning('premarket_briefing: LLM returned empty for user %s', user_id)
        return ''

    # ── Truncate at paragraph boundary if over word cap ──────────────────────
    narrative = _truncate_at_paragraph_boundary(narrative_raw.strip())

    # ── Assemble hybrid output ────────────────────────────────────────────────
    parts = [narrative]

    if has_positions:
        position_list = _format_position_list(open_positions, db_path)
        if position_list:
            parts.append(position_list)

    result = '\n\n'.join(parts)
    _log.info(
        'premarket_briefing: generated for user %s (%d positions, %d atoms, %d words)',
        user_id, len(open_positions), len(ranked_atoms),
        len(result.split()),
    )
    return result
