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


# ── P6: Monday briefing extra sections ───────────────────────────────────────

def _esc(s: str) -> str:
    """Escape MarkdownV2 special characters."""
    special = r'\_*[]()~`>#+-=|{}.!'
    return ''.join('\\' + c if c in special else c for c in str(s))


def _build_your_week_section(user_id: str, db_path: str) -> str:
    """
    Build the YOUR WEEK section for Monday briefings.
    Summarises the user's trades from the past 7 days.
    Returns a MarkdownV2-formatted string, or '' if no data.
    """
    try:
        from users.user_store import get_journal_stats, get_pattern_breakdown, get_journal_closed
        from datetime import timedelta

        recent = get_journal_closed(db_path, user_id, since_days=7)
        if not recent:
            return ''

        total  = len(recent)
        wins   = sum(1 for t in recent if t['status'] in ('closed','t2_hit','t1_hit','stopped_out') and (t.get('r_multiple') or 0) > 0)
        stops  = sum(1 for t in recent if t['status'] in ('stopped', 'expired'))
        r_vals = [t['r_multiple'] for t in recent if t.get('r_multiple') is not None]
        avg_r  = round(sum(r_vals) / len(r_vals), 2) if r_vals else None

        # Best trade by R
        best = max(recent, key=lambda t: t.get('r_multiple') or -99)
        best_txt = ''
        if best.get('r_multiple') and best['r_multiple'] > 0:
            r = best['r_multiple']
            best_txt = f"\nBest: {_esc(best['ticker'])} {_esc(best.get('pattern_type','').upper())} {_esc(('+' if r>=0 else '') + str(r))}R"

        # Pattern of the week (most wins)
        from collections import defaultdict as _dd
        pat_wins  = _dd(int); pat_total = _dd(int)
        for t in recent:
            pt = t.get('pattern_type')
            if pt:
                pat_total[pt] += 1
                if t['status'] == 'closed' and (t.get('r_multiple') or 0) > 0:
                    pat_wins[pt] += 1
        pow_txt = ''
        if pat_total:
            best_pat = max(pat_total, key=lambda p: (pat_wins[p], pat_total[p]))
            pow_txt = f"\nPattern of the week: {_esc(best_pat.upper().replace('_',' '))} {pat_wins[best_pat]}/{pat_total[best_pat]} wins"

        # Lifetime win rate
        all_stats = get_journal_stats(db_path, user_id)
        lifetime = f" \\(lifetime: {all_stats['win_rate']}%\\)" if all_stats.get('win_rate') is not None else ''
        week_wr  = round(wins / total * 100) if total else 0
        avg_r_txt = f"\nAvg R: {_esc(('+' if (avg_r or 0)>=0 else '') + str(avg_r))}R" if avg_r is not None else ''

        section = (
            f"📊 *YOUR WEEK*\n"
            f"{_DIVIDER}\n"
            f"{_esc(str(total))} trade{'s' if total != 1 else ''} closed · "
            f"{_esc(str(wins))} win{'s' if wins != 1 else ''} · "
            f"{_esc(str(stops))} stop{'s' if stops != 1 else ''}\n"
            f"Win rate: {_esc(str(week_wr))}%{lifetime}"
            f"{avg_r_txt}"
            f"{best_txt}"
            f"{pow_txt}"
        )
        return section
    except Exception as e:
        _log.debug('premarket_briefing: your_week_section failed: %s', e)
        return ''


def _build_kb_performance_section(db_path: str, min_samples: int = 30, max_rows: int = 4) -> str:
    """
    Build the KB PATTERN PERFORMANCE section for Monday briefings.
    Shows top collective calibration hit rates from signal_calibration.
    Returns a MarkdownV2-formatted string, or '' if no data.
    """
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        rows = conn.execute(
            """SELECT ticker, pattern_type, timeframe, market_regime,
                      hit_rate_t1, sample_size
               FROM signal_calibration
               WHERE sample_size >= ?
               ORDER BY hit_rate_t1 DESC
               LIMIT ?""",
            (min_samples, max_rows * 3),  # over-fetch to de-dup pattern_type
        ).fetchall()
        conn.close()

        if not rows:
            return ''

        # De-dup: show one row per pattern_type (best regime)
        seen_types: set = set()
        selected = []
        for r in rows:
            ptype = r[1]
            if ptype not in seen_types:
                seen_types.add(ptype)
                selected.append(r)
            if len(selected) >= max_rows:
                break

        if not selected:
            return ''

        lines = [f"📈 *KB PATTERN PERFORMANCE*\n{_DIVIDER}"]
        for _, ptype, tf, regime, hr, n in selected:
            ptype_fmt = _esc((ptype or '').upper().replace('_', ' '))
            tf_fmt    = _esc(tf or '')
            regime_fmt = _esc((regime or 'all regimes').replace('_', ' '))
            hr_pct    = _esc(str(round(hr * 100)))
            n_fmt     = _esc(str(n))
            lines.append(f"{ptype_fmt} {tf_fmt} \\({regime_fmt}\\): {hr_pct}% hit rate \\({n_fmt} samples\\)")

        return '\n'.join(lines)
    except Exception as e:
        _log.debug('premarket_briefing: kb_performance_section failed: %s', e)
        return ''


def _build_regime_outlook_section(db_path: str, min_observations: int = 5) -> str:
    """
    Build the REGIME OUTLOOK section for Monday briefings.
    Shows current state + top 2 most likely transitions with probabilities.
    Only included when transition data has >= min_observations for the current state.
    Returns a MarkdownV2-formatted string, or '' if no data.
    """
    try:
        from analytics.state_transitions import TransitionEngine
        engine = TransitionEngine(db_path)
        forecast = engine.get_current_state_forecast(scope='global', subject='market')

        if not forecast or forecast.total_observations < min_observations:
            return ''

        cs = forecast.current_state
        state_str = cs.label() or cs.state_id.replace('_', ' ')
        avg_days = round(forecast.avg_persistence_hours / 24, 1) if forecast.avg_persistence_hours else None

        lines = [f"🔮 *REGIME OUTLOOK*\n{_DIVIDER}"]
        lines.append(f"Current: {_esc(state_str)}")
        if avg_days:
            lines.append(f"Avg persistence: {_esc(str(avg_days))} days")

        top2 = [t for t in forecast.transitions[:2] if t.probability > 0.05]
        for t in top2:
            t_days = round(t.avg_hours_to_transition / 24, 1) if t.avg_hours_to_transition else None
            pct    = round(t.probability * 100)
            label  = t.to_state.label() or t.to_state_id.replace('_', ' ')
            day_str = f" avg {_esc(str(t_days))} days" if t_days else ''
            lines.append(f"→ {_esc(str(pct))}% chance of {_esc(label)}{day_str}")

        lines.append(f"Based on {_esc(str(forecast.total_observations))} similar historical periods\\.")
        return '\n'.join(lines)
    except Exception as e:
        _log.debug('premarket_briefing: regime_outlook_section failed: %s', e)
        return ''


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

    # ── Monday-only sections (P6) ─────────────────────────────────────────────
    # YOUR WEEK and KB PERFORMANCE are appended only on Mondays so the briefing
    # includes a weekly review without bloating daily deliveries.
    from datetime import datetime, timezone as _tz
    is_monday = datetime.now(_tz.utc).weekday() == 0

    if is_monday:
        your_week = _build_your_week_section(user_id, db_path)
        if your_week:
            parts.append(your_week)

        kb_perf = _build_kb_performance_section(db_path)
        if kb_perf:
            parts.append(kb_perf)

        regime_outlook = _build_regime_outlook_section(db_path)
        if regime_outlook:
            parts.append(regime_outlook)

        fleet_discoveries = _build_fleet_discoveries_section(user_id, db_path)
        if fleet_discoveries:
            parts.append(fleet_discoveries)

        cal_update = _build_calibration_update_section(db_path)
        if cal_update:
            parts.append(cal_update)

    result = '\n\n'.join(parts)
    _log.info(
        'premarket_briefing: generated for user %s (%d positions, %d atoms, %d words, monday=%s)',
        user_id, len(open_positions), len(ranked_atoms),
        len(result.split()), is_monday,
    )
    return result


def _build_calibration_update_section(db_path: str) -> str:
    """
    Monday-only section: calibration pipeline health and top/bottom cells.
    Returns formatted string or '' if insufficient data.
    """
    try:
        import sqlite3 as _sq
        from datetime import timedelta as _td
        from datetime import datetime as _dt
        from datetime import timezone as _tz

        conn = _sq.connect(db_path, timeout=5)
        conn.row_factory = _sq.Row
        try:
            # Check calibration_observations table exists
            tbl = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='calibration_observations'"
            ).fetchone()
            if not tbl:
                return ''

            # Total observations and weekly new
            total_obs = conn.execute(
                "SELECT COUNT(*) FROM calibration_observations"
            ).fetchone()[0]
            if not total_obs:
                return ''

            week_ago = (_dt.now(_tz.utc) - _td(days=7)).isoformat()
            weekly_new = conn.execute(
                "SELECT COUNT(*) FROM calibration_observations WHERE observed_at >= ?",
                (week_ago,)
            ).fetchone()[0]

            # Active bots
            try:
                active_bots = conn.execute(
                    "SELECT COUNT(*) FROM paper_bot_configs WHERE active=1"
                ).fetchone()[0]
                max_gen = conn.execute(
                    "SELECT COALESCE(MAX(generation),0) FROM paper_bot_configs"
                ).fetchone()[0]
                elite_bots = conn.execute(
                    "SELECT COUNT(*) FROM paper_agent_log WHERE event_type='promoted' "
                    "AND created_at >= ?", (week_ago,)
                ).fetchone()[0]
            except Exception:
                active_bots = elite_bots = max_gen = None

            # Top performing cell
            top = conn.execute(
                """SELECT pattern_type, sector, hit_rate_t1, sample_size
                   FROM signal_calibration
                   WHERE sample_size >= 10 AND hit_rate_t1 IS NOT NULL
                   ORDER BY hit_rate_t1 DESC LIMIT 1"""
            ).fetchone()

            # Weakest cell (below 40% with ≥10 samples)
            weak = conn.execute(
                """SELECT pattern_type, sector, hit_rate_t1, sample_size
                   FROM signal_calibration
                   WHERE sample_size >= 10 AND hit_rate_t1 IS NOT NULL AND hit_rate_t1 < 0.4
                   ORDER BY hit_rate_t1 ASC LIMIT 1"""
            ).fetchone()
        finally:
            conn.close()

        sign = '+' if weekly_new > 0 else ''
        lines = [
            '━━━━━━━━━━━━━━━━',
            '📊 CALIBRATION UPDATE',
            '━━━━━━━━━━━━━━━━',
            f'{total_obs:,} forward observations ({sign}{weekly_new:,} this week)',
        ]
        if top:
            pat  = (top['pattern_type'] or '?').upper()
            sec  = top['sector'] or ''
            hr   = round((top['hit_rate_t1'] or 0) * 100)
            n    = top['sample_size']
            desc = f'{pat} + {sec}'.strip(' +')
            lines.append(f'Top edge: {desc} → {hr}% hit rate ({n} trades)')
        if weak:
            pat  = (weak['pattern_type'] or '?').upper()
            hr   = round((weak['hit_rate_t1'] or 0) * 100)
            n    = weak['sample_size']
            lines.append(f'Weakest: {pat} → {hr}% hit rate ({n} trades, disproven)')
        if active_bots is not None:
            elite_str = f' · {elite_bots} promoted' if elite_bots else ''
            lines.append(f'Bot fleet: {active_bots} active · Gen {max_gen}{elite_str}')

        return '\n'.join(lines)
    except Exception as e:
        _log.debug('_build_calibration_update_section failed: %s', e)
        return ''


def _build_fleet_discoveries_section(user_id: str, db_path: str) -> str:
    """
    Monday-only section: surface top calibration discoveries from the bot fleet.
    Returns a formatted string or '' if no meaningful discoveries exist.
    """
    try:
        from analytics.strategy_evolution import StrategyEvolution
        engine = StrategyEvolution(db_path)
        data   = engine.get_discoveries(user_id)
        discoveries = data.get('discoveries', [])
        total_obs   = data.get('total_observations', 0)
        n_cells     = data.get('unique_cells_tested', 0)
        generation  = data.get('generation', 0)

        active = [d for d in discoveries if d.get('status') == 'active' and d.get('sample_size', 0) >= 10]
        if not active:
            return ''

        active.sort(key=lambda d: d.get('sample_size', 0), reverse=True)
        top = active[:3]

        lines = [
            f'📊 FLEET DISCOVERIES (Gen {generation})',
            f'Your fleet ran {total_obs:,} calibration observations across {n_cells} strategy cells this week.',
            '',
        ]
        for i, d in enumerate(top, 1):
            hr  = round(d.get('hit_rate', 0) * 100)
            n   = d.get('sample_size', 0)
            pat = d.get('pattern_type', '?')
            by  = d.get('discovered_by', 'generalist')
            lines.append(f'{i}. {pat.upper()} → {hr}% hit rate ({n} trades, discovered by {by})')

        return '\n'.join(lines)
    except Exception as e:
        _log.debug('_build_fleet_discoveries_section failed: %s', e)
        return ''
