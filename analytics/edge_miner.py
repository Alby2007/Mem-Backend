"""
analytics/edge_miner.py — Calibration Edge Scanner

Systematically scans signal_calibration for structural edges strong enough to
warrant a dedicated bot, and auto-generates ready-to-use bot genomes.

Entry points:
    scan_calibration_edges() — returns ranked EdgeCandidate list
    genome_from_edge()       — converts candidate to bot genome dict
    edge_miner_summary()     — compact JSON summary for MCP / Observatory
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

_log = logging.getLogger(__name__)

try:
    from extensions import DB_PATH
except ImportError:
    DB_PATH = '/opt/trading-galaxy/data/trading_knowledge.db'


@dataclass
class EdgeCandidate:
    sector:        str
    pattern_type:  str
    timeframe:     str
    avg_hr:        float
    samples:       int
    tickers:       int
    avg_stop_rate: float
    already_covered: bool
    genome:        dict = field(default_factory=dict)


def genome_from_edge(candidate: EdgeCandidate) -> dict:
    """Generate a ready-to-use bot genome from an EdgeCandidate."""
    risk_pct = round(min(3.0, max(1.0, (candidate.avg_hr - 0.50) * 10)), 1)
    min_quality = round(min(0.85, candidate.avg_hr + 0.10), 2)
    scan_interval = 300 if candidate.timeframe in ('15m', '1h') else 3600
    return {
        'strategy_name': (
            f"{candidate.sector.title()} "
            f"{candidate.pattern_type.replace('_', ' ').title()} "
            f"{candidate.timeframe}"
        ),
        'pattern_types':     [candidate.pattern_type],
        'sectors':           [candidate.sector],
        'timeframes':        [candidate.timeframe],
        'direction_bias':    None,
        'min_quality':       min_quality,
        'risk_pct':          risk_pct,
        'max_positions':     3,
        'role':              'seed',
        'scan_interval_sec': scan_interval,
    }


def _covered_cells(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    """
    Return set of (sector, pattern_type, timeframe) cells covered by active bots.
    A bot with no sector filter is treated as covering nothing (global bots don't target sectors).
    """
    covered: set[tuple[str, str, str]] = set()
    try:
        rows = conn.execute(
            "SELECT sectors, pattern_types, timeframes FROM paper_bot_configs "
            "WHERE active=1 AND killed_at IS NULL"
        ).fetchall()
    except Exception as e:
        _log.warning('edge_miner: bot query failed: %s', e)
        return covered

    for (sec_json, pt_json, tf_json) in rows:
        try:
            sectors = json.loads(sec_json or '[]') or []
        except Exception:
            sectors = []
        try:
            patterns = json.loads(pt_json or '[]') or []
        except Exception:
            patterns = []
        try:
            timeframes = json.loads(tf_json or '[]') or []
        except Exception:
            timeframes = []

        if not sectors:
            continue  # global bot — not counting as sector-specific coverage

        for sec in sectors:
            for pt in patterns:
                if timeframes:
                    for tf in timeframes:
                        covered.add((sec.lower(), pt.lower(), tf.lower()))
                else:
                    covered.add((sec.lower(), pt.lower(), ''))

    return covered


def scan_calibration_edges(
    min_hr: float = 0.60,
    min_samples: int = 2000,
    min_tickers: int = 2,
    db_path: str = DB_PATH,
) -> list[EdgeCandidate]:
    """
    Returns ranked list of (sector, pattern_type, timeframe) cells where the
    calibration hit_rate_t1 is strong enough to warrant a dedicated bot.

    Filters:
    - sample_size >= min_samples (statistical reliability)
    - tickers >= min_tickers (not a single-ticker artefact)
    - hit_rate_t1 >= min_hr (actual edge threshold)

    Returns EdgeCandidate dataclass list, ranked by avg_hr desc.
    already_covered=True means an active bot already targets this cell.
    """
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            """SELECT
                   LOWER(sc.sector)       AS sector,
                   LOWER(sc.pattern_type) AS pattern_type,
                   sc.timeframe,
                   AVG(sc.hit_rate_t1)    AS avg_hr,
                   SUM(sc.sample_size)    AS total_samples,
                   COUNT(DISTINCT sc.ticker) AS ticker_count,
                   AVG(sc.stop_rate)      AS avg_stop_rate
               FROM signal_calibration sc
               WHERE sc.sector IS NOT NULL
                 AND sc.sector != ''
                 AND sc.sector != 'unknown'
                 AND sc.hit_rate_t1 >= ?
                 AND sc.sample_size  >= ?
               GROUP BY LOWER(sc.sector), LOWER(sc.pattern_type), sc.timeframe
               HAVING total_samples >= ? AND ticker_count >= ?
               ORDER BY avg_hr DESC""",
            (min_hr, min_samples // 4, min_samples, min_tickers),
        ).fetchall()
        covered = _covered_cells(conn)
        conn.close()
    except Exception as e:
        _log.error('edge_miner: calibration scan failed: %s', e)
        return []

    candidates: list[EdgeCandidate] = []
    for (sector, pattern_type, timeframe, avg_hr, total_samples, ticker_count, avg_stop_rate) in rows:
        cell = (sector, pattern_type, (timeframe or '').lower())
        already_covered = cell in covered
        candidate = EdgeCandidate(
            sector=sector,
            pattern_type=pattern_type,
            timeframe=timeframe or '',
            avg_hr=round(float(avg_hr), 4),
            samples=int(total_samples),
            tickers=int(ticker_count),
            avg_stop_rate=round(float(avg_stop_rate or 0), 4),
            already_covered=already_covered,
        )
        candidate.genome = genome_from_edge(candidate)
        candidates.append(candidate)

    return candidates


def edge_miner_summary(
    min_hr: float = 0.60,
    min_samples: int = 2000,
    db_path: str = DB_PATH,
) -> dict:
    """Compact summary for MCP tool and Observatory use."""
    candidates = scan_calibration_edges(min_hr=min_hr, min_samples=min_samples, db_path=db_path)
    uncovered = [c for c in candidates if not c.already_covered]
    covered   = [c for c in candidates if c.already_covered]
    return {
        'total_edges':     len(candidates),
        'uncovered_edges': len(uncovered),
        'covered_edges':   len(covered),
        'top_uncovered': [
            {
                'sector':        c.sector,
                'pattern_type':  c.pattern_type,
                'timeframe':     c.timeframe,
                'avg_hr':        c.avg_hr,
                'samples':       c.samples,
                'tickers':       c.tickers,
                'avg_stop_rate': c.avg_stop_rate,
                'genome':        c.genome,
            }
            for c in uncovered[:15]
        ],
        'all_edges': [
            {
                'sector':          c.sector,
                'pattern_type':    c.pattern_type,
                'timeframe':       c.timeframe,
                'avg_hr':          c.avg_hr,
                'samples':         c.samples,
                'tickers':         c.tickers,
                'avg_stop_rate':   c.avg_stop_rate,
                'already_covered': c.already_covered,
                'genome':          c.genome,
            }
            for c in candidates
        ],
    }
