"""routes_v2/discovery.py â€” Internal discovery fleet API routes.

These routes are NOT user-facing. They are called by the companion MCP server
using a shared secret header (X-Internal-Secret).
"""

from __future__ import annotations

import os
import sqlite3

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from middleware.fastapi_auth import get_current_user, user_path_auth

import extensions as ext
from services.discovery_fleet import (
    ensure_discovery_user,
    seed_discovery_fleet,
    get_discovery_report,
    get_discovery_status,
    DISCOVERY_USER_ID,
)

router = APIRouter()

INTERNAL_SECRET = os.environ.get('DISCOVERY_SECRET', 'dev-secret-change-me')


def _internal_auth(x_internal_secret: str = Header(None)):
    if x_internal_secret != INTERNAL_SECRET:
        raise HTTPException(403, detail='forbidden')


def _get_runner():
    runner = getattr(ext, 'bot_runner', None)
    if runner is None:
        from services.bot_runner import BotRunner
        runner = BotRunner(ext.DB_PATH)
    return runner


@router.post('/internal/discovery/seed')
async def discovery_seed(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        ensure_discovery_user(conn)
        conn.close()
        runner = _get_runner()
        bot_ids = seed_discovery_fleet(runner)
        return {'seeded': len(bot_ids), 'bot_ids': bot_ids, 'status': 'ok'}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get('/internal/discovery/status')
async def discovery_status(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        result = get_discovery_status(conn)
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get('/internal/discovery/report')
async def discovery_report(
    min_observations: int = Query(5, ge=1),
    limit: int = Query(50, ge=1, le=500),
    x_internal_secret: str = Header(None),
):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        report = get_discovery_report(conn, min_observations=min_observations, limit=limit)
        conn.close()
        return {'report': report}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post('/internal/discovery/reset')
async def discovery_reset(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        runner = _get_runner()

        # Stop all discovery bot threads
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        bot_ids = [r[0] for r in conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE user_id=?", (DISCOVERY_USER_ID,)
        ).fetchall()]
        conn.close()

        for bid in bot_ids:
            try:
                runner.stop_bot(bid)
            except Exception:
                pass

        # Delete positions and bot configs (NOT calibration_observations)
        conn2 = sqlite3.connect(ext.DB_PATH, timeout=15)
        try:
            conn2.execute(
                "DELETE FROM paper_positions WHERE bot_id IN "
                "(SELECT bot_id FROM paper_bot_configs WHERE user_id=?)",
                (DISCOVERY_USER_ID,),
            )
            for bid in bot_ids:
                conn2.execute("DELETE FROM paper_bot_equity WHERE bot_id=?", (bid,))
            conn2.execute(
                "DELETE FROM paper_bot_configs WHERE user_id=?", (DISCOVERY_USER_ID,)
            )
            conn2.commit()
        finally:
            conn2.close()

        return {'deleted_bots': len(bot_ids), 'status': 'reset'}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# â”€â”€ MOT routes (user-authenticated, open to all registered users) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _dev_gate(user_id: str) -> None:
    """Previously restricted to is_dev users. Now open to all authenticated users."""
    pass


@router.get('/users/{user_id}/private-fleet/status')
async def private_fleet_status(user_id: str, _: str = Depends(user_path_auth)):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    result = get_discovery_status(conn)
    conn.close()
    return result


@router.get('/users/{user_id}/private-fleet/report')
async def private_fleet_report(
    user_id: str,
    min_observations: int = Query(3, ge=1),
    limit: int = Query(50, ge=1, le=500),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    result = get_discovery_report(conn, min_observations=min_observations, limit=limit)
    conn.close()
    return {'report': result}


@router.get('/users/{user_id}/private-fleet/closed-positions')
async def pf_closed_positions(
    user_id: str,
    limit: int = Query(200, ge=1, le=1000),
    status: str = Query(None),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        q = """
            SELECT
                pp.id, pp.ticker, pp.direction, pp.pattern_id,
                pp.entry_price, pp.exit_price, pp.stop, pp.t1, pp.t2,
                pp.pnl_r, pp.status, pp.opened_at, pp.closed_at,
                pp.bot_id, pp.note,
                pbc.strategy_name, pbc.pattern_types, pbc.sectors
            FROM paper_positions pp
            LEFT JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pp.status != 'open'
              AND (pp.bot_id LIKE 'disc_%' OR pp.user_id = 'system_discovery')
        """
        params: list = []
        if status:
            q += " AND pp.status = ?"
            params.append(status)
        q += " ORDER BY pp.closed_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(q, params).fetchall()
        cols = ['id', 'ticker', 'direction', 'pattern_id', 'entry_price', 'exit_price',
                'stop', 't1', 't2', 'pnl_r', 'status', 'opened_at', 'closed_at',
                'bot_id', 'note', 'strategy_name', 'pattern_types', 'sectors']
        positions = [dict(zip(cols, r)) for r in rows]

        wins = [p for p in positions if (p.get('pnl_r') or 0) > 0]
        losses = [p for p in positions if (p.get('pnl_r') or 0) < 0]
        gross_profit = sum(p['pnl_r'] for p in wins if p['pnl_r'])
        gross_loss   = abs(sum(p['pnl_r'] for p in losses if p['pnl_r']))
        avg_r = (sum(p['pnl_r'] for p in positions if p.get('pnl_r') is not None) / len(positions)) if positions else 0

        outcome_counts: dict = {}
        for p in positions:
            s = p.get('status') or 'closed'
            outcome_counts[s] = outcome_counts.get(s, 0) + 1

        stats = {
            'win_rate': round(len(wins) / len(positions) * 100, 1) if positions else 0,
            'avg_r':    round(avg_r, 2),
            'gross_profit': round(gross_profit, 2),
            'gross_loss':   round(gross_loss, 2),
            't2_hits':    outcome_counts.get('t2_hit', 0),
            't1_hits':    outcome_counts.get('t1_hit', 0),
            'stopped':    outcome_counts.get('stopped_out', 0),
            'closed':     outcome_counts.get('closed', 0),
        }
        return {'total': len(positions), 'stats': stats, 'positions': positions}
    finally:
        conn.close()


@router.get('/users/{user_id}/private-fleet/bots')
async def pf_bots(
    user_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    pattern: str = Query(None),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        where = "WHERE pbc.user_id=?"
        params: list = [DISCOVERY_USER_ID]
        if pattern:
            where += " AND pbc.pattern_types LIKE ?"
            params.append(f'%{pattern}%')

        total = conn.execute(
            f"SELECT COUNT(*) FROM paper_bot_configs pbc {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""SELECT pbc.bot_id, pbc.strategy_name, pbc.pattern_types, pbc.sectors,
                       pbc.direction_bias, pbc.active, pbc.created_at,
                       COUNT(CASE WHEN pp.status='open' THEN 1 END) as open_pos,
                       COUNT(CASE WHEN pp.status!='open' THEN 1 END) as closed_pos,
                       SUM(CASE WHEN pp.pnl_r > 0 AND pp.status!='open' THEN 1 ELSE 0 END) as wins,
                       AVG(CASE WHEN pp.status!='open' THEN pp.pnl_r END) as avg_r
                FROM paper_bot_configs pbc
                LEFT JOIN paper_positions pp ON pp.bot_id = pbc.bot_id
                {where}
                GROUP BY pbc.bot_id
                ORDER BY closed_pos DESC, pbc.created_at DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

        cols = ['bot_id', 'strategy_name', 'pattern_types', 'sectors', 'direction_bias',
                'active', 'created_at', 'open_pos', 'closed_pos', 'wins', 'avg_r']
        bots = []
        for r in rows:
            b = dict(zip(cols, r))
            closed = b['closed_pos'] or 0
            wins   = b['wins'] or 0
            b['win_rate'] = round(wins / closed * 100, 1) if closed > 0 else None
            b['avg_r']    = round(b['avg_r'], 2) if b['avg_r'] is not None else None
            bots.append(b)

        return {'total': total, 'limit': limit, 'offset': offset, 'bots': bots}
    finally:
        conn.close()


@router.get('/users/{user_id}/private-fleet/calibration-obs')
async def pf_calibration_obs(
    user_id: str,
    limit: int = Query(200, ge=1, le=2000),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        rows = conn.execute("""
            SELECT co.id, co.ticker, co.pattern_type, co.timeframe,
                   co.market_regime, co.outcome, co.source, co.bot_id,
                   co.pnl_r, co.observed_at
            FROM calibration_observations co
            WHERE co.ticker != 'TEST.L'
            ORDER BY co.observed_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        cols = ['id', 'ticker', 'pattern_type', 'timeframe', 'market_regime',
                'outcome', 'source', 'bot_id', 'pnl_r', 'observed_at']
        observations = [dict(zip(cols, r)) for r in rows]

        outcome_breakdown: dict = {}
        pattern_breakdown: dict = {}
        for o in observations:
            oc = o.get('outcome') or 'unknown'
            outcome_breakdown[oc] = outcome_breakdown.get(oc, 0) + 1
            pt = o.get('pattern_type') or 'unknown'
            pattern_breakdown[pt] = pattern_breakdown.get(pt, 0) + 1

        try:
            signal_cells = conn.execute(
                "SELECT COUNT(*) FROM calibration_matrix"
            ).fetchone()[0]
        except Exception:
            signal_cells = 0

        cells_with_obs = len(set(
            (o.get('pattern_type', ''), o.get('timeframe', ''), o.get('market_regime', ''))
            for o in observations
        ))

        return {
            'total': len(observations),
            'signal_cells': signal_cells,
            'cells_with_obs': cells_with_obs,
            'outcome_breakdown': outcome_breakdown,
            'pattern_breakdown': pattern_breakdown,
            'observations': observations,
        }
    finally:
        conn.close()


# â”€â”€ Ops Terminal Briefing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.get('/ops/briefing')
async def ops_briefing(user_id: str = Depends(get_current_user)):
    """Aggregated briefing for the Meridian Operations Terminal."""
    _dev_gate(user_id)

    # 30s in-process cache â€” briefing is called every 60s by Dispatch
    # and costs 6-8s per call due to pattern join queries
    import time as _t
    _now = _t.time()
    if _briefing_cache['data'] is not None and (_now - _briefing_cache['ts']) < _BRIEFING_TTL:
        return _briefing_cache['data']

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        import json as _json
        from datetime import datetime, timezone, timedelta

        # â”€â”€ Regime: KB facts for market, SPY, HYG, TLT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        regime_rows = conn.execute(
            """SELECT subject, predicate, object FROM facts
               WHERE LOWER(subject) IN ('market','spy','hyg','tlt')
                 AND predicate IN ('market_regime','signal_direction','last_price','price_regime')
               ORDER BY timestamp DESC"""
        ).fetchall()

        regime = {'market_regime': '', 'signal_direction': ''}
        proxy_data = {p: {} for p in ('spy', 'hyg', 'tlt')}
        _seen = set()
        for r in regime_rows:
            subj, pred, obj = r['subject'].lower(), r['predicate'], r['object']
            key = f'{subj}|{pred}'
            if key in _seen:
                continue
            _seen.add(key)
            if subj == 'market':
                if pred == 'market_regime':
                    regime['market_regime'] = obj
                elif pred == 'signal_direction':
                    regime['signal_direction'] = obj
            elif subj in proxy_data:
                proxy_data[subj][pred] = obj

        for pn in ('spy', 'hyg', 'tlt'):
            regime[pn] = {
                'last_price': proxy_data[pn].get('last_price', ''),
                'price_regime': proxy_data[pn].get('price_regime', ''),
            }

        # â”€â”€ Overnight closes (last 12h) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        cutoff_12h = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
        ov = conn.execute(
            """SELECT COUNT(*) as n,
                      SUM(CASE WHEN pnl_r > 0 THEN 1 ELSE 0 END) as wins,
                      ROUND(AVG(pnl_r), 3) as avg_r,
                      ROUND(SUM(pnl_r), 1) as gross_r
               FROM paper_positions
               WHERE status IN ('t2_hit','t1_hit','stopped_out','closed')
                 AND closed_at > ?
                 AND (bot_id LIKE 'disc_%%' OR user_id = 'system_discovery')""",
            (cutoff_12h,),
        ).fetchone()
        overnight = {
            'closed_count': ov['n'] or 0,
            'wins': ov['wins'] or 0,
            'avg_r': float(ov['avg_r'] or 0),
            'gross_r': float(ov['gross_r'] or 0),
            'since_hours': 12,
        }

        # â”€â”€ Fleet summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        fleet_status = get_discovery_status(conn)
        open_positions = fleet_status.get('open_positions', [])
        _conv = {}
        for op in open_positions:
            ck = f"{op['ticker']}|{(op.get('direction') or '').lower()}"
            if ck not in _conv:
                _conv[ck] = {'ticker': op['ticker'], 'direction': op.get('direction', ''), 'bot_count': 0}
            _conv[ck]['bot_count'] += 1
        top_convergence = sorted(
            [v for v in _conv.values() if v['bot_count'] >= 3],
            key=lambda x: x['bot_count'], reverse=True,
        )[:10]

        fleet = {
            'open_positions': len(open_positions),
            'active_bots': fleet_status.get('active_bots', 0),
            'top_convergence': top_convergence,
        }

        # â”€â”€ Observatory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        obs_row = conn.execute(
            "SELECT run_at, findings_json FROM observatory_runs ORDER BY run_at DESC LIMIT 1"
        ).fetchone()
        observatory = {'last_run': '', 'findings_count': 0, 'last_findings': []}
        if obs_row:
            observatory['last_run'] = obs_row['run_at'] or ''
            try:
                findings = _json.loads(obs_row['findings_json'] or '[]')
                observatory['findings_count'] = len(findings)
                observatory['last_findings'] = [
                    f.get('summary', f.get('title', '')) for f in findings[:5]
                ] if isinstance(findings, list) else []
            except Exception:
                pass

        # â”€â”€ Pattern pool stats â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        pat = conn.execute(
            """SELECT COUNT(*) as total_open,
                      SUM(CASE WHEN quality_score >= 0.50 THEN 1 ELSE 0 END) as quality_medium,
                      SUM(CASE WHEN quality_score <  0.50 THEN 1 ELSE 0 END) as quality_low
               FROM pattern_signals
               WHERE status NOT IN ('filled','broken','expired')"""
        ).fetchone()
        actionable = conn.execute(
            """SELECT COUNT(*) FROM pattern_signals
               WHERE status NOT IN ('filled','broken','expired')
                 AND quality_score >= 0.50
                 AND formed_at > ?""",
            ((datetime.now(timezone.utc) - timedelta(days=5)).isoformat(),),
        ).fetchone()[0]

        patterns = {
            'total_open': pat['total_open'] or 0,
            'quality_medium': pat['quality_medium'] or 0,
            'quality_low': pat['quality_low'] or 0,
            'actionable': actionable or 0,
        }

        # â”€â”€ Top opportunities â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # CTE approach: fetch top patterns via index first, then join small result set
        # (was 5800ms due to full table scan + LOWER() join blocking indexes)
        opp_rows = conn.execute("""
            WITH top_patterns AS (
                SELECT ticker, pattern_type, direction,
                       ROUND(quality_score, 4) as quality_score,
                       timeframe, kb_conviction, kb_signal_dir
                FROM pattern_signals
                WHERE status NOT IN ('filled', 'broken', 'expired')
                  AND kb_conviction NOT IN ('avoid', '')
                ORDER BY quality_score DESC
                LIMIT 50
            )
            SELECT tp.*,
                   f1.object as sector,
                   f2.object as price_regime
            FROM top_patterns tp
            LEFT JOIN facts f1 ON UPPER(f1.subject) = tp.ticker AND f1.predicate = 'sector'
            LEFT JOIN facts f2 ON UPPER(f2.subject) = tp.ticker AND f2.predicate = 'price_regime'
            WHERE tp.direction != 'bullish'
               OR LOWER(COALESCE(f2.object, '')) NOT IN ('near_52w_high', 'near_high')
            LIMIT 5
        """).fetchall()
        opportunities = [
            {
                'ticker':           r['ticker'],
                'pattern_type':     r['pattern_type'],
                'direction':        r['direction'],
                'quality_score':    r['quality_score'],
                'timeframe':        r['timeframe'],
                'conviction_tier':  r['kb_conviction'],
                'signal_direction': r['kb_signal_dir'],
                'sector':           r['sector'],
                'price_regime':     r['price_regime'],
            }
            for r in opp_rows
        ]

        result = {
            'regime':        regime,
            'overnight':     overnight,
            'fleet':         fleet,
            'observatory':   observatory,
            'patterns':      patterns,
            'opportunities': opportunities,
        }
        _briefing_cache['data'] = result
        _briefing_cache['ts']   = _t.time()
        return result
    finally:
        conn.close()


# â”€â”€ CORPUS â€” Market Objects â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

import time as _time

_corpus_cache: dict = {'data': None, 'ts': 0}
_CORPUS_TTL = 300  # 5 minutes

# Briefing cache: 30s TTL â€” called every 60s by Dispatch, costs 6-8s per call without cache
_briefing_cache: dict = {'data': None, 'ts': 0.0}
_BRIEFING_TTL = 30  # seconds


def _resolve_market_object(ticker: str, facts: dict, pats: list, pos: list) -> dict:
    """Build a single Market Object from pre-fetched data."""

    bear_count = sum(1 for p in pos if (p.get('direction') or '').lower() in ('bearish', 'short'))
    bull_count = len(pos) - bear_count

    # Conviction weight mapping
    conv_map = {'high': 1.0, 'medium': 0.7, 'low': 0.4}
    conviction = facts.get('conviction_tier', '')
    conv_weight = conv_map.get(conviction, 0.5)

    avg_quality = 0
    if pats:
        quals = [float(p.get('quality_score') or 0) for p in pats]
        avg_quality = sum(quals) / len(quals) if quals else 0

    obj_confidence = round(min(1.0, avg_quality * conv_weight + 0.1 * min(len(pats), 5)), 2)

    # Primary direction â€” majority of open pattern directions
    dir_counts: dict[str, int] = {}
    for p in pats:
        d = (p.get('direction') or '').lower()
        if d:
            dir_counts[d] = dir_counts.get(d, 0) + 1
    primary_direction = max(dir_counts, key=dir_counts.get) if dir_counts else ''

    # Numeric helpers for new enrichment fields
    def _float(key, default=0.0):
        try:
            return float(facts.get(key, default))
        except (TypeError, ValueError):
            return default

    return {
        'ticker': ticker,
        'last_price': facts.get('last_price', ''),
        'price_regime': facts.get('price_regime', ''),
        'signal_direction': facts.get('signal_direction', ''),
        'conviction_tier': conviction,
        'macro_confirmed': facts.get('macro_confirmed', '') == 'true',
        'catalyst': facts.get('catalyst', ''),
        'sector': facts.get('sector', ''),
        'open_patterns': [
            {
                'id': p.get('id'),
                'type': p.get('pattern_type'),
                'pattern_type': p.get('pattern_type'),
                'tf': p.get('timeframe'),
                'timeframe': p.get('timeframe'),
                'quality': float(p.get('quality_score') or 0),
                'quality_score': float(p.get('quality_score') or 0),
                'direction': p.get('direction', ''),
                'zone_high': p.get('zone_high'),
                'zone_low': p.get('zone_low'),
            }
            for p in pats
        ],
        'primary_direction': primary_direction,
        'bot_consensus': {'bearish': bear_count, 'bullish': bull_count},
        'regime': facts.get('market_regime', ''),
        'object_confidence': obj_confidence,
        'last_verified': facts.get('_latest_ts', ''),
        # â”€â”€ Enrichment fields (ATLAS + FORGE) â”€â”€
        'upside_pct': _float('upside_pct'),
        'auto_thesis': facts.get('auto_thesis', ''),
        'auto_thesis_score': _float('auto_thesis_score'),
        'macro_confirmation': facts.get('macro_confirmation', 'no_data'),
        'pattern_decay_pct': _float('pattern_decay_pct'),
        'pattern_hours_remaining': _float('pattern_hours_remaining'),
        'volatility_regime': facts.get('volatility_regime', ''),
        'signal_quality': facts.get('signal_quality', ''),
        'thesis_score': _float('thesis_score'),
        'institutional_flow': facts.get('institutional_flow', ''),
        'best_regime': facts.get('best_regime', ''),
        'worst_regime': facts.get('worst_regime', ''),
    }


def _bulk_resolve(conn) -> list[dict]:
    """Resolve all tickers with open patterns into Market Objects."""
    # 1. All tickers with open patterns
    pat_rows = conn.execute(
        """SELECT id, ticker, pattern_type, direction, timeframe, quality_score,
                  zone_high, zone_low, kb_conviction, kb_regime, kb_signal_dir
           FROM pattern_signals
           WHERE status NOT IN ('filled','broken','expired')
             AND quality_score >= 0.35"""
    ).fetchall()

    patterns_by_ticker: dict[str, list] = {}
    all_tickers: set[str] = set()
    for r in pat_rows:
        t = r['ticker']
        all_tickers.add(t)
        patterns_by_ticker.setdefault(t, []).append(dict(r))

    if not all_tickers:
        return []

    # 2. Facts for those tickers
    placeholders = ','.join('?' for _ in all_tickers)
    fact_rows = conn.execute(
        f"""SELECT subject, predicate, object, timestamp FROM facts
            WHERE UPPER(subject) IN ({placeholders})
            ORDER BY timestamp DESC""",
        [t.upper() for t in all_tickers],
    ).fetchall()

    facts_by_ticker: dict[str, dict] = {}
    seen_keys: set[str] = set()
    for r in fact_rows:
        subj = r['subject'].upper()
        pred = r['predicate']
        key = f'{subj}|{pred}'
        if key in seen_keys:
            continue
        seen_keys.add(key)
        bucket = facts_by_ticker.setdefault(subj, {})
        bucket[pred] = r['object']
        if '_latest_ts' not in bucket:
            bucket['_latest_ts'] = r['timestamp'] or ''

    # Also add market-level regime to each ticker's facts
    market_facts = conn.execute(
        """SELECT predicate, object FROM facts
           WHERE LOWER(subject) = 'market'
             AND predicate IN ('market_regime','signal_direction')
           ORDER BY timestamp DESC LIMIT 2"""
    ).fetchall()
    market_regime_data = {}
    for mf in market_facts:
        if mf['predicate'] not in market_regime_data:
            market_regime_data[mf['predicate']] = mf['object']

    for t in all_tickers:
        bucket = facts_by_ticker.setdefault(t.upper(), {})
        if 'market_regime' not in bucket:
            bucket['market_regime'] = market_regime_data.get('market_regime', '')

    # 3. Bot positions for consensus
    pos_rows = conn.execute(
        f"""SELECT ticker, direction FROM paper_positions
            WHERE status = 'open'
              AND UPPER(ticker) IN ({placeholders})""",
        [t.upper() for t in all_tickers],
    ).fetchall()

    positions_by_ticker: dict[str, list] = {}
    for r in pos_rows:
        positions_by_ticker.setdefault(r['ticker'].upper(), []).append(dict(r))

    # 4. Build objects
    objects = []
    for t in sorted(all_tickers):
        obj = _resolve_market_object(
            t,
            facts_by_ticker.get(t.upper(), {}),
            patterns_by_ticker.get(t, []),
            positions_by_ticker.get(t.upper(), [])
        )
        objects.append(obj)

    return objects


@router.get('/ops/corpus/{ticker}')
async def get_corpus_ticker(ticker: str, current_user: str = Depends(get_current_user)):
    """Single Market Object for a ticker â€” resolved from KB atoms, patterns, positions."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        # Patterns
        pat_rows = conn.execute(
            """SELECT id, ticker, pattern_type, direction, timeframe, quality_score,
                      zone_high, zone_low, kb_conviction, kb_regime, kb_signal_dir
               FROM pattern_signals
               WHERE UPPER(ticker) = ?
                 AND status NOT IN ('filled','broken','expired')""",
            (ticker.upper(),),
        ).fetchall()
        patterns_by_ticker = {ticker.upper(): [dict(r) for r in pat_rows]}

        # Facts
        fact_rows = conn.execute(
            """SELECT subject, predicate, object, timestamp FROM facts
               WHERE UPPER(subject) = ?
               ORDER BY timestamp DESC""",
            (ticker.upper(),),
        ).fetchall()
        facts: dict[str, str] = {}
        for r in fact_rows:
            if r['predicate'] not in facts:
                facts[r['predicate']] = r['object']
                if '_latest_ts' not in facts:
                    facts['_latest_ts'] = r['timestamp'] or ''

        # Market regime
        mr = conn.execute(
            """SELECT predicate, object FROM facts
               WHERE LOWER(subject) = 'market'
                 AND predicate IN ('market_regime','signal_direction')
               ORDER BY timestamp DESC LIMIT 2"""
        ).fetchall()
        for mf in mr:
            if mf['predicate'] not in facts:
                facts[mf['predicate']] = mf['object']

        # Bot positions
        pos_rows = conn.execute(
            """SELECT ticker, direction FROM paper_positions
               WHERE status = 'open' AND UPPER(ticker) = ?""",
            (ticker.upper(),),
        ).fetchall()
        positions_by_ticker = {ticker.upper(): [dict(r) for r in pos_rows]}

        return _resolve_market_object(
            ticker.upper(),
            facts,
            patterns_by_ticker.get(ticker.upper(), []),
            positions_by_ticker.get(ticker.upper(), []),
        )
    finally:
        conn.close()


@router.get('/ops/corpus')
async def get_corpus_bulk(current_user: str = Depends(get_current_user)):
    """All active Market Objects (tickers with actionable patterns). 5-min cache."""
    _dev_gate(current_user)

    now = _time.time()
    if _corpus_cache['data'] is not None and (now - _corpus_cache['ts']) < _CORPUS_TTL:
        return {'objects': _corpus_cache['data'], 'cached': True}

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        objects = _bulk_resolve(conn)
        _corpus_cache['data'] = objects
        _corpus_cache['ts'] = now
        return {'objects': objects, 'cached': False}
    finally:
        conn.close()


@router.get('/ops/sentinel/positions')
async def sentinel_positions(current_user: str = Depends(get_current_user)):
    """All open paper positions across all bots â€” for SENTINEL convergence view."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT pp.ticker, pp.direction,
                   pp.entry_price, pp.stop, pp.t1, pp.t2,
                   pp.opened_at, pp.pnl_r, pp.bot_id, pp.user_id,
                   pbc.strategy_name, pbc.pattern_types, pbc.sectors
            FROM paper_positions pp
            LEFT JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pp.status = 'open'
            ORDER BY pp.opened_at DESC
        """).fetchall()
        return {'positions': [dict(r) for r in rows]}
    finally:
        conn.close()


@router.get('/ops/network/leaderboard')
async def network_leaderboard(
    limit: int = Query(50, ge=1, le=200),
    min_samples: int = Query(20, ge=5),
    current_user: str = Depends(get_current_user),
):
    """Top calibration cells by hit rate -- for NETWORK proven cells leaderboard."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT ticker, pattern_type, timeframe, market_regime,
                   hit_rate_t1, hit_rate_t2, stopped_out_rate,
                   sample_size, calibration_confidence
            FROM signal_calibration
            WHERE sample_size >= ?
              AND hit_rate_t1 >= 0.6
            ORDER BY hit_rate_t1 DESC, sample_size DESC
            LIMIT ?
        """, (min_samples, limit)).fetchall()
        return {'cells': [dict(r) for r in rows]}
    finally:
        conn.close()


@router.get('/ops/network/matrix')
async def network_matrix(current_user: str = Depends(get_current_user)):
    """Average hit rate per pattern x regime -- for NETWORK heatmap matrix."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT pattern_type, market_regime,
                   AVG(hit_rate_t1) as avg_hit,
                   SUM(sample_size) as total_n,
                   COUNT(*) as cells
            FROM signal_calibration
            WHERE sample_size >= 10
              AND market_regime IN (
                'risk_off_contraction','risk_on_expansion',
                'stagflation','recovery'
              )
            GROUP BY pattern_type, market_regime
        """).fetchall()
        total_cells = conn.execute(
            "SELECT COUNT(*) FROM signal_calibration WHERE sample_size >= 10"
        ).fetchone()[0]
        proven_cells = conn.execute(
            "SELECT COUNT(*) FROM signal_calibration WHERE sample_size >= 20 AND hit_rate_t1 >= 0.6"
        ).fetchone()[0]
        return {'matrix': [dict(r) for r in rows], 'total_cells': total_cells, 'proven_cells': proven_cells}
    finally:
        conn.close()


# â”€â”€ FORGE Pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

import json as _json
from datetime import datetime as _dt, timezone as _tz


def _parse_json_list(val) -> list:
    if not val:
        return []
    try:
        return _json.loads(val) if isinstance(val, str) else (val if isinstance(val, list) else [])
    except Exception:
        return []


COUNTRY_TO_REGION: dict = {
    'GB': 'UK',  'IE': 'UK',
    'US': 'US',  'CA': 'US',
    'DE': 'Europe', 'FR': 'Europe', 'NL': 'Europe', 'IT': 'Europe',
    'ES': 'Europe', 'SE': 'Europe', 'CH': 'Europe', 'AT': 'Europe', 'BE': 'Europe',
    'NO': 'Europe', 'DK': 'Europe', 'FI': 'Europe', 'PT': 'Europe', 'PL': 'Europe',
    'JP': 'Japan',
    'HK': 'HK',  'CN': 'HK',  'SG': 'HK',  'TW': 'HK',
    'AU': 'Australia', 'NZ': 'Australia',
    'IN': 'India',
    'KR': 'Korea',
}


def _ticker_region(ticker: str) -> str:
    if ticker.endswith('.L'):  return 'UK'
    if ticker.endswith('.T'):  return 'Japan'
    if ticker.endswith(('.HK', '.TW')): return 'HK'
    if ticker.endswith('.AX'): return 'Australia'
    if ticker.endswith(('.KS', '.KQ')): return 'Korea'
    if ticker.endswith(('.NS', '.BO')): return 'India'
    if ticker.endswith(('.DE', '.PA', '.AS', '.MI', '.MC', '.ST', '.OL', '.CO', '.HE')): return 'Europe'
    return 'US'


def _score_pattern_for_user(pat: dict, facts: dict, user_prefs: dict, pipeline_tickers: set) -> tuple:
    """Score a single pattern against user preferences. Returns (score, [reason_tags])."""
    ticker = pat.get('ticker', '')
    if ticker.upper() in pipeline_tickers:
        return (-999, [])

    score = 0.0
    reasons: list[str] = []

    # â”€â”€ Base quality (0â€“30) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    q = float(pat.get('quality_score') or 0)
    score += q * 30

    # â”€â”€ Timeframe match (0â€“20) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    user_tfs = user_prefs.get('timeframes', [])
    pat_tf = pat.get('timeframe', '')
    if pat_tf in user_tfs:
        score += 20
        reasons.append(f'â± {pat_tf}')
    elif any(tf in pat_tf for tf in user_tfs):
        score += 10

    # â”€â”€ Pattern type match (0â€“15) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    user_patterns = user_prefs.get('pattern_types', [])
    pat_type = pat.get('pattern_type', '')
    if user_patterns and pat_type in user_patterns:
        score += 15
        reasons.append(f'â¬¡ {pat_type.replace("_"," ")}')

    # â”€â”€ Sector match (0â€“15) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    user_sectors = [s.lower() for s in user_prefs.get('sectors', [])]
    ticker_sector = (facts.get('sector') or '').lower()
    if user_sectors and ticker_sector:
        sector_hit = any(
            us in ticker_sector or ticker_sector in us
            for us in user_sectors
        )
        if sector_hit:
            score += 15
            reasons.append(f'â—ˆ {ticker_sector}')

    # â”€â”€ Region / home country (0â€“20) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ticker_region = _ticker_region(ticker)
    user_regions = user_prefs.get('regions', [])
    country = (user_prefs.get('country') or '').upper()
    home_region = COUNTRY_TO_REGION.get(country, '')

    if home_region and ticker_region == home_region:
        score += 20
        reasons.append('ðŸ  home market')
    elif user_regions and ticker_region in user_regions:
        score += 12
        reasons.append('â—Ž preferred region')
    elif not user_regions and not home_region:
        if ticker_region in ('US', 'UK'):
            score += 5

    # â”€â”€ Macro alignment (0â€“15) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    macro = facts.get('macro_confirmation', 'no_data')
    macro_pts = {'confirmed': 15, 'partial': 8, 'unconfirmed': 2, 'no_data': 0}.get(macro, 0)
    score += macro_pts
    if macro_pts >= 8:
        reasons.append('âœ¦ macro')

    # â”€â”€ Thesis alignment (0â€“15 / â€“10) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    auto_thesis = (facts.get('auto_thesis') or '').lower()
    pat_dir = (pat.get('direction') or '').lower()
    if auto_thesis and auto_thesis == pat_dir:
        score += 15
        reasons.append('â–² thesis')
    elif auto_thesis and auto_thesis != pat_dir:
        score -= 10

    # â”€â”€ Risk tolerance â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    risk = user_prefs.get('risk_tolerance', 'moderate')
    conviction = (facts.get('conviction_tier') or '').lower()
    if risk == 'conservative':
        if conviction == 'high':
            score += 10
            reasons.append('â˜… high conv')
        elif conviction in ('low', 'avoid'):
            score -= 15
    elif risk == 'moderate':
        if conviction == 'high':
            score += 5
        elif conviction == 'avoid':
            score -= 10
    # aggressive: no conviction penalty

    # â”€â”€ Style timeframe alignment (0â€“8 / â€“5) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    style = user_prefs.get('style_timeframe', 'swing')
    _tf_map = {
        'scalp':    ['1m', '5m', '15m'],
        'intraday': ['15m', '30m', '1h'],
        'swing':    ['1h', '4h', '1d'],
        'position': ['1d', '1w'],
    }
    style_tfs = _tf_map.get(style, [])
    if pat_tf in style_tfs:
        score += 8
    elif style_tfs and pat_tf not in style_tfs:
        score -= 5

    # â”€â”€ Freshness penalty (0 to â€“15) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        decay = float(facts.get('pattern_decay_pct') or 0)
    except (TypeError, ValueError):
        decay = 0.0
    score -= decay * 15

    # â”€â”€ Catalyst bonus (0â€“10) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if facts.get('catalyst'):
        score += 10
        reasons.append('âš¡ catalyst')

    # â”€â”€ KB avoid penalty â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if conviction == 'avoid':
        score -= 20

    # â”€â”€ Calibration bonus (0â€“30) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    _MIN_CAL_SAMPLES = 10
    _CAL_BONUS_MAX   = 30
    try:
        import math as _math
        ticker_upper = ticker.upper()
        pat_type     = pat.get('pattern_type', '')
        tf           = pat.get('timeframe', '')
        current_regime = facts.get('market_regime', '') or facts.get('price_regime', '')

        cal_row = None
        if current_regime:
            cal_row = conn.execute("""
                SELECT hit_rate_t1, hit_rate_t2, stopped_out_rate,
                       sample_size, calibration_confidence
                FROM signal_calibration
                WHERE UPPER(ticker) = ?
                  AND pattern_type  = ?
                  AND timeframe     = ?
                  AND market_regime = ?
                  AND sample_size  >= ?
                ORDER BY sample_size DESC
                LIMIT 1
            """, (ticker_upper, pat_type, tf, current_regime, _MIN_CAL_SAMPLES)).fetchone()

        if not cal_row:
            cal_row = conn.execute("""
                SELECT hit_rate_t1, hit_rate_t2, stopped_out_rate,
                       sample_size, calibration_confidence
                FROM signal_calibration
                WHERE UPPER(ticker) = ?
                  AND pattern_type  = ?
                  AND timeframe     = ?
                  AND (market_regime IS NULL OR market_regime = '')
                  AND sample_size  >= ?
                ORDER BY sample_size DESC
                LIMIT 1
            """, (ticker_upper, pat_type, tf, _MIN_CAL_SAMPLES)).fetchone()

        is_tf_fallback = False
        _tf_discount   = 1.0
        _TF_DISCOUNT   = {'15m': 0.5, '1h': 0.65, '4h': 0.8}
        if not cal_row and tf != '1d':
            if current_regime:
                cal_row = conn.execute("""
                    SELECT hit_rate_t1, hit_rate_t2, stopped_out_rate,
                           sample_size, calibration_confidence
                    FROM signal_calibration
                    WHERE UPPER(ticker) = ?
                      AND pattern_type  = ?
                      AND timeframe     = '1d'
                      AND market_regime = ?
                      AND sample_size  >= ?
                    ORDER BY sample_size DESC LIMIT 1
                """, (ticker_upper, pat_type, current_regime, _MIN_CAL_SAMPLES)).fetchone()
            if not cal_row:
                cal_row = conn.execute("""
                    SELECT hit_rate_t1, hit_rate_t2, stopped_out_rate,
                           sample_size, calibration_confidence
                    FROM signal_calibration
                    WHERE UPPER(ticker) = ?
                      AND pattern_type  = ?
                      AND timeframe     = '1d'
                      AND (market_regime IS NULL OR market_regime = '')
                      AND sample_size  >= ?
                    ORDER BY sample_size DESC LIMIT 1
                """, (ticker_upper, pat_type, _MIN_CAL_SAMPLES)).fetchone()
            if cal_row:
                _tf_discount   = _TF_DISCOUNT.get(tf, 0.6)
                is_tf_fallback = True

        if cal_row:
            _CAL_BASELINE       = 0.50
            _CAL_BASELINE_PROXY = 0.35
            baseline     = _CAL_BASELINE_PROXY if is_tf_fallback else _CAL_BASELINE
            hit_rate     = cal_row[0] or 0.0
            stop_rate    = cal_row[2] or 1.0
            n            = cal_row[3] or 0
            size_weight  = min(1.0, _math.log(n + 1) / _math.log(201))
            hit_score    = max(0.0, (hit_rate - baseline) * 2.0)
            stop_penalty = stop_rate * 0.5
            cal_bonus    = _CAL_BONUS_MAX * hit_score * size_weight * (1.0 - stop_penalty)
            if is_tf_fallback:
                cal_bonus *= _tf_discount
            score += round(cal_bonus, 2)
            if cal_bonus > 5:
                if is_tf_fallback:
                    reasons.append(f'\u25ce {(hit_rate * 100):.0f}% 1d')
                else:
                    reasons.append(f'\u25ce {(hit_rate * 100):.0f}% hist')
    except Exception:
        pass

    return (round(score, 2), reasons)


def _get_pipeline_tickers(conn, user_id: str) -> set:
    """Return set of uppercase tickers already in user's pipeline."""
    from users.user_store import _ensure_tip_followups_table
    _ensure_tip_followups_table(conn)
    rows = conn.execute(
        "SELECT DISTINCT UPPER(ticker) FROM tip_followups "
        "WHERE user_id = ? AND status IN ('watching','staged','active')",
        (user_id,),
    ).fetchall()
    return {r[0] for r in rows}


def _detected_for_user(conn, user_id: str, limit: int = 15) -> list[dict]:
    """Score all open patterns for a user and return top N as Market Objects."""
    # User prefs â€” read all profile fields
    try:
        prow = conn.execute(
            """SELECT tip_timeframes, tip_pattern_types, selected_sectors,
                      style_sector_focus, preferred_regions, country,
                      style_risk_tolerance, style_timeframe, tip_markets,
                      account_currency
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        if prow:
            user_prefs = {
                'timeframes':     _parse_json_list(prow[0]),
                'pattern_types':  _parse_json_list(prow[1]),
                'sectors':        _parse_json_list(prow[2]) or _parse_json_list(prow[3]),
                'regions':        _parse_json_list(prow[4]),
                'country':        prow[5] or '',
                'risk_tolerance': prow[6] or 'moderate',
                'style_timeframe':prow[7] or 'swing',
                'markets':        _parse_json_list(prow[8]),
                'currency':       prow[9] or 'USD',
            }
        else:
            user_prefs = {}
    except Exception:
        user_prefs = {}

    pipeline_tickers = _get_pipeline_tickers(conn, user_id)

    # All open patterns
    pat_rows = conn.execute(
        """SELECT id, ticker, pattern_type, direction, timeframe, quality_score,
                  zone_high, zone_low, kb_conviction, kb_regime, kb_signal_dir
           FROM pattern_signals
           WHERE status NOT IN ('filled','broken','expired')
             AND quality_score >= 0.35"""
    ).fetchall()
        pat_rows = [r for r in pat_rows if (r['kb_conviction'] or '') != 'avoid']

    # Group by ticker
    pats_by_ticker: dict[str, list] = {}
    for r in pat_rows:
        pats_by_ticker.setdefault(r['ticker'], []).append(dict(r))

    # Facts for all tickers
    all_tickers = set(pats_by_ticker.keys())
    if not all_tickers:
        return []
    placeholders = ','.join('?' for _ in all_tickers)
    fact_rows = conn.execute(
        f"SELECT subject, predicate, object FROM facts WHERE UPPER(subject) IN ({placeholders}) ORDER BY timestamp DESC",
        [t.upper() for t in all_tickers],
    ).fetchall()
    facts_by_ticker: dict[str, dict] = {}
    seen: set[str] = set()
    for r in fact_rows:
        subj = r['subject'].upper()
        pred = r['predicate']
        key = f'{subj}|{pred}'
        if key in seen:
            continue
        seen.add(key)
        facts_by_ticker.setdefault(subj, {})[pred] = r['object']

    # Score each pattern and pick top N
    scored: list[tuple] = []
    for ticker, pats in pats_by_ticker.items():
        facts = facts_by_ticker.get(ticker.upper(), {})
        for pat in pats:
            s, reasons = _score_pattern_for_user(pat, facts, user_prefs, pipeline_tickers)
            if s <= -999:
                continue
            scored.append((s, reasons, ticker, pat, facts))

    scored.sort(key=lambda x: x[0], reverse=True)
    # Deduplicate by ticker â€” keep best-scoring pattern per ticker
    seen_tickers: set = set()
    top = []
    for item in scored:
        t = item[2].upper()
        if t not in seen_tickers:
            seen_tickers.add(t)
            top.append(item)
        if len(top) >= limit:
            break

    # Build Market Objects for the top results
    results = []
    for s, reasons, ticker, pat, facts in top:
        obj = _resolve_market_object(
            ticker,
            facts,
            pats_by_ticker.get(ticker, []),
            [],  # no bot positions needed for detected
        )
        obj['_score'] = s
        obj['_reasons'] = reasons
        results.append(obj)

    return results


class PipelineWatchRequest(BaseModel):
    pattern_id: int
    ticker: str


class PipelineStageRequest(BaseModel):
    stage: str
    position_size: float | None = None
    note: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    target_1: float | None = None
    target_2: float | None = None
    target_entry: float | None = None
    target_exit: float | None = None
    initiated_by: str | None = None


@router.get('/ops/pipeline/detected')
async def get_pipeline_detected(current_user: str = Depends(get_current_user)):
    """Personalised top-15 pattern suggestions for FORGE DETECTED column."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        return {'detected': _detected_for_user(conn, current_user)}
    finally:
        conn.close()


@router.get('/ops/pipeline')
async def get_pipeline(current_user: str = Depends(get_current_user)):
    """All six FORGE pipeline columns in one call."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        from users.user_store import _ensure_tip_followups_table
        _ensure_tip_followups_table(conn)

        uid = current_user

        def _followup_rows(where: str, params: tuple = ()) -> list[dict]:
            rows = conn.execute(
                f"""SELECT id, ticker, direction, entry_price, stop_loss,
                           target_1, target_2, target_3, status, pattern_type,
                           timeframe, opened_at, closed_at, position_size,
                           user_note, position_source, pattern_id,
                           regime_at_entry, conviction_at_entry, expires_at,
                           zone_high, zone_low, target_entry, target_exit
                    FROM tip_followups
                    WHERE user_id = ? AND {where}
                    ORDER BY opened_at DESC""",
                (uid,) + params,
            ).fetchall()
            return [dict(r) for r in rows]

        def _enrich_with_kb(rows: list[dict]) -> list[dict]:
            """Merge live KB facts into pipeline rows by ticker."""
            if not rows:
                return rows
            tickers = list({r['ticker'].upper() for r in rows if r.get('ticker')})
            placeholders = ','.join('?' for _ in tickers)
            kb_rows = conn.execute(
                f"""SELECT subject, predicate, object FROM facts
                    WHERE UPPER(subject) IN ({placeholders})
                    ORDER BY timestamp DESC""",
                tickers,
            ).fetchall()
            facts: dict[str, dict] = {}
            seen: set = set()
            for r in kb_rows:
                subj = r['subject'].upper()
                pred = r['predicate']
                key = f'{subj}|{pred}'
                if key in seen:
                    continue
                seen.add(key)
                facts.setdefault(subj, {})[pred] = r['object']

            _KB_FIELDS = [
                'conviction_tier', 'macro_confirmation', 'institutional_flow',
                'signal_quality', 'auto_thesis', 'auto_thesis_score',
                'pattern_decay_pct', 'pattern_hours_remaining', 'catalyst',
                'best_regime', 'worst_regime', 'volatility_regime',
                'upside_pct', 'signal_direction',
            ]
            enriched = []
            for row in rows:
                row = dict(row)
                kb = facts.get((row.get('ticker') or '').upper(), {})
                for field in _KB_FIELDS:
                    if field not in row or row[field] is None:
                        val = kb.get(field)
                        if val is not None:
                            try:
                                row[field] = float(val) if field in (
                                    'auto_thesis_score', 'pattern_decay_pct',
                                    'pattern_hours_remaining', 'upside_pct'
                                ) else val
                            except (TypeError, ValueError):
                                row[field] = val
                enriched.append(row)
            return enriched

        watching = _enrich_with_kb(_followup_rows("status = 'watching'"))
        staged = _enrich_with_kb(_followup_rows("status = 'staged'"))
        active = _enrich_with_kb(_followup_rows("status = 'active' AND position_source IN ('manual','pipeline')"))
        assessing = _followup_rows(
            "status IN ('assessing','closed','stopped_out','hit_t1','hit_t2') AND (user_note IS NULL OR user_note = '')"
        )
        complete = conn.execute(
            """SELECT id, ticker, direction, entry_price, stop_loss,
                      target_1, target_2, target_3, status, pattern_type,
                      timeframe, opened_at, closed_at, position_size,
                      user_note, position_source, pattern_id,
                      regime_at_entry, conviction_at_entry, expires_at,
                      zone_high, zone_low, target_entry, target_exit
               FROM tip_followups
               WHERE user_id = ? AND status = 'complete'
               ORDER BY closed_at DESC LIMIT 10""",
            (uid,),
        ).fetchall()
        complete = [dict(r) for r in complete]

        detected = _detected_for_user(conn, uid)

        return {
            'detected': detected,
            'watching': watching,
            'staged': staged,
            'active': active,
            'assessing': assessing,
            'complete': complete,
        }
    finally:
        conn.close()


@router.post('/ops/pipeline/watch')
async def pipeline_watch(data: PipelineWatchRequest, current_user: str = Depends(get_current_user)):
    """Add a pattern to FORGE pipeline as WATCHING."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        # Look up pattern details
        pat = conn.execute(
            """SELECT id, ticker, pattern_type, direction, timeframe, quality_score,
                      zone_high, zone_low, kb_conviction, kb_regime
               FROM pattern_signals WHERE id = ?""",
            (data.pattern_id,),
        ).fetchone()
        if not pat:
            raise HTTPException(404, detail='Pattern not found')

        pat = dict(pat)
        direction = (pat.get('direction') or 'bullish').lower()
        zh = float(pat['zone_high']) if pat.get('zone_high') else None
        zl = float(pat['zone_low'])  if pat.get('zone_low')  else None

        # Bearish: enter at resistance (zone_high), stop 2.5% above it
        # Bullish: enter at support  (zone_low),  stop 2.5% below it
        if direction == 'bearish':
            entry = zh
            stop  = round(zh * 1.025, 4) if zh else None
        else:
            entry = zl
            stop  = round(zl * 0.975, 4) if zl else None

        # T1/T2: project 1.5Ã— and 3Ã— zone size beyond the zone
        zone_size = abs(zh - zl) if (zh and zl) else 0
        if direction == 'bearish':
            t1 = round(zl - zone_size * 1.5, 4) if (zl and zone_size) else None
            t2 = round(zl - zone_size * 3.0, 4) if (zl and zone_size) else None
        else:
            t1 = round(zh + zone_size * 1.5, 4) if (zh and zone_size) else None
            t2 = round(zh + zone_size * 3.0, 4) if (zh and zone_size) else None

        from users.user_store import upsert_tip_followup
        row_id, _ = upsert_tip_followup(
            ext.DB_PATH,
            user_id=current_user,
            ticker=data.ticker.upper(),
            direction=direction,
            entry_price=entry,
            stop_loss=stop,
            target_1=t1,
            target_2=t2,
            pattern_type=pat.get('pattern_type'),
            timeframe=pat.get('timeframe'),
            zone_low=float(pat['zone_low']) if pat.get('zone_low') else None,
            zone_high=float(pat['zone_high']) if pat.get('zone_high') else None,
            regime_at_entry=pat.get('kb_regime'),
            conviction_at_entry=pat.get('kb_conviction'),
            initial_status='watching',
        )

        # Set position_source and pattern_id
        conn2 = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            conn2.execute(
                "UPDATE tip_followups SET position_source='pipeline', pattern_id=? WHERE id=?",
                (data.pattern_id, row_id),
            )
            conn2.commit()
        finally:
            conn2.close()

        return {'id': row_id, 'ticker': data.ticker.upper(), 'status': 'watching'}
    finally:
        conn.close()


@router.patch('/ops/pipeline/{followup_id}/stage')
async def pipeline_stage(followup_id: int, data: PipelineStageRequest, current_user: str = Depends(get_current_user)):
    """Advance a pipeline item's stage."""
    _dev_gate(current_user)

    valid_transitions = {
        'staged':    ['watching'],
        'active':    ['staged'],
        'assessing': ['active'],
        'complete':  ['closed', 'stopped_out', 'hit_t1', 'hit_t2', 'assessing'],
    }
    allowed_from = valid_transitions.get(data.stage)
    if not allowed_from:
        raise HTTPException(400, detail=f"Invalid target stage: {data.stage}")

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        from users.user_store import _ensure_tip_followups_table
        _ensure_tip_followups_table(conn)
        row = conn.execute(
            "SELECT id, status FROM tip_followups WHERE id = ? AND user_id = ?",
            (followup_id, current_user),
        ).fetchone()
        if not row:
            raise HTTPException(404, detail='Pipeline item not found')
        current_status = row[1]
        if current_status not in allowed_from:
            raise HTTPException(400, detail=f"Cannot transition from '{current_status}' to '{data.stage}'")

        updates = ['status = ?']
        params: list = [data.stage]
        if data.position_size is not None:
            updates.append('position_size = ?')
            params.append(data.position_size)
        if data.entry_price is not None:
            updates.append('entry_price = ?')
            params.append(data.entry_price)
        if data.stop_loss is not None:
            updates.append('stop_loss = ?')
            params.append(data.stop_loss)
        if data.target_1 is not None:
            updates.append('target_1 = ?')
            params.append(data.target_1)
        if data.target_2 is not None:
            updates.append('target_2 = ?')
            params.append(data.target_2)
        if data.target_entry is not None:
            updates.append('target_entry = ?')
            params.append(data.target_entry)
        if data.target_exit is not None:
            updates.append('target_exit = ?')
            params.append(data.target_exit)
        if data.note is not None:
            updates.append('user_note = ?')
            params.append(data.note[:500])
        if data.initiated_by is not None:
            updates.append('initiated_by = ?')
            params.append(data.initiated_by)
        if data.stage in ('complete', 'assessing'):
            from datetime import datetime, timezone
            updates.append('closed_at = ?')
            params.append(datetime.now(timezone.utc).isoformat())

        params.append(followup_id)
        conn.execute(
            f"UPDATE tip_followups SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()
        return {'id': followup_id, 'stage': data.stage}
    finally:
        conn.close()


@router.delete('/ops/pipeline/{followup_id}')
async def pipeline_remove(followup_id: int, current_user: str = Depends(get_current_user)):
    """Remove an item from the FORGE pipeline."""
    _dev_gate(current_user)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        from users.user_store import _ensure_tip_followups_table
        _ensure_tip_followups_table(conn)
        conn.execute(
            "DELETE FROM tip_followups WHERE id = ? AND user_id = ?",
            (followup_id, current_user),
        )
        conn.commit()
        return {'deleted': followup_id}
    finally:
        conn.close()
