"""routes_v2/discovery.py — Internal discovery fleet API routes.

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


# ── Dev-only proxy routes (user-authenticated, is_dev gated) ─────────────────

def _dev_gate(user_id: str) -> None:
    conn = sqlite3.connect(ext.DB_PATH, timeout=5)
    row = conn.execute(
        'SELECT is_dev FROM user_preferences WHERE user_id=?', (user_id,)
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        raise HTTPException(403, detail='dev only')


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


# ── Ops Terminal Briefing ─────────────────────────────────────────────────────

@router.get('/ops/briefing')
async def ops_briefing(user_id: str = Depends(get_current_user)):
    """Aggregated briefing for the Meridian Operations Terminal."""
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        import json as _json
        from datetime import datetime, timezone, timedelta

        # ── Regime: KB facts for market, SPY, HYG, TLT ──────────────────
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

        # ── Overnight closes (last 12h) ─────────────────────────────────
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

        # ── Fleet summary ────────────────────────────────────────────────
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

        # ── Observatory ──────────────────────────────────────────────────
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

        # ── Pattern pool stats ───────────────────────────────────────────
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

        return {
            'regime': regime,
            'overnight': overnight,
            'fleet': fleet,
            'observatory': observatory,
            'patterns': patterns,
        }
    finally:
        conn.close()


# ── CORPUS — Market Objects ───────────────────────────────────────────────────

import time as _time

_corpus_cache: dict = {'data': None, 'ts': 0}
_CORPUS_TTL = 300  # 5 minutes


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
                'tf': p.get('timeframe'),
                'quality': float(p.get('quality_score') or 0),
                'direction': p.get('direction', ''),
            }
            for p in pats
        ],
        'bot_consensus': {'bearish': bear_count, 'bullish': bull_count},
        'regime': facts.get('market_regime', ''),
        'object_confidence': obj_confidence,
        'last_verified': facts.get('_latest_ts', ''),
    }


def _bulk_resolve(conn) -> list[dict]:
    """Resolve all tickers with open patterns into Market Objects."""
    # 1. All tickers with open patterns
    pat_rows = conn.execute(
        """SELECT id, ticker, pattern_type, direction, timeframe, quality_score,
                  zone_high, zone_low, kb_conviction, kb_regime, kb_signal_dir
           FROM pattern_signals
           WHERE status NOT IN ('filled','broken','expired')
             AND quality_score >= 0.50"""
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
    """Single Market Object for a ticker — resolved from KB atoms, patterns, positions."""
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
