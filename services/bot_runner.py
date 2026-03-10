"""
services/bot_runner.py — Evolutionary Strategy Bot Runner

Manages the lifecycle of genome-defined strategy bots.
Each bot runs its own scan thread, using the same _should_enter() logic
as the generalist but with upstream genome filters applied.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

import extensions as ext

_logger = logging.getLogger('bot_runner')

# ── Genome helpers ─────────────────────────────────────────────────────────────

_PATTERN_POOL = ['fvg', 'ifvg', 'order_block', 'breaker', 'mitigation', 'bos', 'choch']
_SECTOR_POOL  = ['technology', 'energy', 'financials', 'healthcare', 'consumer', 'utilities', 'industrials']
_VOL_POOL     = ['low', 'medium', 'high', 'extreme']
_REGIME_POOL  = ['risk_on_expansion', 'risk_off_contraction', 'recovery', 'stagflation']


def _hash_genome(genome: dict) -> str:
    """Stable SHA-256 hash of the parameter vector (excludes identity/lifecycle fields)."""
    keys = ['pattern_types', 'sectors', 'exchanges', 'volatility', 'regimes',
            'timeframes', 'direction_bias', 'risk_pct', 'max_positions', 'min_quality']
    vector = {k: genome.get(k) for k in keys}
    return hashlib.sha256(json.dumps(vector, sort_keys=True).encode()).hexdigest()[:16]


def _name_genome(genome: dict) -> str:
    """Human-readable name from genome genes."""
    parts = []
    pt = genome.get('pattern_types')
    if pt:
        pts = json.loads(pt) if isinstance(pt, str) else pt
        parts.append('+'.join(p.upper() for p in pts[:2]))
    sec = genome.get('sectors')
    if sec:
        secs = json.loads(sec) if isinstance(sec, str) else sec
        if secs:
            parts.append(secs[0].title())
    vol = genome.get('volatility')
    if vol:
        vols = json.loads(vol) if isinstance(vol, str) else vol
        if vols:
            parts.append(vols[0].title() + ' Vol')
    direction = genome.get('direction_bias')
    if direction:
        parts.append(direction.title())
    role = genome.get('role', 'seed')
    if role in ('explore', 'mutant'):
        gid = genome.get('genome_id', '')[:4]
        parts.append(f'#{gid}')
    return ' '.join(parts) if parts else f"Bot {genome.get('genome_id', '')[:8]}"


def generate_random_genome() -> dict:
    """Fully randomised genome. Role = explore."""
    n_pat = random.randint(1, 3)
    pat = random.sample(_PATTERN_POOL, k=n_pat)
    sec = random.sample(_SECTOR_POOL, k=random.randint(1, 2)) if random.random() > 0.4 else None
    vol = random.sample(_VOL_POOL, k=random.randint(1, 2)) if random.random() > 0.4 else None
    reg = random.sample(_REGIME_POOL, k=random.randint(1, 2)) if random.random() > 0.5 else None
    direction = random.choice([None, 'bullish', 'bearish'])
    g = {
        'pattern_types':  json.dumps(pat),
        'sectors':        json.dumps(sec) if sec else None,
        'exchanges':      None,
        'volatility':     json.dumps(vol) if vol else None,
        'regimes':        json.dumps(reg) if reg else None,
        'timeframes':     None,
        'direction_bias': direction,
        'risk_pct':       round(random.uniform(0.5, 2.0), 2),
        'max_positions':  random.randint(2, 6),
        'min_quality':    round(random.uniform(0.55, 0.75), 2),
        'role':           'explore',
        'generation':     0,
        'parent_id':      None,
    }
    g['genome_id'] = _hash_genome(g)
    g['strategy_name'] = _name_genome(g)
    return g


# ── Seed templates ─────────────────────────────────────────────────────────────

def _make_seed_templates() -> list[dict]:
    templates = [
        {
            'strategy_name': 'FVG Scanner',
            'pattern_types':  json.dumps(['fvg', 'ifvg']),
            'sectors':        None,
            'exchanges':      None,
            'volatility':     None,
            'regimes':        None,
            'timeframes':     None,
            'direction_bias': None,
            'risk_pct':       1.0,
            'max_positions':  4,
            'min_quality':    0.65,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
        {
            'strategy_name': 'Order Block Hunter',
            'pattern_types':  json.dumps(['order_block']),
            'sectors':        None,
            'exchanges':      None,
            'volatility':     None,
            'regimes':        None,
            'timeframes':     None,
            'direction_bias': None,
            'risk_pct':       1.0,
            'max_positions':  4,
            'min_quality':    0.70,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
        {
            'strategy_name': 'Tech Momentum',
            'pattern_types':  json.dumps(['fvg', 'order_block']),
            'sectors':        json.dumps(['technology']),
            'exchanges':      None,
            'volatility':     json.dumps(['high', 'extreme']),
            'regimes':        json.dumps(['risk_on_expansion', 'recovery']),
            'timeframes':     None,
            'direction_bias': 'bullish',
            'risk_pct':       1.5,
            'max_positions':  4,
            'min_quality':    0.65,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
        {
            'strategy_name': 'Energy Rotation',
            'pattern_types':  json.dumps(['order_block', 'breaker']),
            'sectors':        json.dumps(['energy']),
            'exchanges':      None,
            'volatility':     None,
            'regimes':        None,
            'timeframes':     None,
            'direction_bias': None,
            'risk_pct':       1.0,
            'max_positions':  4,
            'min_quality':    0.65,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
        {
            'strategy_name': 'Risk-Off Shorts',
            'pattern_types':  json.dumps(['breaker', 'mitigation']),
            'sectors':        None,
            'exchanges':      None,
            'volatility':     json.dumps(['high', 'extreme']),
            'regimes':        json.dumps(['risk_off_contraction', 'stagflation']),
            'timeframes':     None,
            'direction_bias': 'bearish',
            'risk_pct':       1.0,
            'max_positions':  3,
            'min_quality':    0.65,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
        {
            'strategy_name': 'UK Blue Chips',
            'pattern_types':  json.dumps(['fvg', 'order_block']),
            'sectors':        None,
            'exchanges':      json.dumps(['.L']),
            'volatility':     None,
            'regimes':        None,
            'timeframes':     None,
            'direction_bias': None,
            'risk_pct':       1.0,
            'max_positions':  4,
            'min_quality':    0.65,
            'role':           'seed',
            'generation':     0,
            'parent_id':      None,
        },
    ]
    # Two random explorers
    for _ in range(2):
        g = generate_random_genome()
        g['role'] = 'explore'
        templates.append(g)
    # Assign genome_ids and names where missing
    for t in templates:
        if 'genome_id' not in t:
            t['genome_id'] = _hash_genome(t)
        if 'strategy_name' not in t or not t['strategy_name']:
            t['strategy_name'] = _name_genome(t)
    return templates


# ── BotRunner ─────────────────────────────────────────────────────────────────

class BotRunner:
    """Manages lifecycle of all genome-defined strategy bots."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._threads: dict[str, threading.Thread] = {}
        self._stop_events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    # ── Schema ────────────────────────────────────────────────────────────────

    def _ensure_tables(self, conn):
        from services.paper_trading import ensure_paper_tables
        ensure_paper_tables(conn)

    # ── Fleet management ──────────────────────────────────────────────────────

    def count_bots(self, user_id: str) -> int:
        """Return number of active bots for this user."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            self._ensure_tables(conn)
            n = conn.execute(
                "SELECT COUNT(*) FROM paper_bot_configs WHERE user_id=? AND active=1",
                (user_id,)
            ).fetchone()[0]
            conn.close()
            return n
        except Exception as e:
            _logger.warning('count_bots error for %s: %s', user_id, e)
            return 0

    def seed_fleet(self, user_id: str, total_balance: float) -> list[str]:
        """Create 8 seed bots, split capital equally, start all scanner threads."""
        templates = _make_seed_templates()
        per_bot = round(total_balance / len(templates), 2)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(self.db_path, timeout=10)
        self._ensure_tables(conn)
        bot_ids = []
        for tmpl in templates:
            bot_id = 'bot_' + str(uuid.uuid4()).replace('-', '')[:12]
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO paper_bot_configs
                    (user_id, bot_id, genome_id, strategy_name, generation, parent_id,
                     pattern_types, sectors, exchanges, volatility, regimes, timeframes,
                     direction_bias, risk_pct, max_positions, min_quality,
                     virtual_balance, initial_balance, role, active,
                     scan_interval_sec, min_trades_eval, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1800,25,?)
                """, (
                    user_id, bot_id,
                    tmpl['genome_id'], tmpl['strategy_name'],
                    tmpl.get('generation', 0), tmpl.get('parent_id'),
                    tmpl.get('pattern_types'), tmpl.get('sectors'), tmpl.get('exchanges'),
                    tmpl.get('volatility'), tmpl.get('regimes'), tmpl.get('timeframes'),
                    tmpl.get('direction_bias'),
                    tmpl.get('risk_pct', 1.0), tmpl.get('max_positions', 4),
                    tmpl.get('min_quality', 0.65),
                    per_bot, per_bot,
                    tmpl.get('role', 'seed'),
                    now_iso,
                ))
                bot_ids.append(bot_id)
            except Exception as e:
                _logger.warning('seed_fleet: failed to insert bot %s: %s', bot_id, e)
        conn.commit()
        conn.close()
        _logger.info('seed_fleet: created %d bots for %s (£%.0f each)', len(bot_ids), user_id, per_bot)
        # Start threads with 15s stagger
        for i, bot_id in enumerate(bot_ids):
            delay = i * 15
            self.start_bot(bot_id, startup_delay=delay)
        return bot_ids

    def start_bot(self, bot_id: str, startup_delay: int = 0) -> bool:
        """Start a scanner thread for this bot. Returns True if started."""
        with self._lock:
            if bot_id in self._threads and self._threads[bot_id].is_alive():
                return False  # already running
            stop_event = threading.Event()
            self._stop_events[bot_id] = stop_event
            t = threading.Thread(
                target=self._bot_scan_loop,
                args=(bot_id, stop_event, startup_delay),
                name=f'bot-{bot_id[:8]}',
                daemon=True,
            )
            self._threads[bot_id] = t
            t.start()
            _logger.info('start_bot: %s started (delay=%ds)', bot_id, startup_delay)
            return True

    def stop_bot(self, bot_id: str) -> bool:
        """Stop a bot's scanner thread, write paused_at."""
        with self._lock:
            ev = self._stop_events.get(bot_id)
            if ev:
                ev.set()
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute(
                "UPDATE paper_bot_configs SET paused_at=? WHERE bot_id=?",
                (now_iso, bot_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            _logger.warning('stop_bot: DB update failed for %s: %s', bot_id, e)
        _logger.info('stop_bot: %s stopped', bot_id)
        return True

    def kill_bot(self, bot_id: str, reason: str = 'evolution') -> bool:
        """Stop, close all positions at live price, mark killed."""
        self.stop_bot(bot_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            conn = sqlite3.connect(self.db_path, timeout=15)
            conn.row_factory = sqlite3.Row
            self._ensure_tables(conn)
            # Find user_id for this bot
            cfg = conn.execute(
                "SELECT user_id FROM paper_bot_configs WHERE bot_id=?", (bot_id,)
            ).fetchone()
            user_id = cfg['user_id'] if cfg else None
            # Close all open positions
            open_pos = conn.execute(
                "SELECT * FROM paper_positions WHERE bot_id=? AND status='open'", (bot_id,)
            ).fetchall()
            if open_pos:
                from services.paper_trading import fetch_live_prices
                tickers = [p['ticker'] for p in open_pos]
                live = fetch_live_prices(tickers)
                for pos in open_pos:
                    ticker = pos['ticker']
                    ep = live.get(ticker) or pos['entry_price']
                    qty = float(pos['quantity'])
                    from services.paper_trading import compute_pnl_r
                    pnl_r = compute_pnl_r(pos['direction'], pos['entry_price'], ep, pos['stop'])
                    conn.execute(
                        "UPDATE paper_positions SET status='closed', exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
                        (ep, pnl_r, now_iso, pos['id'])
                    )
                    # Restore position value to bot balance
                    conn.execute(
                        "UPDATE paper_bot_configs SET virtual_balance = virtual_balance + ? WHERE bot_id=?",
                        (ep * qty, bot_id)
                    )
            # Mark killed
            conn.execute(
                "UPDATE paper_bot_configs SET active=0, killed_at=?, paused_at=? WHERE bot_id=?",
                (now_iso, now_iso, bot_id)
            )
            if user_id:
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                    (user_id, 'evolution_kill', None,
                     f'Bot {bot_id[:8]} killed: {reason} ({len(open_pos)} positions closed)',
                     bot_id, now_iso)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            _logger.warning('kill_bot: error for %s: %s', bot_id, e)
        _logger.info('kill_bot: %s killed (%s)', bot_id, reason)
        return True

    def restore_bots(self, startup_delay: int = 90) -> int:
        """Re-launch all active bots at server startup."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            self._ensure_tables(conn)
            rows = conn.execute(
                "SELECT bot_id FROM paper_bot_configs WHERE active=1 AND paused_at IS NULL"
            ).fetchall()
            conn.close()
            started = 0
            for i, row in enumerate(rows):
                delay = startup_delay + i * 10
                if self.start_bot(row[0], startup_delay=delay):
                    started += 1
            _logger.info('restore_bots: re-launched %d bots', started)
            return started
        except Exception as e:
            _logger.warning('restore_bots error: %s', e)
            return 0

    def stop_all(self) -> None:
        """Stop all running bot threads (used on server shutdown)."""
        with self._lock:
            for ev in self._stop_events.values():
                ev.set()

    # ── Query builder ─────────────────────────────────────────────────────────

    def _build_filtered_query(self, config: dict) -> tuple[str, list]:
        """
        Translate genome filters into SQL WHERE clauses for pattern_signals.
        Returns (sql_fragment, params).
        """
        clauses = []
        params = []

        # Pattern types filter
        pt_raw = config.get('pattern_types')
        if pt_raw:
            pts = json.loads(pt_raw) if isinstance(pt_raw, str) else pt_raw
            if pts:
                placeholders = ','.join('?' for _ in pts)
                clauses.append(f'LOWER(p.pattern_type) IN ({placeholders})')
                params.extend([x.lower() for x in pts])

        # Sector filter (EXISTS subquery into facts)
        sec_raw = config.get('sectors')
        if sec_raw:
            secs = json.loads(sec_raw) if isinstance(sec_raw, str) else sec_raw
            if secs:
                sec_ph = ','.join('?' for _ in secs)
                clauses.append(
                    f"EXISTS (SELECT 1 FROM facts WHERE LOWER(subject)=LOWER(p.ticker) "
                    f"AND predicate='sector' AND LOWER(object) IN ({sec_ph}))"
                )
                params.extend([s.lower() for s in secs])

        # Exchange filter
        exc_raw = config.get('exchanges')
        if exc_raw:
            excs = json.loads(exc_raw) if isinstance(exc_raw, str) else exc_raw
            if excs:
                exc_parts = []
                for ex in excs:
                    if ex.startswith('.'):
                        exc_parts.append(f"p.ticker LIKE ?")
                        params.append(f'%{ex}')
                    else:
                        exc_parts.append(f"p.ticker NOT LIKE '%.%'")
                if exc_parts:
                    clauses.append('(' + ' OR '.join(exc_parts) + ')')

        # Direction bias filter
        direction = config.get('direction_bias')
        if direction:
            clauses.append('LOWER(p.direction) = ?')
            params.append(direction.lower())

        return (' AND '.join(clauses), params)

    # ── Bot scan loop ─────────────────────────────────────────────────────────

    def _bot_scan_loop(self, bot_id: str, stop_event: threading.Event, startup_delay: int = 0):
        """Main scan loop for a single bot."""
        _logger.info('Bot scan loop started: %s (delay=%ds)', bot_id, startup_delay)
        if startup_delay and stop_event.wait(startup_delay):
            return  # stop fired during startup delay
        while not stop_event.is_set():
            try:
                result = self._bot_scan_once(bot_id)
                _logger.debug('Bot %s scan: %s', bot_id, result)
            except Exception as e:
                _logger.error('Bot %s scan error: %s', bot_id, e)
            # Read interval from config (may have been updated)
            try:
                conn = sqlite3.connect(self.db_path, timeout=5)
                row = conn.execute(
                    "SELECT scan_interval_sec FROM paper_bot_configs WHERE bot_id=?", (bot_id,)
                ).fetchone()
                conn.close()
                interval = int(row[0]) if row else 1800
            except Exception:
                interval = 1800
            stop_event.wait(interval)

    def _bot_scan_once(self, bot_id: str) -> dict:
        """Single scan cycle for one bot."""
        from services.paper_trading import (
            _should_enter, _is_market_open, compute_pnl_r,
            fetch_live_prices, _PAPER_MAX_NEW_PER_SCAN,
        )
        from analytics.signal_calibration import update_calibration, get_calibration
        from datetime import timedelta

        now_iso = datetime.now(timezone.utc).isoformat()

        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        conn.row_factory = sqlite3.Row

        try:
            self._ensure_tables(conn)

            # Load bot config
            cfg_row = conn.execute(
                "SELECT * FROM paper_bot_configs WHERE bot_id=? AND active=1",
                (bot_id,)
            ).fetchone()
            if not cfg_row:
                conn.close()
                return {'skipped': True, 'reason': 'bot not active'}
            config = dict(cfg_row)
            user_id   = config['user_id']
            max_pos   = config['max_positions']
            min_qual  = config['min_quality']
            risk_pct  = config['risk_pct']
            balance   = float(config['virtual_balance'])

            # Monitor open positions first
            self._monitor_bot_positions(bot_id, conn, now_iso)

            # Count open positions for this bot
            open_rows = conn.execute(
                "SELECT ticker FROM paper_positions WHERE bot_id=? AND status='open'", (bot_id,)
            ).fetchall()
            open_tickers = {r['ticker'] for r in open_rows}

            if len(open_tickers) >= max_pos:
                self._write_bot_equity(bot_id, conn, balance, len(open_tickers), now_iso)
                conn.commit()
                conn.close()
                return {'entries': 0, 'skips': 0, 'reason': 'max_positions reached'}

            # 24h cooldown (bot-specific)
            _cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            cooled = {r['ticker'] for r in conn.execute(
                "SELECT DISTINCT ticker FROM paper_positions "
                "WHERE bot_id=? AND status='stopped_out' AND closed_at > ?",
                (bot_id, _cutoff)
            ).fetchall()}

            # Build filtered candidate query
            filter_sql, filter_params = self._build_filtered_query(config)
            quality_floor = max(min_qual - 0.05, 0.55)
            base_where = (
                "p.status NOT IN ('filled','broken') "
                f"AND p.quality_score >= {quality_floor}"
            )
            full_where = f"{base_where} AND {filter_sql}" if filter_sql else base_where

            rows = conn.execute(
                f"""SELECT p.id, p.ticker, p.pattern_type, p.direction,
                           p.zone_high, p.zone_low, p.quality_score,
                           p.kb_conviction, p.kb_regime, p.kb_signal_dir
                    FROM pattern_signals p
                    WHERE {full_where}
                    ORDER BY p.quality_score DESC
                    LIMIT 100""",
                filter_params
            ).fetchall()

            candidates = [dict(r) for r in rows]

            # Enrich with calibration
            for c in candidates:
                try:
                    cal = get_calibration(
                        ticker=c['ticker'],
                        pattern_type=c.get('pattern_type', ''),
                        timeframe='4h',
                        db_path=self.db_path,
                    )
                    c['cal_hit_rate'] = cal.hit_rate_t1 if cal else None
                    c['cal_samples']  = cal.sample_size  if cal else 0
                except Exception:
                    c['cal_hit_rate'] = None
                    c['cal_samples']  = 0

            # Sort: open markets first, then calibration, then quality
            candidates.sort(key=lambda x: (
                1 if _is_market_open(x['ticker']) else 0,
                x.get('cal_hit_rate') or 0.0,
                x.get('quality_score') or 0.0,
            ), reverse=True)
            candidates = candidates[:50]

            # Post-query volatility/regime filter
            vol_filter = config.get('volatility')
            if vol_filter:
                vol_list = [v.lower() for v in (json.loads(vol_filter) if isinstance(vol_filter, str) else vol_filter)]
            else:
                vol_list = None
            reg_filter = config.get('regimes')
            if reg_filter:
                reg_list = [r.lower() for r in (json.loads(reg_filter) if isinstance(reg_filter, str) else reg_filter)]
            else:
                reg_list = None

            risk_per_trade = balance * risk_pct / 100.0
            max_pos_value  = balance * 0.20
            remaining_cash = balance
            entries = 0
            skips   = 0

            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                (user_id, 'scan_start', None,
                 f'Bot {config["strategy_name"]}: {len(candidates)} candidates, {len(open_tickers)}/{max_pos} slots',
                 bot_id, now_iso)
            )

            for c in candidates:
                ticker    = c['ticker']
                direction = c['direction']
                quality   = c.get('quality_score') or 0
                regime    = (c.get('kb_regime') or '').lower()

                if entries >= _PAPER_MAX_NEW_PER_SCAN:
                    break
                if ticker in open_tickers or ticker in cooled:
                    skips += 1
                    continue
                if not _is_market_open(ticker):
                    skips += 1
                    continue

                # Post-query volatility filter
                if vol_list:
                    ticker_vol = self._get_ticker_atom(ticker, 'volatility_regime', conn)
                    if ticker_vol and ticker_vol.lower() not in vol_list:
                        skips += 1
                        continue

                # Post-query regime filter
                if reg_list and regime:
                    if not any(r in regime for r in reg_list):
                        skips += 1
                        continue

                # Quality gate
                if quality < min_qual:
                    skips += 1
                    continue

                # Regime misalignment
                if regime and (
                    (direction == 'bullish' and any(x in regime for x in ('risk_off', 'bearish')))
                    or (direction == 'bearish' and any(x in regime for x in ('risk_on', 'bullish')))
                ):
                    skips += 1
                    continue

                zone_low  = float(c.get('zone_low') or 0)
                zone_high = float(c.get('zone_high') or 0)
                midpoint  = (zone_low + zone_high) / 2.0 if zone_low and zone_high else None
                if not midpoint or midpoint <= 0:
                    skips += 1
                    continue

                if direction == 'bullish':
                    entry_p = midpoint
                    stop_p  = round(zone_low * 0.995, 6)
                    risk    = entry_p - stop_p
                    t1_p    = round(entry_p + risk * 2, 6)
                    t2_p    = round(entry_p + risk * 3, 6)
                else:
                    entry_p = midpoint
                    stop_p  = round(zone_high * 1.005, 6)
                    risk    = stop_p - entry_p
                    t1_p    = round(entry_p - risk * 2, 6)
                    t2_p    = round(entry_p - risk * 3, 6)

                if risk <= 0:
                    skips += 1
                    continue

                should_enter, reason, size_mult = _should_enter(c, remaining_cash, risk_per_trade)
                if not should_enter:
                    skips += 1
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                        (user_id, 'skip', ticker, reason, bot_id, now_iso)
                    )
                    continue

                eff_risk  = risk_per_trade * size_mult
                qty = min(eff_risk / risk, (max_pos_value * size_mult) / entry_p)
                qty = max(round(qty, 4), 0.0001)
                pos_value = round(entry_p * qty, 2)
                if pos_value > remaining_cash:
                    if remaining_cash < risk_per_trade * 2:
                        skips += 1
                        continue
                    qty = round(remaining_cash / entry_p, 4)
                    pos_value = round(entry_p * qty, 2)

                # Deduct from bot balance
                conn.execute(
                    "UPDATE paper_bot_configs SET virtual_balance = virtual_balance - ? WHERE bot_id=?",
                    (pos_value, bot_id)
                )
                remaining_cash -= pos_value

                conn.execute(
                    """INSERT INTO paper_positions
                       (user_id, pattern_id, ticker, direction, entry_price, stop, t1, t2,
                        quantity, status, partial_closed, opened_at, note, ai_reasoning, bot_id)
                       VALUES (?,?,?,?,?,?,?,?,?,'open',0,?,?,?,?)""",
                    (user_id, c['id'], ticker, direction,
                     entry_p, stop_p, t1_p, t2_p, qty,
                     now_iso, f'Bot: {config["strategy_name"]}',
                     f'{c.get("pattern_type","?")} {direction} | q={quality:.2f} | {reason}',
                     bot_id)
                )
                open_tickers.add(ticker)
                entries += 1
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                    (user_id, 'entry', ticker,
                     f'{c.get("pattern_type","?")} {direction} entry={entry_p:.4f} stop={stop_p:.4f} t1={t1_p:.4f} value=£{pos_value:,.2f} | {reason}',
                     bot_id, now_iso)
                )

            # Write equity snapshot
            fresh_bal = conn.execute(
                "SELECT virtual_balance FROM paper_bot_configs WHERE bot_id=?", (bot_id,)
            ).fetchone()
            bal_now = float(fresh_bal[0]) if fresh_bal else balance
            self._write_bot_equity(bot_id, conn, bal_now, len(open_tickers), now_iso)
            conn.commit()
            conn.close()
            return {'entries': entries, 'skips': skips}

        except Exception as e:
            _logger.error('_bot_scan_once error for %s: %s', bot_id, e)
            try:
                conn.close()
            except Exception:
                pass
            return {'error': str(e)}

    def _monitor_bot_positions(self, bot_id: str, conn, now_iso: str):
        """Check open positions for this bot and apply stop/target logic."""
        from services.paper_trading import _is_market_open, compute_pnl_r, fetch_live_prices
        from analytics.signal_calibration import update_calibration

        open_pos = conn.execute(
            "SELECT * FROM paper_positions WHERE bot_id=? AND status='open'", (bot_id,)
        ).fetchall()
        if not open_pos:
            return

        # Fetch user_id for logging
        cfg = conn.execute(
            "SELECT user_id FROM paper_bot_configs WHERE bot_id=?", (bot_id,)
        ).fetchone()
        user_id = cfg['user_id'] if cfg else 'system'

        tickers = [p['ticker'] for p in open_pos]
        live = fetch_live_prices(tickers)

        for pos in open_pos:
            ticker = pos['ticker']
            if not _is_market_open(ticker):
                continue
            price = live.get(ticker, 0)
            if price <= 0:
                continue
            entry     = pos['entry_price']
            stop_     = pos['stop']
            t1        = pos['t1']
            t2        = pos['t2']
            direction = pos['direction']
            qty       = float(pos['quantity'])
            new_status = None
            exit_p     = None

            if direction == 'bullish':
                if price <= stop_:
                    new_status = 'stopped_out'; exit_p = price
                elif t2 is not None and price >= t2:
                    new_status = 't2_hit'; exit_p = price
                elif not pos['partial_closed'] and price >= t1:
                    half_qty = round(qty / 2, 6)
                    partial_val = round(price * half_qty, 2)
                    conn.execute(
                        'UPDATE paper_positions SET partial_closed=1, quantity=? WHERE id=?',
                        (half_qty, pos['id'])
                    )
                    conn.execute(
                        "UPDATE paper_bot_configs SET virtual_balance = virtual_balance + ? WHERE bot_id=?",
                        (partial_val, bot_id)
                    )
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                        (user_id, 't1_hit', ticker,
                         f't1_hit at {price:.4f} partial close {half_qty} units £{partial_val:,.2f}',
                         bot_id, now_iso)
                    )
            else:
                if price >= stop_:
                    new_status = 'stopped_out'; exit_p = price
                elif t2 is not None and price <= t2:
                    new_status = 't2_hit'; exit_p = price
                elif not pos['partial_closed'] and price <= t1:
                    half_qty = round(qty / 2, 6)
                    partial_val = round(price * half_qty, 2)
                    conn.execute(
                        'UPDATE paper_positions SET partial_closed=1, quantity=? WHERE id=?',
                        (half_qty, pos['id'])
                    )
                    conn.execute(
                        "UPDATE paper_bot_configs SET virtual_balance = virtual_balance + ? WHERE bot_id=?",
                        (partial_val, bot_id)
                    )
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                        (user_id, 't1_hit', ticker,
                         f't1_hit at {price:.4f} partial close {half_qty} units £{partial_val:,.2f}',
                         bot_id, now_iso)
                    )

            if new_status and exit_p is not None:
                pnl_r = compute_pnl_r(direction, entry, exit_p, stop_)
                conn.execute(
                    "UPDATE paper_positions SET status=?, exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
                    (new_status, exit_p, pnl_r, now_iso, pos['id'])
                )
                conn.execute(
                    "UPDATE paper_bot_configs SET virtual_balance = virtual_balance + ? WHERE bot_id=?",
                    (exit_p * qty, bot_id)
                )
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                    (user_id, new_status, ticker,
                     f'exit={exit_p:.4f} pnl_r={pnl_r:+.2f}',
                     bot_id, now_iso)
                )
                # Calibration feedback
                if pos['pattern_id']:
                    try:
                        pat = conn.execute(
                            "SELECT pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                            (pos['pattern_id'],)
                        ).fetchone()
                        if pat:
                            outcome = 'hit_t2' if new_status == 't2_hit' else 'stopped_out'
                            update_calibration(
                                ticker=ticker,
                                pattern_type=(pat[0] or 'unknown'),
                                timeframe=(pat[1] or '4h'),
                                market_regime=pat[2],
                                outcome=outcome,
                                db_path=self.db_path,
                            )
                    except Exception as _ce:
                        _logger.debug('bot calibration update failed %s: %s', ticker, _ce)

    def _write_bot_equity(self, bot_id: str, conn, balance: float, open_count: int, now_iso: str):
        """Write equity snapshot for this bot."""
        try:
            pos_rows = conn.execute(
                "SELECT entry_price, quantity FROM paper_positions WHERE bot_id=? AND status='open'",
                (bot_id,)
            ).fetchall()
            open_value = sum(float(r[0]) * float(r[1]) for r in pos_rows)
            equity = round(balance + open_value, 2)
            conn.execute(
                "INSERT INTO paper_bot_equity (bot_id, equity_value, cash_balance, open_positions, logged_at) VALUES (?,?,?,?,?)",
                (bot_id, equity, balance, open_count, now_iso)
            )
        except Exception as e:
            _logger.debug('_write_bot_equity failed for %s: %s', bot_id, e)

    def _get_ticker_atom(self, ticker: str, predicate: str, conn) -> Optional[str]:
        """Read a single KB atom for a ticker."""
        try:
            row = conn.execute(
                "SELECT object FROM facts WHERE LOWER(subject)=? AND predicate=? ORDER BY timestamp DESC LIMIT 1",
                (ticker.lower(), predicate)
            ).fetchone()
            return row[0] if row else None
        except Exception:
            return None

    # ── Performance metrics ───────────────────────────────────────────────────

    def get_bot_performance(self, bot_id: str) -> dict:
        """Compute fitness metrics for a bot."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            self._ensure_tables(conn)

            cfg = conn.execute(
                "SELECT * FROM paper_bot_configs WHERE bot_id=?", (bot_id,)
            ).fetchone()
            if not cfg:
                conn.close()
                return {}
            config = dict(cfg)

            closed = conn.execute(
                "SELECT pnl_r, status FROM paper_positions "
                "WHERE bot_id=? AND status NOT IN ('open') AND pnl_r IS NOT NULL",
                (bot_id,)
            ).fetchall()
            equity_rows = conn.execute(
                "SELECT equity_value FROM paper_bot_equity WHERE bot_id=? ORDER BY logged_at ASC",
                (bot_id,)
            ).fetchall()
            conn.close()

            total = len(closed)
            if total == 0:
                return {
                    'bot_id': bot_id, 'total_closed': 0, 'win_rate': 0.0,
                    'avg_r': 0.0, 'sharpe': 0.0, 'profit_factor': 0.0,
                    'max_drawdown_pct': 0.0, 'fitness': 0.0,
                    'tier': 'immature', 'strategy_name': config.get('strategy_name', ''),
                }

            pnls = [float(r['pnl_r']) for r in closed]
            wins = sum(1 for p in pnls if p > 0)
            win_rate   = wins / total
            avg_r      = sum(pnls) / total
            gross_pos  = sum(p for p in pnls if p > 0) or 0.0
            gross_neg  = abs(sum(p for p in pnls if p < 0)) or 1e-9
            profit_factor = gross_pos / gross_neg

            import statistics as _stats
            std_r = _stats.stdev(pnls) if len(pnls) > 1 else 0.0
            sharpe = avg_r / std_r if std_r > 0 else 0.0

            # Max drawdown from equity curve
            max_drawdown_pct = 0.0
            if equity_rows:
                eqs = [float(r['equity_value']) for r in equity_rows]
                peak = eqs[0]
                for eq in eqs:
                    peak = max(peak, eq)
                    if peak > 0:
                        dd = (peak - eq) / peak
                        max_drawdown_pct = max(max_drawdown_pct, dd)

            fitness = (
                avg_r * 0.4
                + win_rate * 0.3
                + sharpe * 0.2
                + (1 - min(max_drawdown_pct, 1.0)) * 0.1
            )

            min_eval = config.get('min_trades_eval', 25)
            if total < min_eval:
                tier = 'immature'
            elif fitness <= 0 or max_drawdown_pct > 0.40:
                tier = 'failing'
            else:
                tier = 'viable'  # elite is assigned by evolution engine

            balance = float(config.get('virtual_balance', 5000))
            initial = float(config.get('initial_balance', 5000))
            return_pct = round((balance - initial) / initial * 100, 2) if initial > 0 else 0.0

            return {
                'bot_id': bot_id,
                'strategy_name': config.get('strategy_name', ''),
                'role': config.get('role', 'seed'),
                'generation': config.get('generation', 0),
                'active': bool(config.get('active', 1)),
                'total_closed': total,
                'win_rate': round(win_rate, 4),
                'avg_r': round(avg_r, 3),
                'sharpe': round(sharpe, 3),
                'profit_factor': round(profit_factor, 3),
                'max_drawdown_pct': round(max_drawdown_pct, 4),
                'fitness': round(fitness, 4),
                'tier': tier,
                'virtual_balance': balance,
                'initial_balance': initial,
                'return_pct': return_pct,
            }
        except Exception as e:
            _logger.warning('get_bot_performance error for %s: %s', bot_id, e)
            return {}

    def list_bots(self, user_id: str) -> list[dict]:
        """Return all bots for a user with live performance metrics."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            self._ensure_tables(conn)
            rows = conn.execute(
                "SELECT bot_id FROM paper_bot_configs WHERE user_id=? ORDER BY created_at ASC",
                (user_id,)
            ).fetchall()
            conn.close()
            bots = []
            for row in rows:
                perf = self.get_bot_performance(row[0])
                if perf:
                    bots.append(perf)
            return bots
        except Exception as e:
            _logger.warning('list_bots error for %s: %s', user_id, e)
            return []

    def create_manual_bot(self, user_id: str, genome: dict, balance: float) -> str:
        """Create a manual bot (role='manual', never killed by evolution)."""
        genome['role'] = 'manual'
        genome['genome_id'] = _hash_genome(genome)
        genome['strategy_name'] = genome.get('strategy_name') or _name_genome(genome)
        now_iso = datetime.now(timezone.utc).isoformat()
        bot_id = 'bot_' + str(uuid.uuid4()).replace('-', '')[:12]
        conn = sqlite3.connect(self.db_path, timeout=10)
        self._ensure_tables(conn)
        conn.execute("""
            INSERT INTO paper_bot_configs
            (user_id, bot_id, genome_id, strategy_name, generation, parent_id,
             pattern_types, sectors, exchanges, volatility, regimes, timeframes,
             direction_bias, risk_pct, max_positions, min_quality,
             virtual_balance, initial_balance, role, active,
             scan_interval_sec, min_trades_eval, created_at)
            VALUES (?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1800,25,?)
        """, (
            user_id, bot_id,
            genome['genome_id'], genome['strategy_name'],
            genome.get('parent_id'),
            genome.get('pattern_types'), genome.get('sectors'), genome.get('exchanges'),
            genome.get('volatility'), genome.get('regimes'), genome.get('timeframes'),
            genome.get('direction_bias'),
            float(genome.get('risk_pct', 1.0)), int(genome.get('max_positions', 4)),
            float(genome.get('min_quality', 0.65)),
            balance, balance,
            now_iso,
        ))
        conn.commit()
        conn.close()
        self.start_bot(bot_id)
        return bot_id
