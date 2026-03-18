"""
analytics/strategy_evolution.py — Evolutionary selection pressure engine

Runs every 6 hours via StrategyEvolutionAdapter.
Scores all bots, classifies into tiers, kills failures, spawns replacements.
"""

from __future__ import annotations

import copy
import json
import logging
import random
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import extensions as ext

_logger = logging.getLogger('strategy_evolution')

_MIN_FLEET_SIZE = 8
_MAX_FLEET_SIZE = 12


class StrategyEvolution:
    """Fitness scoring, tier classification, mutation, crossover, spawn/kill cycle."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ── Main cycle ────────────────────────────────────────────────────────────

    def evaluate(self, user_id: str) -> dict:
        """
        Full evolutionary cycle for one user.
        Returns a summary dict of what happened.
        """
        from services.bot_runner import BotRunner
        runner = BotRunner(self.db_path)

        now_iso = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        from services.paper_trading import ensure_paper_tables
        ensure_paper_tables(conn)

        # 1. Load all active bots
        bot_rows = conn.execute(
            "SELECT * FROM paper_bot_configs WHERE user_id=? AND active=1",
            (user_id,)
        ).fetchall()
        configs = [dict(r) for r in bot_rows]
        conn.close()

        if not configs:
            return {'user_id': user_id, 'bots_evaluated': 0}

        # 2. Score all bots
        scored = []
        for cfg in configs:
            perf = runner.get_bot_performance(cfg['bot_id'])
            if perf:
                scored.append({**cfg, **perf})

        min_eval = 5   # lowered from 15 — in low-activity regimes bots close
                       # only 2-3 trades/week; 15 meant evolution was dormant for weeks

        # Separate by maturity
        mature   = [b for b in scored if b.get('total_closed', 0) >= b.get('min_trades_eval', min_eval)]
        immature = [b for b in scored if b.get('total_closed', 0) <  b.get('min_trades_eval', min_eval)]

        # HARD GUARD: never evaluate a fleet with zero mature bots.
        if not mature:
            _logger.info('StrategyEvolution: no mature bots yet — skipping cycle for %s', user_id)
            self._log_event(user_id, 'evolution_skip', None,
                            f'Skipped: 0/{len(scored)} bots have >={min_eval} trades', now_iso)
            return {'user_id': user_id, 'bots_evaluated': len(scored), 'skipped': True,
                    'reason': 'no mature bots'}

        # Skip manual bots from evolution entirely
        eligible = [b for b in mature if b.get('role') not in ('manual', 'discovery')]

        kills     = []
        elites    = []
        viables   = []
        summary   = {'killed': 0, 'spawned': 0, 'promoted': 0, 'elites': 0, 'viables': 0}

        if eligible:
            # Rank by fitness descending
            eligible.sort(key=lambda b: b.get('fitness', 0.0), reverse=True)
            n = len(eligible)
            elite_count = max(1, round(n * 0.20))

            for i, bot in enumerate(eligible):
                bot_id  = bot['bot_id']
                fitness = bot.get('fitness', 0.0)
                max_dd  = bot.get('max_drawdown_pct', 0.0)
                role    = bot.get('role', 'seed')

                if i < elite_count:
                    # Elite: top 20%
                    elites.append(bot)
                    self._log_event(user_id, 'evolution_elite', bot_id,
                                    f'Elite: fitness={fitness:.3f} WR={bot.get("win_rate",0):.0%} '
                                    f'avgR={bot.get("avg_r",0):.2f} {bot.get("total_closed",0)} trades',
                                    now_iso)
                    # Increase capital 50% — but ONLY if balance is positive
                    # and below 8x initial (prevents runaway compounding from bugs)
                    cur_bal  = float(bot.get('virtual_balance', 5000))
                    init_bal = float(bot.get('initial_balance', 5000)) or 5000
                    _MAX_BAL = init_bal * 8
                    if cur_bal > 0 and cur_bal < _MAX_BAL:
                        new_bal = round(min(cur_bal * 1.5, _MAX_BAL), 2)
                        self._update_bot(bot_id, {
                            'role': 'exploit',
                            'promoted_at': now_iso,
                            'virtual_balance': new_bal,
                        })
                        summary['elites'] += 1
                        self._log_event(user_id, 'evolution_promote', bot_id,
                                        f'Capital +50% → £{new_bal:,.0f}',
                                        now_iso)
                    elif cur_bal <= 0:
                        # Negative balance — demote to failing instead of promoting
                        kills.append(bot)
                        self._log_event(user_id, 'evolution_promote_blocked', bot_id,
                                        f'Promotion blocked: negative balance £{cur_bal:,.0f}',
                                        now_iso)
                    else:
                        # At the 8x cap — redistribute excess above 6x to seed a new explorer.
                        # This recycles accumulated profits back into the fleet rather than
                        # leaving capital idle in a maxed-out exploit bot.
                        _RECYCLE_FLOOR = init_bal * 6
                        excess = max(0.0, cur_bal - _RECYCLE_FLOOR)
                        recycle_amt = round(excess * 0.20, 2)  # 20% of excess above 6x floor
                        if recycle_amt >= init_bal * 0.5:
                            # Enough to meaningfully seed a new bot — trim exploit, bank the rest
                            new_exploit_bal = round(cur_bal - recycle_amt, 2)
                            self._update_bot(bot_id, {
                                'role': 'exploit',
                                'promoted_at': now_iso,
                                'virtual_balance': new_exploit_bal,
                            })
                            self._log_event(user_id, 'evolution_capital_recycle', bot_id,
                                            f'Capital recycled: £{recycle_amt:,.0f} carved off '
                                            f'(exploit {cur_bal:,.0f}→{new_exploit_bal:,.0f})',
                                            now_iso)
                            summary['recycled_capital'] = summary.get('recycled_capital', 0) + recycle_amt
                        else:
                            self._update_bot(bot_id, {'role': 'exploit', 'promoted_at': now_iso})
                        summary['elites'] += 1
                        self._log_event(user_id, 'evolution_promote', bot_id,
                                        f'Elite (balance capped at £{cur_bal:,.0f})',
                                        now_iso)

                elif fitness > 0 and max_dd <= 0.60:
                    # Viable
                    viables.append(bot)
                    self._log_event(user_id, 'evolution_viable', bot_id,
                                    f'Viable: fitness={fitness:.3f} {bot.get("total_closed",0)} trades',
                                    now_iso)
                    summary['viables'] += 1

                else:
                    # Failing
                    kills.append(bot)

                # Hard kill: negative virtual_balance regardless of fitness rank
                if float(bot.get('virtual_balance', 0)) < 0 and bot not in kills:
                    kills.append(bot)
                    self._log_event(user_id, 'evolution_negative_balance', bot_id,
                                    f'Hard kill: negative balance £{float(bot.get("virtual_balance",0)):,.0f}',
                                    now_iso)

        # Kill failing bots
        for bot in kills:
            # NEVER kill immature bots — they haven't had a chance to trade
            if bot.get('total_closed', 0) < bot.get('min_trades_eval', 5):
                continue
            bot_id = bot['bot_id']
            runner.kill_bot(bot_id, reason=f'fitness={bot.get("fitness",0):.3f} dd={bot.get("max_drawdown_pct",0):.1%}')
            self._log_event(user_id, 'evolution_kill', bot_id,
                            f'Killed: fitness={bot.get("fitness",0):.3f} '
                            f'dd={bot.get("max_drawdown_pct",0):.1%} '
                            f'{bot.get("total_closed",0)} trades',
                            now_iso)
            summary['killed'] += 1

            # Spawn one replacement per kill
            replacement = self._spawn_replacement(elites, viables, user_id, now_iso)
            if replacement:
                per_bot_balance = summary.pop('recycled_capital', None) or self._avg_initial_balance(user_id)
                new_bot_id = self._insert_bot(user_id, replacement, per_bot_balance, now_iso)
                if new_bot_id:
                    runner.start_bot(new_bot_id, startup_delay=30)
                    self._log_event(user_id, 'evolution_spawn', new_bot_id,
                                    f'Spawned {replacement.get("strategy_name","")} '
                                    f'(parent={bot_id[:8]}) gen={replacement.get("generation",0)}',
                                    now_iso)
                    summary['spawned'] += 1

        # Maintain minimum fleet size (immature bots already count — don't over-spawn)
        active_count = self._count_active(user_id)
        while active_count < _MIN_FLEET_SIZE:
            from services.bot_runner import generate_random_genome
            g = generate_random_genome()
            per_bot_balance = self._avg_initial_balance(user_id)
            new_bot_id = self._insert_bot(user_id, g, per_bot_balance, now_iso)
            if new_bot_id:
                runner.start_bot(new_bot_id, startup_delay=60)
                self._log_event(user_id, 'evolution_spawn', new_bot_id,
                                f'Explorer spawned to maintain fleet size (gen={g.get("generation",0)})',
                                now_iso)
                summary['spawned'] += 1
            active_count += 1

        summary['bots_evaluated'] = len(eligible)
        summary['immature']       = len(immature)
        _logger.info('evolution.evaluate %s: %s', user_id, summary)
        return summary

    # ── Genome operators ──────────────────────────────────────────────────────

    def mutate(self, genome: dict) -> dict:
        """Create a child genome by changing 1-2 genes."""
        from services.bot_runner import _hash_genome, _name_genome, _PATTERN_POOL, _SECTOR_POOL, _VOL_POOL, _REGIME_POOL

        child = copy.deepcopy(genome)
        child['parent_id']  = genome.get('genome_id')
        child['generation'] = genome.get('generation', 0) + 1
        child['role']       = 'mutant'

        mutable = ['pattern_types', 'sectors', 'volatility', 'regimes',
                   'direction_bias', 'risk_pct', 'min_quality', 'max_positions']
        n_mutations = random.choice([1, 1, 1, 2])
        for gene in random.sample(mutable, k=n_mutations):
            if gene == 'pattern_types':
                child['pattern_types'] = json.dumps(random.sample(_PATTERN_POOL, k=random.randint(1, 3)))
            elif gene == 'sectors':
                pool = _SECTOR_POOL
                child['sectors'] = json.dumps(random.sample(pool, k=random.randint(1, 2))) if random.random() > 0.3 else None
            elif gene == 'volatility':
                child['volatility'] = json.dumps(random.sample(_VOL_POOL, k=random.randint(1, 2))) if random.random() > 0.4 else None
            elif gene == 'regimes':
                child['regimes'] = json.dumps(random.sample(_REGIME_POOL, k=random.randint(1, 2))) if random.random() > 0.4 else None
            elif gene == 'direction_bias':
                child['direction_bias'] = random.choice([None, 'bullish', 'bearish'])
            elif gene == 'risk_pct':
                child['risk_pct'] = round(random.uniform(0.5, 2.0), 2)
            elif gene == 'min_quality':
                child['min_quality'] = round(random.uniform(0.55, 0.80), 2)
            elif gene == 'max_positions':
                child['max_positions'] = random.randint(2, 6)

        child['genome_id']     = _hash_genome(child)
        child['strategy_name'] = _name_genome(child)
        return child

    def crossover(self, genome_a: dict, genome_b: dict) -> dict:
        """
        Combine two elite genomes.
        Randomly: filter genes from A + sizing genes from B, or vice versa.
        """
        from services.bot_runner import _hash_genome, _name_genome

        if random.random() > 0.5:
            src_filter, src_sizing = genome_a, genome_b
        else:
            src_filter, src_sizing = genome_b, genome_a

        child = {
            'pattern_types':  src_filter.get('pattern_types'),
            'sectors':        src_filter.get('sectors'),
            'exchanges':      src_filter.get('exchanges'),
            'volatility':     src_filter.get('volatility'),
            'regimes':        src_filter.get('regimes'),
            'timeframes':     src_filter.get('timeframes'),
            'direction_bias': src_filter.get('direction_bias'),
            'risk_pct':       src_sizing.get('risk_pct', 1.0),
            'max_positions':  src_sizing.get('max_positions', 4),
            'min_quality':    src_sizing.get('min_quality', 0.65),
            'parent_id':      genome_a.get('genome_id'),
            'generation':     max(genome_a.get('generation', 0), genome_b.get('generation', 0)) + 1,
            'role':           'mutant',
        }
        child['genome_id']     = _hash_genome(child)
        child['strategy_name'] = _name_genome(child)
        return child

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _spawn_replacement(self, elites: list, viables: list, user_id: str, now_iso: str) -> Optional[dict]:
        """Decide which genome operator to use for the replacement."""
        if elites and viables:
            return self.crossover(random.choice(elites), random.choice(viables))
        elif elites and len(elites) >= 2:
            return self.crossover(elites[0], random.choice(elites[1:]))
        elif viables:
            return self.mutate(random.choice(viables))
        elif elites:
            return self.mutate(random.choice(elites))
        else:
            from services.bot_runner import generate_random_genome
            return generate_random_genome()

    def _insert_bot(self, user_id: str, genome: dict, balance: float, now_iso: str) -> Optional[str]:
        """Insert a new bot config row and return its bot_id."""
        import uuid as _uuid
        bot_id = 'bot_' + str(_uuid.uuid4()).replace('-', '')[:12]
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            from services.paper_trading import ensure_paper_tables
            ensure_paper_tables(conn)
            conn.execute("""
                INSERT INTO paper_bot_configs
                (user_id, bot_id, genome_id, strategy_name, generation, parent_id,
                 pattern_types, sectors, exchanges, volatility, regimes, timeframes,
                 direction_bias, risk_pct, max_positions, min_quality,
                 virtual_balance, initial_balance, role, active,
                 scan_interval_sec, min_trades_eval, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1800,25,?)
            """, (
                user_id, bot_id,
                genome.get('genome_id', ''), genome.get('strategy_name', 'Unknown'),
                genome.get('generation', 0), genome.get('parent_id'),
                genome.get('pattern_types'), genome.get('sectors'), genome.get('exchanges'),
                genome.get('volatility'), genome.get('regimes'), genome.get('timeframes'),
                genome.get('direction_bias'),
                float(genome.get('risk_pct', 1.0)), int(genome.get('max_positions', 4)),
                float(genome.get('min_quality', 0.65)),
                balance, balance,
                genome.get('role', 'mutant'),
                now_iso,
            ))
            conn.commit()
            conn.close()
            return bot_id
        except Exception as e:
            _logger.warning('_insert_bot error: %s', e)
            return None

    def _update_bot(self, bot_id: str, fields: dict) -> None:
        """Update arbitrary fields on a bot config row."""
        try:
            sets = ', '.join(f'{k}=?' for k in fields)
            vals = list(fields.values()) + [bot_id]
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute(f"UPDATE paper_bot_configs SET {sets} WHERE bot_id=?", vals)
            conn.commit()
            conn.close()
        except Exception as e:
            _logger.warning('_update_bot error for %s: %s', bot_id, e)

    def _log_event(self, user_id: str, event_type: str, bot_id: str, detail: str, now_iso: str) -> None:
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, bot_id, created_at) VALUES (?,?,?,?,?,?)",
                (user_id, event_type, None, detail, bot_id, now_iso)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            _logger.debug('_log_event error: %s', e)

    def _count_active(self, user_id: str) -> int:
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            n = conn.execute(
                "SELECT COUNT(*) FROM paper_bot_configs WHERE user_id=? AND active=1",
                (user_id,)
            ).fetchone()[0]
            conn.close()
            return n
        except Exception:
            return 0

    def _avg_initial_balance(self, user_id: str) -> float:
        """Return the average initial_balance of existing bots for proportional seeding."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            row = conn.execute(
                "SELECT AVG(initial_balance) FROM paper_bot_configs WHERE user_id=? AND active=1",
                (user_id,)
            ).fetchone()
            conn.close()
            return float(row[0]) if row and row[0] else 5000.0
        except Exception:
            return 5000.0

    def get_discoveries(self, user_id: str) -> dict:
        """
        Pull calibration cells that were updated by bots for this user.
        Returns structured discovery records.
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            from services.paper_trading import ensure_paper_tables
            ensure_paper_tables(conn)

            # Get all bots for this user (including killed)
            bot_rows = conn.execute(
                "SELECT bot_id, strategy_name, pattern_types, sectors, volatility "
                "FROM paper_bot_configs WHERE user_id=?",
                (user_id,)
            ).fetchall()

            # Get calibration data
            try:
                cal_rows = conn.execute(
                    "SELECT ticker, pattern_type, timeframe, market_regime, "
                    "hit_rate_t1, sample_size, avg_r FROM signal_calibration "
                    "WHERE sample_size >= 10 ORDER BY sample_size DESC"
                ).fetchall()
            except Exception:
                cal_rows = []

            conn.close()

            discoveries = []
            total_observations = 0

            for cal in cal_rows:
                sample_size = cal['sample_size'] or 0
                total_observations += sample_size
                hit_rate = float(cal['hit_rate_t1'] or 0)
                avg_r    = float(cal['avg_r'] or 0) if 'avg_r' in cal.keys() else None

                if sample_size < 10:
                    continue

                confidence = 'established' if sample_size >= 25 else 'preliminary'
                status = 'active' if hit_rate >= 0.50 else 'disproven'

                # Try to match this calibration cell to a bot
                discovered_by = None
                for bot in bot_rows:
                    pt_raw = bot['pattern_types']
                    pts = json.loads(pt_raw) if pt_raw else []
                    if cal['pattern_type'] and cal['pattern_type'].lower() in [p.lower() for p in pts]:
                        discovered_by = bot['strategy_name']
                        break

                discoveries.append({
                    'pattern_type': cal['pattern_type'],
                    'sector':       None,
                    'volatility':   None,
                    'regime':       cal['market_regime'],
                    'sample_size':  sample_size,
                    'hit_rate':     round(hit_rate, 3),
                    'avg_r':        round(avg_r, 3) if avg_r is not None else None,
                    'discovered_by': discovered_by or 'generalist',
                    'confidence':   confidence,
                    'status':       status,
                })

            # Get current generation
            try:
                conn2 = sqlite3.connect(self.db_path, timeout=5)
                gen_row = conn2.execute(
                    "SELECT MAX(generation) FROM paper_bot_configs WHERE user_id=?",
                    (user_id,)
                ).fetchone()
                conn2.close()
                current_gen = int(gen_row[0]) if gen_row and gen_row[0] else 0
            except Exception:
                current_gen = 0

            return {
                'discoveries':          discoveries[:50],
                'total_experiments':    len(bot_rows),
                'total_observations':   total_observations,
                'unique_cells_tested':  len(discoveries),
                'generation':           current_gen,
            }
        except Exception as e:
            _logger.warning('get_discoveries error for %s: %s', user_id, e)
            return {'discoveries': [], 'total_experiments': 0, 'total_observations': 0,
                    'unique_cells_tested': 0, 'generation': 0}

    def get_evolution_history(self, user_id: str) -> list[dict]:
        """Return timeline of evolutionary events grouped by generation."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            from services.paper_trading import ensure_paper_tables
            ensure_paper_tables(conn)

            evo_types = (
                'evolution_elite', 'evolution_viable', 'evolution_kill',
                'evolution_spawn', 'evolution_crossover', 'evolution_promote',
            )
            placeholders = ','.join('?' for _ in evo_types)
            rows = conn.execute(
                f"SELECT * FROM paper_agent_log WHERE user_id=? AND event_type IN ({placeholders}) "
                f"ORDER BY created_at DESC LIMIT 200",
                (user_id,) + evo_types
            ).fetchall()

            # Get generation for each bot_id
            bot_gen = {}
            for row in rows:
                if row['bot_id'] and row['bot_id'] not in bot_gen:
                    cfg = conn.execute(
                        "SELECT generation FROM paper_bot_configs WHERE bot_id=?",
                        (row['bot_id'],)
                    ).fetchone()
                    bot_gen[row['bot_id']] = int(cfg['generation']) if cfg else 0

            conn.close()

            events = []
            for row in rows:
                events.append({
                    'event_type': row['event_type'],
                    'bot_id':     row['bot_id'],
                    'detail':     row['detail'],
                    'created_at': row['created_at'],
                    'generation': bot_gen.get(row['bot_id'], 0),
                })
            return events
        except Exception as e:
            _logger.warning('get_evolution_history error for %s: %s', user_id, e)
            return []

    def get_fleet_performance(self, user_id: str) -> dict:
        """Aggregate fleet stats: total equity, best/worst bot, generations, leaderboard."""
        from services.bot_runner import BotRunner
        runner = BotRunner(self.db_path)

        try:
            bots = runner.list_bots(user_id)
            if not bots:
                return {'user_id': user_id, 'bots': [], 'total_equity': 0.0}

            initial_eq   = sum(b.get('initial_balance', 0) for b in bots)
            total_trades = sum(b.get('total_closed', 0) for b in bots)
            max_gen      = max((b.get('generation', 0) for b in bots), default=0)

            sorted_bots = sorted(bots, key=lambda b: b.get('fitness', 0.0), reverse=True)
            active_bots = [b for b in sorted_bots if b.get('active')]
            best  = sorted_bots[0] if sorted_bots else None
            worst = sorted_bots[-1] if sorted_bots else None

            # Add equity sparklines (last 6 points) and compute true equity (cash + open positions)
            # Mark-to-market: use live prices so fitness scores reflect current value
            from services.paper_trading import fetch_live_prices as _flp
            conn = sqlite3.connect(self.db_path, timeout=10)
            total_equity = 0.0
            for b in bots:
                cash = b.get('virtual_balance', 0)
                try:
                    pos_rows = conn.execute(
                        "SELECT ticker, quantity FROM paper_positions WHERE bot_id=? AND status='open'",
                        (b['bot_id'],)
                    ).fetchall()
                    open_value = 0.0
                    if pos_rows:
                        tickers = [r[0] for r in pos_rows]
                        live = _flp(tickers)
                        for ticker, qty in pos_rows:
                            price = live.get(ticker) or 0.0
                            open_value += float(price) * float(qty)
                except Exception:
                    open_value = 0.0
                b['equity'] = round(cash + open_value, 2)
                total_equity += b['equity']
                try:
                    eq_rows = conn.execute(
                        "SELECT equity_value FROM paper_bot_equity WHERE bot_id=? ORDER BY logged_at DESC LIMIT 6",
                        (b['bot_id'],)
                    ).fetchall()
                    b['sparkline'] = [float(r[0]) for r in reversed(eq_rows)]
                except Exception:
                    b['sparkline'] = []
            conn.close()

            return_pct = round((total_equity - initial_eq) / initial_eq * 100, 2) if initial_eq > 0 else 0.0

            return {
                'user_id':       user_id,
                'bots':          sorted_bots,
                'total_equity':  round(total_equity, 2),
                'initial_equity': round(initial_eq, 2),
                'return_pct':    return_pct,
                'total_trades':  total_trades,
                'active_bots':   len(active_bots),
                'generation':    max_gen,
                'best_bot':      best,
                'worst_bot':     worst,
            }
        except Exception as e:
            _logger.warning('get_fleet_performance error for %s: %s', user_id, e)
            return {'user_id': user_id, 'bots': [], 'total_equity': 0.0}
