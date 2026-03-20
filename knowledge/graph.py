"""
Trading Knowledge Graph — WAL mode, Trading Taxonomy, and Comprehensive Search
"""

import json
import logging
import re
import sqlite3
import threading
from datetime import datetime
from typing import List, Dict, Optional

_logger = logging.getLogger(__name__)

try:
    from knowledge.decay import ensure_decay_column
    HAS_DECAY = True
except ImportError:
    HAS_DECAY = False

try:
    from knowledge.contradiction import ensure_conflicts_table, get_detector
    HAS_CONTRADICTION = True
except ImportError:
    HAS_CONTRADICTION = False


def _ensure_hit_count_column(conn: sqlite3.Connection) -> None:
    """
    Idempotent migration: add `hit_count` column to facts table if absent.
    hit_count will be incremented on retrieval by the hit-tracking PR.
    Used as the frequency term (δ) in the graph importance formula.
    Safe to call on every startup.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(facts)")
    columns = {row[1] for row in cursor.fetchall()}
    if 'hit_count' not in columns:
        cursor.execute("ALTER TABLE facts ADD COLUMN hit_count INTEGER DEFAULT 0")
        conn.commit()


class TradingKnowledgeGraph:
    """
    Trading KB RDF triple store with:
    - WAL mode for better performance
    - Trading taxonomy (instruments, theses, regimes, companies, reports)
    - Comprehensive search with query sanitization
    - Fallback mechanisms
    """
    
    def __init__(self, db_path: str = "trading_knowledge.db"):
        self.db_path = db_path
        self.conn = None
        self._local = threading.local()
        self._shock_engine = None   # set via set_shock_engine() in api.py
        self._ledger = None         # set via set_ledger() in api.py
        self._thesis_monitor = None # set via set_thesis_monitor() in api.py
        self._initialize_db()

    def set_shock_engine(self, engine) -> None:
        """Inject CausalShockEngine. Called once at api.py startup."""
        self._shock_engine = engine

    def set_ledger(self, ledger) -> None:
        """Inject PredictionLedger for intraday price resolution. Called once at api.py startup."""
        self._ledger = ledger

    def set_thesis_monitor(self, monitor) -> None:
        """Inject ThesisMonitor for proactive thesis invalidation alerts. Called once at api.py startup."""
        self._thesis_monitor = monitor

    def thread_local_conn(self) -> sqlite3.Connection:
        """Return a per-thread SQLite connection. Safe for use in Flask request handlers."""
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA busy_timeout=30000')
            self._local.conn = conn
        return conn
    
    def _initialize_db(self):
        """Create database with WAL mode and taxonomy"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        
        # Enable WAL mode for better performance
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA busy_timeout=30000')
        self.conn.execute('PRAGMA cache_size=-64000')  # 64MB cache
        
        cursor = self.conn.cursor()
        
        # Main facts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT NOT NULL,
                predicate TEXT NOT NULL,
                object TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                source TEXT,
                timestamp TEXT,
                metadata TEXT,
                UNIQUE(subject, predicate, object)
            )
        """)
        
        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_subject ON facts(subject)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_predicate ON facts(predicate)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_object ON facts(object)")
        # Composite index for the dominant query pattern: WHERE subject=? AND predicate=?
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_subject_predicate ON facts(subject, predicate)")
        
        # Full-text search
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts 
            USING fts5(subject, predicate, object, content=facts)
        """)
        
        # Taxonomy table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS taxonomy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL UNIQUE,
                parent_category TEXT,
                description TEXT,
                keywords TEXT
            )
        """)
        
        # Fact-taxonomy mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_categories (
                fact_id INTEGER,
                category TEXT,
                confidence REAL DEFAULT 1.0,
                FOREIGN KEY(fact_id) REFERENCES facts(id),
                FOREIGN KEY(category) REFERENCES taxonomy(category),
                UNIQUE(fact_id, category)
            )
        """)
        
        self._init_taxonomy(cursor)
        self.conn.commit()

        # Epistemic hygiene schema migrations (idempotent)
        if HAS_DECAY:
            try:
                ensure_decay_column(self.conn)
            except Exception:
                pass
        if HAS_CONTRADICTION:
            try:
                ensure_conflicts_table(self.conn)
            except Exception:
                pass
        try:
            _ensure_hit_count_column(self.conn)
        except Exception:
            pass
    
    def _init_taxonomy(self, cursor):
        """Initialize trading domain taxonomy"""
        categories = [
            ('instrument', None, 'Tradable instruments', 'ticker,equity,crypto,forex,futures,options,etf'),
            ('thesis', None, 'Trade theses & ideas', 'thesis,idea,setup,position,trade,long,short'),
            ('macro', None, 'Macro regime & drivers', 'macro,regime,rates,inflation,gdp,fed,central bank'),
            ('company', 'instrument', 'Company fundamentals', 'earnings,revenue,ebitda,guidance,management'),
            ('signal', 'instrument', 'Trading signals', 'signal,momentum,trend,breakout,reversal'),
            ('risk', None, 'Risk factors', 'risk,drawdown,volatility,correlation,tail'),
            ('research', None, 'Research & reports', 'analyst,report,rating,target,upgrade,downgrade'),
            ('catalyst', None, 'Event catalysts', 'catalyst,event,earnings,fed,data,announcement'),
            ('sector', None, 'Sector & industry', 'sector,industry,rotation,cyclical,defensive'),
            ('temporal', None, 'Time-based context', 'current,intraday,daily,weekly,monthly,horizon'),
        ]
        
        for cat, parent, desc, keywords in categories:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO taxonomy (category, parent_category, description, keywords)
                    VALUES (?, ?, ?, ?)
                """, (cat, parent, desc, keywords))
            except sqlite3.Error as e:
                _logger.warning('taxonomy init error for %r: %s', cat, e)
    
    _SIGNAL_DIRECTION_NORMALISE: Dict[str, str] = {
        'buy':            'bullish',
        'strong_buy':     'bullish',
        'outperform':     'bullish',
        'overweight':     'bullish',
        'accumulate':     'bullish',
        'add':            'bullish',
        'positive':       'bullish',
        'long':           'bullish',
        'near_high':      'bullish',
        'sell':           'bearish',
        'strong_sell':    'bearish',
        'underperform':   'bearish',
        'underweight':    'bearish',
        'reduce':         'bearish',
        'negative':       'bearish',
        'short':          'bearish',
        'near_low':       'bearish',
        'hold':           'neutral',
        'market_perform': 'neutral',
        'in_line':        'neutral',
        'equal_weight':   'neutral',
        'sector_perform': 'neutral',
    }

    def _update_fts_shadow(self, cursor, conn, fact_id, subj, pred, obj):
        """Update the SQLite FTS5 shadow index for a fact (keeps search() working)."""
        try:
            cursor.execute('DELETE FROM facts_fts WHERE rowid = ?', (fact_id,))
            cursor.execute(
                "INSERT INTO facts_fts (rowid, subject, predicate, object) VALUES (?,?,?,?)",
                (fact_id, subj, pred, obj))
            conn.commit()
        except Exception:
            pass  # FTS shadow update failure is non-critical

    def _add_fact_sqlite(self, cursor, conn, subj, pred, obj, confidence, source, now, meta_str, upsert):
        """SQLite-only write path for facts (used when PG is not available)."""
        new_id = None
        is_new = False
        if upsert:
            cursor.execute("""
                SELECT id FROM facts WHERE subject = ? AND predicate = ? AND source = ?
            """, (subj, pred, source))
            row = cursor.fetchone()
            if row:
                existing_id = row['id']
                cursor.execute("""
                    SELECT id FROM facts
                    WHERE subject = ? AND predicate = ? AND object = ? AND id != ?
                """, (subj, pred, obj, existing_id))
                collision = cursor.fetchone()
                if collision:
                    cursor.execute('DELETE FROM facts WHERE id = ?', (existing_id,))
                    cursor.execute('DELETE FROM facts_fts WHERE rowid = ?', (existing_id,))
                    cursor.execute("""
                        UPDATE facts SET confidence = MAX(confidence, ?), timestamp = ?, metadata = ?
                        WHERE id = ?
                    """, (confidence, now, meta_str, collision['id']))
                else:
                    cursor.execute("""
                        UPDATE facts
                        SET object = ?, confidence = ?, timestamp = ?, metadata = ?
                        WHERE id = ?
                    """, (obj, confidence, now, meta_str, existing_id))
                    cursor.execute('DELETE FROM facts_fts WHERE rowid = ?', (existing_id,))
                    cursor.execute("""
                        INSERT INTO facts_fts (rowid, subject, predicate, object)
                        VALUES (?, ?, ?, ?)
                    """, (existing_id, subj, pred, obj))
                conn.commit()
                return False

        try:
            cursor.execute("""
                INSERT INTO facts (subject, predicate, object, confidence, source, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (subj, pred, obj, confidence, source, now, meta_str))
            new_id = cursor.lastrowid
            is_new = True
            cursor.execute("""
                INSERT INTO facts_fts (rowid, subject, predicate, object)
                VALUES (?, ?, ?, ?)
            """, (new_id, subj, pred, obj))
            conn.commit()
        except sqlite3.IntegrityError:
            cursor.execute("""
                SELECT id FROM facts
                WHERE subject = ? AND predicate = ? AND object = ?
            """, (subj, pred, obj))
            row = cursor.fetchone()
            existing_id = row['id'] if row else None
            cursor.execute("""
                UPDATE facts
                SET confidence = MAX(confidence, ?), timestamp = ?
                WHERE subject = ? AND predicate = ? AND object = ?
            """, (confidence, now, subj, pred, obj))
            if existing_id is not None:
                cursor.execute('DELETE FROM facts_fts WHERE rowid = ?', (existing_id,))
                cursor.execute("""
                    INSERT INTO facts_fts (rowid, subject, predicate, object)
                    VALUES (?, ?, ?, ?)
                """, (existing_id, subj, pred, obj))
            conn.commit()
            return False
        return is_new

    def add_fact(self, subject: str, predicate: str, object: str,
                 confidence: float = 0.5, source: str = 'unknown',
                 metadata: Optional[Dict] = None,
                 upsert: bool = False) -> bool:
        """
        Add fact to knowledge graph.

        upsert=True: key on (subject, predicate, source) — updates object,
        confidence and timestamp if a matching row already exists.  Use this
        for time-series predicates (last_price, price_target, signal_direction)
        where each ingest run should replace the previous value from the same
        source rather than appending a new row.

        upsert=False (default): key on (subject, predicate, object) — the
        existing insert-or-update-confidence behaviour is preserved.
        """
        conn = self.thread_local_conn()
        cursor = conn.cursor()

        subj = subject.lower().strip()
        pred = predicate.lower().strip()
        obj  = object.lower().strip()

        # Semantic normalisation — collapse synonymous signal_direction values
        # before write so 'buy' and 'bullish' don't create spurious conflicts.
        if pred == 'signal_direction':
            obj = self._SIGNAL_DIRECTION_NORMALISE.get(obj, obj)
        now  = datetime.now().isoformat()
        meta_str = json.dumps(metadata) if metadata else None

        new_id = None
        is_new = False

        # ── Postgres path (source of truth) ───────────────────────────────
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    if upsert:
                        # Source-keyed upsert
                        cur.execute(
                            "SELECT id FROM facts WHERE subject=%s AND predicate=%s AND source=%s",
                            (subj, pred, source))
                        row = cur.fetchone()
                        if row:
                            existing_id = row['id']
                            cur.execute(
                                "SELECT id FROM facts WHERE subject=%s AND predicate=%s AND object=%s AND id!=%s",
                                (subj, pred, obj, existing_id))
                            collision = cur.fetchone()
                            if collision:
                                cur.execute("DELETE FROM facts WHERE id=%s", (existing_id,))
                                cur.execute(
                                    "UPDATE facts SET confidence=GREATEST(confidence,%s), timestamp=%s, metadata=%s WHERE id=%s",
                                    (confidence, now, meta_str, collision['id']))
                            else:
                                cur.execute(
                                    "UPDATE facts SET object=%s, confidence=%s, timestamp=%s, metadata=%s WHERE id=%s",
                                    (obj, confidence, now, meta_str, existing_id))
                            # Update SQLite FTS shadow index
                            self._update_fts_shadow(cursor, conn, existing_id, subj, pred, obj)
                            return False
                    # INSERT with ON CONFLICT for (subject, predicate, object)
                    cur.execute(
                        """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp, metadata)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (subject, predicate, object) DO UPDATE
                           SET confidence = GREATEST(facts.confidence, EXCLUDED.confidence),
                               timestamp = EXCLUDED.timestamp
                           RETURNING id, (xmax = 0) AS inserted""",
                        (subj, pred, obj, confidence, source, now, meta_str))
                    result = cur.fetchone()
                    new_id = result['id'] if result else None
                    is_new = result['inserted'] if result else False
                # Update SQLite FTS shadow index
                if new_id:
                    self._update_fts_shadow(cursor, conn, new_id, subj, pred, obj)
            except Exception as _pg_e:
                _logger.debug('PG facts write failed, falling back to SQLite: %s', _pg_e)
                # Fall through to SQLite path below
                return self._add_fact_sqlite(cursor, conn, subj, pred, obj, confidence, source, now, meta_str, upsert)
        else:
            return self._add_fact_sqlite(cursor, conn, subj, pred, obj, confidence, source, now, meta_str, upsert)

        # Skip SQLite path — PG is source of truth

        # Contradiction detection — runs after successful new insert only
        if is_new and HAS_CONTRADICTION:
            try:
                result = get_detector().check(
                    conn, new_id, subj, pred, obj,
                )
                if result.detected:
                    _logger.info(
                        "[KB] conflict detected: '%s' superseded by '%s' (%s)",
                        result.loser_obj, result.winner_obj, result.reason,
                    )
            except Exception:
                pass  # never let conflict detection break the ingest path

        # Causal shock propagation — fires for ~5 macro predicates, O(1) check
        if self._shock_engine is not None:
            try:
                self._shock_engine.on_atom_written(subj, pred, obj)
            except Exception:
                pass  # never let shock engine break the ingest path

        # Prediction ledger intraday resolution — fires only for last_price atoms
        if self._ledger is not None and pred == 'last_price':
            try:
                price = float(obj.split()[0].replace(',', ''))
                self._ledger.on_price_written(subj, price)
            except Exception:
                pass  # never let ledger break the ingest path

        # Thesis monitor — checks for invalidation condition approach on macro predicates
        if self._thesis_monitor is not None:
            try:
                self._thesis_monitor.on_atom_written(subj, pred, obj)
            except Exception:
                pass  # never let thesis monitor break the ingest path

        return True
    
    def search(self, query_text: str, limit: int = 20, category: str = None) -> List[Dict]:
        """Comprehensive search with sanitization"""
        cursor = self.thread_local_conn().cursor()
        
        # Sanitize query
        sanitized = self._sanitize_query(query_text)
        if not sanitized:
            return []
        
        try:
            if category:
                cursor.execute("""
                    SELECT DISTINCT f.subject, f.predicate, f.object, f.confidence, f.source, f.timestamp
                    FROM facts_fts fts
                    JOIN facts f ON fts.rowid = f.id
                    JOIN fact_categories fc ON f.id = fc.fact_id
                    WHERE facts_fts MATCH ? AND fc.category = ?
                    ORDER BY rank, f.confidence DESC
                    LIMIT ?
                """, (sanitized, category, limit))
            else:
                cursor.execute("""
                    SELECT f.subject, f.predicate, f.object, f.confidence, f.source, f.timestamp
                    FROM facts_fts fts
                    JOIN facts f ON fts.rowid = f.id
                    WHERE facts_fts MATCH ?
                    ORDER BY rank, f.confidence DESC
                    LIMIT ?
                """, (sanitized, limit))

            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            _logger.debug('FTS search failed (%s), falling back to LIKE', e)
            return self._fallback_search(query_text, limit, category)
    
    def _sanitize_query(self, query: str) -> str:
        """Sanitize FTS5 query"""
        # Remove special chars
        sanitized = re.sub(r'[\"()\[\]{},;:*^]', ' ', query)
        words = [w.strip() for w in sanitized.split() if len(w.strip()) > 1]
        return ' OR '.join(words) if words else ''
    
    def _fallback_search(self, query: str, limit: int, category: str = None) -> List[Dict]:
        """LIKE-based fallback search"""
        from db import HAS_POSTGRES, get_pg
        pattern = f'%{query.lower()}%'

        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    if category:
                        cur.execute("""
                            SELECT DISTINCT f.subject, f.predicate, f.object, f.confidence, f.source, f.timestamp
                            FROM facts f
                            JOIN fact_categories fc ON f.id = fc.fact_id
                            WHERE (f.subject ILIKE %s OR f.predicate ILIKE %s OR f.object ILIKE %s)
                            AND fc.category = %s
                            ORDER BY f.confidence DESC LIMIT %s
                        """, (pattern, pattern, pattern, category, limit))
                    else:
                        cur.execute("""
                            SELECT subject, predicate, object, confidence, source, timestamp
                            FROM facts
                            WHERE subject ILIKE %s OR predicate ILIKE %s OR object ILIKE %s
                            ORDER BY confidence DESC LIMIT %s
                        """, (pattern, pattern, pattern, limit))
                    return [dict(row) for row in cur.fetchall()]
            except Exception:
                pass  # fall through to SQLite

        cursor = self.thread_local_conn().cursor()
        try:
            if category:
                cursor.execute("""
                    SELECT DISTINCT f.subject, f.predicate, f.object, f.confidence, f.source, f.timestamp
                    FROM facts f
                    JOIN fact_categories fc ON f.id = fc.fact_id
                    WHERE (f.subject LIKE ? OR f.predicate LIKE ? OR f.object LIKE ?)
                    AND fc.category = ?
                    ORDER BY f.confidence DESC LIMIT ?
                """, (pattern, pattern, pattern, category, limit))
            else:
                cursor.execute("""
                    SELECT subject, predicate, object, confidence, source, timestamp
                    FROM facts
                    WHERE subject LIKE ? OR predicate LIKE ? OR object LIKE ?
                    ORDER BY confidence DESC LIMIT ?
                """, (pattern, pattern, pattern, limit))

            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            _logger.error('fallback search failed: %s', e)
            return []
    
    def query(self, subject: str = None, predicate: str = None,
              object: str = None, limit: int = 100) -> List[Dict]:
        """Query with filters"""
        from db import HAS_POSTGRES, get_pg
        conditions, params = [], []
        if HAS_POSTGRES:
            if subject:
                conditions.append("subject ILIKE %s")
                params.append(f"%{subject.lower()}%")
            if predicate:
                conditions.append("predicate ILIKE %s")
                params.append(f"%{predicate.lower()}%")
            if object:
                conditions.append("object ILIKE %s")
                params.append(f"%{object.lower()}%")
            where = " AND ".join(conditions) if conditions else "1=1"
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute(f"""
                        SELECT subject, predicate, object, confidence, source, timestamp, metadata
                        FROM facts WHERE {where}
                        ORDER BY confidence DESC, timestamp DESC LIMIT %s
                    """, params + [limit])
                    return [dict(row) for row in cur.fetchall()]
            except Exception:
                pass  # fall through to SQLite

        cursor = self.thread_local_conn().cursor()
        if subject:
            conditions.append("subject LIKE ?")
            params.append(f"%{subject.lower()}%")
        if predicate:
            conditions.append("predicate LIKE ?")
            params.append(f"%{predicate.lower()}%")
        if object:
            conditions.append("object LIKE ?")
            params.append(f"%{object.lower()}%")
        where = " AND ".join(conditions) if conditions else "1=1"
        cursor.execute(f"""
            SELECT subject, predicate, object, confidence, source, timestamp, metadata
            FROM facts WHERE {where}
            ORDER BY confidence DESC, timestamp DESC LIMIT ?
        """, params + [limit])
        return [dict(row) for row in cursor.fetchall()]
    
    def get_context(self, entity: str, depth: int = 1) -> List[Dict]:
        """Get context around entity"""
        facts = []
        facts.extend(self.query(subject=entity, limit=50))
        facts.extend(self.query(object=entity, limit=50))
        
        # Remove duplicates
        seen = set()
        unique = []
        for f in facts:
            key = (f['subject'], f['predicate'], f['object'])
            if key not in seen:
                seen.add(key)
                unique.append(f)
        
        return unique
    
    def get_stats(self) -> Dict:
        """Get statistics"""
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute("SELECT COUNT(*) as count FROM facts")
                    total = cur.fetchone()['count']
                    cur.execute("SELECT COUNT(DISTINCT subject) as count FROM facts")
                    subjects = cur.fetchone()['count']
                    cur.execute("SELECT COUNT(DISTINCT predicate) as count FROM facts")
                    predicates = cur.fetchone()['count']
                    return {'total_facts': total, 'unique_subjects': subjects, 'unique_predicates': predicates}
            except Exception:
                pass
        cursor = self.thread_local_conn().cursor()
        cursor.execute("SELECT COUNT(*) as count FROM facts")
        total = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(DISTINCT subject) as count FROM facts")
        subjects = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(DISTINCT predicate) as count FROM facts")
        predicates = cursor.fetchone()['count']
        return {'total_facts': total, 'unique_subjects': subjects, 'unique_predicates': predicates}
