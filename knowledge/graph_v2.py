"""
Enhanced Knowledge Graph with Versioning and Conflict Resolution
Addresses: versioning, deletion, confidence decay, conflict resolution, async safety
"""

import sqlite3
import aiosqlite
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
import asyncio
from contextlib import asynccontextmanager

from ..utils.logger import get_logger

logger = get_logger(__name__)

class KnowledgeGraphV2:
    """
    Enhanced RDF triple store with:
    - Fact versioning
    - Conflict resolution
    - Confidence decay
    - Async-safe operations
    - Fact deletion/expiration
    """
    
    def __init__(self, db_path: str = "jarvis_knowledge.db", config=None):
        self.db_path = db_path
        self.config = config
        self._lock = asyncio.Lock()
        self._initialized = False
    
    async def initialize(self):
        """Initialize database with enhanced schema"""
        if self._initialized:
            return
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subject TEXT NOT NULL,
                    predicate TEXT NOT NULL,
                    object TEXT NOT NULL,
                    confidence REAL DEFAULT 0.5,
                    source TEXT,
                    timestamp TEXT,
                    metadata TEXT,
                    version INTEGER DEFAULT 1,
                    superseded_by INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    last_accessed TEXT,
                    access_count INTEGER DEFAULT 0,
                    UNIQUE(subject, predicate, object, version)
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fact_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fact_id_1 INTEGER,
                    fact_id_2 INTEGER,
                    conflict_type TEXT,
                    resolution TEXT,
                    resolved_at TEXT,
                    FOREIGN KEY(fact_id_1) REFERENCES facts(id),
                    FOREIGN KEY(fact_id_2) REFERENCES facts(id)
                )
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_subject ON facts(subject)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_predicate ON facts(predicate)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_object ON facts(object)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_active ON facts(is_active)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_version ON facts(version)
            """)
            
            await db.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts 
                USING fts5(subject, predicate, object, content=facts)
            """)
            
            await db.commit()
        
        self._initialized = True
        logger.info(f"Knowledge graph initialized: {self.db_path}")
    
    @asynccontextmanager
    async def _get_connection(self):
        """Async-safe database connection"""
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                yield db
    
    async def add_fact(
        self,
        subject: str,
        predicate: str,
        object: str,
        confidence: float = 0.5,
        source: str = 'unknown',
        metadata: Optional[Dict] = None
    ) -> Tuple[bool, Optional[int]]:
        """
        Add fact with conflict detection and versioning
        
        Returns:
            (success, fact_id)
        """
        if not self._initialized:
            await self.initialize()
        
        subject = subject.lower().strip()
        predicate = predicate.lower().strip()
        object = object.lower().strip()
        
        async with self._get_connection() as db:
            try:
                # Check for existing fact
                cursor = await db.execute("""
                    SELECT id, confidence, version, object
                    FROM facts
                    WHERE subject = ? AND predicate = ? AND is_active = 1
                """, (subject, predicate))
                
                existing = await cursor.fetchone()
                
                if existing:
                    # Check for conflict
                    if existing['object'] != object:
                        conflict_id = await self._handle_conflict(
                            db, existing, subject, predicate, object, confidence, source
                        )
                        return (True, conflict_id)
                    else:
                        # Update existing fact
                        new_confidence = max(existing['confidence'], confidence)
                        await db.execute("""
                            UPDATE facts
                            SET confidence = ?,
                                timestamp = ?,
                                last_accessed = ?,
                                access_count = access_count + 1
                            WHERE id = ?
                        """, (new_confidence, datetime.now().isoformat(), 
                              datetime.now().isoformat(), existing['id']))
                        await db.commit()
                        logger.debug(f"Updated fact: {subject} {predicate} {object}")
                        return (True, existing['id'])
                
                # Insert new fact
                cursor = await db.execute("""
                    INSERT INTO facts (
                        subject, predicate, object, confidence, 
                        source, timestamp, metadata, last_accessed
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    subject, predicate, object, confidence,
                    source, datetime.now().isoformat(),
                    json.dumps(metadata) if metadata else None,
                    datetime.now().isoformat()
                ))
                
                fact_id = cursor.lastrowid
                
                # Update FTS index
                await db.execute("""
                    INSERT INTO facts_fts (rowid, subject, predicate, object)
                    VALUES (?, ?, ?, ?)
                """, (fact_id, subject, predicate, object))
                
                await db.commit()
                logger.info(f"Added fact: {subject} {predicate} {object} (confidence={confidence:.2f})")
                return (True, fact_id)
            
            except Exception as e:
                logger.error(f"Failed to add fact: {e}", exc_info=True)
                return (False, None)
    
    async def _handle_conflict(
        self,
        db: aiosqlite.Connection,
        existing: aiosqlite.Row,
        subject: str,
        predicate: str,
        new_object: str,
        new_confidence: float,
        source: str
    ) -> Optional[int]:
        """Handle conflicting facts"""
        logger.warning(
            f"Conflict detected: {subject} {predicate} "
            f"{existing['object']} vs {new_object}"
        )
        
        if not self.config or not self.config.knowledge_graph.enable_conflict_resolution:
            return None
        
        # Resolution strategy: higher confidence wins
        if new_confidence > existing['confidence']:
            # Supersede old fact
            new_version = existing['version'] + 1
            
            cursor = await db.execute("""
                INSERT INTO facts (
                    subject, predicate, object, confidence, source,
                    timestamp, version, last_accessed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                subject, predicate, new_object, new_confidence, source,
                datetime.now().isoformat(), new_version,
                datetime.now().isoformat()
            ))
            
            new_fact_id = cursor.lastrowid
            
            # Mark old fact as superseded
            await db.execute("""
                UPDATE facts
                SET is_active = 0, superseded_by = ?
                WHERE id = ?
            """, (new_fact_id, existing['id']))
            
            # Record conflict
            await db.execute("""
                INSERT INTO fact_conflicts (
                    fact_id_1, fact_id_2, conflict_type, resolution, resolved_at
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                existing['id'], new_fact_id, 'value_conflict',
                'higher_confidence', datetime.now().isoformat()
            ))
            
            await db.commit()
            logger.info(f"Resolved conflict: new fact supersedes old (v{new_version})")
            return new_fact_id
        else:
            # Keep existing fact, record conflict
            await db.execute("""
                INSERT INTO fact_conflicts (
                    fact_id_1, fact_id_2, conflict_type, resolution, resolved_at
                )
                VALUES (?, NULL, ?, ?, ?)
            """, (
                existing['id'], 'value_conflict',
                'kept_existing', datetime.now().isoformat()
            ))
            await db.commit()
            logger.info("Resolved conflict: kept existing fact")
            return existing['id']
    
    async def query(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        object: Optional[str] = None,
        limit: int = 100,
        include_inactive: bool = False
    ) -> List[Dict]:
        """Query with confidence decay applied"""
        if not self._initialized:
            await self.initialize()
        
        async with self._get_connection() as db:
            conditions = ["is_active = 1"] if not include_inactive else []
            params = []
            
            if subject:
                conditions.append("subject LIKE ?")
                params.append(f"%{subject.lower()}%")
            
            if predicate:
                conditions.append("predicate LIKE ?")
                params.append(f"%{predicate.lower()}%")
            
            if object:
                conditions.append("object LIKE ?")
                params.append(f"%{object.lower()}%")
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            cursor = await db.execute(f"""
                SELECT id, subject, predicate, object, confidence, 
                       source, timestamp, metadata, version, last_accessed
                FROM facts
                WHERE {where_clause}
                ORDER BY confidence DESC, timestamp DESC
                LIMIT ?
            """, params + [limit])
            
            rows = await cursor.fetchall()
            results = []
            
            for row in rows:
                # Apply confidence decay
                confidence = await self._apply_decay(row)
                
                results.append({
                    'id': row['id'],
                    'subject': row['subject'],
                    'predicate': row['predicate'],
                    'object': row['object'],
                    'confidence': confidence,
                    'source': row['source'],
                    'timestamp': row['timestamp'],
                    'metadata': json.loads(row['metadata']) if row['metadata'] else None,
                    'version': row['version']
                })
            
            # Update access tracking
            if results:
                fact_ids = [r['id'] for r in results]
                placeholders = ','.join('?' * len(fact_ids))
                await db.execute(f"""
                    UPDATE facts
                    SET last_accessed = ?, access_count = access_count + 1
                    WHERE id IN ({placeholders})
                """, [datetime.now().isoformat()] + fact_ids)
                await db.commit()
            
            return results
    
    async def _apply_decay(self, row: aiosqlite.Row) -> float:
        """Apply time-based confidence decay"""
        if not self.config or self.config.knowledge_graph.confidence_decay_rate == 0:
            return row['confidence']
        
        try:
            timestamp = datetime.fromisoformat(row['timestamp'])
            age_days = (datetime.now() - timestamp).days
            
            decay_rate = self.config.knowledge_graph.confidence_decay_rate
            decayed = row['confidence'] * (1 - decay_rate * age_days / 365)
            
            return max(0.0, min(1.0, decayed))
        except Exception as e:
            logger.error(f"Decay calculation failed: {e}")
            return row['confidence']
    
    async def delete_fact(self, fact_id: int) -> bool:
        """Soft delete a fact"""
        if not self._initialized:
            await self.initialize()
        
        async with self._get_connection() as db:
            try:
                await db.execute("""
                    UPDATE facts SET is_active = 0 WHERE id = ?
                """, (fact_id,))
                await db.commit()
                logger.info(f"Deleted fact: {fact_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete fact: {e}")
                return False
    
    async def cleanup_old_facts(self):
        """Remove facts older than max_fact_age_days"""
        if not self.config or not self.config.knowledge_graph.max_fact_age_days:
            return
        
        if not self._initialized:
            await self.initialize()
        
        cutoff = datetime.now() - timedelta(days=self.config.knowledge_graph.max_fact_age_days)
        
        async with self._get_connection() as db:
            cursor = await db.execute("""
                UPDATE facts
                SET is_active = 0
                WHERE timestamp < ? AND is_active = 1
            """, (cutoff.isoformat(),))
            
            await db.commit()
            logger.info(f"Cleaned up {cursor.rowcount} old facts")
    
    async def get_stats(self) -> Dict:
        """Get knowledge graph statistics"""
        if not self._initialized:
            await self.initialize()
        
        async with self._get_connection() as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM facts WHERE is_active = 1
            """)
            total_facts = (await cursor.fetchone())[0]
            
            cursor = await db.execute("""
                SELECT COUNT(DISTINCT subject) FROM facts WHERE is_active = 1
            """)
            unique_subjects = (await cursor.fetchone())[0]
            
            cursor = await db.execute("""
                SELECT COUNT(DISTINCT predicate) FROM facts WHERE is_active = 1
            """)
            unique_predicates = (await cursor.fetchone())[0]
            
            cursor = await db.execute("""
                SELECT AVG(confidence) FROM facts WHERE is_active = 1
            """)
            avg_confidence = (await cursor.fetchone())[0] or 0.0
            
            cursor = await db.execute("""
                SELECT COUNT(*) FROM fact_conflicts
            """)
            total_conflicts = (await cursor.fetchone())[0]
            
            cursor = await db.execute("""
                SELECT COUNT(*) FROM facts WHERE is_active = 0
            """)
            inactive_facts = (await cursor.fetchone())[0]
            
            return {
                'total_facts': total_facts,
                'unique_subjects': unique_subjects,
                'unique_predicates': unique_predicates,
                'average_confidence': avg_confidence,
                'total_conflicts': total_conflicts,
                'inactive_facts': inactive_facts
            }
    
    async def get_context(self, entity: str, depth: int = 1) -> List[Dict]:
        """Get context around an entity"""
        facts = []
        facts.extend(await self.query(subject=entity, limit=50))
        facts.extend(await self.query(object=entity, limit=50))
        
        if depth > 1:
            connected = set()
            for fact in facts:
                connected.add(fact['subject'])
                connected.add(fact['object'])
            
            for conn_entity in list(connected)[:10]:
                if conn_entity != entity:
                    facts.extend(await self.query(subject=conn_entity, limit=10))
        
        # Remove duplicates
        seen = set()
        unique_facts = []
        for fact in facts:
            key = (fact['subject'], fact['predicate'], fact['object'])
            if key not in seen:
                seen.add(key)
                unique_facts.append(fact)
        
        unique_facts.sort(key=lambda x: x['confidence'], reverse=True)
        return unique_facts[:50]
    
    async def search(self, query_text: str, limit: int = 20) -> List[Dict]:
        """Full-text search"""
        if not self._initialized:
            await self.initialize()
        
        async with self._get_connection() as db:
            cursor = await db.execute("""
                SELECT f.id, f.subject, f.predicate, f.object, 
                       f.confidence, f.source, f.timestamp, f.version
                FROM facts_fts fts
                JOIN facts f ON fts.rowid = f.id
                WHERE facts_fts MATCH ? AND f.is_active = 1
                ORDER BY rank, f.confidence DESC
                LIMIT ?
            """, (query_text, limit))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
