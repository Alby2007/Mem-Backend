"""
Enhanced Knowledge Graph with WAL mode, Taxonomy, and Comprehensive Search
"""

import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
import json
import re


class EnhancedKnowledgeGraph:
    """
    Advanced RDF triple store with:
    - WAL mode for better performance
    - Taxonomy system for categorization
    - Comprehensive search with query sanitization
    - Fallback mechanisms
    """
    
    def __init__(self, db_path: str = "jarvis_knowledge.db"):
        self.db_path = db_path
        self.conn = None
        self._initialize_db()
    
    def _initialize_db(self):
        """Create database with WAL mode and taxonomy"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        # Enable WAL mode for better performance
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
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
        
        # Atom salience — experience-shaped retrieval weight
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS atom_salience (
                fact_id             INTEGER PRIMARY KEY REFERENCES facts(id),
                usage_count         INTEGER DEFAULT 0,
                last_used           TEXT,
                reinforcement_score REAL DEFAULT 1.0
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_salience_score ON atom_salience(reinforcement_score)"
        )

        self._init_taxonomy(cursor)
        self.conn.commit()
    
    def _init_taxonomy(self, cursor):
        """Initialize default taxonomy"""
        categories = [
            ('identity', None, 'Personal identity', 'name,person,user,alby'),
            ('technical', None, 'Technical systems', 'code,system,api,model,architecture'),
            ('research', None, 'Research findings', 'research,theory,discovery,finding'),
            ('consciousness', 'research', 'Consciousness & IIT', 'phi,consciousness,integration'),
            ('capabilities', 'technical', 'System capabilities', 'capability,can,enables'),
            ('relationships', None, 'Connections', 'created,built,uses,requires'),
            ('goals', None, 'Goals & intentions', 'goal,want,need,vision'),
            ('achievements', None, 'Accomplishments', 'achievement,built,accomplished'),
            ('preferences', None, 'Preferences', 'likes,prefers,values'),
            ('temporal', None, 'Time-based info', 'current,future,past,timeline'),
        ]
        
        for cat, parent, desc, keywords in categories:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO taxonomy (category, parent_category, description, keywords)
                    VALUES (?, ?, ?, ?)
                """, (cat, parent, desc, keywords))
            except:
                pass
    
    def add_fact(self, subject: str, predicate: str, object: str,
                 confidence: float = 0.5, source: str = 'unknown',
                 metadata: Optional[Dict] = None) -> bool:
        """Add fact to knowledge graph"""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO facts (subject, predicate, object, confidence, source, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                subject.lower().strip(),
                predicate.lower().strip(),
                object.lower().strip(),
                confidence,
                source,
                datetime.now().isoformat(),
                json.dumps(metadata) if metadata else None
            ))
            
            # Update FTS index
            cursor.execute("""
                INSERT INTO facts_fts (rowid, subject, predicate, object)
                VALUES (last_insert_rowid(), ?, ?, ?)
            """, (subject, predicate, object))
            
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Update if duplicate
            cursor.execute("""
                UPDATE facts 
                SET confidence = MAX(confidence, ?), timestamp = ?
                WHERE subject = ? AND predicate = ? AND object = ?
            """, (confidence, datetime.now().isoformat(), subject, predicate, object))
            self.conn.commit()
            return False
    
    def search(self, query_text: str, limit: int = 20, category: str = None) -> List[Dict]:
        """Comprehensive search with sanitization"""
        cursor = self.conn.cursor()
        
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
        except:
            return self._fallback_search(query_text, limit, category)
    
    def _sanitize_query(self, query: str) -> str:
        """Sanitize FTS5 query"""
        # Remove special chars
        sanitized = re.sub(r'[\"()\[\]{},;:*^]', ' ', query)
        words = [w.strip() for w in sanitized.split() if len(w.strip()) > 1]
        return ' OR '.join(words) if words else ''
    
    def _fallback_search(self, query: str, limit: int, category: str = None) -> List[Dict]:
        """LIKE-based fallback search"""
        cursor = self.conn.cursor()
        pattern = f'%{query.lower()}%'
        
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
        except:
            return []
    
    def query(self, subject: str = None, predicate: str = None, 
              object: str = None, limit: int = 100) -> List[Dict]:
        """Query with filters"""
        cursor = self.conn.cursor()
        conditions, params = [], []
        
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
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM facts")
        total = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(DISTINCT subject) as count FROM facts")
        subjects = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(DISTINCT predicate) as count FROM facts")
        predicates = cursor.fetchone()['count']
        
        return {
            'total_facts': total,
            'unique_subjects': subjects,
            'unique_predicates': predicates
        }
