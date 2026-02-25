"""
Manually seed the extraction queue from recent RSS headlines already in the KB,
then run LLM extraction once. Use this to bootstrap without restarting the server.
"""
import sys, sqlite3, os
sys.path.insert(0, '.')

DB = 'trading_knowledge.db'

# ── Seed extraction_queue from existing news_wire_ key_finding atoms ──────────
conn = sqlite3.connect(DB)
from ingest.rss_adapter import _ensure_extraction_queue
_ensure_extraction_queue(conn)

# Check current queue state
pending = conn.execute("SELECT COUNT(*) FROM extraction_queue WHERE processed=0").fetchone()[0]
print(f"Queue pending before seed: {pending}")

# Pull unextracted news atoms — use key_finding headlines as raw text
conn.execute("""
    INSERT INTO extraction_queue (text, url, source, fetched_at)
    SELECT
        f.object,
        COALESCE(json_extract(f.metadata, '$.url'), ''),
        f.source,
        COALESCE(json_extract(f.metadata, '$.fetched_at'), f.timestamp)
    FROM facts f
    WHERE f.predicate = 'key_finding'
      AND f.source LIKE 'news_wire_%'
      AND NOT EXISTS (
          SELECT 1 FROM extraction_queue q WHERE q.text = f.object
      )
    LIMIT 200
""")
seeded = conn.total_changes
conn.commit()
print(f"Seeded {seeded} news headlines into extraction_queue")

pending = conn.execute("SELECT COUNT(*) FROM extraction_queue WHERE processed=0").fetchone()[0]
print(f"Queue pending after seed: {pending}")
conn.close()

# ── Run LLM extraction ─────────────────────────────────────────────────────────
from ingest.llm_extraction_adapter import LLMExtractionAdapter
from knowledge.graph import TradingKnowledgeGraph

kg = TradingKnowledgeGraph(DB)
adapter = LLMExtractionAdapter(db_path=DB)
atoms = adapter.run()
result = adapter.push(atoms, kg)
print(f"\nLLM extraction: ingested={result['ingested']} skipped={result['skipped']}")

if atoms:
    print(f"\nSample extracted atoms ({min(15, len(atoms))} of {len(atoms)}):")
    for a in atoms[:15]:
        print(f"  {a.subject:<8} | {a.predicate:<20} | {a.object[:60]:<60} conf={a.confidence:.2f}")
else:
    print("\nNo atoms extracted (Ollama unreachable or queue empty)")
