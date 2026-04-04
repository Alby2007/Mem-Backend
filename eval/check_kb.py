import sqlite3, os, glob

db = os.environ.get('TRADING_KB_DB', '')
for candidate in [db, 'eval/trading_knowledge_eval.db', 'trading_knowledge.db', 'trading_galaxy.db']:
    if candidate and os.path.exists(candidate):
        db = candidate
        break
else:
    candidates = glob.glob('*.db') + glob.glob('**/*.db', recursive=True)
    print('No DB found. Candidates:', candidates[:5])
    exit(1)

print(f'Using DB: {db}')
conn = sqlite3.connect(db)
c = conn.cursor()

print('\n=== MACRO ATOMS ===')
c.execute("SELECT subject, predicate, object, confidence FROM facts WHERE predicate IN ('market_regime','yield_curve_regime','yield_curve_slope','central_bank_stance','fed_funds_rate','yield_curve_tlt_shy','tlt_close','tlt_1d_change_pct') LIMIT 20")
rows = c.fetchall()
print(f'Count: {len(rows)}')
for r in rows:
    print(' ', r)

print('\n=== NOTREAL99 ATOMS ===')
c.execute("SELECT subject, predicate, object FROM facts WHERE LOWER(subject) LIKE '%notreal%' OR LOWER(object) LIKE '%notreal%' LIMIT 10")
rows2 = c.fetchall()
print(f'Count: {len(rows2)}')
for r in rows2:
    print(' ', r)

print('\n=== TOTAL ATOM COUNT ===')
c.execute("SELECT COUNT(*) FROM facts")
print('Total:', c.fetchone()[0])

print('\n=== SAMPLE SUBJECTS ===')
c.execute("SELECT DISTINCT subject FROM facts LIMIT 30")
for r in c.fetchall():
    print(' ', r[0])

conn.close()
