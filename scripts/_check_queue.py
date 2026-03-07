import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(DB)

cols = [r[1] for r in c.execute('PRAGMA table_info(extraction_queue)').fetchall()]
print('EXTRACTION_QUEUE COLUMNS:', cols)

total = c.execute('SELECT COUNT(*) FROM extraction_queue').fetchone()[0]
pending = c.execute('SELECT COUNT(*) FROM extraction_queue WHERE processed = 0').fetchone()[0]
failed = c.execute('SELECT COUNT(*) FROM extraction_queue WHERE failed_attempts >= 3').fetchone()[0]
print(f'total={total}  pending={pending}  failed_attempts>=3={failed}')

sample = c.execute('SELECT * FROM extraction_queue ORDER BY id DESC LIMIT 2').fetchall()
print('SAMPLE ROWS:')
for r in sample:
    print(' ', r)
