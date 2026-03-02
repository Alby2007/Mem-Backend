import sqlite3
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(DB)
rows = c.execute(
    """SELECT subject, predicate, object, timestamp FROM facts
       WHERE predicate IN ('delta_atm','gamma_atm','theta_atm','vega_atm',
                           'iv_true','put_call_oi_ratio','gamma_exposure')
       ORDER BY timestamp DESC LIMIT 30"""
).fetchall()
if rows:
    for r in rows:
        print(r)
else:
    print('NO atoms found yet')
    # Check if adapter even ran
    recent = c.execute(
        """SELECT subject, predicate, object, source, timestamp FROM facts
           WHERE source LIKE 'polygon_options%'
           ORDER BY timestamp DESC LIMIT 5"""
    ).fetchall()
    print(f'polygon_options source rows: {len(recent)}')
    # Check POLYGON_API_KEY is in env on OCI
    import os
    key = os.environ.get('POLYGON_API_KEY','')
    print(f'POLYGON_API_KEY set: {bool(key)} ({key[:8]}... if set)')
c.close()
