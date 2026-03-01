import sqlite3, os
_DB = os.environ.get('KB_DB_PATH', '/home/ubuntu/trading-galaxy/trading_kb.db')
c = sqlite3.connect(_DB)
for src in ['gdelt_tension','acled_unrest','ucdp_conflict','financial_news','rss_news']:
    n = c.execute("SELECT COUNT(*) FROM facts WHERE source=?", (src,)).fetchone()[0]
    print(f"{src}: {n} atoms")

print("\nWar-related object atoms:")
rows = c.execute("""
    SELECT source, subject, predicate, object, timestamp FROM facts
    WHERE LOWER(object) LIKE '%war%' OR LOWER(object) LIKE '%conflict%'
       OR LOWER(object) LIKE '%ukraine%' OR LOWER(object) LIKE '%russia%'
       OR LOWER(object) LIKE '%israel%' OR LOWER(object) LIKE '%gaza%'
    ORDER BY timestamp DESC LIMIT 20
""").fetchall()
for r in rows:
    print(r)

print("\nGeo subject atoms (sample):")
rows2 = c.execute("""
    SELECT source, subject, predicate, object FROM facts
    WHERE subject IN ('gdelt_tension','acled_unrest','ucdp_conflict','geo_exposure','usgs_risk')
    ORDER BY rowid DESC LIMIT 20
""").fetchall()
for r in rows2:
    print(r)
