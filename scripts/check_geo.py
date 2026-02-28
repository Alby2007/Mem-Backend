import sqlite3, sys

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

GEO_SUBJECTS = ['gdelt_tension','acled_unrest','geo_exposure','ucdp_conflict','usgs_risk','usgs_seismic']
GEO_NEWS = ['news_wire_bbc_world','news_wire_al_jazeera','news_wire_defense_news','news_wire_reuters_world']

print("=== GEO ATOMS ===")
c.execute("SELECT subject, predicate, object FROM facts WHERE subject IN ({}) LIMIT 20".format(
    ','.join('?' for _ in GEO_SUBJECTS)), GEO_SUBJECTS)
rows = c.fetchall()
print(f"Count: {len(rows)}")
for r in rows: print(r)

print("\n=== GEO NEWS ===")
c.execute("SELECT subject, COUNT(*) FROM facts WHERE subject IN ({}) GROUP BY subject".format(
    ','.join('?' for _ in GEO_NEWS)), GEO_NEWS)
for r in c.fetchall(): print(r)

print("\n=== ALL DISTINCT SUBJECTS (sample) ===")
c.execute("SELECT DISTINCT subject FROM facts ORDER BY subject LIMIT 50")
for r in c.fetchall(): print(r[0])

print("\n=== REGIME ATOMS ===")
c.execute("SELECT subject, predicate, object FROM facts WHERE predicate IN ('regime_label','market_regime','current_regime') LIMIT 5")
for r in c.fetchall(): print(r)
