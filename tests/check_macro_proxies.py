import sqlite3
conn = sqlite3.connect('trading_knowledge.db')
c = conn.cursor()

print("=== Macro proxy signal_direction atoms (HYG, TLT, SPY) ===")
c.execute("SELECT subject, predicate, object, confidence, source FROM facts WHERE subject IN ('hyg','tlt','spy') AND predicate IN ('signal_direction','last_price','return_1m')")
rows = c.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]:5s} | {r[1]:<16} | {r[2]:<20} conf={r[3]:.2f} src={r[4]}")
else:
    print("  NONE — HYG/TLT/SPY have no signal_direction atoms")

print()
print("=== macro_confirmation distribution ===")
c.execute("SELECT object, COUNT(*) FROM facts WHERE predicate='macro_confirmation' GROUP BY object ORDER BY COUNT(*) DESC")
for r in c.fetchall():
    print(f"  {r[1]:4d}  {r[0]}")

print()
print("=== FRED macro atoms ===")
c.execute("SELECT subject, predicate, object, confidence FROM facts WHERE source LIKE 'macro_data%' OR source LIKE 'fred%'")
for r in c.fetchall():
    print(f"  {r[0]:12s} | {r[1]:<25} | {r[2][:60]:<60} conf={r[3]:.2f}")

conn.close()
