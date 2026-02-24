import sqlite3
c = sqlite3.connect('trading_knowledge.db').cursor()
c.execute("SELECT subject, predicate, object FROM facts WHERE predicate='thesis_risk_level' LIMIT 10")
for r in c.fetchall():
    print(repr(r[0]), r[1], r[2])
