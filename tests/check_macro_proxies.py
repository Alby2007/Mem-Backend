import sqlite3
c = sqlite3.connect('trading_knowledge.db').cursor()
c.execute("SELECT subject, predicate, object FROM facts WHERE subject IN ('hyg','tlt','spy') AND predicate='signal_direction'")
for r in c.fetchall():
    print(r[0], r[1], r[2])
