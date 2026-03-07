import extensions as ext, sqlite3
c = sqlite3.connect(ext.DB_PATH)
rows = c.execute(
    "SELECT predicate, object FROM facts WHERE LOWER(subject)='coin' ORDER BY predicate"
).fetchall()
for r in rows[:60]:
    print(r)
c.close()
