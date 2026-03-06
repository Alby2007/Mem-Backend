import sqlite3
c = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')

tickers = ['barc', 'hsba', 'lloy', 'lseg', 'nwg', 'stan']
print("=== geopolitical_risk_exposure atoms ===")
for t in tickers:
    rows = c.execute(
        "SELECT subject, predicate, object, source FROM facts WHERE LOWER(subject)=? AND predicate='geopolitical_risk_exposure'",
        (t,)
    ).fetchall()
    if rows:
        for r in rows:
            print(r)
    else:
        print(f"  {t.upper()}: NONE")

print("\n=== catalyst/risk_factor atoms mentioning Iran/war for these tickers ===")
for t in tickers:
    rows = c.execute(
        """SELECT subject, predicate, object FROM facts
           WHERE LOWER(subject)=?
           AND predicate IN ('catalyst','risk_factor')
           AND (LOWER(object) LIKE '%iran%' OR LOWER(object) LIKE '%war%'
                OR LOWER(object) LIKE '%russia%' OR LOWER(object) LIKE '%conflict%')""",
        (t,)
    ).fetchall()
    if rows:
        for r in rows:
            print(r)
    else:
        print(f"  {t.upper()}: no geo catalyst atoms")
