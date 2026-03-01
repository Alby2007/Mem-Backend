import sqlite3, os, sys

_DB = os.environ.get('TRADING_KB_DB', '/home/ubuntu/trading-galaxy/trading_knowledge.db')
conn = sqlite3.connect(_DB)

# Step 1: discover actual table names
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("Tables:", tables)

# Step 2: find the main atoms/facts table
tbl = None
for candidate in ('atoms', 'facts', 'knowledge', 'kb_atoms'):
    if candidate in tables:
        tbl = candidate
        break
if tbl is None and tables:
    tbl = tables[0]
if tbl is None:
    print("No tables found"); sys.exit(1)

print(f"Using table: {tbl}")
cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")]
print("Columns:", cols)

# Step 3: check Russia/Ukraine atoms using actual schema
subj_col = 'subject' if 'subject' in cols else cols[0]
pred_col = 'predicate' if 'predicate' in cols else cols[1]
obj_col  = 'object' if 'object' in cols else cols[2]
src_col  = 'source' if 'source' in cols else None

print("\n--- ISO subject lookup (rus, ukr) ---")
for iso in ('rus', 'ukr', 'irn', 'isr'):
    rows = conn.execute(
        f"SELECT {subj_col},{pred_col},{obj_col} FROM {tbl} WHERE LOWER({subj_col})=? LIMIT 5",
        (iso,)
    ).fetchall()
    print(f"  {iso}: {rows}")

print("\n--- Predicate LIKE russia ---")
rows = conn.execute(
    f"SELECT {subj_col},{pred_col},{obj_col} FROM {tbl} WHERE LOWER({pred_col}) LIKE '%russia%' LIMIT 10"
).fetchall()
for r in rows: print(" ", r)

print("\n--- Object LIKE russia/ukraine ---")
for kw in ('%russia%', '%ukraine%', '%russian%'):
    rows = conn.execute(
        f"SELECT {subj_col},{pred_col},{obj_col} FROM {tbl} WHERE LOWER({obj_col}) LIKE ? LIMIT 5",
        (kw,)
    ).fetchall()
    print(f"  {kw}: {rows}")

print("\n--- gdelt_tension subject atoms ---")
rows = conn.execute(
    f"SELECT {subj_col},{pred_col},{obj_col} FROM {tbl} WHERE LOWER({subj_col})='gdelt_tension' LIMIT 10"
).fetchall()
for r in rows: print(" ", r)

conn.close()
