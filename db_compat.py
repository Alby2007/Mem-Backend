"""db_compat.py — SQL dialect normalisation.

Converts SQLite SQL idioms to Postgres equivalents so the same
query string can be used with either backend during the migration.
"""
from __future__ import annotations

import re


def sqlite_to_pg(sql: str) -> str:
    """Convert SQLite SQL idioms to Postgres equivalents."""
    # Placeholders: ? → %s
    sql = sql.replace('?', '%s')
    # datetime functions
    sql = re.sub(r"datetime\('now'\)", "NOW()", sql)
    sql = re.sub(
        r"datetime\('now',\s*'([^']+)'\)",
        lambda m: f"NOW() + INTERVAL '{m.group(1).replace('-', '').replace('+', '')}'",
        sql,
    )
    sql = re.sub(r"strftime\('%Y-%m-%d',\s*([^)]+)\)", r"TO_CHAR(\1, 'YYYY-MM-DD')", sql)
    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
    if 'INSERT INTO' in sql.upper() and 'ON CONFLICT' not in sql.upper():
        sql += ' ON CONFLICT DO NOTHING'
    # INSERT OR REPLACE → plain INSERT (caller must supply ON CONFLICT clause)
    sql = re.sub(r'INSERT\s+OR\s+REPLACE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
    # AUTOINCREMENT → handled by BIGSERIAL in PG schema
    sql = sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'BIGSERIAL PRIMARY KEY')
    return sql
