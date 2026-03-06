#!/usr/bin/env python3
"""Diagnose why paper agent finds no candidate patterns."""
import sqlite3, os

DB = '/home/ubuntu/trading-galaxy/knowledge.db'
c = sqlite3.connect(DB)

# List all tables
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print(f'Tables in DB: {tables}')

# Find the pattern table
pat_table = next((t for t in tables if 'pattern' in t.lower()), None)
print(f'Pattern table: {pat_table}')
if not pat_table:
    c.close()
    raise SystemExit('No pattern table found')

total = c.execute(f'SELECT COUNT(*) FROM {pat_table}').fetchone()[0]
print(f'Total {pat_table} rows: {total}')

by_status = c.execute(f'SELECT status, COUNT(*) FROM {pat_table} GROUP BY status').fetchall()
print(f'By status: {by_status}')

# Check columns available
cols = [r[1] for r in c.execute(f'PRAGMA table_info({pat_table})').fetchall()]
print(f'Columns: {cols}')

if 'kb_conviction' in cols:
    conviction = c.execute(f'SELECT kb_conviction, COUNT(*) FROM {pat_table} GROUP BY kb_conviction').fetchall()
    print(f'Conviction values: {conviction}')

if 'quality_score' in cols:
    quality = c.execute(f'SELECT MIN(quality_score), MAX(quality_score), AVG(quality_score) FROM {pat_table} WHERE status NOT IN ("filled","broken")').fetchone()
    print(f'Quality (active only) min/max/avg: {quality}')

# How many pass the agent filter?
if 'kb_conviction' in cols and 'quality_score' in cols:
    passing = c.execute(f"""
        SELECT COUNT(*) FROM {pat_table}
        WHERE status NOT IN ('filled','broken')
          AND quality_score >= 0.70
          AND LOWER(kb_conviction) IN ('high','confirmed','strong')
    """).fetchone()[0]
    print(f'Pass agent filter (quality>=0.70, conviction HIGH/CONFIRMED/STRONG): {passing}')

# Looser — just active
active = c.execute(f"SELECT COUNT(*) FROM {pat_table} WHERE status NOT IN ('filled','broken')").fetchone()[0]
print(f'Active (not filled/broken): {active}')

# Sample 5 active rows
sample = c.execute(f"SELECT ticker, quality_score, kb_conviction, status FROM {pat_table} WHERE status NOT IN ('filled','broken') ORDER BY quality_score DESC LIMIT 5").fetchall()
print(f'Top 5 active by quality: {sample}')

c.close()
