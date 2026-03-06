#!/usr/bin/env python3
"""Diagnose why paper agent finds no candidate patterns."""
import sqlite3, os

DB = '/home/ubuntu/trading-galaxy/knowledge.db'
c = sqlite3.connect(DB)

total = c.execute('SELECT COUNT(*) FROM pattern_signals').fetchone()[0]
print(f'Total pattern_signals rows: {total}')

by_status = c.execute('SELECT status, COUNT(*) FROM pattern_signals GROUP BY status').fetchall()
print(f'By status: {by_status}')

conviction = c.execute('SELECT kb_conviction, COUNT(*) FROM pattern_signals GROUP BY kb_conviction').fetchall()
print(f'Conviction values: {conviction}')

quality = c.execute('SELECT MIN(quality_score), MAX(quality_score), AVG(quality_score) FROM pattern_signals WHERE status NOT IN ("filled","broken")').fetchone()
print(f'Quality (active only) min/max/avg: {quality}')

# How many pass the agent filter?
passing = c.execute("""
    SELECT COUNT(*) FROM pattern_signals
    WHERE status NOT IN ('filled','broken')
      AND quality_score >= 0.70
      AND LOWER(kb_conviction) IN ('high','confirmed','strong')
""").fetchone()[0]
print(f'Pass agent filter (quality>=0.70, conviction HIGH/CONFIRMED/STRONG): {passing}')

# Looser — just active
active = c.execute("SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken')").fetchone()[0]
print(f'Active (not filled/broken): {active}')

# Sample 5 active rows
sample = c.execute("SELECT ticker, quality_score, kb_conviction, status FROM pattern_signals WHERE status NOT IN ('filled','broken') ORDER BY quality_score DESC LIMIT 5").fetchall()
print(f'Top 5 active by quality: {sample}')

c.close()
