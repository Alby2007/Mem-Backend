#!/usr/bin/env python3
"""Inspect GPR XLS structure to find correct column names. Run on OCI."""
import sys, urllib.request, xlrd

req = urllib.request.Request(
    'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls',
    headers={'User-Agent': 'TradingGalaxyKB/1.0'},
)
raw = urllib.request.urlopen(req, timeout=30).read()
print(f'Downloaded {len(raw)} bytes')

wb = xlrd.open_workbook(file_contents=raw)
print(f'Sheets ({wb.nsheets}): {wb.sheet_names()}')

for si in range(wb.nsheets):
    ws = wb.sheet_by_index(si)
    print(f'\n=== Sheet {si}: {ws.name!r}  ({ws.nrows} rows x {ws.ncols} cols) ===')
    for r in range(min(4, ws.nrows)):
        vals = [repr(ws.cell_value(r, c))[:14] for c in range(min(ws.ncols, 20))]
        print(f'  row {r}: {vals}')
    print('  ...')
    for r in range(max(0, ws.nrows - 3), ws.nrows):
        vals = [repr(ws.cell_value(r, c))[:14] for c in range(min(ws.ncols, 20))]
        print(f'  row {r}: {vals}')
