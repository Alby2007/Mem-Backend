// ── PRIVATE FLEET — Internal discovery fleet observation ──────────────────────

let _pfPollTimer  = null;
let _pfView       = 'fleet';
let _pfLastStatus = null;
let _pfLastReport = null;
let _pfClosedCache = null;
let _pfCalibCache  = null;
let _pfSortCol     = null;
let _pfSortAsc     = true;
let _pfFilter      = 'all';

// ── helpers ───────────────────────────────────────────────────────────────────

function _pfAge(ts) {
  if (!ts) return '—';
  const ms = Date.now() - new Date(ts).getTime();
  const h  = Math.floor(ms / 3600000);
  const m  = Math.floor((ms % 3600000) / 60000);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function _pfFmt(n) {
  if (n == null) return '—';
  return typeof n === 'number' ? n.toLocaleString(undefined, {maximumFractionDigits: 2}) : n;
}

function _pfDirHtml(dir) {
  if (!dir) return '—';
  const col   = dir === 'bullish' ? 'var(--green)' : dir === 'bearish' ? 'var(--red)' : 'var(--muted)';
  const arrow = dir === 'bullish' ? '▲' : dir === 'bearish' ? '▼' : '—';
  return `<span style="color:${col};">${arrow} ${dir}</span>`;
}

function _pfSideHtml(dir) {
  if (!dir) return '—';
  const isLong = dir === 'bullish';
  const col    = isLong ? 'var(--green)' : 'var(--red)';
  const arrow  = isLong ? '▲' : '▼';
  const label  = isLong ? 'Long' : 'Short';
  return `<span style="color:${col};">${arrow} ${label}</span>`;
}

function _pfStatusLabel(s) {
  return { t2_hit: 'T2 Hit', t1_hit: 'T1 Hit', stopped_out: 'Stopped', closed: 'Closed' }[s] || (s || '—');
}

function _pfOutcomeColor(o) {
  if (!o) return 'var(--muted)';
  if (o.includes('t2') || o.includes('hit')) return 'var(--green)';
  if (o.includes('stop')) return 'var(--red)';
  return 'var(--muted)';
}

function _pfTabs(tabs, active, onclick) {
  return `<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;">
    ${tabs.map(t => `<button onclick="${onclick}('${t.value}')"
      style="padding:4px 12px;border-radius:20px;border:1px solid ${t.value===active?'var(--accent)':'var(--border)'};
             background:${t.value===active?'rgba(255,200,0,0.1)':'transparent'};
             color:${t.value===active?'var(--accent)':'var(--muted)'};font-size:11px;cursor:pointer;">
      ${escHtml(t.label)}
    </button>`).join('')}
  </div>`;
}

function _pfSortHeader(label, key, currentCol, asc, fn) {
  const active = currentCol === key;
  const arrow  = active ? (asc ? ' ▲' : ' ▼') : '';
  return `<th onclick="${fn}('${key}')"
    style="padding:6px;text-align:left;font-size:10px;font-weight:600;
           color:${active?'var(--accent)':'var(--muted)'};cursor:pointer;white-space:nowrap;">
    ${label}${arrow}
  </th>`;
}

function _pfSummaryBar(items) {
  return `<div style="display:flex;flex-wrap:wrap;gap:16px;padding:12px 14px;
      background:var(--card);border:1px solid var(--border);border-radius:8px;margin-bottom:14px;">
    ${items.map(i => `<div>
      <div style="font-size:10px;color:var(--muted);letter-spacing:.08em;text-transform:uppercase;">${i.label}</div>
      <div style="font-size:16px;font-weight:700;color:${i.color||'var(--text)'};">${i.value}</div>
    </div>`).join('')}
  </div>`;
}

function _pfDetailShell(title, bodyHtml) {
  return `
    <div id="pf-detail-header" style="display:flex;align-items:center;gap:12px;margin-bottom:20px;border-bottom:1px solid var(--border);padding-bottom:12px;">
      <button onclick="_pfShowView('fleet')"
        style="background:transparent;border:1px solid var(--border);border-radius:6px;padding:4px 10px;
               color:var(--muted);font-size:12px;cursor:pointer;">← Back</button>
      <span style="font-size:11px;font-weight:700;letter-spacing:2px;color:var(--accent);">◈ ${title}</span>
      <div style="flex:1"></div>
      <span id="pf-detail-updated" style="font-size:10px;color:var(--muted);"></span>
    </div>
    <div id="pf-detail-body">${bodyHtml}</div>`;
}

function _pfLoading() {
  return `<div style="color:var(--muted);font-size:13px;padding:30px 0;text-align:center;">Loading…</div>`;
}

// ── navigation ────────────────────────────────────────────────────────────────

function _pfShowView(v) {
  _pfView   = v;
  _pfFilter = 'all';
  _pfSortCol = null;
  _pfSortAsc = true;
  const wrap = document.getElementById('pf-wrap');
  if (!wrap) return;

  if (v === 'fleet') {
    _pfRenderFleet(wrap);
    return;
  }

  // Render shell immediately with loading spinner, then populate
  wrap.innerHTML = `<div style="max-width:960px;margin:0 auto;padding:20px 16px;">
    ${_pfDetailShell(_pfViewTitle(v), _pfLoading())}
  </div>`;

  if (v === 'open')   _pfPopulateOpen();
  if (v === 'closed') _pfPopulateClosed();
  if (v === 'bots')   _pfPopulateBots();
  if (v === 'calib')  _pfPopulateCalib();
}

function _pfViewTitle(v) {
  return { bots: 'DISCOVERY BOTS', open: 'OPEN POSITIONS', closed: 'CLOSED POSITIONS', calib: 'CALIBRATION OBS' }[v] || v.toUpperCase();
}

// ── entry point ───────────────────────────────────────────────────────────────

async function loadPrivateFleet() {
  if (!state.userId || !state.isDev) return;
  const sc = document.getElementById('screen-private-fleet');
  if (!sc) return;

  // Reset state on every screen entry
  _pfView        = 'fleet';
  _pfFilter      = 'all';
  _pfLastStatus  = null;
  _pfLastReport  = null;
  _pfClosedCache = null;
  _pfCalibCache  = null;

  sc.innerHTML = `<div id="pf-wrap" style="max-width:960px;margin:0 auto;padding:20px 16px;"></div>`;
  const wrap = document.getElementById('pf-wrap');
  _pfRenderFleet(wrap);

  await _pfRefresh();

  if (_pfPollTimer) clearInterval(_pfPollTimer);
  _pfPollTimer = setInterval(() => {
    if (document.getElementById('screen-private-fleet')?.classList.contains('active')) {
      if (_pfView === 'fleet') _pfRefresh();
    } else {
      clearInterval(_pfPollTimer);
      _pfPollTimer = null;
    }
  }, 30000);
}

function _pfRenderFleet(wrap) {
  wrap.innerHTML = `
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;border-bottom:1px solid var(--border);padding-bottom:12px;">
      <span style="font-size:11px;font-weight:700;letter-spacing:2px;color:var(--accent);">◈ PRIVATE FLEET</span>
      <span style="font-size:10px;color:var(--muted);background:rgba(255,200,0,0.08);border:1px solid rgba(255,200,0,0.2);padding:2px 8px;border-radius:10px;">INTERNAL · FOUNDERS ONLY</span>
      <div style="flex:1"></div>
      <span id="pf-last-updated" style="font-size:10px;color:var(--muted);"></span>
    </div>
    <div id="pf-stats-row" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;"></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
      <div>
        <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;color:var(--muted);margin-bottom:10px;">SIGNAL LEADERBOARD</div>
        <div id="pf-report"></div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;color:var(--muted);margin-bottom:10px;">OPEN POSITIONS</div>
        <div id="pf-positions"></div>
      </div>
    </div>`;

  // Re-populate from cache if available
  if (_pfLastStatus) {
    _pfRenderStats(_pfLastStatus);
    _pfRenderPositions(_pfLastStatus.open_positions || []);
  }
  if (_pfLastReport) _pfRenderReport(_pfLastReport);
}

async function _pfRefresh() {
  try {
    const [status, report] = await Promise.all([
      apiFetch(`/users/${state.userId}/private-fleet/status`),
      apiFetch(`/users/${state.userId}/private-fleet/report?min_observations=3&limit=20`),
    ]);
    _pfLastStatus = status;
    _pfLastReport = report?.report || [];

    if (_pfView === 'fleet') {
      _pfRenderStats(status);
      _pfRenderReport(_pfLastReport);
      _pfRenderPositions(status?.open_positions || []);
      const el = document.getElementById('pf-last-updated');
      if (el) el.textContent = 'Updated ' + new Date().toLocaleTimeString('en-GB');
    }
  } catch(e) {
    console.error('Private fleet load error:', e);
  }
}

// ── fleet overview renderers ──────────────────────────────────────────────────

function _pfRenderStats(s) {
  const el = document.getElementById('pf-stats-row');
  if (!el || !s) return;
  const openCount = s.open_positions?.length || 0;
  const cards = [
    { label: 'DISCOVERY BOTS',   value: s.total_bots || 0,                               sub: `${s.active_bots || 0} active`,  view: 'bots'   },
    { label: 'OPEN POSITIONS',   value: openCount,                                        sub: 'live entries',                   view: 'open'   },
    { label: 'CLOSED POSITIONS', value: (s.total_positions_closed || 0).toLocaleString(), sub: 'all time',                       view: 'closed' },
    { label: 'CALIBRATION OBS',  value: (s.total_observations || 0).toLocaleString(),     sub: 'signal cells',                   view: 'calib'  },
  ];
  el.innerHTML = cards.map(c => `
    <div onclick="_pfShowView('${c.view}')"
      onmouseenter="this.style.borderColor='var(--accent)'"
      onmouseleave="this.style.borderColor='var(--border)'"
      style="background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;
             cursor:pointer;transition:border-color .15s;">
      <div style="font-size:10px;color:var(--muted);letter-spacing:1px;margin-bottom:4px;">${c.label}</div>
      <div style="font-size:22px;font-weight:700;color:var(--text);">${c.value}</div>
      <div style="font-size:10px;color:var(--muted);margin-top:2px;">${c.sub} →</div>
    </div>`).join('');
}

function _pfRenderReport(rows) {
  const el = document.getElementById('pf-report');
  if (!el) return;
  if (!rows.length) {
    el.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:20px 0;">No signal cells yet — check back once positions start closing.</div>`;
    return;
  }
  el.innerHTML = rows.map((r, i) => {
    const hit    = r.hit_rate != null ? (r.hit_rate * 100).toFixed(0) + '%' : '—';
    const hitN   = r.hit_rate != null ? r.hit_rate : 0;
    const col    = hitN >= 0.6 ? 'var(--green)' : hitN >= 0.45 ? 'var(--accent)' : 'var(--red)';
    const obs    = r.observations || 0;
    const qual   = r.avg_samples != null ? r.avg_samples.toFixed(0) : '—';
    const pat    = (r.pattern_type || '—').replace(/_/g, ' ');
    const sec    = r.sectors ? r.sectors.replace(/[\[\]"]/g, '').split(',')[0] : 'all sectors';
    const dir    = r.direction_bias || 'any';
    const dirCol = dir === 'bullish' ? 'var(--green)' : dir === 'bearish' ? 'var(--red)' : 'var(--muted)';
    return `
      <div style="display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--card);border:1px solid var(--border);border-radius:6px;margin-bottom:6px;">
        <span style="color:var(--muted);font-size:10px;width:16px;flex-shrink:0;">${i+1}.</span>
        <div style="flex:1;min-width:0;">
          <div style="font-size:12px;font-weight:600;color:var(--text);">${escHtml(pat)}</div>
          <div style="font-size:10px;color:var(--muted);">${escHtml(sec)} · <span style="color:${dirCol};">${dir}</span></div>
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-size:14px;font-weight:700;color:${col};">${hit}</div>
          <div style="font-size:10px;color:var(--muted);">${obs} obs · n̄=${qual}</div>
        </div>
      </div>`;
  }).join('');
}

function _pfRenderPositions(positions) {
  const el = document.getElementById('pf-positions');
  if (!el) return;
  if (!positions || !positions.length) {
    el.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:12px;background:var(--card);border:1px solid var(--border);border-radius:8px;">No open positions yet.</div>`;
    return;
  }
  const rows = positions.map(p => {
    const dir = p.direction || '';
    const pat = (() => { try { return JSON.parse(p.pattern)[0] || '—'; } catch { return p.pattern || '—'; } })();
    const sec = (() => { try { return JSON.parse(p.sector)?.[0] || 'all'; } catch { return p.sector || 'all'; } })();
    return `
      <tr style="border-bottom:1px solid var(--border);">
        <td style="padding:7px 6px;font-weight:600;font-size:12px;">${escHtml(p.ticker)}</td>
        <td style="padding:7px 6px;font-size:11px;">${_pfDirHtml(dir)}</td>
        <td style="padding:7px 6px;font-size:11px;color:var(--muted);">${escHtml(pat.replace(/_/g,' '))}</td>
        <td style="padding:7px 6px;font-size:11px;color:var(--muted);">${escHtml(sec)}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;">${_pfFmt(p.entry)}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--green);">${_pfFmt(p.t1)}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--red);">${_pfFmt(p.stop)}</td>
        <td style="padding:7px 6px;font-size:10px;color:var(--muted);text-align:right;">${_pfAge(p.opened_at)}</td>
      </tr>`;
  }).join('');
  el.innerHTML = `
    <div style="overflow-x:auto;">
      <table style="width:100%;border-collapse:collapse;font-size:12px;">
        <thead><tr style="border-bottom:1px solid var(--border);">
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">TICKER</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">DIR</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">PATTERN</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">SECTOR</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">ENTRY</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">T1</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">STOP</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">AGE</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

// ── detail view: OPEN POSITIONS ───────────────────────────────────────────────

function _pfPopulateOpen() {
  const positions = _pfLastStatus?.open_positions || [];
  _pfRenderOpenDetail(positions, _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfSetOpenFilter(f) {
  _pfFilter = f;
  _pfRenderOpenDetail(_pfLastStatus?.open_positions || [], f, _pfSortCol, _pfSortAsc);
}

function _pfSortOpen(col) {
  if (_pfSortCol === col) { _pfSortAsc = !_pfSortAsc; } else { _pfSortCol = col; _pfSortAsc = true; }
  _pfRenderOpenDetail(_pfLastStatus?.open_positions || [], _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfRenderOpenDetail(positions, filter, sortCol, sortAsc) {
  const body = document.getElementById('pf-detail-body');
  if (!body) return;

  // Build dynamic sector tabs
  const sectors = [...new Set(positions.map(p => {
    try { return JSON.parse(p.sector)?.[0] || 'all'; } catch { return p.sector || 'all'; }
  }))].filter(Boolean);
  const tabValues = ['all', 'bearish', 'bullish', ...sectors.filter(s => s !== 'all')];
  const tabs = tabValues.map(v => ({ label: v === 'all' ? 'ALL' : v.toUpperCase(), value: v }));

  // Filter
  let rows = positions;
  if (filter !== 'all') {
    rows = rows.filter(p => {
      const sec = (() => { try { return JSON.parse(p.sector)?.[0] || 'all'; } catch { return p.sector || 'all'; } })();
      return p.direction === filter || sec === filter;
    });
  }

  // Sort
  if (sortCol) {
    rows = [...rows].sort((a, b) => {
      let av = a[sortCol], bv = b[sortCol];
      if (sortCol === 'age') { av = new Date(a.opened_at||0).getTime(); bv = new Date(b.opened_at||0).getTime(); }
      if (av == null) return 1; if (bv == null) return -1;
      return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
  } else {
    rows = [...rows].sort((a, b) => new Date(b.opened_at||0) - new Date(a.opened_at||0));
  }

  // Summary
  const bears = positions.filter(p => p.direction === 'bearish').length;
  const bulls = positions.filter(p => p.direction === 'bullish').length;
  const ages  = positions.filter(p => p.opened_at).map(p => Date.now() - new Date(p.opened_at).getTime());
  const avgAge = ages.length ? ages.reduce((a,b) => a+b, 0) / ages.length : 0;
  const avgH  = Math.floor(avgAge / 3600000);
  const avgM  = Math.floor((avgAge % 3600000) / 60000);

  const summary = _pfSummaryBar([
    { label: 'Total', value: positions.length },
    { label: 'Bearish', value: bears, color: 'var(--red)' },
    { label: 'Bullish', value: bulls, color: 'var(--green)' },
    { label: 'Avg Age', value: avgH > 0 ? `${avgH}h ${avgM}m` : `${avgM}m` },
  ]);

  const thCols = [
    { label: 'TICKER', key: 'ticker' }, { label: 'SIDE', key: 'direction' },
    { label: 'PATTERN', key: null }, { label: 'SECTOR', key: null },
    { label: 'ENTRY', key: 'entry' }, { label: 'T1', key: 't1' },
    { label: 'STOP', key: 'stop' }, { label: 'AGE', key: 'age' },
  ];

  const tableRows = rows.map(p => {
    const pat = (() => { try { return JSON.parse(p.pattern)[0] || '—'; } catch { return p.pattern || '—'; } })();
    const sec = (() => { try { return JSON.parse(p.sector)?.[0] || 'all'; } catch { return p.sector || 'all'; } })();
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:7px 6px;font-weight:600;font-size:12px;white-space:nowrap;">${escHtml(p.ticker)}</td>
      <td style="padding:7px 6px;font-size:11px;white-space:nowrap;">${_pfSideHtml(p.direction)}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(pat.replace(/_/g,' '))}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(sec)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;white-space:nowrap;">${_pfFmt(p.entry)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--green);white-space:nowrap;">${_pfFmt(p.t1)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--red);white-space:nowrap;">${_pfFmt(p.stop)}</td>
      <td style="padding:7px 6px;font-size:10px;color:var(--muted);text-align:right;white-space:nowrap;">${_pfAge(p.opened_at)}</td>
    </tr>`;
  }).join('');

  body.innerHTML = summary
    + _pfTabs(tabs, filter, '_pfSetOpenFilter')
    + `<div style="overflow-x:auto;"><table style="width:100%;min-width:620px;border-collapse:collapse;font-size:12px;">
      <thead><tr style="border-bottom:1px solid var(--border);">
        ${thCols.map(c => c.key ? _pfSortHeader(c.label, c.key, sortCol, sortAsc, '_pfSortOpen')
          : `<th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;white-space:nowrap;">${c.label}</th>`).join('')}
      </tr></thead>
      <tbody>${tableRows || '<tr><td colspan="8" style="padding:20px;text-align:center;color:var(--muted);">No positions.</td></tr>'}</tbody>
    </table></div>`;

  const upd = document.getElementById('pf-detail-updated');
  if (upd) upd.textContent = `${rows.length} of ${positions.length} shown`;
}

// ── detail view: CLOSED POSITIONS ────────────────────────────────────────────

async function _pfPopulateClosed() {
  if (!_pfClosedCache) {
    try {
      _pfClosedCache = await apiFetch(`/users/${state.userId}/private-fleet/closed-positions?limit=200`);
    } catch(e) {
      document.getElementById('pf-detail-body').innerHTML =
        `<div style="color:var(--red);padding:20px;">Failed to load: ${escHtml(String(e))}</div>`;
      return;
    }
  }
  _pfRenderClosedDetail(_pfClosedCache, _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfSetClosedFilter(f) {
  _pfFilter = f;
  _pfRenderClosedDetail(_pfClosedCache, f, _pfSortCol, _pfSortAsc);
}

function _pfSortClosed(col) {
  if (_pfSortCol === col) { _pfSortAsc = !_pfSortAsc; } else { _pfSortCol = col; _pfSortAsc = true; }
  _pfRenderClosedDetail(_pfClosedCache, _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfRenderClosedDetail(data, filter, sortCol, sortAsc) {
  const body = document.getElementById('pf-detail-body');
  if (!body || !data) return;

  const { stats, positions } = data;

  // Outcome breakdown bar
  const total = positions.length || 1;
  const outcomeCounts = { t2_hit: stats.t2_hits, t1_hit: stats.t1_hits, stopped_out: stats.stopped, closed: stats.closed };
  const outcomeColors = { t2_hit: '#34c759', t1_hit: '#30d158', stopped_out: '#ff453a', closed: 'var(--muted)' };
  const breakdownBar = `<div style="margin-bottom:14px;">
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.08em;">Outcome Breakdown</div>
    <div style="display:flex;height:8px;border-radius:4px;overflow:hidden;gap:1px;">
      ${Object.entries(outcomeCounts).map(([k,v]) => {
        const pct = Math.round(v / total * 100);
        return pct > 0 ? `<div title="${_pfStatusLabel(k)}: ${v} (${pct}%)"
          style="flex:${pct};background:${outcomeColors[k]};min-width:2px;"></div>` : '';
      }).join('')}
    </div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:6px;">
      ${Object.entries(outcomeCounts).map(([k,v]) => `
        <span style="font-size:10px;color:${outcomeColors[k]};">● ${_pfStatusLabel(k)}: ${v}</span>`).join('')}
    </div>
  </div>`;

  // Filter
  const tabs = [
    { label: 'ALL', value: 'all' },
    { label: 'T2 HIT', value: 't2_hit' },
    { label: 'T1 HIT', value: 't1_hit' },
    { label: 'STOPPED', value: 'stopped_out' },
    { label: 'CLOSED', value: 'closed' },
  ];
  let rows = positions;
  if (filter !== 'all') rows = rows.filter(p => p.status === filter);

  // Sort
  if (sortCol) {
    rows = [...rows].sort((a, b) => {
      let av = a[sortCol], bv = b[sortCol];
      if (av == null) return 1; if (bv == null) return -1;
      return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
  } else {
    rows = [...rows].sort((a, b) => new Date(b.closed_at||0) - new Date(a.closed_at||0));
  }

  const thCols = [
    { label: 'TICKER', key: 'ticker' }, { label: 'SIDE', key: 'direction' },
    { label: 'PATTERN', key: null }, { label: 'STATUS', key: 'status' },
    { label: 'ENTRY', key: 'entry_price' }, { label: 'EXIT', key: 'exit_price' },
    { label: 'P&L R', key: 'pnl_r' }, { label: 'CLOSED', key: 'closed_at' },
  ];

  const tableRows = rows.map(p => {
    const pnl    = p.pnl_r != null ? p.pnl_r.toFixed(2) + 'R' : '—';
    const pnlCol = p.pnl_r > 0 ? 'var(--green)' : p.pnl_r < 0 ? 'var(--red)' : 'var(--muted)';
    const pat    = (() => { try { const a = JSON.parse(p.pattern_types); return (Array.isArray(a) ? a[0] : a) || p.pattern_id || '—'; } catch { return p.pattern_types || p.pattern_id || '—'; } })().replace(/_/g, ' ');
    const closed = p.closed_at ? new Date(p.closed_at).toLocaleDateString('en-GB', {day:'2-digit',month:'short'}) : '—';
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:7px 6px;font-weight:600;font-size:12px;white-space:nowrap;">${escHtml(p.ticker)}</td>
      <td style="padding:7px 6px;font-size:11px;white-space:nowrap;">${_pfSideHtml(p.direction)}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(pat)}</td>
      <td style="padding:7px 6px;font-size:11px;white-space:nowrap;color:${outcomeColors[p.status]||'var(--muted)'};">${ _pfStatusLabel(p.status)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;white-space:nowrap;">${_pfFmt(p.entry_price)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;white-space:nowrap;">${_pfFmt(p.exit_price)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;font-weight:600;white-space:nowrap;color:${pnlCol};">${pnl}</td>
      <td style="padding:7px 6px;font-size:10px;color:var(--muted);white-space:nowrap;">${closed}</td>
    </tr>`;
  }).join('');

  const summary = _pfSummaryBar([
    { label: 'Win Rate',     value: stats.win_rate + '%', color: stats.win_rate >= 50 ? 'var(--green)' : 'var(--red)' },
    { label: 'Avg R',        value: stats.avg_r + 'R',    color: stats.avg_r > 0 ? 'var(--green)' : 'var(--red)' },
    { label: 'Gross Profit', value: stats.gross_profit + 'R', color: 'var(--green)' },
    { label: 'Gross Loss',   value: stats.gross_loss + 'R',   color: 'var(--red)' },
  ]);

  body.innerHTML = summary + breakdownBar
    + _pfTabs(tabs, filter, '_pfSetClosedFilter')
    + `<div style="overflow-x:auto;"><table style="width:100%;min-width:640px;border-collapse:collapse;font-size:12px;">
      <thead><tr style="border-bottom:1px solid var(--border);">
        ${thCols.map(c => c.key ? _pfSortHeader(c.label, c.key, sortCol, sortAsc, '_pfSortClosed')
          : `<th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;white-space:nowrap;">${c.label}</th>`).join('')}
      </tr></thead>
      <tbody>${tableRows || '<tr><td colspan="8" style="padding:20px;text-align:center;color:var(--muted);">No positions.</td></tr>'}</tbody>
    </table></div>`;

  const upd = document.getElementById('pf-detail-updated');
  if (upd) upd.textContent = `${rows.length} of ${positions.length} shown`;
}

// ── detail view: DISCOVERY BOTS ───────────────────────────────────────────────

async function _pfPopulateBots() {
  const s = _pfLastStatus;
  if (!s) {
    document.getElementById('pf-detail-body').innerHTML = _pfLoading();
    return;
  }

  let perf = {};
  try {
    const r = await apiFetch(`/users/${state.userId}/bots/fleet-performance`);
    (r?.bots || []).forEach(b => { perf[b.bot_id] = b; });
  } catch(e) { /* perf stays empty */ }

  _pfRenderBotsDetail(s, perf, _pfFilter);
}

function _pfSetBotsFilter(f) {
  _pfFilter = f;
  if (_pfLastStatus) _pfRenderBotsDetail(_pfLastStatus, window._pfPerfCache || {}, f);
}

function _pfRenderBotsDetail(s, perf, filter) {
  window._pfPerfCache = perf;
  const body = document.getElementById('pf-detail-body');
  if (!body) return;

  const openByBot = {};
  (s.open_positions || []).forEach(p => {
    openByBot[p.bot_id] = (openByBot[p.bot_id] || 0) + 1;
  });

  // Get bot list from perf response + coverage (no disc_ prefix filter — bot_ids vary)
  const botIds = [...new Set([
    ...Object.keys(perf),
    ...(s.coverage || []).map(c => c.pattern).filter(Boolean),
  ])].filter(Boolean);

  // Use coverage patterns for filter tabs
  const coveragePatterns = [...new Set((s.coverage || []).map(c => {
    try { return JSON.parse(c.pattern)?.[0] || c.pattern; } catch { return c.pattern; }
  }).filter(Boolean))];

  const tabs = [
    { label: 'ALL', value: 'all' },
    ...coveragePatterns.slice(0, 6).map(p => ({ label: p.replace(/_/g,' ').toUpperCase(), value: p })),
  ];

  let bots = botIds;
  if (filter !== 'all') {
    bots = bots.filter(id => id.includes(filter));
  }

  const summary = _pfSummaryBar([
    { label: 'Total Bots',  value: s.total_bots  || botIds.length },
    { label: 'Active',      value: s.active_bots  || 0, color: 'var(--green)' },
    { label: 'Open Pos',    value: s.open_positions?.length || 0 },
  ]);

  const tableRows = bots.length ? bots.map(id => {
    const cleanId = id.replace(/^disc_/, '');
    const label   = cleanId.replace(/_/g, ' · ');
    const parts   = cleanId.split('_');
    const pat     = parts[0] || '—';
    const sec     = parts[1] || '—';
    const dir     = parts[2] || 'any';
    const open    = openByBot[id] || 0;
    const closed  = perf[id]?.closed || 0;
    const wr      = perf[id]?.win_rate != null ? (perf[id].win_rate * 100).toFixed(0) + '%' : '—';
    const wrCol   = perf[id]?.win_rate >= 0.6 ? 'var(--green)' : perf[id]?.win_rate >= 0.45 ? 'var(--accent)' : 'var(--muted)';
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:7px 6px;font-size:11px;font-weight:500;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(label)}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(pat.replace(/_/g,' '))}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(sec)}</td>
      <td style="padding:7px 6px;font-size:11px;white-space:nowrap;">${_pfSideHtml(dir === 'any' ? null : dir)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;">${open}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--muted);">${closed}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;font-weight:600;color:${wrCol};">${wr}</td>
    </tr>`;
  }).join('') : `<tr><td colspan="7" style="padding:30px;text-align:center;color:var(--muted);">No bot performance data yet — positions will appear once bots start closing trades.</td></tr>`;

  // If no bots from perf, show coverage grid instead
  const showCoverage = botIds.length === 0;
  const coverageRows = showCoverage ? (s.coverage || []).map(c => {
    let pat = c.pattern || '—';
    try { pat = JSON.parse(c.pattern)?.[0] || pat; } catch {}
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);">${escHtml(pat.replace(/_/g,' '))}</td>
      <td style="padding:7px 6px;font-size:11px;white-space:nowrap;">${_pfSideHtml(c.direction === 'any' ? null : c.direction)}</td>
      <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--accent);">${c.count}</td>
    </tr>`;
  }).join('') : null;

  body.innerHTML = summary
    + (showCoverage ? '' : _pfTabs(tabs, filter, '_pfSetBotsFilter'))
    + `<div style="overflow-x:auto;"><table style="width:100%;min-width:500px;border-collapse:collapse;font-size:12px;">
      <thead><tr style="border-bottom:1px solid var(--border);">
        ${showCoverage ? `
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">PATTERN</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">SIDE</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">BOTS</th>
        ` : `
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">BOT</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">PATTERN</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">SECTOR</th>
          <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">SIDE</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">OPEN</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">CLOSED</th>
          <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">WIN%</th>
        `}
      </tr></thead>
      <tbody>${showCoverage ? (coverageRows || '<tr><td colspan="3" style="padding:20px;text-align:center;color:var(--muted);">No coverage data.</td></tr>') : tableRows}</tbody>
    </table></div>`;

  const upd = document.getElementById('pf-detail-updated');
  if (upd) upd.textContent = `${bots.length} bots`;
}

// ── detail view: CALIBRATION OBS ─────────────────────────────────────────────

async function _pfPopulateCalib() {
  if (!_pfCalibCache) {
    try {
      _pfCalibCache = await apiFetch(`/users/${state.userId}/private-fleet/calibration-obs?limit=200`);
    } catch(e) {
      document.getElementById('pf-detail-body').innerHTML =
        `<div style="color:var(--red);padding:20px;">Failed to load: ${escHtml(String(e))}</div>`;
      return;
    }
  }
  _pfRenderCalibDetail(_pfCalibCache, _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfSetCalibFilter(f) {
  _pfFilter = f;
  _pfRenderCalibDetail(_pfCalibCache, f, _pfSortCol, _pfSortAsc);
}

function _pfSortCalib(col) {
  if (_pfSortCol === col) { _pfSortAsc = !_pfSortAsc; } else { _pfSortCol = col; _pfSortAsc = true; }
  _pfRenderCalibDetail(_pfCalibCache, _pfFilter, _pfSortCol, _pfSortAsc);
}

function _pfRenderCalibDetail(data, filter, sortCol, sortAsc) {
  const body = document.getElementById('pf-detail-body');
  if (!body || !data) return;

  const { observations, outcome_breakdown, pattern_breakdown, signal_cells, cells_with_obs } = data;

  // Pattern breakdown bar
  const totalPat = Object.values(pattern_breakdown).reduce((a,b) => a+b, 0) || 1;
  const patColors = ['var(--accent)', '#af52de', '#30d158', '#64d2ff', '#ff9f0a', '#ff453a'];
  const patEntries = Object.entries(pattern_breakdown).sort((a,b) => b[1]-a[1]);
  const patBar = patEntries.length ? `<div style="margin-bottom:16px;">
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.08em;">Pattern Breakdown</div>
    <div style="display:flex;height:8px;border-radius:4px;overflow:hidden;gap:1px;">
      ${patEntries.map(([k,v],i) => {
        const pct = Math.round(v/totalPat*100);
        return `<div title="${k}: ${v} (${pct}%)" style="flex:${pct};background:${patColors[i%patColors.length]};min-width:2px;"></div>`;
      }).join('')}
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:6px;">
      ${patEntries.map(([k,v],i) => `<span style="font-size:10px;color:${patColors[i%patColors.length]};">
        ● ${escHtml(k.replace(/_/g,' '))}: ${v} (${Math.round(v/totalPat*100)}%)</span>`).join('')}
    </div>
  </div>` : '';

  // Filter tabs
  const outcomes = [...new Set(observations.map(o => o.outcome).filter(Boolean))];
  const tabs = [
    { label: 'ALL', value: 'all' },
    ...outcomes.map(o => ({ label: o.replace(/_/g,' ').toUpperCase(), value: o })),
  ];

  // Filter + sort
  let rows = observations;
  if (filter !== 'all') rows = rows.filter(o => o.outcome === filter);
  if (sortCol) {
    rows = [...rows].sort((a,b) => {
      let av = a[sortCol], bv = b[sortCol];
      if (av == null) return 1; if (bv == null) return -1;
      return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
  } else {
    rows = [...rows].sort((a,b) => new Date(b.observed_at||0) - new Date(a.observed_at||0));
  }

  const thCols = [
    { label: 'TICKER', key: 'ticker' }, { label: 'PATTERN', key: 'pattern_type' },
    { label: 'TF', key: 'timeframe' }, { label: 'REGIME', key: 'market_regime' },
    { label: 'OUTCOME', key: 'outcome' }, { label: 'BOT', key: null },
    { label: 'DATE', key: 'observed_at' },
  ];

  const tableRows = rows.map(o => {
    const oc   = o.outcome || '—';
    const ocCol = _pfOutcomeColor(oc);
    const ocLbl = oc.replace(/_/g,' ');
    const ocArr = oc.includes('t2') || oc.includes('hit') ? '▲' : oc.includes('stop') ? '▼' : '—';
    const bot  = (o.bot_id || o.source || '—').replace('disc_','').slice(0, 20);
    const date = o.observed_at ? new Date(o.observed_at).toLocaleDateString('en-GB', {day:'2-digit',month:'short'}) : '—';
    const safe = encodeURIComponent(JSON.stringify(o));
    return `<tr onclick="_pfCalibRowClick(decodeURIComponent('${safe}'))"
      onmouseenter="this.style.background='rgba(255,255,255,0.03)'"
      onmouseleave="this.style.background=''"
      style="border-bottom:1px solid var(--border);cursor:pointer;">
      <td style="padding:7px 6px;font-weight:600;font-size:12px;white-space:nowrap;">${escHtml(o.ticker)}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml((o.pattern_type||'—').replace(/_/g,' '))}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml(o.timeframe||'—')}</td>
      <td style="padding:7px 6px;font-size:11px;color:var(--muted);white-space:nowrap;">${escHtml((o.market_regime||'—').replace(/_/g,' '))}</td>
      <td style="padding:7px 6px;font-size:11px;font-weight:600;white-space:nowrap;color:${ocCol};">${ocArr} ${escHtml(ocLbl)}</td>
      <td style="padding:7px 6px;font-size:10px;color:var(--muted);white-space:nowrap;">${escHtml(bot)}</td>
      <td style="padding:7px 6px;font-size:10px;color:var(--muted);white-space:nowrap;">${date}</td>
    </tr>`;
  }).join('');

  const summary = _pfSummaryBar([
    { label: 'Observations', value: data.total },
    { label: 'Signal Cells', value: (signal_cells || 0).toLocaleString() },
    { label: 'Cells w/ Obs', value: cells_with_obs || 0 },
    { label: 'T2 Hits',  value: outcome_breakdown.hit_t2 || outcome_breakdown.t2_hit || 0, color: 'var(--green)' },
    { label: 'Stopped',  value: outcome_breakdown.stopped_out || 0, color: 'var(--red)' },
  ]);

  body.innerHTML = summary + patBar
    + _pfTabs(tabs, filter, '_pfSetCalibFilter')
    + `<div style="overflow-x:auto;"><table style="width:100%;min-width:580px;border-collapse:collapse;font-size:12px;">
      <thead><tr style="border-bottom:1px solid var(--border);">
        ${thCols.map(c => c.key ? _pfSortHeader(c.label, c.key, sortCol, sortAsc, '_pfSortCalib')
          : `<th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;white-space:nowrap;">${c.label}</th>`).join('')}
      </tr></thead>
      <tbody>${tableRows || '<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--muted);">No observations.</td></tr>'}</tbody>
    </table></div>`;

  const upd = document.getElementById('pf-detail-updated');
  if (upd) upd.textContent = `${rows.length} of ${data.total} shown`;
}

// ── Calibration obs row detail card ──────────────────────────────────────────

function _pfCalibRowClick(jsonStr) {
  let o;
  try { o = JSON.parse(jsonStr); } catch { return; }

  const existing = document.getElementById('pf-calib-modal');
  if (existing) existing.remove();

  const oc     = o.outcome || '—';
  const ocCol  = _pfOutcomeColor(oc);
  const ocArr  = oc.includes('t2') || oc.includes('hit') ? '▲' : oc.includes('stop') ? '▼' : '—';
  const ocLbl  = oc.replace(/_/g, ' ');
  const pnl    = o.pnl_r != null ? o.pnl_r.toFixed(2) + 'R' : '—';
  const pnlCol = o.pnl_r > 0 ? 'var(--green)' : o.pnl_r < 0 ? 'var(--red)' : 'var(--muted)';
  const date   = o.observed_at ? new Date(o.observed_at).toLocaleString('en-GB', {day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
  const botFull = o.bot_id || o.source || '—';

  const rows = [
    { label: 'Ticker',       value: escHtml(o.ticker || '—'),                               color: 'var(--accent)',  large: true },
    { label: 'Outcome',      value: `${ocArr} ${escHtml(ocLbl)}`,                            color: ocCol },
    { label: 'P&L R',        value: escHtml(pnl),                                            color: pnlCol },
    { label: 'Pattern',      value: escHtml((o.pattern_type || '—').replace(/_/g, ' ')) },
    { label: 'Timeframe',    value: escHtml(o.timeframe || '—') },
    { label: 'Regime',       value: escHtml((o.market_regime || '—').replace(/_/g, ' ')) },
    { label: 'Bot',          value: escHtml(botFull.replace('disc_', '').replace(/_/g, ' · ')) },
    { label: 'Observed',     value: escHtml(date) },
    { label: 'ID',           value: `<span style="font-size:10px;color:var(--muted);word-break:break-all;">${escHtml(o.id || '—')}</span>` },
  ];

  const modal = document.createElement('div');
  modal.id = 'pf-calib-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px;';
  modal.innerHTML = `
    <div style="background:var(--bg);border:1px solid var(--border);border-radius:12px;padding:24px;max-width:420px;width:100%;position:relative;box-shadow:0 8px 40px rgba(0,0,0,0.6);">
      <button onclick="document.getElementById('pf-calib-modal').remove()"
        style="position:absolute;top:12px;right:14px;background:transparent;border:none;color:var(--muted);font-size:18px;cursor:pointer;line-height:1;">×</button>
      <div style="font-size:10px;font-weight:700;letter-spacing:2px;color:var(--accent);margin-bottom:16px;">◈ OBSERVATION DETAIL</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
        ${rows.map(r => `
          <div style="${r.large ? 'grid-column:1/-1;' : ''}">
            <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px;">${r.label}</div>
            <div style="font-size:${r.large ? '20px' : '13px'};font-weight:${r.large ? '700' : '500'};color:${r.color || 'var(--text)'}">${r.value}</div>
          </div>`).join('')}
      </div>
    </div>`;

  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
  document.body.appendChild(modal);
}
