// ── PRIVATE FLEET — Internal discovery fleet observation ──────────────────────

let _pfPollTimer = null;

async function loadPrivateFleet() {
  if (!state.userId || !state.isDev) return;
  const sc = document.getElementById('screen-private-fleet');
  if (!sc) return;

  sc.innerHTML = `
    <div id="pf-wrap" style="max-width:960px;margin:0 auto;padding:20px 16px;">
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
      </div>
    </div>`;

  await _pfRefresh();

  if (_pfPollTimer) clearInterval(_pfPollTimer);
  _pfPollTimer = setInterval(() => {
    if (document.getElementById('screen-private-fleet')?.classList.contains('active')) {
      _pfRefresh();
    } else {
      clearInterval(_pfPollTimer);
      _pfPollTimer = null;
    }
  }, 30000);
}

async function _pfRefresh() {
  try {
    const [status, report] = await Promise.all([
      apiFetch(`/users/${state.userId}/private-fleet/status`),
      apiFetch(`/users/${state.userId}/private-fleet/report?min_observations=3&limit=20`),
    ]);

    _pfRenderStats(status);
    _pfRenderReport(report?.report || []);
    _pfRenderPositions(status?.open_positions || []);

    const el = document.getElementById('pf-last-updated');
    if (el) el.textContent = 'Updated ' + new Date().toLocaleTimeString('en-GB');
  } catch(e) {
    console.error('Private fleet load error:', e);
  }
}

function _pfRenderStats(s) {
  const el = document.getElementById('pf-stats-row');
  if (!el || !s) return;
  const openCount = s.open_positions?.length || 0;
  const cards = [
    { label: 'Discovery Bots',   value: s.total_bots || 0,                              sub: `${s.active_bots || 0} active` },
    { label: 'Open Positions',   value: openCount,                                       sub: 'live entries' },
    { label: 'Closed Positions', value: (s.total_positions_closed || 0).toLocaleString(), sub: 'all time' },
    { label: 'Calibration Obs',  value: (s.total_observations || 0).toLocaleString(),     sub: 'signal cells' },
  ];
  el.innerHTML = cards.map(c => `
    <div style="background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;">
      <div style="font-size:10px;color:var(--muted);letter-spacing:1px;margin-bottom:4px;">${c.label.toUpperCase()}</div>
      <div style="font-size:22px;font-weight:700;color:var(--text);">${c.value}</div>
      <div style="font-size:10px;color:var(--muted);margin-top:2px;">${c.sub}</div>
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
    const dirCol = dir === 'bullish' ? 'var(--green)' : dir === 'bearish' ? 'var(--red)' : 'var(--muted)';
    const dirArrow = dir === 'bullish' ? '▲' : dir === 'bearish' ? '▼' : '—';
    const pat = (() => { try { return JSON.parse(p.pattern)[0] || '—'; } catch { return p.pattern || '—'; } })();
    const sec = (() => { try { return JSON.parse(p.sector)?.[0] || 'all'; } catch { return p.sector || 'all'; } })();
    const age = (() => {
      if (!p.opened_at) return '—';
      const ms = Date.now() - new Date(p.opened_at).getTime();
      const h = Math.floor(ms / 3600000);
      const m = Math.floor((ms % 3600000) / 60000);
      return h > 0 ? `${h}h ${m}m` : `${m}m`;
    })();
    return `
      <tr style="border-bottom:1px solid var(--border);">
        <td style="padding:7px 6px;font-weight:600;font-size:12px;">${escHtml(p.ticker)}</td>
        <td style="padding:7px 6px;font-size:11px;color:${dirCol};">${dirArrow} ${dir}</td>
        <td style="padding:7px 6px;font-size:11px;color:var(--muted);">${escHtml(pat.replace(/_/g,' '))}</td>
        <td style="padding:7px 6px;font-size:11px;color:var(--muted);">${escHtml(sec)}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;">${p.entry?.toLocaleString() ?? '—'}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--green);">${p.t1?.toLocaleString() ?? '—'}</td>
        <td style="padding:7px 6px;font-size:11px;text-align:right;color:var(--red);">${p.stop?.toLocaleString() ?? '—'}</td>
        <td style="padding:7px 6px;font-size:10px;color:var(--muted);text-align:right;">${age}</td>
      </tr>`;
  }).join('');

  el.innerHTML = `
    <div style="overflow-x:auto;">
      <table style="width:100%;border-collapse:collapse;font-size:12px;">
        <thead>
          <tr style="border-bottom:1px solid var(--border);">
            <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">TICKER</th>
            <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">DIR</th>
            <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">PATTERN</th>
            <th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);font-weight:600;">SECTOR</th>
            <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">ENTRY</th>
            <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">T1</th>
            <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">STOP</th>
            <th style="padding:6px;text-align:right;font-size:10px;color:var(--muted);font-weight:600;">AGE</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}
