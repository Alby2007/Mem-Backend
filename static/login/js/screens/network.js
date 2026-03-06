// ── NETWORK ───────────────────────────────────────────────────────────────────
async function loadNetwork() {
  // Coverage leaderboard
  try {
    const d = await apiFetch('/universe/coverage');
    const rows = (d?.tickers || []).slice(0, 20);
    const maxCov = Math.max(...rows.map(r => r.coverage_count || 0), 1);
    const el = document.getElementById('net-leaderboard');
    if (!rows.length) { el.innerHTML = '<div class="empty text-sm text-muted">No coverage data</div>'; return; }
    el.innerHTML = rows.map((r, i) => `
      <div class="lb-row">
        <span class="lb-rank">${i+1}</span>
        <span class="lb-ticker mono-amber">${escHtml(r.ticker)}</span>
        <div class="lb-bar-wrap"><div class="lb-bar" style="width:${((r.coverage_count||0)/maxCov*100).toFixed(1)}%"></div></div>
        <span class="lb-count">${r.coverage_count||0}</span>
      </div>`).join('');
  } catch { document.getElementById('net-leaderboard').innerHTML = '<div class="empty text-sm text-muted">Unavailable</div>'; }

  // Trending
  try {
    const d = await apiFetch('/universe/trending');
    const rows = (d?.trending || []).slice(0, 15);
    const el = document.getElementById('net-trending');
    if (!rows.length) { el.innerHTML = '<div class="empty text-sm text-muted">No trending data</div>'; return; }
    el.innerHTML = rows.map(r => `
      <div class="lb-row">
        <span class="lb-ticker mono-amber">${escHtml(r.ticker)}</span>
        <span class="text-xs text-muted" style="flex:1">${escHtml(r.sector_label||'')}</span>
        <span class="mono-green text-xs">+${fmt(r.growth_rate*100,0)}%</span>
      </div>`).join('');
  } catch { document.getElementById('net-trending').innerHTML = '<div class="empty text-sm text-muted">Unavailable</div>'; }

  // Health
  try {
    const d = await apiFetch('/network/health');
    const el = document.getElementById('net-health');
    el.innerHTML = `<div class="card" style="font-size:12px;display:flex;flex-direction:column;gap:12px;">
      ${[
        ['Total Tickers', d.total_tickers],
        ['Total Users', d.total_users],
        ['Flywheel Velocity', fmt(d.flywheel_velocity)],
        ['Cohort Signals Active', d.cohort_signals_active],
      ].map(([k,v]) => `<div class="flex-center" style="justify-content:space-between;">
        <span class="text-muted">${k}</span>
        <span class="mono-amber fw-700">${v ?? '—'}</span>
      </div>`).join('')}
      ${d.tickers_by_tier ? `<div style="margin-top:4px;">
        <div class="text-xs text-muted mb-8">Tickers by Tier</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;">
          ${Object.entries(d.tickers_by_tier).map(([k,v]) => `${tierBadge(k)} <span class="mono text-xs">${v}</span>`).join('')}
        </div>
      </div>` : ''}
    </div>`;
  } catch { document.getElementById('net-health').innerHTML = '<div class="empty text-sm text-muted">Unavailable</div>'; }
}

