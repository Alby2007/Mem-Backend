// ── PATTERNS ──────────────────────────────────────────────────────────────────
async function loadPatterns() {
  const grid = document.getElementById('patterns-grid');
  grid.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  const params = new URLSearchParams();
  const type   = document.getElementById('pf-type').value;
  const tf     = document.getElementById('pf-tf').value;
  const dir    = document.getElementById('pf-dir').value;
  const ticker = document.getElementById('pf-ticker').value.trim().toUpperCase();
  if (type)   params.set('pattern_type', type);
  if (tf)     params.set('timeframe', tf);
  if (dir)    params.set('direction', dir);
  if (ticker) params.set('ticker', ticker);
  params.set('min_quality', '0.5');
  params.set('limit', ticker ? '100' : '200');
  document.getElementById('pf-shift-hint').style.display = '';
  try {
    const d = await apiFetch(`/patterns/live?${params}`);
    let pats = d?.patterns || [];
    // If no ticker filter, cap to 3 results per ticker to avoid flooding
    if (!ticker) {
      const seenCounts = {};
      pats = pats.filter(p => {
        seenCounts[p.ticker] = (seenCounts[p.ticker] || 0) + 1;
        return seenCounts[p.ticker] <= 3;
      }).slice(0, 60);
    }
    document.getElementById('pf-count').textContent = `${pats.length} result${pats.length===1?'':'s'}`;
    if (!pats.length) { grid.innerHTML = '<div class="empty text-sm text-muted">No open patterns match filters</div>'; return; }
    window._patternCache = {};
    pats.forEach(p => { window._patternCache[p.id] = p; });
    grid.innerHTML = pats.map(p => `
      <div class="pattern-card" title="Ctrl+click to discuss in Chat" onclick="handlePatternClick(event,this,${p.id})">
        <div class="flex-center gap-8 mb-8">
          <span class="mono-amber fw-700">${escHtml(p.ticker)}</span>
          ${dirBadge(p.direction)}
          <span class="badge badge-open" style="margin-left:auto;">${escHtml(p.pattern_type||'').replace(/_/g,' ')}</span>
        </div>
        <div class="flex-center gap-12 text-sm mb-8">
          <span class="text-muted">TF</span><span class="mono">${escHtml(p.timeframe||'—')}</span>
          <span class="text-muted">Q</span><span class="mono-amber">${fmt(p.quality_score)}</span>
        </div>
        <div class="text-xs text-muted">Zone: <span class="mono">${fmt(p.zone_low)} – ${fmt(p.zone_high)}</span></div>
        <div class="text-xs text-muted mt-8">Formed: ${fmtDate(p.formed_at)}</div>
        <div class="pattern-detail" id="pd-${p.id}"><div class="spinner"></div></div>
      </div>`).join('');
  } catch(e) { grid.innerHTML = `<div class="empty text-sm" style="color:var(--red)">${escHtml(e.message)}</div>`; }
}

window.handlePatternClick = function(event, el, id) {
  if (event.ctrlKey) {
    const p = (window._patternCache || {})[id];
    if (p) sendPatternToChat(p);
    return;
  }
  togglePattern(el, id);
};

function sendPatternToChat(p) {
  showScreen('chat');
  const inp = document.getElementById('chat-input');
  const tag = `[PATTERN:${p.ticker} ${(p.direction||'').toUpperCase()} ${(p.pattern_type||'').replace(/_/g,' ').toUpperCase()} ${p.timeframe||'1d'} Q${fmt(p.quality_score)} Zone${fmt(p.zone_low)}-${fmt(p.zone_high)}] `;
  inp.value = tag + inp.value;
  inp.focus();
  inp.setSelectionRange(inp.value.length, inp.value.length);
  inp.style.height = 'auto';
  inp.style.height = inp.scrollHeight + 'px';
}

window.togglePattern = async function(el, id) {
  el.classList.toggle('expanded');
  const detail = document.getElementById(`pd-${id}`);
  if (!el.classList.contains('expanded')) return;
  if (detail.dataset.loaded) return;
  detail.dataset.loaded = '1';
  try {
    const qs = state.userId ? `?user_id=${state.userId}` : '';
    const d = await apiFetch(`/patterns/${id}${qs}`);
    const p = d?.pattern || d || {};
    const pos = d?.position_recommendation;
    detail.innerHTML = `
      <div style="font-size:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;">
        <div><span class="text-muted">Conviction</span><br><span class="mono-amber">${escHtml(p.kb_conviction||'—')}</span></div>
        <div><span class="text-muted">Regime</span><br><span class="mono">${escHtml((p.kb_regime||'—').replace(/_/g,' '))}</span></div>
        <div><span class="text-muted">Signal Dir</span><br>${dirBadge(p.kb_signal_dir)}</div>
        <div><span class="text-muted">Status</span><br><span class="badge badge-open">${escHtml(p.status||'open')}</span></div>
      </div>
      ${pos ? `<div class="card-sm text-sm">
        <div class="text-xs text-muted mb-8">Position Sizing</div>
        <div class="flex-center gap-12">
          <span><span class="text-muted">Entry</span> <span class="mono-amber">${fmt(pos.entry_price)}</span></span>
          <span><span class="text-muted">Stop</span> <span class="mono-red">${fmt(pos.stop_loss)}</span></span>
          <span><span class="text-muted">Units</span> <span class="mono-green">${pos.position_units || '—'}</span></span>
          <span><span class="text-muted">R/R</span> <span class="mono">${fmt(pos.rr_ratio)}</span></span>
        </div>
      </div>` : ''}`;
  } catch(e) { detail.innerHTML = `<span class="text-sm" style="color:var(--red)">${escHtml(e.message)}</span>`; }
};

document.getElementById('pf-search-btn').addEventListener('click', loadPatterns);
['pf-type','pf-tf','pf-dir'].forEach(id => {
  document.getElementById(id).addEventListener('change', loadPatterns);
});

