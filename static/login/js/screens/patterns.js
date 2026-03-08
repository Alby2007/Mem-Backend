// ── PATTERNS ──────────────────────────────────────────────────────────────────

// Change 1 — relative timestamps
function _relTime(ts) {
  const diff = Date.now() - new Date(ts).getTime();
  const h = diff / 3600000;
  if (h < 1)  return Math.round(diff / 60000) + 'm ago';
  if (h < 24) return Math.round(h) + 'h ago';
  if (h < 48) return 'Yesterday';
  return Math.round(h / 24) + 'd ago';
}

// Change 2 — conviction colour
function _qualColour(q) {
  if (q >= 0.90) return 'qual-high';
  if (q >= 0.75) return 'qual-med';
  return 'qual-low';
}

// Change 3 — distance to zone
function _zoneStatus(p, currentPrice) {
  if (!currentPrice) return null;
  const mid = (p.zone_high + p.zone_low) / 2;
  const inZone = currentPrice >= p.zone_low && currentPrice <= p.zone_high;
  if (inZone) return { label: '● IN ZONE', cls: 'zone-in' };
  const pct = ((mid - currentPrice) / currentPrice * 100);
  const dist = Math.abs(pct).toFixed(1);
  if (p.direction === 'bullish') {
    return pct > 0
      ? { label: `▼ ${dist}% to zone`,   cls: 'zone-near' }
      : { label: `▲ ${dist}% past zone`, cls: 'zone-past' };
  } else {
    return pct < 0
      ? { label: `▲ ${dist}% to zone`,   cls: 'zone-near' }
      : { label: `▼ ${dist}% past zone`, cls: 'zone-past' };
  }
}

// Change 5 — pagination state
let _patOffset = 0;
let _patFilters = {};
let _patTotal   = null;

async function loadPatterns() {
  const grid = document.getElementById('patterns-grid');
  grid.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  const type   = document.getElementById('pf-type').value;
  const tf     = document.getElementById('pf-tf').value;
  const dir    = document.getElementById('pf-dir').value;
  const ticker = document.getElementById('pf-ticker').value.trim().toUpperCase();
  // Reset pagination on a fresh search
  _patOffset  = 0;
  _patTotal   = null;
  _patFilters = { type, tf, dir, ticker };
  document.getElementById('pf-shift-hint').style.display = 'none';
  // Fetch snapshot prices for zone indicator
  if (!window._snapshotPrices) {
    apiFetch('/market/snapshot').then(d => {
      const syms = d?.symbols || {};
      window._snapshotPrices = {};
      Object.entries(syms).forEach(([sym, data]) => {
        if (data?.price != null) window._snapshotPrices[sym] = data.price;
      });
    }).catch(() => {});
  }
  await _fetchAndRenderPatterns(grid, ticker, false);
}

async function _fetchAndRenderPatterns(grid, ticker, append) {
  if (!append) grid.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  const { type, tf, dir } = _patFilters;
  const params = new URLSearchParams();
  if (type)   params.set('pattern_type', type);
  if (tf)     params.set('timeframe', tf);
  if (dir)    params.set('direction', dir);
  if (ticker) params.set('ticker', ticker);
  params.set('min_quality', '0.5');
  const PAGE = 60;
  params.set('limit',  ticker ? '100' : String(PAGE));
  params.set('offset', String(_patOffset));
  try {
    const d = await apiFetch(`/patterns/live?${params}`);
    let pats = d?.patterns || [];
    if (d?.total != null) _patTotal = d.total;
    // If no ticker filter, cap to 3 results per ticker to avoid flooding
    if (!ticker) {
      const seenCounts = {};
      pats = pats.filter(p => {
        seenCounts[p.ticker] = (seenCounts[p.ticker] || 0) + 1;
        return seenCounts[p.ticker] <= 3;
      });
    }
    if (!append) {
      window._patternCache = {};
    }
    pats.forEach(p => { window._patternCache[p.id] = p; });
    const totalLoaded = (_patOffset || 0) + pats.length;
    if (_patTotal != null) {
      document.getElementById('pf-count').textContent = `${totalLoaded} of ${_patTotal} result${_patTotal===1?'':'s'}`;
    } else {
      document.getElementById('pf-count').textContent = `${totalLoaded} result${totalLoaded===1?'':'s'}`;
    }
    if (!pats.length && !append) {
      grid.innerHTML = '<div class="empty text-sm text-muted">No open patterns match filters</div>';
      document.getElementById('pf-load-more').style.display = 'none';
      return;
    }
    const prices = window._snapshotPrices || {};
    const cards = pats.map(p => {
      const formedRel  = _relTime(p.formed_at);
      const formedFull = fmtDate(p.formed_at);
      const detectedRel  = p.detected_at ? _relTime(p.detected_at) : null;
      const zs = _zoneStatus(p, prices[p.ticker]);
      const zoneTag = zs ? `<span class="zone-tag ${zs.cls}">${zs.label}</span>` : '';
      return `
      <div class="pattern-card" onclick="handlePatternClick(event,this,${p.id})">
        <div class="flex-center gap-8 mb-8">
          <span class="mono-amber fw-700">${escHtml(p.ticker)}</span>
          ${dirBadge(p.direction)}
          <span class="badge badge-open" style="margin-left:auto;">${escHtml(p.pattern_type||'').replace(/_/g,' ')}</span>
        </div>
        <div class="flex-center gap-12 text-sm mb-8">
          <span class="text-muted">TF</span><span class="mono">${escHtml(p.timeframe||'—')}</span>
          <span class="text-muted">Q</span><span class="qual-dot ${_qualColour(p.quality_score)}"></span><span class="mono-amber">${fmt(p.quality_score)}</span>
        </div>
        <div class="text-xs text-muted">Zone: <span class="mono">${fmt(p.zone_low)} – ${fmt(p.zone_high)}</span> ${zoneTag}</div>
        <div class="text-xs text-muted mt-8" title="${escHtml(formedFull)}">Formed: ${escHtml(formedRel)}${detectedRel ? ` · detected ${escHtml(detectedRel)}` : ''}</div>
        <div class="pattern-detail" id="pd-${p.id}"><div class="spinner"></div></div>
        <button class="pat-chat-btn" onclick="event.stopPropagation();sendPatternToChat(window._patternCache[${p.id}])" title="Discuss in Chat">💬 Discuss</button>
      </div>`;
    }).join('');
    if (append) {
      const loadMoreEl = document.getElementById('pf-load-more');
      loadMoreEl.insertAdjacentHTML('beforebegin', cards);
    } else {
      grid.innerHTML = cards;
    }
    // Change 5 — load more footer
    const loadMoreEl = document.getElementById('pf-load-more');
    const hasMore = pats.length >= PAGE && !ticker;
    if (hasMore) {
      const shown = totalLoaded;
      const ofStr = _patTotal != null ? ` of ${_patTotal}` : '+';
      loadMoreEl.innerHTML = `<span>Showing ${shown}${ofStr} patterns</span><button onclick="loadMorePatterns()">Load more</button>`;
      loadMoreEl.style.display = '';
    } else {
      loadMoreEl.style.display = 'none';
    }
  } catch(e) {
    if (!append) grid.innerHTML = `<div class="empty text-sm" style="color:var(--red)">${escHtml(e.message)}</div>`;
  }
}

window.loadMorePatterns = async function() {
  const PAGE = 60;
  _patOffset += PAGE;
  const grid = document.getElementById('patterns-grid');
  await _fetchAndRenderPatterns(grid, _patFilters.ticker || '', true);
};

window.handlePatternClick = function(event, el, id) {
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

