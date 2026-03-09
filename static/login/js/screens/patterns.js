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

// ── Pattern performance stats cache ─────────────────────────────────────────
let _patStatsCache = null;  // { 'ifvg|4h|bullish': { win_rate, total, wins }, ... }

async function _ensurePatStats() {
  if (_patStatsCache) return _patStatsCache;
  try {
    const d = await apiFetch('/patterns/stats?min_samples=10');
    _patStatsCache = {};
    (d?.stats || []).forEach(s => {
      const key = `${(s.pattern_type||'').toLowerCase()}|${(s.timeframe||'').toLowerCase()}|${(s.direction||'').toLowerCase()}`;
      _patStatsCache[key] = s;
    });
  } catch(e) {
    _patStatsCache = {};
  }
  return _patStatsCache;
}

function _patStatKey(p) {
  return `${(p.pattern_type||'').toLowerCase()}|${(p.timeframe||'').toLowerCase()}|${(p.direction||'').toLowerCase()}`;
}

function _statPill(stat) {
  if (!stat) return '';
  const wr  = Math.round((stat.win_rate || 0) * 100);
  const cls = wr >= 55 ? 'stat-win-high' : wr >= 45 ? 'stat-win-mid' : 'stat-win-low';
  return `<span class="pat-stat-pill ${cls}">${wr}% <span class="psp-n">n=${stat.total}</span></span>`;
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
  // Pre-warm stats cache in background (don't block render)
  _ensurePatStats();
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
      window._patternList    = [];
      window._patternListIdx = {};
    }
    pats.forEach((p, i) => {
      window._patternCache[p.id] = p;
      const offset = _patOffset || 0;
      window._patternList.push(p.id);
      window._patternListIdx[p.id] = (append ? (window._patternList.length - 1) : (offset + i));
    });
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
    const stats = _patStatsCache || {};
    const cards = pats.map(p => {
      const formedRel  = _relTime(p.formed_at);
      const formedFull = fmtDate(p.formed_at);
      const detectedRel  = p.detected_at ? _relTime(p.detected_at) : null;
      const zs = _zoneStatus(p, prices[p.ticker]);
      const zoneTag = zs ? `<span class="zone-tag ${zs.cls}">${zs.label}</span>` : '';
      const pill = _statPill(stats[_patStatKey(p)]);
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
          ${pill}
        </div>
        <div class="text-xs text-muted">Zone: <span class="mono">${fmt(p.zone_low)} – ${fmt(p.zone_high)}</span> ${zoneTag}</div>
        <div class="text-xs text-muted mt-8" title="${escHtml(formedFull)}">Formed: ${escHtml(formedRel)}${detectedRel ? ` · detected ${escHtml(detectedRel)}` : ''}</div>
        <button class="pat-chat-btn" onclick="event.stopPropagation();sendPatternToChat(window._patternCache[${p.id}])" title="Ctrl+click to discuss in Chat">💬 Discuss</button>
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
    const hasMore = pats.length >= 30 && !ticker;
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
  if (event.ctrlKey || event.metaKey) {
    sendPatternToChat(window._patternCache[id]);
    return;
  }
  openPatternModal(id);
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

// ── Pattern list tracking for keyboard nav ───────────────────────────────────
window._patternList    = [];
window._patternListIdx = {};

// ── TF → TradingView interval map ────────────────────────────────────────────
const _TV_INTERVAL = { '1m':'1','5m':'5','15m':'15','30m':'30','1h':'60','2h':'120','4h':'240','1d':'D','1w':'W' };

function _tvSymbol(ticker) {
  if (!ticker) return ticker;
  if (ticker.toUpperCase().endsWith('.L')) return 'LSE:' + ticker.slice(0, -2).toUpperCase();
  if (ticker.toUpperCase().endsWith('.l')) return 'LSE:' + ticker.slice(0, -2).toUpperCase();
  return ticker.toUpperCase();
}

// ── Zone bar SVG ─────────────────────────────────────────────────────────────
function _buildZoneBar(p, atoms) {
  const atomMap = {};
  (atoms || []).forEach(a => { atomMap[a.predicate] = a.value; });
  const price     = (window._snapshotPrices || {})[p.ticker] || null;
  const zl        = p.zone_low,  zh = p.zone_high;
  const target    = parseFloat(atomMap['price_target'] || atomMap['target_price'] || 0) || null;
  const inv       = parseFloat(atomMap['invalidation_price'] || atomMap['invalidation'] || 0) || null;

  const lo = Math.min(zl, inv || zl, price || zl) * 0.985;
  const hi = Math.max(zh, target || zh, price || zh) * 1.015;
  const range = hi - lo || 1;
  const pct = v => ((v - lo) / range * 100).toFixed(2);

  const zonePctL = pct(zl), zonePctH = pct(zh);
  const zoneW = (zh - zl) / range * 100;

  const pricePin = price ? `<div class="pzb-pin" style="left:${pct(price)}%" title="Current price: ${fmt(price)}"></div>` : '';
  const targetPin = target ? `<div class="pzb-target" style="left:${pct(target)}%" title="Target: ${fmt(target)}">▼</div>` : '';
  const invPin    = inv    ? `<div class="pzb-inv"    style="left:${pct(inv)}%"    title="Invalidation: ${fmt(inv)}">✕</div>` : '';

  return `<div class="pat-zone-bar">
    <div class="pzb-label">Zone</div>
    <div class="pzb-track">
      <div class="pzb-zone" style="left:${zonePctL}%;width:${zoneW.toFixed(2)}%"></div>
      ${pricePin}${targetPin}${invPin}
    </div>
    <div class="pzb-vals">
      <span>${fmt(zl)}</span><span class="pzb-dash">–</span><span>${fmt(zh)}</span>
      ${target ? `<span class="pzb-tgt">T: ${fmt(target)}</span>` : ''}
      ${inv    ? `<span class="pzb-inv-lbl">✕: ${fmt(inv)}</span>` : ''}
    </div>
  </div>`;
}

// ── Quality dots ─────────────────────────────────────────────────────────────
function _qualDots(q) {
  const filled = Math.round((q || 0) * 5);
  return Array.from({length:5}, (_,i) =>
    `<span class="qdot${i < filled ? ' qdot-on' : ''}"></span>`).join('');
}

// ── Modal open / close ────────────────────────────────────────────────────────
let _modalPatId = null;

function openPatternModal(id) {
  const p = window._patternCache[id];
  if (!p) return;
  _modalPatId = id;
  closePatternModal(true);

  const tvSym  = _tvSymbol(p.ticker);
  const tvInt  = _TV_INTERVAL[p.timeframe] || 'D';
  const ifrSrc = `https://api.trading-galaxy.uk/markets/chart?sym=${encodeURIComponent(tvSym)}&interval=${tvInt}&zone_high=${encodeURIComponent(p.zone_high||'')}&zone_low=${encodeURIComponent(p.zone_low||'')}&pattern_type=${encodeURIComponent(p.pattern_type||'')}&direction=${encodeURIComponent(p.direction||'')}`;

  const dirCls = (p.direction||'').toLowerCase().includes('bull') ? 'color:#22c55e' : 'color:#ef4444';

  const backdrop = document.createElement('div');
  backdrop.id = 'pat-modal-backdrop';
  backdrop.className = 'pat-modal-backdrop';
  backdrop.innerHTML = `
  <div class="pat-modal" id="pat-modal" role="dialog" aria-modal="true">
    <div class="pat-modal-header">
      <span class="pm-ticker">${escHtml(p.ticker)}</span>
      ${dirBadge(p.direction)}
      <span class="pm-type">${escHtml((p.pattern_type||'').replace(/_/g,' '))}</span>
      <span class="pm-tf">${escHtml(p.timeframe||'—')}</span>
      <span class="pm-qual">${_qualDots(p.quality_score)}<span class="pm-qual-num">${fmt(p.quality_score)}</span></span>
      <button class="pm-close" id="pm-close" aria-label="Close">&times;</button>
    </div>
    <div class="pat-modal-chart" id="pm-chart">
      <div class="pm-chart-placeholder" id="pm-chart-ph"><div class="spinner"></div><div class="pm-loading-text">Loading chart…</div></div>
      <iframe id="pm-iframe" src="${ifrSrc}" allowtransparency="true" frameborder="0" style="display:none;"></iframe>
      <div id="pm-zone-bar">${_buildZoneBar(p, [])}</div>
      <div id="pm-stats-block" class="pm-stats-block pm-stats-loading"></div>
    </div>
    <div class="pat-modal-right" id="pm-right">
      <div class="pat-modal-section">
        <div class="pat-modal-section-title">Pattern Details</div>
        <div class="pat-modal-details-grid">
          <div class="pm-detail-row"><span class="pm-dlabel">Zone</span><span class="pm-dval mono">${fmt(p.zone_low)} – ${fmt(p.zone_high)}</span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Quality</span><span class="pm-dval">${_qualDots(p.quality_score)} <span class="mono-amber">${fmt(p.quality_score)}</span></span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Formed</span><span class="pm-dval">${escHtml(_relTime(p.formed_at))}</span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Status</span><span class="pm-dval"><span class="badge badge-open">${escHtml(p.status||'open')}</span></span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Conviction</span><span class="pm-dval mono-amber" id="pm-conviction">…</span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Regime</span><span class="pm-dval mono" id="pm-regime">…</span></div>
          <div class="pm-detail-row"><span class="pm-dlabel">Signal Dir</span><span class="pm-dval" id="pm-sigdir">…</span></div>
        </div>
      </div>
      <div class="pat-modal-section" id="pm-evidence-section">
        <div class="pat-modal-section-title" id="pm-evidence-title">KB Evidence</div>
        <div id="pm-evidence-rows">
          <div class="pat-evidence-skeleton"></div>
          <div class="pat-evidence-skeleton"></div>
          <div class="pat-evidence-skeleton"></div>
        </div>
      </div>
      <div class="pat-modal-footer">
        <button class="pat-discuss-btn" id="pm-discuss">💬 Discuss in Chat &rarr;</button>
      </div>
    </div>
  </div>`;

  document.body.appendChild(backdrop);

  // Show iframe once loaded
  const iframe = document.getElementById('pm-iframe');
  iframe.addEventListener('load', () => {
    document.getElementById('pm-chart-ph').style.display = 'none';
    iframe.style.display = 'block';
  });

  // Close handlers
  document.getElementById('pm-close').addEventListener('click', closePatternModal);
  backdrop.addEventListener('click', e => { if (e.target === backdrop) closePatternModal(); });

  // Discuss CTA
  document.getElementById('pm-discuss').addEventListener('click', () => {
    closePatternModal();
    sendPatternToChat(p);
  });

  // Load context + stats async
  _loadPatternContext(id, p);
  _loadPatternStats(p);
}

function closePatternModal(silent) {
  const el = document.getElementById('pat-modal-backdrop');
  if (el) el.remove();
  if (!silent) _modalPatId = null;
}

async function _loadPatternStats(p) {
  const el = document.getElementById('pm-stats-block');
  if (!el) return;
  const stats = await _ensurePatStats();
  const s = stats[_patStatKey(p)];
  if (!s) {
    el.innerHTML = '<span class="pms-none">Insufficient data for this pattern type + timeframe</span>';
    el.classList.remove('pm-stats-loading');
    return;
  }
  const wr    = Math.round((s.win_rate || 0) * 100);
  const wrCls = wr >= 55 ? 'stat-win-high' : wr >= 45 ? 'stat-win-mid' : 'stat-win-low';
  const barW  = Math.max(2, wr);
  const label = `${(p.pattern_type||'').replace(/_/g,' ').toUpperCase()} · ${p.timeframe||''} · ${(p.direction||'').toLowerCase()}`;
  el.innerHTML = `
    <div class="pms-label">${escHtml(label)}</div>
    <div class="pms-row">
      <div class="pms-stat">
        <span class="pms-val ${wrCls}">${wr}%</span>
        <span class="pms-key">win rate</span>
      </div>
      <div class="pms-stat">
        <span class="pms-val">${s.total}</span>
        <span class="pms-key">sample size</span>
      </div>
      <div class="pms-stat">
        <span class="pms-val">${s.wins}<span class="pms-sub">W</span> ${s.losses}<span class="pms-sub">L</span></span>
        <span class="pms-key">record</span>
      </div>
      <div class="pms-stat">
        <span class="pms-val">${s.avg_quality != null ? s.avg_quality : '—'}</span>
        <span class="pms-key">avg quality</span>
      </div>
    </div>
    <div class="pms-bar-track"><div class="pms-bar-fill ${wrCls}" style="width:${barW}%"></div></div>`;
  el.classList.remove('pm-stats-loading');
}

async function _loadPatternContext(id, p) {
  let data;
  try {
    data = await apiFetch(`/patterns/${id}/context`);
  } catch(e) {
    const evEl = document.getElementById('pm-evidence-rows');
    if (evEl) evEl.innerHTML = `<div class="text-xs" style="color:var(--red);padding:8px 16px;">${escHtml(e.message)}</div>`;
    return;
  }

  const d = data || {};
  const pat = d.pattern || p;
  const atoms = d.atoms || [];

  // Fill conviction / regime / signal dir
  const convEl = document.getElementById('pm-conviction');
  const regEl  = document.getElementById('pm-regime');
  const sdEl   = document.getElementById('pm-sigdir');
  if (convEl) convEl.textContent = pat.kb_conviction || '—';
  if (regEl)  regEl.textContent  = (pat.kb_regime || '—').replace(/_/g,' ');
  if (sdEl)   sdEl.innerHTML     = dirBadge(pat.kb_signal_dir);

  // Update zone bar with actual atom data
  const zoneEl = document.getElementById('pm-zone-bar');
  if (zoneEl) zoneEl.innerHTML = _buildZoneBar(pat, atoms);

  // Evidence title
  const titleEl = document.getElementById('pm-evidence-title');
  if (titleEl) titleEl.textContent = `KB Evidence  (${d.atom_count || atoms.length} atoms)`;

  // Render evidence rows
  const evEl = document.getElementById('pm-evidence-rows');
  if (!evEl) return;
  if (!atoms.length) {
    evEl.innerHTML = '<div class="text-xs text-muted" style="padding:8px 16px;">No KB atoms found for this ticker.</div>';
    return;
  }
  const SHOW = 8;
  const top  = atoms.slice(0, SHOW);
  const rest = atoms.slice(SHOW);
  const renderRow = a => `
    <div class="pat-evidence-row">
      <span class="pe-pred">${escHtml(a.predicate.replace(/_/g,' '))}</span>
      <span class="pe-val">${escHtml(a.value)}<span class="pe-conf">(${a.confidence})</span></span>
    </div>`;
  let html = top.map(renderRow).join('');
  if (rest.length) {
    html += `<div class="pe-show-more" id="pm-show-more">+ ${rest.length} more</div>`;
    html += `<div id="pm-rest-rows" style="display:none;">${rest.map(renderRow).join('')}</div>`;
  }
  evEl.innerHTML = html;
  const moreBtn = document.getElementById('pm-show-more');
  if (moreBtn) {
    moreBtn.addEventListener('click', () => {
      const restEl = document.getElementById('pm-rest-rows');
      if (restEl) { restEl.style.display = ''; moreBtn.remove(); }
    });
  }
}

// ── Keyboard nav ──────────────────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && _modalPatId !== null) { closePatternModal(); return; }
  if (!_modalPatId) return;
  if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
    const list = window._patternList || [];
    const idx  = window._patternListIdx || {};
    const cur  = idx[_modalPatId];
    if (cur === undefined) return;
    const next = e.key === 'ArrowRight' ? cur + 1 : cur - 1;
    if (next < 0 || next >= list.length) return;
    openPatternModal(list[next]);
  }
});

document.getElementById('pf-search-btn').addEventListener('click', loadPatterns);
['pf-type','pf-tf','pf-dir'].forEach(id => {
  document.getElementById(id).addEventListener('change', loadPatterns);
});

