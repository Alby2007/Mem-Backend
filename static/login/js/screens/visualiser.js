// ── KB VISUALISER ─────────────────────────────────────────────────────────────
let _visData      = null;
let _visView      = 'bubble';
let _visFilter    = '';   // 'bullish' | 'bearish' | 'neutral' | ''
let _visSearch    = '';
let _visSimActive = false;

// ── Sector normalisation (mirrors backend) ────────────────────────────────────
const _VIS_SECTOR_NORM = {
  'financial services': 'Financial Services', 'financial_services': 'Financial Services',
  'financials': 'Financial Services', 'financial': 'Financial Services',
  'technology': 'Technology', 'information technology': 'Technology', 'tech': 'Technology',
  'healthcare': 'Healthcare', 'health care': 'Healthcare',
  'consumer cyclical': 'Consumer', 'consumer discretionary': 'Consumer',
  'consumer defensive': 'Consumer', 'consumer staples': 'Consumer', 'consumer': 'Consumer',
  'energy': 'Energy',
  'industrials': 'Industrials', 'industrial': 'Industrials',
  'communication services': 'Communication', 'communications': 'Communication', 'communication': 'Communication',
  'real estate': 'Real Estate', 'reits': 'Real Estate',
  'utilities': 'Utilities',
  'basic materials': 'Materials', 'materials': 'Materials',
};
function _visNormSector(raw) {
  if (!raw) return 'Other';
  return _VIS_SECTOR_NORM[(raw || '').toLowerCase()] || raw;
}

// ── Colour helpers ────────────────────────────────────────────────────────────
function _visColour(t) {
  const dir = (t.signal_direction || '').toLowerCase();
  if (dir.includes('bull')) {
    const mc = (t.macro_confirmation || '').toLowerCase();
    const op = mc.includes('confirm') ? 1.0 : mc.includes('partial') ? 0.7 : 0.4;
    return { hex: '#22c55e', opacity: op };
  }
  if (dir.includes('bear')) return { hex: '#ef4444', opacity: 0.9 };
  return { hex: '#6b7280', opacity: 0.7 };
}

function _visRadiusPx(upside) {
  if (!upside || isNaN(upside)) return 10;
  const capped = Math.min(Math.abs(upside), 200);
  return 8 + (capped / 200) * 40;
}

// ── Filtered ticker list ──────────────────────────────────────────────────────
function _visFiltered() {
  if (!_visData) return [];
  return _visData.tickers.filter(t => {
    if (_visFilter) {
      const dir = (t.signal_direction || '').toLowerCase();
      if (_visFilter === 'bullish'  && !dir.includes('bull')) return false;
      if (_visFilter === 'bearish'  && !dir.includes('bear')) return false;
      if (_visFilter === 'neutral'  && (dir.includes('bull') || dir.includes('bear'))) return false;
    }
    if (_visSearch) {
      return (t.ticker || '').toUpperCase().includes(_visSearch.toUpperCase());
    }
    return true;
  });
}

// ── Load + entry point ────────────────────────────────────────────────────────
async function loadVisualiser() {
  _visWireEvents();
  const container = document.getElementById('vis-canvas');
  if (!container) return;

  // Apply sector prefilter from markets screen click
  if (window._visPrefilterSector) {
    _visSearch = '';
    _visFilter = '';
    const searchEl = document.getElementById('vis-search');
    if (searchEl) searchEl.value = '';
    // We'll pass the sector filter through a sector-specific approach after load
  }

  // Treat a cached empty/invalid object as no data — retry
  if (_visData && !_visData.tickers) _visData = null;

  if (!_visData) {
    container.innerHTML = '<div style="color:var(--muted);padding:40px;text-align:center;"><div class="spinner"></div><div style="margin-top:12px;font-size:12px;">Loading KB data…</div></div>';
    let fetched;
    try {
      fetched = await apiFetch('/kb/visualiser');
    } catch(e) {
      container.innerHTML = `<div style="color:#ef4444;padding:40px;text-align:center;">Error: ${escHtml(e.message)}</div>`;
      return;
    }
    if (!fetched || !fetched.tickers) {
      container.innerHTML = `<div style="color:#ef4444;padding:40px;text-align:center;">Failed to load KB data — response: ${escHtml(JSON.stringify(fetched))}</div>`;
      return;
    }
    _visData = fetched;
  }

  // Update last-updated
  const luEl = document.getElementById('vis-last-updated');
  if (luEl && _visData.as_of) {
    const d = new Date(_visData.as_of);
    luEl.textContent = 'Updated: ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  }

  if (window._visPrefilterSector) {
    _setVisView('heatmap');
    window._visPrefilterSector = null;
  } else {
    _setVisView(_visView);
  }
}

function _setVisView(v) {
  _visView = v;
  document.querySelectorAll('.vis-toggle-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.view === v);
  });
  if (v === 'bubble')  _renderBubble();
  if (v === 'heatmap') _renderHeatmap();
  if (v === 'radar')   _renderRadar();
}

// ── VIEW A — Bubble Map ───────────────────────────────────────────────────────
function _renderBubble() {
  const container = document.getElementById('vis-canvas');
  if (!container || !_visData) return;
  container.innerHTML = '';

  // clientWidth/Height can be 0 if parent is scroll-sized — fall back progressively
  const mainEl = document.getElementById('main');
  const W = container.clientWidth  || (mainEl ? mainEl.clientWidth  : 0) || (window.innerWidth  - 184);
  const H = container.clientHeight || (mainEl ? mainEl.clientHeight - 80 : 0) || (window.innerHeight - 128);

  // DEBUG: show dimensions
  const _dbg = document.createElement('div');
  _dbg.id = 'vis-debug';
  _dbg.style.cssText = 'position:absolute;top:4px;right:4px;background:#1a1a2a;color:#f59e0b;font-size:10px;padding:4px 8px;border-radius:4px;z-index:10;font-family:monospace;';
  _dbg.textContent = `W=${W} H=${H} canvas=${container.clientWidth}x${container.clientHeight}`;
  container.appendChild(_dbg);

  const tickers = _visFiltered();
  if (!tickers.length) {
    container.innerHTML = '<div style="color:var(--muted);padding:40px;text-align:center;">No tickers match current filters.</div>';
    return;
  }

  // Group by sector for cluster centres
  const bySector = {};
  tickers.forEach(t => {
    const s = _visNormSector(t.sector);
    if (!bySector[s]) bySector[s] = [];
    bySector[s].push(t);
  });
  const sectors = Object.keys(bySector);
  const nSectors = sectors.length;

  // Compute sector cluster centres in a grid
  const cols = Math.ceil(Math.sqrt(nSectors));
  const rows = Math.ceil(nSectors / cols);
  const sectorCx = {};
  const sectorCy = {};
  sectors.forEach((s, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    sectorCx[s] = (col + 0.5) / cols * W;
    sectorCy[s] = (row + 0.5) / rows * H;
  });

  // Build D3 nodes
  const nodes = tickers.map(t => {
    const s = _visNormSector(t.sector);
    const c = _visColour(t);
    return {
      ticker: t.ticker, sector: s, upside: t.upside_pct,
      r: _visRadiusPx(t.upside_pct),
      fill: c.hex, opacity: c.opacity,
      signal: t.signal_direction || 'neutral',
      macro: t.macro_confirmation || '',
      vol: t.volatility_regime || '',
      r1m: t.return_1m, r1y: t.return_1y,
      x: sectorCx[s] + (Math.random() - 0.5) * 60,
      y: sectorCy[s] + (Math.random() - 0.5) * 60,
    };
  });

  // SVG
  const svg = d3.select(container).append('svg')
    .attr('width', W).attr('height', H)
    .style('display', 'block').style('background', 'transparent');

  // Sector labels
  sectors.forEach(s => {
    svg.append('text')
      .attr('x', sectorCx[s]).attr('y', sectorCy[s] - H / rows / 2 + 18)
      .attr('text-anchor', 'middle')
      .attr('fill', '#f59e0b88')
      .attr('font-size', '10px')
      .attr('font-family', 'var(--mono, monospace)')
      .attr('letter-spacing', '0.08em')
      .text(s.toUpperCase());
  });

  // Circles group
  const node = svg.append('g').selectAll('g')
    .data(nodes).join('g')
    .style('cursor', 'pointer')
    .on('click', (event, d) => {
      navigate('chat');
      setTimeout(() => {
        const inp = document.getElementById('chat-input');
        if (inp) {
          inp.value = `Tell me about ${d.ticker}`;
          inp.dispatchEvent(new Event('input'));
          document.getElementById('chat-send-btn')?.click();
        }
      }, 150);
    });

  node.append('circle')
    .attr('r', d => d.r)
    .attr('fill', d => d.fill)
    .attr('opacity', d => d.opacity)
    .attr('stroke', d => d.fill)
    .attr('stroke-width', 0.5)
    .attr('stroke-opacity', 0.5);

  node.filter(d => d.r > 16)
    .append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', '0.35em')
    .attr('fill', '#fff')
    .attr('font-size', d => Math.min(d.r * 0.55, 12) + 'px')
    .attr('font-family', 'var(--mono, monospace)')
    .attr('font-weight', '700')
    .attr('pointer-events', 'none')
    .text(d => d.ticker.length > 5 ? d.ticker.slice(0, 5) : d.ticker);

  // Tooltip
  const tooltip = d3.select(container).append('div')
    .attr('class', 'vis-tooltip')
    .style('display', 'none');

  node.on('mouseover', (event, d) => {
    const up = d.upside != null ? d.upside.toFixed(2) + '%' : '—';
    const r1m = d.r1m != null ? parseFloat(d.r1m).toFixed(2) + '%' : '—';
    const r1y = d.r1y != null ? parseFloat(d.r1y).toFixed(2) + '%' : '—';
    tooltip.style('display', 'block')
      .html(`<div class="vis-tt-ticker">${escHtml(d.ticker)} <span class="vis-tt-sector">· ${escHtml(d.sector)}</span></div>
<div class="vis-tt-row"><span>Upside</span><span>${escHtml(up)}</span></div>
<div class="vis-tt-row"><span>Signal</span><span>${escHtml(d.signal)}</span></div>
<div class="vis-tt-row"><span>Macro</span><span>${escHtml(d.macro || '—')}</span></div>
<div class="vis-tt-row"><span>Vol</span><span>${escHtml(d.vol || '—')}</span></div>
<div class="vis-tt-row"><span>Return 1m</span><span>${escHtml(r1m)}</span></div>
<div class="vis-tt-row"><span>Return 1y</span><span>${escHtml(r1y)}</span></div>
<div class="vis-tt-hint">Click to discuss in Chat</div>`);
  })
  .on('mousemove', (event) => {
    const rect = container.getBoundingClientRect();
    let x = event.clientX - rect.left + 14;
    let y = event.clientY - rect.top - 10;
    if (x + 180 > W) x = event.clientX - rect.left - 195;
    tooltip.style('left', x + 'px').style('top', y + 'px');
  })
  .on('mouseout', () => tooltip.style('display', 'none'));

  // Run simulation synchronously — no rAF/tick callbacks needed, renders instantly
  d3.forceSimulation(nodes)
    .force('x', d3.forceX(d => sectorCx[d.sector]).strength(0.12))
    .force('y', d3.forceY(d => sectorCy[d.sector]).strength(0.12))
    .force('collide', d3.forceCollide(d => d.r + 2).strength(0.9))
    .force('charge', d3.forceManyBody().strength(-8))
    .stop()
    .tick(120);

  // Apply final positions in one synchronous DOM update
  node.attr('transform', d => `translate(${
    Math.max(d.r, Math.min(W - d.r, d.x))
  },${
    Math.max(d.r, Math.min(H - d.r, d.y))
  })`);
}

// ── VIEW B — Sector Heatmap ───────────────────────────────────────────────────
function _renderHeatmap() {
  const container = document.getElementById('vis-canvas');
  if (!container || !_visData) return;
  container.innerHTML = '';

  const stats = _visData.sector_stats || {};
  const entries = Object.entries(stats).sort((a, b) => b[1].count - a[1].count);

  if (!entries.length) {
    container.innerHTML = '<div style="color:var(--muted);padding:40px;text-align:center;">No sector data available.</div>';
    return;
  }

  const maxUpside = Math.max(...entries.map(([, s]) => s.avg_upside || 0));

  const grid = document.createElement('div');
  grid.className = 'vis-heatmap-grid';
  container.appendChild(grid);

  entries.forEach(([name, s]) => {
    const intensity = maxUpside > 0 ? (s.avg_upside || 0) / maxUpside : 0;
    const r = Math.round(26  + intensity * 48);
    const g = Math.round(26  + intensity * 90);
    const b = Math.round(26  + intensity * 14);
    const bg = `rgb(${r},${g},${b})`;
    const bullClr = s.bullish_pct >= 60 ? '#22c55e' : s.bullish_pct >= 40 ? '#f59e0b' : '#6b7280';

    const tile = document.createElement('div');
    tile.className = 'vis-heatmap-tile';
    tile.style.background = bg;
    tile.innerHTML = `
      <div class="vis-ht-name">${escHtml(name)}</div>
      <div class="vis-ht-count">${s.count} tickers</div>
      <div class="vis-ht-upside" style="color:#f59e0b">${s.avg_upside != null ? 'Avg +' + s.avg_upside + '%' : '—'}</div>
      <div class="vis-ht-bull" style="color:${bullClr}">${s.bullish_pct}% bullish</div>
      <div class="vis-ht-top">${(s.top || []).join(' · ')}</div>`;
    grid.appendChild(tile);
  });
}

// ── VIEW C — Signal Radar ─────────────────────────────────────────────────────
function _renderRadar() {
  const container = document.getElementById('vis-canvas');
  if (!container || !_visData) return;
  container.innerHTML = '';

  const stats = _visData.sector_stats || {};
  const entries = Object.entries(stats)
    .filter(([, s]) => s.count >= 2)
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 8);

  if (entries.length < 3) {
    container.innerHTML = '<div style="color:var(--muted);padding:40px;text-align:center;">Not enough sector data for radar chart.</div>';
    return;
  }

  const W = Math.min(container.clientWidth || 600, 600);
  const H = Math.min(container.clientHeight || 500, 500);
  const cx = W / 2, cy = H / 2;
  const R  = Math.min(W, H) / 2 - 60;
  const n  = entries.length;

  const svg = d3.select(container).append('svg')
    .attr('width', W).attr('height', H)
    .style('display', 'block').style('margin', '0 auto');

  // Draw grid rings
  [0.25, 0.5, 0.75, 1.0].forEach(frac => {
    const pts = entries.map((_, i) => {
      const angle = (i / n) * 2 * Math.PI - Math.PI / 2;
      return [cx + Math.cos(angle) * R * frac, cy + Math.sin(angle) * R * frac];
    });
    svg.append('polygon')
      .attr('points', pts.map(p => p.join(',')).join(' '))
      .attr('fill', 'none')
      .attr('stroke', '#2a2a2a')
      .attr('stroke-width', 1);
  });

  // Draw axes
  entries.forEach((_, i) => {
    const angle = (i / n) * 2 * Math.PI - Math.PI / 2;
    svg.append('line')
      .attr('x1', cx).attr('y1', cy)
      .attr('x2', cx + Math.cos(angle) * R)
      .attr('y2', cy + Math.sin(angle) * R)
      .attr('stroke', '#333').attr('stroke-width', 1);
  });

  // Axis labels
  entries.forEach(([name], i) => {
    const angle = (i / n) * 2 * Math.PI - Math.PI / 2;
    const lx = cx + Math.cos(angle) * (R + 22);
    const ly = cy + Math.sin(angle) * (R + 22);
    svg.append('text')
      .attr('x', lx).attr('y', ly)
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'middle')
      .attr('fill', '#6b7280')
      .attr('font-size', '9px')
      .attr('font-family', 'var(--mono, monospace)')
      .text(name.length > 12 ? name.slice(0, 11) + '…' : name);
  });

  // Helper: polygon from values (0–100)
  function _polyPts(vals) {
    return vals.map((v, i) => {
      const frac = Math.max(0, Math.min(v / 100, 1));
      const angle = (i / n) * 2 * Math.PI - Math.PI / 2;
      return [cx + Math.cos(angle) * R * frac, cy + Math.sin(angle) * R * frac];
    });
  }

  // All-tickers polygon (amber outline) — avg upside normalised
  const maxUpside = Math.max(...entries.map(([, s]) => s.avg_upside || 0), 1);
  const allVals = entries.map(([, s]) => ((s.avg_upside || 0) / maxUpside) * 100);
  const allPts  = _polyPts(allVals);
  svg.append('polygon')
    .attr('points', allPts.map(p => p.join(',')).join(' '))
    .attr('fill', '#f59e0b22')
    .attr('stroke', '#f59e0b')
    .attr('stroke-width', 1.5);

  // Bullish polygon (green fill) — bullish_pct
  const bullVals = entries.map(([, s]) => s.bullish_pct || 0);
  const bullPts  = _polyPts(bullVals);
  svg.append('polygon')
    .attr('points', bullPts.map(p => p.join(',')).join(' '))
    .attr('fill', '#22c55e22')
    .attr('stroke', '#22c55e')
    .attr('stroke-width', 1.5);

  // Legend
  const leg = svg.append('g').attr('transform', `translate(${W - 140}, ${H - 52})`);
  leg.append('line').attr('x1', 0).attr('y1', 0).attr('x2', 16).attr('y2', 0)
    .attr('stroke', '#f59e0b').attr('stroke-width', 1.5);
  leg.append('text').attr('x', 20).attr('y', 4).attr('fill', '#6b7280')
    .attr('font-size', '9px').text('Avg upside');
  leg.append('line').attr('x1', 0).attr('y1', 16).attr('x2', 16).attr('y2', 16)
    .attr('stroke', '#22c55e').attr('stroke-width', 1.5);
  leg.append('text').attr('x', 20).attr('y', 20).attr('fill', '#6b7280')
    .attr('font-size', '9px').text('% bullish');
}

// ── Event wiring (lazy, runs once after screen first shown) ──────────────────
let _visWired = false;
function _visWireEvents() {
  if (_visWired) return;
  _visWired = true;

  document.querySelectorAll('.vis-toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => _setVisView(btn.dataset.view));
  });

  document.querySelectorAll('.vis-pill').forEach(pill => {
    pill.addEventListener('click', () => {
      const v = pill.dataset.filter;
      _visFilter = (_visFilter === v) ? '' : v;
      document.querySelectorAll('.vis-pill').forEach(p => p.classList.toggle('active', p.dataset.filter === _visFilter));
      _setVisView(_visView);
    });
  });

  const searchEl = document.getElementById('vis-search');
  if (searchEl) {
    searchEl.addEventListener('input', () => {
      _visSearch = searchEl.value.trim();
      if (_visView === 'bubble') _renderBubble();
    });
  }
}
