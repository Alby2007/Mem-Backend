// ── PAPER TRADER — Evolutionary Strategy Ecosystem ───────────────────────────
// Three views: Fleet Overview (default) | Bot Detail (click a row) | Evolution Timeline

let _ptPollTimer   = null;
let _ptCurrentView = 'fleet';   // 'fleet' | 'bot' | 'timeline'
let _ptCurrentBot  = null;      // bot object when in detail view
let _ptFleetData   = null;      // last fleet-performance response

async function loadPaperTrader() {
  if (!state.userId) return;
  await new Promise(r => setTimeout(r, 50));
  _ptRenderShell();
  _ptSwitchView('fleet');
  if (_ptPollTimer) clearInterval(_ptPollTimer);
  _ptPollTimer = setInterval(() => {
    if (document.getElementById('screen-paper')?.classList.contains('active')) {
      if (_ptCurrentView === 'fleet')    _ptLoadFleet();
      else if (_ptCurrentView === 'bot') _ptRefreshBotDetail();
    } else {
      clearInterval(_ptPollTimer);
      _ptPollTimer = null;
    }
  }, 15000);
}

// ── Shell renderer — injects the three-view structure into #screen-paper ──────

function _ptRenderShell() {
  const sc = document.getElementById('screen-paper');
  if (!sc) return;
  sc.innerHTML = `
    <div id="pt-shell" style="max-width:900px;margin:0 auto;padding:16px;">
      <!-- top nav tabs -->
      <div style="display:flex;gap:8px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:10px;">
        <button id="pt-tab-fleet"    class="btn btn-ghost btn-sm pt-nav-tab active" onclick="_ptSwitchView('fleet')">⚡ Fleet</button>
        <button id="pt-tab-timeline" class="btn btn-ghost btn-sm pt-nav-tab"        onclick="_ptSwitchView('timeline')">📈 Timeline</button>
        <div style="flex:1;"></div>
        <button id="pt-generalist-toggle" class="btn btn-ghost btn-sm" onclick="_ptToggleGeneralist(this)">▾ Generalist</button>
      </div>
      <!-- view containers -->
      <div id="pt-view-fleet"></div>
      <div id="pt-view-bot"      style="display:none;"></div>
      <div id="pt-view-timeline" style="display:none;"></div>
      <!-- generalist panel (collapsible) -->
      <div id="pt-generalist-panel" style="margin-top:16px;display:none;">
        <div class="evo-section-header">GENERALIST AGENT</div>
        <div id="pt-generalist-body"></div>
      </div>
    </div>`;
}

function _ptSwitchView(view) {
  _ptCurrentView = view;
  ['fleet','bot','timeline'].forEach(v => {
    const el = document.getElementById(`pt-view-${v}`);
    if (el) el.style.display = v === view ? '' : 'none';
  });
  document.querySelectorAll('.pt-nav-tab').forEach(b => b.classList.remove('active'));
  const active = document.getElementById(`pt-tab-${view === 'bot' ? 'fleet' : view}`);
  if (active) active.classList.add('active');
  if (view === 'fleet')    _ptLoadFleet();
  if (view === 'timeline') _ptLoadTimeline();
}

function _ptToggleGeneralist(btn) {
  const panel = document.getElementById('pt-generalist-panel');
  if (!panel) return;
  const visible = panel.style.display !== 'none';
  panel.style.display = visible ? 'none' : '';
  btn.textContent = visible ? '▾ Generalist' : '▴ Generalist';
  if (!visible) _ptLoadGeneralist();
}

// ── VIEW 1: Fleet Overview ────────────────────────────────────────────────────

async function _ptLoadFleet() {
  const el = document.getElementById('pt-view-fleet');
  if (!el || !state.userId) return;
  el.innerHTML = '<div class="evo-loading">Loading fleet…</div>';
  try {
    const [fleet, discoveries, acct] = await Promise.all([
      apiFetch(`/users/${state.userId}/bots/fleet-performance`),
      apiFetch(`/users/${state.userId}/bots/discoveries`).catch(() => null),
      apiFetch(`/users/${state.userId}/paper/account`).catch(() => null),
    ]);
    _ptFleetData = fleet;
    if (acct?.account_size_set === false) { _ptShowOnboarding(); return; }
    el.innerHTML = _ptRenderFleetHTML(fleet, discoveries);
  } catch(e) {
    if (e.message?.includes('paper_trading_requires_pro')) { _ptShowUpsell(); return; }
    el.innerHTML = `<div style="color:var(--red);padding:20px;">${escHtml(e.message)}</div>`;
  }
}

function _ptRenderFleetHTML(fleet, disc) {
  const bots      = fleet?.bots || [];
  const totalEq   = fleet?.total_equity || 0;
  const initEq    = fleet?.initial_equity || 0;
  const retPct    = fleet?.return_pct ?? 0;
  const trades    = fleet?.total_trades || 0;
  const gen       = fleet?.generation || 0;
  const sign      = retPct >= 0 ? '+' : '';
  const retCol    = retPct >= 0 ? 'var(--green)' : 'var(--red)';
  const fmtK      = v => v >= 1000 ? '£' + (v/1000).toFixed(1) + 'k' : '£' + (v||0).toFixed(0);

  // Fleet equity sparkline (sum of all bot sparklines last 6 pts)
  const sparkData = _ptFleetSparkline(bots);
  const sparkSvg  = sparkData.length >= 2 ? _ptMiniSpark(sparkData, 300, 60, retPct >= 0) : '';

  let html = `
    <div class="evo-fleet-header">
      <div>
        <span class="evo-title">STRATEGY ECOSYSTEM</span>
        <span class="evo-gen-pill">Gen ${gen}</span>
        <span style="color:var(--muted);font-size:12px;margin-left:8px;">${bots.filter(b=>b.active).length} active</span>
      </div>
      <button class="btn btn-primary btn-sm" onclick="_ptEvolveNow(this)">⚡ Evolve Now</button>
    </div>
    <div class="evo-fleet-stats">
      <span class="evo-stat-big">${fmtK(totalEq)}</span>
      <span style="color:${retCol};font-size:14px;font-weight:600;">${sign}${retPct.toFixed(1)}%</span>
      <span class="evo-stat-sub">${bots.length} bots · ${trades} trades · Gen ${gen}</span>
    </div>`;

  if (sparkSvg) {
    html += `<div class="evo-fleet-chart">${sparkSvg}</div>`;
  }

  // Leaderboard
  html += `<div class="evo-section-header" style="margin-top:16px;">LEADERBOARD</div>
    <div id="evo-leaderboard">`;
  if (!bots.length) {
    html += '<div style="color:var(--muted);padding:16px;font-size:13px;">No bots yet — set your paper balance to auto-seed the fleet.</div>';
  } else {
    bots.forEach((bot, i) => { html += _ptBotRow(bot, i + 1); });
  }
  html += '</div>';

  // Discoveries panel
  const dlist = disc?.discoveries || [];
  if (dlist.length) {
    html += `<div class="evo-section-header" style="margin-top:20px;">DISCOVERIES</div><div class="evo-discoveries">`;
    dlist.slice(0, 5).forEach(d => {
      const hr   = (d.hit_rate * 100).toFixed(0);
      const stat = d.status === 'active' ? 'var(--green)' : 'var(--red)';
      const mark = d.status === 'active' ? '' : ' ✗';
      html += `<div class="evo-discovery-row">
        <span style="color:${stat};">"${escHtml(d.pattern_type)}${d.sector?' + '+escHtml(d.sector):''} → ${hr}% hit rate (${d.sample_size} samples, ${escHtml(d.discovered_by||'?')})${mark}"</span>
      </div>`;
    });
    html += `<div style="color:var(--muted);font-size:11px;margin-top:6px;">Fleet total: ${disc.total_observations||0} calibration observations across ${disc.unique_cells_tested||0} cells</div>`;
    html += '</div>';
  }

  // Add manual bot button
  html += `<div style="margin-top:16px;">
    <button class="btn btn-ghost btn-sm" onclick="_ptShowManualBotModal()">+ Manual Bot</button>
  </div>`;

  return html;
}

function _ptBotRow(bot, rank) {
  const tier    = _ptClassifyTier(bot);
  const icon    = tier.icon;
  const tierCls = `evo-tier-${tier.key}`;
  const retPct  = bot.return_pct ?? 0;
  const retSign = retPct >= 0 ? '+' : '';
  const retCol  = retPct >= 0 ? 'var(--green)' : 'var(--red)';
  const wr      = bot.win_rate != null ? (bot.win_rate * 100).toFixed(0) + '%' : '—';
  const avgr    = bot.avg_r != null ? (bot.avg_r >= 0 ? '+' : '') + bot.avg_r.toFixed(1) + 'R' : '—';
  const spark   = (bot.sparkline && bot.sparkline.length >= 2) ? _ptMiniSpark(bot.sparkline, 60, 20, retPct >= 0) : '';
  const killed  = !bot.active;
  const style   = killed ? 'opacity:0.5;' : 'cursor:pointer;';
  const onclick = killed ? '' : `onclick="_ptOpenBotDetail('${escHtml(bot.bot_id)}')"`;

  // Find spawned child if killed (from fleet data)
  let spawnNote = '';
  if (killed && _ptFleetData) {
    const child = (_ptFleetData.bots || []).find(b => b.parent_id === bot.bot_id || (b.strategy_name || '').includes(bot.strategy_name?.slice(0,8) || '~~~'));
    if (child) spawnNote = `<div style="color:var(--muted);font-size:10px;padding-left:28px;">→ spawned mutant: ${escHtml(child.strategy_name)}</div>`;
  }

  // Gene tags
  let geneTags = '';
  try {
    const pts = bot.pattern_types ? JSON.parse(bot.pattern_types) : null;
    if (pts?.length) geneTags += pts.slice(0,3).map(p => `<span class="evo-genome-tag">${escHtml(p)}</span>`).join('');
  } catch(e) {}

  return `<div class="evo-leaderboard-row ${tierCls}" style="${style}" ${onclick}>
    <div style="display:flex;align-items:center;gap:8px;">
      <span style="font-size:16px;width:20px;flex-shrink:0;">${icon}</span>
      <span style="color:var(--muted);font-size:11px;width:16px;flex-shrink:0;">${rank}.</span>
      <span class="evo-bot-name">${escHtml(bot.strategy_name || bot.bot_id?.slice(0,10) || '?')}</span>
      <span class="evo-gen-pill">G${bot.generation||0}</span>
      <span style="color:${retCol};font-weight:700;font-size:13px;">${retSign}${retPct.toFixed(1)}%</span>
      <span style="color:var(--muted);font-size:12px;">${wr} WR</span>
      <span style="color:var(--muted);font-size:12px;">${avgr}</span>
      <span class="evo-tier-badge ${tierCls}">${tier.label}</span>
      <span style="flex:1;"></span>
      ${spark ? `<span class="evo-sparkline">${spark}</span>` : ''}
      <span style="color:var(--muted);font-size:11px;">${bot.total_closed||0} trades</span>
    </div>
    <div style="padding-left:36px;margin-top:2px;">${geneTags}</div>
    ${spawnNote}
  </div>`;
}

function _ptClassifyTier(bot) {
  if (!bot.active) return { key: 'killed', icon: '☠️', label: 'KILLED' };
  const total  = bot.total_closed || 0;
  const minEval = 25;
  const fitness = bot.fitness ?? 0;
  const maxDD   = bot.max_drawdown_pct ?? 0;
  if (total < minEval) return { key: 'immature', icon: '🟡', label: 'IMMATURE' };
  if (fitness <= 0 || maxDD > 0.40) return { key: 'failing', icon: '🔴', label: 'FAILING' };
  if (bot.role === 'exploit') return { key: 'elite', icon: '🟢', label: 'ELITE' };
  return { key: 'viable', icon: '🟡', label: 'VIABLE' };
}

async function _ptEvolveNow(btn) {
  btn.disabled = true;
  btn.textContent = '⟳ Evolving…';
  try {
    const r = await apiFetch(`/users/${state.userId}/bots/evolve-now`, { method: 'POST' });
    showToast(`Evolution cycle complete: ${r.killed||0} killed, ${r.spawned||0} spawned`, 'ok');
    _ptLoadFleet();
  } catch(e) { showToast(e.message || 'Evolution failed', 'error'); }
  finally { btn.disabled = false; btn.textContent = '⚡ Evolve Now'; }
}

// ── VIEW 2: Bot Detail ────────────────────────────────────────────────────────

async function _ptOpenBotDetail(botId) {
  const bots = _ptFleetData?.bots || [];
  _ptCurrentBot = bots.find(b => b.bot_id === botId) || { bot_id: botId };
  _ptCurrentView = 'bot';
  ['fleet','timeline'].forEach(v => {
    const el = document.getElementById(`pt-view-${v}`);
    if (el) el.style.display = 'none';
  });
  const el = document.getElementById('pt-view-bot');
  if (el) { el.style.display = ''; el.innerHTML = '<div class="evo-loading">Loading bot…</div>'; }
  await _ptRefreshBotDetail();
}

async function _ptRefreshBotDetail() {
  if (!_ptCurrentBot) return;
  const bot   = _ptCurrentBot;
  const botId = bot.bot_id;
  const el    = document.getElementById('pt-view-bot');
  if (!el) return;
  try {
    const [perf, equity, positions, log, fleetBots] = await Promise.all([
      apiFetch(`/users/${state.userId}/bots/fleet-performance`).then(d => (d?.bots||[]).find(b=>b.bot_id===botId) || bot),
      apiFetch(`/users/${state.userId}/bots/${botId}/equity?days=90`),
      apiFetch(`/users/${state.userId}/bots/${botId}/positions`),
      apiFetch(`/users/${state.userId}/bots/${botId}/log?limit=20`),
      apiFetch(`/users/${state.userId}/bots/fleet-performance`).then(d => d?.bots||[]).catch(()=>[]),
    ]);
    _ptCurrentBot = perf;
    el.innerHTML = _ptRenderBotDetailHTML(perf, equity, positions, log, fleetBots);
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);padding:20px;">${escHtml(e.message)}</div>`;
  }
}

function _ptRenderBotDetailHTML(bot, equity, positions, log, fleetBots) {
  const tier    = _ptClassifyTier(bot);
  const retPct  = bot.return_pct ?? 0;
  const retSign = retPct >= 0 ? '+' : '';
  const retCol  = retPct >= 0 ? 'var(--green)' : 'var(--red)';
  const fmtK    = v => v >= 1000 ? '£' + (v/1000).toFixed(1) + 'k' : '£' + (v||0).toFixed(0);

  // Equity chart
  const eqs  = (equity?.equity || []);
  const eqSvg = eqs.length >= 2 ? _ptEquityCurve(eqs.map(r=>r.equity_value), 680, 100) : '<div style="color:var(--muted);font-size:12px;">No equity data yet</div>';

  // Genome tags
  const genomeRows = _ptGenotypeRows(bot);

  // Positions
  const openPos    = (positions?.positions || []).filter(p => p.status === 'open');
  const closedPos  = (positions?.positions || []).filter(p => p.status !== 'open').slice(0, 10);

  // Lineage
  const parent   = fleetBots.find(b => b.bot_id === bot.parent_id);
  const children = fleetBots.filter(b => b.parent_id === bot.bot_id);

  // Log lines
  const logLines = (log?.log || []).map(r => _ptFeedLine(r)).join('');

  return `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
      <button class="btn btn-ghost btn-sm" onclick="_ptSwitchView('fleet')">← Fleet</button>
      <span class="evo-title">${escHtml(bot.strategy_name||bot.bot_id||'')}</span>
      <span class="evo-tier-badge evo-tier-${tier.key}">${tier.label}</span>
      <span class="evo-gen-pill">Gen ${bot.generation||0}</span>
      <div style="flex:1;"></div>
      ${bot.active ? `
        <button class="btn btn-ghost btn-sm" onclick="_ptBotPause('${escHtml(bot.bot_id)}',this)">⏸ Pause</button>
        <button class="btn btn-ghost btn-sm" onclick="_ptBotSizeModal('${escHtml(bot.bot_id)}')">✏ Size</button>
      ` : `<button class="btn btn-ghost btn-sm" onclick="_ptBotResume('${escHtml(bot.bot_id)}',this)">▶ Resume</button>`}
    </div>

    <div class="evo-detail-grid">
      <div class="evo-detail-card">
        <div class="evo-section-header">GENOME</div>
        ${genomeRows}
        <div style="margin-top:6px;font-size:11px;color:var(--muted);">Parent: ${parent ? escHtml(parent.strategy_name) : (bot.parent_id ? bot.parent_id.slice(0,10) : 'none (seed)')}</div>
      </div>
      <div class="evo-detail-card">
        <div class="evo-section-header">PERFORMANCE</div>
        <div class="evo-perf-grid">
          <div><span class="evo-stat-label">Balance</span><span class="evo-stat-val" style="color:${retCol};">${fmtK(bot.virtual_balance||0)} (${retSign}${retPct.toFixed(1)}%)</span></div>
          <div><span class="evo-stat-label">Trades</span><span class="evo-stat-val">${bot.total_closed||0}</span></div>
          <div><span class="evo-stat-label">Win Rate</span><span class="evo-stat-val">${bot.win_rate!=null?(bot.win_rate*100).toFixed(0)+'%':'—'}</span></div>
          <div><span class="evo-stat-label">Avg R</span><span class="evo-stat-val">${bot.avg_r!=null?bot.avg_r.toFixed(2)+'R':'—'}</span></div>
          <div><span class="evo-stat-label">Sharpe</span><span class="evo-stat-val">${bot.sharpe!=null?bot.sharpe.toFixed(2):'—'}</span></div>
          <div><span class="evo-stat-label">Max DD</span><span class="evo-stat-val">${bot.max_drawdown_pct!=null?(bot.max_drawdown_pct*100).toFixed(1)+'%':'—'}</span></div>
          <div><span class="evo-stat-label">Profit Factor</span><span class="evo-stat-val">${bot.profit_factor!=null?bot.profit_factor.toFixed(2):'—'}</span></div>
          <div><span class="evo-stat-label">Fitness</span><span class="evo-stat-val">${bot.fitness!=null?bot.fitness.toFixed(3):'—'}</span></div>
        </div>
      </div>
    </div>

    <div class="evo-section-header" style="margin-top:16px;">EQUITY CURVE</div>
    <div class="evo-equity-chart">${eqSvg}</div>

    <div class="evo-section-header" style="margin-top:16px;">OPEN POSITIONS (${openPos.length})</div>
    ${openPos.length ? openPos.map(p => _ptPositionRow(p)).join('') : '<div style="color:var(--muted);font-size:12px;padding:8px;">No open positions</div>'}

    <div class="evo-section-header" style="margin-top:16px;">CLOSED TRADES (last 10)</div>
    ${closedPos.length ? closedPos.map(p => _ptClosedRow(p)).join('') : '<div style="color:var(--muted);font-size:12px;padding:8px;">No closed trades yet</div>'}

    <div class="evo-section-header" style="margin-top:16px;">LINEAGE</div>
    <div style="font-size:12px;color:var(--muted);padding:6px 0;">
      ${children.length ? 'Spawned: ' + children.map(c=>`<span class="evo-genome-tag">${escHtml(c.strategy_name)} (G${c.generation})</span>`).join(' ') : 'No children yet'}
    </div>

    <div class="evo-section-header" style="margin-top:16px;">AGENT LOG (last 20)</div>
    <div class="pt-feed">${logLines || '<span style="color:var(--muted);">No activity yet</span>'}</div>`;
}

function _ptGenotypeRows(bot) {
  const rows = [];
  try {
    const pts = bot.pattern_types ? JSON.parse(bot.pattern_types) : null;
    if (pts) rows.push(`<div><span class="evo-genome-label">Patterns</span> ${pts.map(p=>`<span class="evo-genome-tag">${escHtml(p)}</span>`).join('')}</div>`);
    const secs = bot.sectors ? JSON.parse(bot.sectors) : null;
    if (secs) rows.push(`<div><span class="evo-genome-label">Sectors</span> ${secs.map(s=>`<span class="evo-genome-tag">${escHtml(s)}</span>`).join('')}</div>`);
    else rows.push(`<div><span class="evo-genome-label">Sectors</span> <span style="color:var(--muted);">all</span></div>`);
    const vols = bot.volatility ? JSON.parse(bot.volatility) : null;
    if (vols) rows.push(`<div><span class="evo-genome-label">Volatility</span> ${vols.map(v=>`<span class="evo-genome-tag">${escHtml(v)}</span>`).join('')}</div>`);
    else rows.push(`<div><span class="evo-genome-label">Volatility</span> <span style="color:var(--muted);">all</span></div>`);
    rows.push(`<div><span class="evo-genome-label">Direction</span> <span style="color:var(--muted);">${bot.direction_bias||'both'}</span></div>`);
    rows.push(`<div><span class="evo-genome-label">Quality ≥</span> <span>${bot.min_quality||0.65}</span> &nbsp; <span class="evo-genome-label">Risk</span> <span>${bot.risk_pct||1.0}%</span> &nbsp; <span class="evo-genome-label">Max pos</span> <span>${bot.max_positions||4}</span></div>`);
  } catch(e) {}
  return rows.join('') || '<div style="color:var(--muted);">No genome data</div>';
}

function _ptPositionRow(p) {
  const dirCol = p.direction === 'bullish' ? 'var(--green)' : 'var(--red)';
  const unr    = p.unrealised_pnl_r;
  const unrStr = unr != null ? (unr >= 0 ? '+' : '') + unr + 'R' : '';
  return `<div class="evo-pos-row">
    <span style="color:var(--accent);font-weight:700;">${escHtml(p.ticker)}</span>
    <span style="color:${dirCol};">${p.direction}</span>
    <span class="mono">entry=${_ptFmt(p.entry_price)} stop=${_ptFmt(p.stop)} t1=${_ptFmt(p.t1)}</span>
    ${unrStr ? `<span style="color:${unr>=0?'var(--green)':'var(--red)'};">${unrStr}</span>` : ''}
  </div>`;
}

function _ptClosedRow(p) {
  const pnl    = p.pnl_r;
  const pnlStr = pnl != null ? (pnl >= 0 ? '+' : '') + pnl + 'R' : '—';
  const pnlCol = pnl != null ? (pnl >= 0 ? 'var(--green)' : 'var(--red)') : 'var(--muted)';
  const status = (p.status || '').replace(/_/g,' ');
  const dur    = p.opened_at && p.closed_at ? _ptDuration(p.opened_at, p.closed_at) : '';
  return `<div class="evo-pos-row">
    <span style="color:var(--accent);font-weight:700;">${escHtml(p.ticker)}</span>
    <span style="color:${pnlCol};font-weight:700;">${pnlStr}</span>
    <span style="color:var(--muted);font-size:11px;">${escHtml(status)}</span>
    ${dur ? `<span style="color:var(--muted);font-size:11px;">${dur}</span>` : ''}
    <span style="color:var(--muted);font-size:10px;">${escHtml(p.pattern_type||'')} ${escHtml(p.direction||'')}</span>
  </div>`;
}

async function _ptBotPause(botId, btn) {
  btn.disabled = true;
  try {
    await apiFetch(`/users/${state.userId}/bots/${botId}/stop`, { method: 'POST' });
    showToast('Bot paused', 'ok');
    _ptRefreshBotDetail();
  } catch(e) { showToast(e.message, 'error'); }
  finally { btn.disabled = false; }
}

async function _ptBotResume(botId, btn) {
  btn.disabled = true;
  try {
    await apiFetch(`/users/${state.userId}/bots/${botId}/start`, { method: 'POST' });
    showToast('Bot resumed', 'ok');
    _ptRefreshBotDetail();
  } catch(e) { showToast(e.message, 'error'); }
  finally { btn.disabled = false; }
}

function _ptBotSizeModal(botId) {
  const bot = _ptCurrentBot;
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
  overlay.innerHTML = `
    <div style="background:var(--card);border:1px solid var(--border);border-radius:10px;padding:24px 28px;max-width:340px;width:90%;">
      <div style="font-size:14px;font-weight:700;margin-bottom:14px;">Edit Sizing Genes</div>
      <div style="display:flex;flex-direction:column;gap:10px;">
        <label style="font-size:12px;color:var(--muted);">Risk % <input id="sz-risk" type="number" step="0.1" min="0.5" max="2.0" value="${bot?.risk_pct||1.0}" class="evo-input"></label>
        <label style="font-size:12px;color:var(--muted);">Max Positions <input id="sz-maxpos" type="number" step="1" min="2" max="6" value="${bot?.max_positions||4}" class="evo-input"></label>
        <label style="font-size:12px;color:var(--muted);">Min Quality <input id="sz-minq" type="number" step="0.01" min="0.55" max="0.80" value="${bot?.min_quality||0.65}" class="evo-input"></label>
      </div>
      <div style="display:flex;gap:8px;margin-top:16px;">
        <button id="sz-confirm" class="btn btn-primary" style="flex:1;">Save</button>
        <button class="btn btn-ghost" onclick="this.closest('[style*=fixed]').remove()">Cancel</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.querySelector('#sz-confirm').addEventListener('click', async () => {
    const risk    = parseFloat(overlay.querySelector('#sz-risk').value);
    const maxpos  = parseInt(overlay.querySelector('#sz-maxpos').value, 10);
    const minq    = parseFloat(overlay.querySelector('#sz-minq').value);
    try {
      await apiFetch(`/users/${state.userId}/bots/${botId}`, {
        method: 'PATCH',
        body: JSON.stringify({ risk_pct: risk, max_positions: maxpos, min_quality: minq }),
      });
      showToast('Sizing updated', 'ok');
      overlay.remove();
      _ptRefreshBotDetail();
    } catch(e) { showToast(e.message, 'error'); }
  });
}

// ── VIEW 3: Evolution Timeline ────────────────────────────────────────────────

async function _ptLoadTimeline() {
  const el = document.getElementById('pt-view-timeline');
  if (!el || !state.userId) return;
  el.innerHTML = '<div class="evo-loading">Loading timeline…</div>';
  try {
    const data = await apiFetch(`/users/${state.userId}/bots/evolution-history`);
    el.innerHTML = _ptRenderTimeline(data?.events || []);
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);padding:20px;">${escHtml(e.message)}</div>`;
  }
}

function _ptRenderTimeline(events) {
  if (!events.length) return '<div style="color:var(--muted);padding:20px;">No evolutionary events yet — evolution runs every 6 hours.</div>';

  // Group by generation descending
  const byGen = {};
  events.forEach(e => {
    const g = e.generation ?? 0;
    if (!byGen[g]) byGen[g] = [];
    byGen[g].push(e);
  });

  const gens = Object.keys(byGen).map(Number).sort((a,b) => b-a);
  const _evoIcon = t => ({
    evolution_elite:    '🟢',
    evolution_viable:   '🟡',
    evolution_kill:     '☠️',
    evolution_spawn:    '🔄',
    evolution_crossover:'✂️',
    evolution_promote:  '⭐',
  }[t] || '·');

  let html = '<div class="evo-section-header">EVOLUTION TIMELINE</div>';
  gens.forEach(gen => {
    const genEvents = byGen[gen];
    const firstTs   = genEvents[genEvents.length-1]?.created_at?.slice(0,10) || '';
    html += `<div class="evo-timeline-gen">
      <div class="evo-timeline-gen-label">Gen ${gen}${firstTs?' <span style="color:var(--muted);font-size:10px;">('+firstTs+')</span>':''}</div>`;
    genEvents.forEach(ev => {
      html += `<div class="evo-timeline-event">
        <span style="font-size:14px;width:20px;flex-shrink:0;">${_evoIcon(ev.event_type)}</span>
        <span style="color:var(--muted);font-size:11px;">${ev.bot_id?.slice(0,8)||''}</span>
        <span style="font-size:12px;">${escHtml(ev.detail||'')}</span>
        <span style="color:var(--muted);font-size:10px;margin-left:auto;">${ev.created_at?.slice(0,16).replace('T',' ')||''}</span>
      </div>`;
    });
    html += '</div>';
  });
  return html;
}

// ── Generalist Panel ──────────────────────────────────────────────────────────

async function _ptLoadGeneralist() {
  const el = document.getElementById('pt-generalist-body');
  if (!el || !state.userId) return;
  try {
    const [acct, equity] = await Promise.all([
      apiFetch(`/users/${state.userId}/paper/account`),
      apiFetch(`/users/${state.userId}/paper/equity?days=90`),
    ]);
    const retPct  = acct?.account_value && acct?.virtual_balance ? ((acct.account_value - (acct.initial_balance || acct.account_value)) / (acct.initial_balance || acct.account_value) * 100).toFixed(1) : '0.0';
    const fmtMoney = v => v >= 1000 ? '£' + (v/1000).toFixed(1) + 'k' : '£' + (v||0).toFixed(0);
    const eqVals   = (equity?.equity||[]).map(r=>r.equity_value);
    const spark    = eqVals.length >= 2 ? _ptMiniSpark(eqVals, 120, 24, true) : '';
    const wr       = acct?.win_rate_pct;
    const avgR     = acct?.avg_r;
    el.innerHTML = `<div style="display:flex;align-items:center;gap:12px;padding:10px;background:var(--card);border:1px solid var(--border);border-radius:8px;">
      <span style="font-size:13px;font-weight:600;">${fmtMoney(acct?.account_value||0)}</span>
      <span style="font-size:11px;color:var(--muted);">${acct?.closed_trades||0} trades</span>
      ${wr != null ? `<span style="color:${wr>=50?'var(--green)':'var(--red)'};">${wr}% WR</span>` : ''}
      ${avgR != null ? `<span style="color:${avgR>=0?'var(--green)':'var(--red)'};">${avgR}R avg</span>` : ''}
      ${spark ? `<span>${spark}</span>` : ''}
      <div style="flex:1;"></div>
      <button class="btn btn-ghost btn-sm" id="gen-start-btn" onclick="_ptGenToggle(this)">▶ Start</button>
    </div>`;
    const d = await apiFetch(`/users/${state.userId}/paper/agent/status`).catch(()=>null);
    const startBtn = document.getElementById('gen-start-btn');
    if (startBtn && d?.running) startBtn.textContent = '■ Stop';
  } catch(e) {}
}

async function _ptGenToggle(btn) {
  const isRunning = btn.textContent.includes('Stop');
  btn.disabled = true;
  try {
    if (isRunning) {
      await apiFetch(`/users/${state.userId}/paper/agent/stop`, { method: 'POST' });
      btn.textContent = '▶ Start';
      showToast('Generalist stopped', 'ok');
    } else {
      await apiFetch(`/users/${state.userId}/paper/agent/start`, { method: 'POST' });
      btn.textContent = '■ Stop';
      showToast('Generalist started', 'ok');
    }
  } catch(e) { showToast(e.message, 'error'); }
  finally { btn.disabled = false; }
}

// ── Manual bot creation modal ─────────────────────────────────────────────────

function _ptShowManualBotModal() {
  if (document.getElementById('pt-manual-bot-modal')) return;
  const overlay = document.createElement('div');
  overlay.id = 'pt-manual-bot-modal';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:9999;display:flex;align-items:center;justify-content:center;';
  overlay.innerHTML = `
    <div style="background:var(--card);border:1px solid var(--border);border-radius:10px;padding:24px 28px;max-width:420px;width:95%;max-height:90vh;overflow-y:auto;">
      <div style="font-size:14px;font-weight:700;margin-bottom:4px;">Manual Bot Override</div>
      <div style="font-size:11px;color:var(--muted);margin-bottom:14px;">This bot won't be subject to evolutionary selection.</div>
      <div style="display:flex;flex-direction:column;gap:10px;">
        <label class="evo-form-row">Name (optional)<input id="mb-name" type="text" placeholder="My Strategy" class="evo-input"></label>
        <label class="evo-form-row">Pattern types (comma-sep)<input id="mb-patterns" type="text" placeholder="fvg,order_block" class="evo-input"></label>
        <label class="evo-form-row">Sectors (comma-sep, blank=all)<input id="mb-sectors" type="text" placeholder="technology,energy" class="evo-input"></label>
        <label class="evo-form-row">Volatility (blank=all)<input id="mb-vol" type="text" placeholder="high,extreme" class="evo-input"></label>
        <label class="evo-form-row">Direction bias<select id="mb-dir" class="evo-input"><option value="">both</option><option value="bullish">bullish</option><option value="bearish">bearish</option></select></label>
        <label class="evo-form-row">Risk % <input id="mb-risk" type="number" step="0.1" min="0.5" max="2.0" value="1.0" class="evo-input"></label>
        <label class="evo-form-row">Max positions <input id="mb-maxpos" type="number" step="1" min="2" max="6" value="4" class="evo-input"></label>
        <label class="evo-form-row">Min quality <input id="mb-minq" type="number" step="0.01" min="0.55" max="0.80" value="0.65" class="evo-input"></label>
        <label class="evo-form-row">Starting balance (£) <input id="mb-bal" type="number" step="500" min="500" value="5000" class="evo-input"></label>
      </div>
      <div style="display:flex;gap:8px;margin-top:16px;">
        <button id="mb-confirm" class="btn btn-primary" style="flex:1;">Create Bot</button>
        <button class="btn btn-ghost" onclick="document.getElementById('pt-manual-bot-modal').remove()">Cancel</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.querySelector('#mb-confirm').addEventListener('click', async () => {
    const toJson = v => { const s=v.trim(); if(!s) return null; return JSON.stringify(s.split(',').map(x=>x.trim()).filter(Boolean)); };
    const genome = {
      strategy_name:  overlay.querySelector('#mb-name').value.trim() || null,
      pattern_types:  toJson(overlay.querySelector('#mb-patterns').value),
      sectors:        toJson(overlay.querySelector('#mb-sectors').value),
      volatility:     toJson(overlay.querySelector('#mb-vol').value),
      direction_bias: overlay.querySelector('#mb-dir').value || null,
      risk_pct:       parseFloat(overlay.querySelector('#mb-risk').value),
      max_positions:  parseInt(overlay.querySelector('#mb-maxpos').value, 10),
      min_quality:    parseFloat(overlay.querySelector('#mb-minq').value),
    };
    const bal = parseFloat(overlay.querySelector('#mb-bal').value);
    try {
      await apiFetch(`/users/${state.userId}/bots`, {
        method: 'POST',
        body: JSON.stringify({ ...genome, virtual_balance: bal }),
      });
      showToast('Manual bot created', 'ok');
      overlay.remove();
      _ptLoadFleet();
    } catch(e) { showToast(e.message||'Error', 'error'); }
  });
}

// ── Onboarding modal ──────────────────────────────────────────────────────────

let _ptOnboardingShown = false;
async function _ptShowOnboarding() {
  if (_ptOnboardingShown || document.getElementById('pt-onboarding-modal')) return;
  _ptOnboardingShown = true;
  const overlay = document.createElement('div');
  overlay.id = 'pt-onboarding-modal';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
  overlay.innerHTML = `
    <div style="background:var(--card);border:1px solid var(--border);border-radius:10px;padding:28px 32px;max-width:400px;width:90%;">
      <div style="font-size:16px;font-weight:700;margin-bottom:6px;">Set your paper account size</div>
      <div style="color:var(--muted);font-size:13px;margin-bottom:8px;">The system will auto-seed 8 strategy bots and split the capital equally across them.</div>
      <div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;">
        <button class="btn btn-ghost btn-sm pt-preset" data-v="5000">£5k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="10000">£10k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="25000">£25k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="50000">£50k</button>
      </div>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:18px;">
        <span style="color:var(--muted);font-size:14px;">£</span>
        <input id="pt-acct-input" type="number" min="1000" max="100000" step="1000" value="10000" class="evo-input" style="flex:1;">
      </div>
      <div style="display:flex;gap:8px;">
        <button id="pt-acct-confirm" class="btn btn-primary" style="flex:1;">Confirm &amp; Seed Fleet</button>
        <button id="pt-acct-dismiss" class="btn btn-ghost">Not now</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.querySelectorAll('.pt-preset').forEach(btn => {
    btn.addEventListener('click', () => { document.getElementById('pt-acct-input').value = btn.dataset.v; });
  });
  const close = async (save) => {
    if (save) {
      const val = parseFloat(document.getElementById('pt-acct-input').value);
      try {
        await apiFetch(`/users/${state.userId}/paper/account`, {
          method: 'PATCH',
          body: JSON.stringify({ virtual_balance: val, mark_set: true }),
        });
        showToast('Fleet seeding in progress…', 'ok');
      } catch(e) { /* best effort */ }
    }
    overlay.remove();
    _ptLoadFleet();
  };
  document.getElementById('pt-acct-confirm').addEventListener('click', () => close(true));
  document.getElementById('pt-acct-dismiss').addEventListener('click', () => close(false));
}

function _ptShowUpsell() {
  const sc = document.getElementById('screen-paper');
  if (!sc) return;
  sc.innerHTML = `
    <div style="max-width:520px;margin:60px auto;text-align:center;">
      <div style="font-size:32px;margin-bottom:16px;">◫</div>
      <div style="font-size:20px;font-weight:700;margin-bottom:10px;">Strategy Ecosystem</div>
      <div style="color:var(--muted);font-size:13px;line-height:1.7;margin-bottom:24px;">
        The evolutionary trading engine is a <strong style="color:var(--accent);">Pro / Premium</strong> feature.<br>
        Set a capital allocation and the system discovers profitable strategies autonomously.
      </div>
      <button class="btn btn-primary" onclick="navigate('subscription')">Upgrade to Pro →</button>
    </div>`;
}

// ── Shared helpers ────────────────────────────────────────────────────────────

function _ptFmt(v) {
  if (v === null || v === undefined) return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return n >= 100 ? n.toFixed(2) : n.toPrecision(5).replace(/\.?0+$/, '');
}

function _ptDuration(openedAt, closedAt) {
  try {
    const ms = new Date(closedAt) - new Date(openedAt);
    const h  = Math.floor(ms / 3600000);
    const d  = Math.floor(h / 24);
    return d > 0 ? `${d}d ${h % 24}h` : `${h}h`;
  } catch(e) { return ''; }
}

function _ptFleetSparkline(bots) {
  if (!bots.length) return [];
  const maxLen = 6;
  const matrix = bots.map(b => b.sparkline || []).filter(s => s.length >= 2);
  if (!matrix.length) return [];
  const len = Math.min(...matrix.map(s => s.length), maxLen);
  const result = [];
  for (let i = 0; i < len; i++) {
    result.push(matrix.reduce((sum, s) => sum + (s[s.length - len + i] || 0), 0));
  }
  return result;
}

function _ptMiniSpark(vals, W, H, up) {
  if (!vals || vals.length < 2) return '';
  const minV  = Math.min(...vals);
  const maxV  = Math.max(...vals);
  const range = maxV - minV || 1;
  const PX = 2, PY = 2;
  const iW = W - PX * 2, iH = H - PY * 2;
  const pts = vals.map((v, i) => {
    const x = PX + (i / Math.max(vals.length - 1, 1)) * iW;
    const y = PY + iH - ((v - minV) / range) * iH;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const col = up ? 'var(--green)' : 'var(--red)';
  return `<svg viewBox="0 0 ${W} ${H}" style="width:${W}px;height:${H}px;display:inline-block;vertical-align:middle;"><polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
}

function _ptEquityCurve(vals, W, H) {
  if (!vals || vals.length < 2) return '';
  const minV  = Math.min(...vals);
  const maxV  = Math.max(...vals);
  const range = maxV - minV || 1;
  const PX = 6, PY = 8;
  const iW = W - PX * 2, iH = H - PY * 2;
  const pts = vals.map((v, i) => {
    const x = PX + (i / Math.max(vals.length - 1, 1)) * iW;
    const y = PY + iH - ((v - minV) / range) * iH;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const up  = vals[vals.length-1] >= vals[0];
  const col = up ? 'var(--green)' : 'var(--red)';
  const fmt = v => v >= 1000 ? '£' + (v/1000).toFixed(1) + 'k' : '£' + v.toFixed(0);
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px;display:block;">
    <polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>
    <text x="${PX+iW}" y="${PY-2}" fill="${col}" font-size="9" text-anchor="end" font-family="monospace">${fmt(vals[vals.length-1])}</text>
  </svg>`;
}

function _ptFeedLine(r) {
  const ts     = r.created_at ? r.created_at.slice(0,16).replace('T',' ') : '';
  const ticker = r.ticker ? `<span style="color:var(--accent);font-weight:700;margin:0 4px;">${escHtml(r.ticker)}</span>` : '';
  let icon, color;
  switch(r.event_type) {
    case 'scan_start':       icon = '🔍'; color = 'var(--muted)';   break;
    case 'entry':            icon = '▶';  color = 'var(--green)';   break;
    case 'skip':             icon = '—';  color = 'var(--muted)';   break;
    case 't1_hit':           icon = '✓';  color = 'var(--green)';   break;
    case 't2_hit':           icon = '✓✓'; color = '#22d3ee';        break;
    case 'stopped_out':      icon = '✗';  color = 'var(--red)';     break;
    case 'evolution_kill':   icon = '☠️'; color = 'var(--red)';     break;
    case 'evolution_spawn':  icon = '🔄'; color = 'var(--green)';   break;
    case 'evolution_elite':  icon = '⭐'; color = 'var(--green)';   break;
    case 'evolution_promote':icon = '🟢'; color = 'var(--green)';   break;
    default:                 icon = '·';  color = 'var(--muted)';   break;
  }
  return `<div style="border-bottom:1px solid var(--border);padding:3px 0;display:flex;gap:6px;align-items:baseline;">
    <span style="width:18px;text-align:center;flex-shrink:0;">${icon}</span>
    ${ticker}
    <span style="color:${color};flex:1;font-size:12px;">${escHtml(r.detail||r.event_type||'')}</span>
    <span style="color:var(--muted);font-size:10px;flex-shrink:0;">${ts}</span>
  </div>`;
}

// ── Legacy stubs (keep backward-compat with HTML that still references these) ─

async function _ptSyncStatus() {
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/agent/status`);
    const startBtn = document.getElementById('gen-start-btn');
    if (startBtn && d?.running) startBtn.textContent = '■ Stop';
  } catch(e) {}
}

function _ptSetRunning(running) {}      // no-op — legacy HTML hook
function _ptLoadAccount() {}           // no-op — handled by fleet view
function _ptLoadPositions() {}         // no-op — handled by bot detail view
function _ptLoadAgentLog() {}          // no-op — handled by bot detail view
function _ptLoadEquity() {}            // no-op — handled by fleet view
function ptSwitchLogTab() {}           // no-op — old tab system removed
function _ptLoadStats() {}             // no-op — old stats tab removed
