// ── PAPER TRADER ─────────────────────────────────────────────────────────────

let _ptPollTimer = null;

async function loadPaperTrader() {
  if (!state.userId) return;
  await new Promise(r => setTimeout(r, 50)); // let showScreen finish activating the DOM
  await Promise.all([_ptLoadAccount(), _ptLoadPositions(), _ptLoadAgentLog(), _ptSyncStatus(), _ptLoadEquity()]);
  if (_ptPollTimer) clearInterval(_ptPollTimer);
  _ptPollTimer = setInterval(() => {
    if (document.getElementById('screen-paper')?.classList.contains('active')) {
      _ptLoadAccount();
      _ptLoadPositions();
      _ptLoadAgentLog();
      _ptSyncStatus();
      _ptLoadEquity();
    } else {
      clearInterval(_ptPollTimer);
      _ptPollTimer = null;
    }
  }, 15000);
}

async function _ptSyncStatus() {
  if (!state.userId) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/agent/status`);
    _ptSetRunning(d?.running === true);
  } catch(e) { /* ignore */ }
}

function _ptSetRunning(running) {
  const btn  = document.getElementById('pt-start-btn');
  const dot  = document.getElementById('pt-scanner-dot');
  if (!btn || !dot) return;
  if (running) {
    btn.textContent = '■ Stop Agent';
    btn.className = 'btn btn-sm';
    btn.style.background = 'var(--red)';
    btn.style.color = '#fff';
    btn.style.border = 'none';
    dot.style.background = 'var(--green)';
    dot.style.boxShadow = '0 0 6px var(--green)';
  } else {
    btn.textContent = '▶ Start Agent';
    btn.className = 'btn btn-primary btn-sm';
    btn.style.background = '';
    btn.style.color = '';
    btn.style.border = '';
    dot.style.background = 'var(--muted)';
    dot.style.boxShadow = '';
  }
}

async function _ptLoadAccount() {
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/account`);
    if (!d) return;
    const acctVal = d.account_value ?? d.virtual_balance ?? 10000;
    document.getElementById('pt-balance').textContent = acctVal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
    const unreal = d.unrealised_pnl ?? 0;
    const subEl = document.getElementById('pt-balance-sub');
    if (subEl) {
      const sign = unreal >= 0 ? '+' : '';
      subEl.textContent = `${d.currency || 'GBP'} · unrealised ${sign}${unreal.toFixed(2)}`;
      subEl.style.color = unreal > 0 ? 'var(--green)' : unreal < 0 ? 'var(--red)' : '';
    }
    document.getElementById('pt-open-count').textContent = d.open_positions ?? 0;
    const wr = d.win_rate_pct;
    const wrEl = document.getElementById('pt-win-rate');
    wrEl.textContent = wr !== null && wr !== undefined ? wr + '%' : '—';
    wrEl.style.color = wr !== null ? (wr >= 50 ? 'var(--green)' : 'var(--red)') : '';
    const ar = d.avg_r;
    const arEl = document.getElementById('pt-avg-r');
    arEl.textContent = ar !== null && ar !== undefined ? ar + 'R' : '—';
    arEl.style.color = ar !== null ? (ar >= 0 ? 'var(--green)' : 'var(--red)') : '';
    document.getElementById('pt-closed-count').textContent = (d.closed_trades ?? 0) + ' closed trades';
    // Show onboarding modal if account size not yet set
    if (d.account_size_set === false) _ptShowOnboarding();
  } catch(e) {
    if (e.message && e.message.includes('paper_trading_requires_pro')) {
      _ptShowUpsell();
    }
  }
}

async function _ptLoadEquity() {
  const el = document.getElementById('pt-equity-chart');
  const lbl = document.getElementById('pt-equity-label');
  if (!el || !state.userId) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/equity?days=90`);
    const rows = d?.equity || [];
    if (!rows.length) return; // keep placeholder
    const vals = rows.map(r => r.equity_value);
    const times = rows.map(r => r.logged_at);
    const minV = Math.min(...vals);
    const maxV = Math.max(...vals);
    const range = maxV - minV || 1;
    const W = 560, H = 120, PX = 8, PY = 12;
    const iW = W - PX * 2, iH = H - PY * 2;
    const pts = vals.map((v, i) => {
      const x = PX + (i / Math.max(vals.length - 1, 1)) * iW;
      const y = PY + iH - ((v - minV) / range) * iH;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    const last = vals[vals.length - 1];
    const first = vals[0];
    const up = last >= first;
    const lineCol = up ? 'var(--green)' : 'var(--red)';
    const lastX = PX + iW;
    const lastY = PY + iH - ((last - minV) / range) * iH;
    const fmt = v => v >= 1000 ? '£' + (v/1000).toFixed(1) + 'k' : '£' + v.toFixed(0);
    const pct = first > 0 ? ((last - first) / first * 100).toFixed(1) : '0.0';
    const sign = up ? '+' : '';
    if (lbl) lbl.textContent = `${sign}${pct}% · ${fmt(last)} · ${rows.length} data points`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:120px;display:block;">
      <polyline points="${pts}" fill="none" stroke="${lineCol}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="3.5" fill="${lineCol}"/>
      <text x="${(lastX - 2).toFixed(1)}" y="${(lastY - 7).toFixed(1)}" fill="${lineCol}" font-size="9" text-anchor="end" font-family="monospace">${fmt(last)}</text>
    </svg>`;
  } catch(e) { /* leave placeholder */ }
}

let _ptOnboardingShown = false;
async function _ptShowOnboarding() {
  if (_ptOnboardingShown || document.getElementById('pt-onboarding-modal')) return;
  _ptOnboardingShown = true;
  const overlay = document.createElement('div');
  overlay.id = 'pt-onboarding-modal';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
  overlay.innerHTML = `
    <div style="background:var(--card);border:1px solid var(--border);border-radius:10px;padding:28px 32px;max-width:380px;width:90%;">
      <div style="font-size:16px;font-weight:700;margin-bottom:8px;">Set your paper account size</div>
      <div style="color:var(--muted);font-size:13px;margin-bottom:20px;">How much would you like to simulate trading with?</div>
      <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">
        <button class="btn btn-ghost btn-sm pt-preset" data-v="5000">£5k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="10000">£10k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="25000">£25k</button>
        <button class="btn btn-ghost btn-sm pt-preset" data-v="50000">£50k</button>
      </div>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:20px;">
        <span style="color:var(--muted);font-size:14px;">£</span>
        <input id="pt-acct-input" type="number" min="1000" max="100000" step="1000" value="10000"
          style="flex:1;background:var(--input-bg,#1a1a2e);border:1px solid var(--border);border-radius:6px;padding:8px 10px;color:var(--text);font-size:14px;">
      </div>
      <div style="display:flex;gap:8px;">
        <button id="pt-acct-confirm" class="btn btn-primary" style="flex:1;">Confirm</button>
        <button id="pt-acct-dismiss" class="btn btn-ghost">Not now</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.querySelectorAll('.pt-preset').forEach(btn => {
    btn.addEventListener('click', () => {
      document.getElementById('pt-acct-input').value = btn.dataset.v;
    });
  });
  const close = async (save) => {
    const val = save ? parseFloat(document.getElementById('pt-acct-input').value) : null;
    try {
      await apiFetch(`/users/${state.userId}/paper/account`, {
        method: 'PATCH',
        body: JSON.stringify({ virtual_balance: val || 500000, mark_set: true }),
      });
    } catch(e) { /* best effort */ }
    overlay.remove();
    _ptLoadAccount();
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
      <div style="font-size:20px;font-weight:700;margin-bottom:10px;">Paper Trader</div>
      <div style="color:var(--muted);font-size:13px;line-height:1.7;margin-bottom:24px;">
        Paper trading is a <strong style="color:var(--accent);">Pro / Premium</strong> feature.<br>
        The AI agent thinks, plans and places trades autonomously using live KB signals.
      </div>
      <button class="btn btn-primary" onclick="navigate('subscription')">Upgrade to Pro →</button>
    </div>`;
}

async function _ptLoadPositions() {
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/positions?status=all`);
    if (!d) return;
    const open   = (d.positions || []).filter(p => p.status === 'open');
    const closed = (d.positions || []).filter(p => p.status !== 'open');
    _ptRenderOpen(open);
    _ptRenderClosed(closed);
    const lbl = document.getElementById('pt-open-label');
    if (lbl) lbl.textContent = open.length ? open.length + ' active' : '';
  } catch(e) { /* handled in account */ }
}

async function _ptLoadAgentLog() {
  const feed = document.getElementById('pt-feed');
  if (!feed || !state.userId) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/agent/log`);
    const rows = d?.log || [];
    if (!rows.length) {
      feed.innerHTML = '<span style="color:var(--muted);">No agent activity yet — click ▶ Start Agent to begin</span>';
      return;
    }
    feed.innerHTML = rows.map(r => _ptFeedLine(r)).join('');
  } catch(e) {
    feed.innerHTML = `<span style="color:var(--red);">${escHtml(e.message)}</span>`;
  }
}

function _ptParseDetail(event_type, raw) {
  if (!raw) return { main: '', sub: '' };
  // Parse key=value pairs out of the raw detail string
  const kv = {};
  raw.replace(/(\w+)=([^\s|]+)/g, (_, k, v) => { kv[k] = v; });
  // Split reasoning after " | "
  const pipeIdx = raw.indexOf(' | ');
  const reasoning = pipeIdx >= 0 ? raw.slice(pipeIdx + 3) : '';
  if (event_type === 'entry') {
    const entry = kv.entry ? parseFloat(kv.entry).toFixed(2) : '?';
    const stop  = kv.stop  ? parseFloat(kv.stop).toFixed(2)  : '?';
    const t1    = kv.t1    ? parseFloat(kv.t1).toFixed(2)    : '?';
    const val   = kv.value ? kv.value.replace('£','') : (kv['value='] || '?');
    const valStr = kv.value || (raw.match(/value=(£[\d,\.]+)/) || [])[1] || '';
    return {
      main: `entry ${entry} → T1 ${t1}  stop ${stop}  ${valStr}`,
      sub:  reasoning,
    };
  }
  if (event_type === 'stopped_out') {
    const exit = kv.exit ? parseFloat(kv.exit).toFixed(2) : '?';
    const pnl  = kv['P&L'] || kv.pnl || '';
    return { main: `stopped out at ${exit}  ${pnl ? pnl + 'R' : ''}`, sub: '' };
  }
  if (event_type === 't1_hit') {
    const exit = kv.exit ? parseFloat(kv.exit).toFixed(2) : '?';
    return { main: `T1 hit at ${exit}`, sub: '' };
  }
  if (event_type === 't2_hit') {
    const exit = kv.exit ? parseFloat(kv.exit).toFixed(2) : '?';
    const pnl  = kv['P&L'] || '';
    return { main: `T2 hit at ${exit}  ${pnl ? pnl + 'R' : ''}`, sub: '' };
  }
  return { main: raw, sub: '' };
}

function _ptFeedLine(r) {
  const ts = r.created_at ? r.created_at.slice(0,16).replace('T',' ') : '';
  const ticker = r.ticker ? `<span style="color:var(--accent);font-weight:700;margin:0 4px;">${escHtml(r.ticker)}</span>` : '';
  let icon, color;
  switch(r.event_type) {
    case 'scan_start':  icon = '🔍'; color = 'var(--muted)';   break;
    case 'entry':       icon = '▶';  color = 'var(--green)';   break;
    case 'skip':        icon = '—';  color = 'var(--muted)';   break;
    case 't1_hit':      icon = '✓';  color = 'var(--green)';   break;
    case 't2_hit':      icon = '✓✓'; color = '#22d3ee';        break;
    case 'stopped_out': icon = '✗';  color = 'var(--red)';     break;
    case 'monitor_run': icon = '⟳';  color = 'var(--muted)';   break;
    default:            icon = '·';  color = 'var(--muted)';   break;
  }
  const parsed = _ptParseDetail(r.event_type, r.detail || '');
  const mainText = escHtml(parsed.main);
  const subText  = parsed.sub ? `<div style="color:var(--muted);font-size:10px;margin-top:1px;padding-left:26px;">${escHtml(parsed.sub)}</div>` : '';
  return `<div style="border-bottom:1px solid var(--border);padding:4px 0;">
    <div style="display:flex;gap:8px;align-items:baseline;">
      <span style="color:${color};width:18px;text-align:center;flex-shrink:0;">${icon}</span>
      ${ticker}
      <span style="color:${color};flex:1;">${mainText}</span>
      <span style="color:var(--muted);font-size:10px;flex-shrink:0;">${ts}</span>
    </div>${subText}
  </div>`;
}

function _ptRenderOpen(rows) {
  const tbody = document.getElementById('pt-open-tbody');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--muted);padding:20px;font-size:12px;">No open positions — agent runs every 30 min</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(p => {
    const statusCls  = 'pt-status-' + (p.status || 'open').replace(/[^a-z_]/g,'');
    const dirCls     = p.direction === 'bullish' ? 'kb-dir-bullish' : 'kb-dir-bearish';
    const t2         = p.t2 !== null && p.t2 !== undefined ? _ptFmt(p.t2) : '—';
    const dateStr    = p.opened_at ? p.opened_at.slice(0,10) : '—';
    const partialDot = p.partial_closed ? '<span title="T1 hit" style="color:var(--accent);margin-left:4px;">●T1</span>' : '';
    const reasoning  = p.ai_reasoning ? `<span class="text-xs text-muted" title="${escHtml(p.ai_reasoning)}">${escHtml(p.ai_reasoning.slice(0,40))}${p.ai_reasoning.length>40?'…':''}</span>` : '<span class="text-xs text-muted">—</span>';
    const nowPrice   = p.current_price != null ? _ptFmt(p.current_price) : '<span style="color:var(--muted);">…</span>';
    const unr        = p.unrealised_pnl_r;
    const unrStr     = unr != null ? (unr >= 0 ? '+' : '') + unr + 'R' : '—';
    const unrCls     = unr != null ? (unr >= 0 ? 'pt-pnl-pos' : 'pt-pnl-neg') : '';
    const qty        = p.quantity != null ? p.quantity : 1;
    const posVal     = p.entry_price != null ? (p.entry_price * qty) : null;
    const posValStr  = posVal != null ? '£' + posVal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '—';
    return `<tr>
      <td><span class="mono" style="font-weight:700;">${escHtml(p.ticker)}</span></td>
      <td><span class="${dirCls}">${p.direction === 'bullish' ? '▲' : '▼'}</span></td>
      <td class="mono">${_ptFmt(p.entry_price)}</td>
      <td class="mono mono-red">${_ptFmt(p.stop)}</td>
      <td class="mono mono-amber">${_ptFmt(p.t1)}</td>
      <td class="mono mono-green">${t2}</td>
      <td class="mono">${nowPrice}</td>
      <td class="mono" style="color:var(--accent);">${posValStr}</td>
      <td class="${unrCls}" style="font-weight:600;">${unrStr}</td>
      <td><span class="${statusCls}">${(p.status||'open').toUpperCase()}${partialDot}</span></td>
      <td class="mono-muted" style="font-size:10px;">${dateStr}</td>
      <td>${reasoning}</td>
    </tr>`;
  }).join('');
}

function _ptRenderClosed(rows) {
  const tbody = document.getElementById('pt-closed-tbody');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px;font-size:12px;">No closed trades yet</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(p => {
    const dirCls    = p.direction === 'bullish' ? 'kb-dir-bullish' : 'kb-dir-bearish';
    const pnr       = p.pnl_r !== null && p.pnl_r !== undefined ? p.pnl_r : null;
    const pnlCls    = pnr !== null ? (pnr >= 0 ? 'pt-pnl-pos' : 'pt-pnl-neg') : '';
    const pnlStr    = pnr !== null ? (pnr >= 0 ? '+' : '') + pnr + 'R' : '—';
    const statusCls = 'pt-status-' + (p.status||'closed');
    const dateStr   = (p.closed_at || '').slice(0,10) || '—';
    const tickerLabel = p.ticker ? escHtml(p.ticker) : '<span style="color:var(--muted)">—</span>';
    return `<tr>
      <td><span class="mono" style="font-weight:700;">${tickerLabel}</span></td>
      <td><span class="${dirCls}">${p.direction === 'bullish' ? '▲' : '▼'}</span></td>
      <td class="mono">${_ptFmt(p.entry_price)}</td>
      <td class="mono">${p.exit_price !== null && p.exit_price !== undefined ? _ptFmt(p.exit_price) : '—'}</td>
      <td><span class="${statusCls}">${(p.status||'').toUpperCase()}</span></td>
      <td class="${pnlCls}">${pnlStr}</td>
      <td class="mono-muted" style="font-size:10px;">${dateStr}</td>
    </tr>`;
  }).join('');
}

function _ptFmt(v) {
  if (v === null || v === undefined) return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return n >= 100 ? n.toFixed(2) : n.toPrecision(5).replace(/\.?0+$/, '');
}

document.getElementById('pt-start-btn').addEventListener('click', async function() {
  if (!state.userId) return;
  const isRunning = this.textContent.includes('Stop');
  this.disabled = true;
  try {
    if (isRunning) {
      await apiFetch(`/users/${state.userId}/paper/agent/stop`, { method: 'POST' });
      _ptSetRunning(false);
      showToast('Scanner stopped', 'ok');
    } else {
      const d = await apiFetch(`/users/${state.userId}/paper/agent/start`, { method: 'POST' });
      _ptSetRunning(true);
      showToast(d?.message || 'Scanner started', 'ok');
      setTimeout(() => { _ptLoadAgentLog(); _ptLoadPositions(); _ptLoadAccount(); }, 5000);
    }
  } catch(e) { showToast(e.message || 'Error'); }
  finally { this.disabled = false; }
});

document.getElementById('pt-download-btn').addEventListener('click', function() {
  if (!state.userId) return;
  const token = document.cookie.split(';').map(c => c.trim()).find(c => c.startsWith('tg_access='));
  const url = `/users/${state.userId}/paper/agent/log/export`;
  const a = document.createElement('a');
  a.href = `https://api.trading-galaxy.uk${url}`;
  a.download = '';
  a.target = '_blank';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
});

document.getElementById('pt-reset-btn').addEventListener('click', function() {
  const modal = document.getElementById('pt-reset-modal');
  modal.style.display = 'flex';
});

document.getElementById('pt-reset-cancel').addEventListener('click', function() {
  document.getElementById('pt-reset-modal').style.display = 'none';
});

document.getElementById('pt-reset-modal').addEventListener('click', function(e) {
  if (e.target === this) this.style.display = 'none';
});

document.getElementById('pt-reset-confirm').addEventListener('click', async function() {
  if (!state.userId) return;
  this.disabled = true;
  this.textContent = 'Resetting…';
  try {
    await apiFetch(`/users/${state.userId}/paper/reset`, { method: 'DELETE' });
    document.getElementById('pt-reset-modal').style.display = 'none';
    showToast('Paper trader reset — starting fresh', 'ok');
    await loadPaperTrader();
  } catch(e) {
    showToast(e.message || 'Reset failed', 'error');
  } finally {
    this.disabled = false;
    this.textContent = 'Delete Everything';
  }
});

function ptSwitchLogTab(btn, tab) {
  document.querySelectorAll('.pt-log-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('pt-log-closed').style.display = tab === 'closed' ? '' : 'none';
  document.getElementById('pt-log-stats').style.display  = tab === 'stats'  ? '' : 'none';
  if (tab === 'stats') _ptLoadStats();
}

async function _ptLoadStats() {
  const el = document.getElementById('pt-stats-body');
  if (!el || !state.userId) return;
  el.innerHTML = '<div style="color:var(--muted);font-size:12px;text-align:center;padding:20px;"><div class="spinner"></div></div>';
  try {
    const d = await apiFetch(`/users/${state.userId}/paper/stats`);
    if (!d || !d.total_closed) {
      el.innerHTML = '<div style="color:var(--muted);font-size:12px;text-align:center;padding:20px;">No stats yet — the AI agent needs to close some trades first.</div>';
      return;
    }
    let html = '';
    if (d.by_conviction && d.by_conviction.length) {
      html += '<div class="pt-stats-group"><div class="pt-stats-group-title">By Conviction Tier</div>';
      d.by_conviction.forEach(r => {
        const wrColor = r.win_rate_pct >= 50 ? 'var(--green)' : 'var(--red)';
        html += `<div class="pt-stats-row"><span style="text-transform:uppercase;font-size:11px;">${escHtml(r.label)}</span><span><span class="mono" style="color:${wrColor}">${r.win_rate_pct}%</span> <span class="mono-muted">${r.wins}/${r.trades} · avg ${r.avg_r}R</span></span></div>`;
      });
      html += '</div>';
    }
    if (d.by_pattern_type && d.by_pattern_type.length) {
      html += '<div class="pt-stats-group"><div class="pt-stats-group-title">By Pattern Type</div>';
      d.by_pattern_type.forEach(r => {
        const wrColor = r.win_rate_pct >= 50 ? 'var(--green)' : 'var(--red)';
        html += `<div class="pt-stats-row"><span style="text-transform:uppercase;font-size:11px;">${escHtml(r.label)}</span><span><span class="mono" style="color:${wrColor}">${r.win_rate_pct}%</span> <span class="mono-muted">${r.wins}/${r.trades} · avg ${r.avg_r}R</span></span></div>`;
      });
      html += '</div>';
    }
    if (d.best_trade || d.worst_trade) {
      html += '<div class="pt-best-worst">';
      if (d.best_trade) {
        const b = d.best_trade;
        html += `<div class="pt-bw-card"><div class="pt-bw-label">Best Trade</div><div class="pt-bw-ticker">${escHtml(b.ticker)}</div><div class="pt-bw-r pt-pnl-pos">+${b.pnl_r}R</div><div style="font-size:10px;color:var(--muted);margin-top:3px;">${(b.closed_at||'').slice(0,10)}</div></div>`;
      }
      if (d.worst_trade) {
        const w = d.worst_trade;
        html += `<div class="pt-bw-card"><div class="pt-bw-label">Worst Trade</div><div class="pt-bw-ticker">${escHtml(w.ticker)}</div><div class="pt-bw-r pt-pnl-neg">${w.pnl_r}R</div><div style="font-size:10px;color:var(--muted);margin-top:3px;">${(w.closed_at||'').slice(0,10)}</div></div>`;
      }
      html += '</div>';
    }
    el.innerHTML = html || '<div style="color:var(--muted);font-size:12px;text-align:center;padding:20px;">No stats yet.</div>';
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);font-size:12px;padding:16px;">${escHtml(e.message)}</div>`;
  }
}

