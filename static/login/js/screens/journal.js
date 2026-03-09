// ── JOURNAL ────────────────────────────────────────────────────────────────────
// Trade Journal screen — open positions, closed trades, stats breakdown.
// Partial-close modal with slider + live preview.

let _jnlOpenData      = [];
let _jnlPartialTarget = null;   // { followup_id, ticker, entry_price, position_size, current_price }

// ── Tab switch ─────────────────────────────────────────────────────────────────
function jnlSwitchTab(btn, tab) {
  document.querySelectorAll('#screen-journal .jnl-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('#screen-journal .jnl-tab-panel').forEach(p => p.style.display = 'none');
  const panel = document.getElementById(`jnl-tab-${tab}`);
  if (panel) panel.style.display = '';
  if (tab === 'open')   loadJournalOpen();
  if (tab === 'closed') loadJournalClosed();
  if (tab === 'stats')  loadJournalStats();
}

// ── Load journal screen ────────────────────────────────────────────────────────
async function loadJournal() {
  if (!state.userId) return;
  await loadJournalOpen();
}

// ── Open positions ─────────────────────────────────────────────────────────────
async function loadJournalOpen() {
  if (!state.userId) return;
  const el = document.getElementById('jnl-open-list');
  if (!el) return;
  el.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  try {
    const d = await apiFetch(`/users/${state.userId}/journal/open`);
    _jnlOpenData = d?.positions || [];
    if (!_jnlOpenData.length) {
      el.innerHTML = '<div class="empty text-sm text-muted" style="padding:32px;">No open positions. Accept a tip to start tracking.</div>';
      return;
    }
    el.innerHTML = _jnlOpenData.map(p => renderOpenCard(p)).join('');
    // Wire note blur-to-save
    el.querySelectorAll('.jnl-note-input').forEach(inp => {
      inp.addEventListener('blur', () => saveJournalNote(inp));
    });
  } catch (e) {
    el.innerHTML = `<div class="empty text-sm red" style="padding:24px;">Failed to load: ${escHtml(String(e))}</div>`;
  }
}

function renderOpenCard(p) {
  const dir   = p.direction === 'bearish' ? '▼' : '▲';
  const dirCls = p.direction === 'bearish' ? 'red' : 'green';
  const pnl   = p.live_pnl_pct;
  const pnlTxt = pnl != null ? `${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%` : '—';
  const pnlCls  = pnl == null ? 'muted' : pnl >= 0 ? 'green' : 'red';
  const rMult  = p.r_multiple;
  const rTxt   = rMult != null ? `${rMult >= 0 ? '+' : ''}${rMult.toFixed(2)}R` : '—';
  const rCls   = rMult == null ? 'muted' : rMult >= 0 ? 'green' : 'red';
  const hours  = p.holding_hours;
  const holdTxt = hours == null ? '—' : hours < 24 ? `${hours}h` : `${Math.floor(hours/24)}d ${hours%24}h`;
  const price  = p.current_price != null ? p.current_price.toFixed(p.current_price < 10 ? 4 : 2) : '—';
  const entry  = p.entry_price  != null ? p.entry_price.toFixed(p.entry_price  < 10 ? 4 : 2) : '—';
  const stop   = p.stop_loss    != null ? p.stop_loss.toFixed(p.stop_loss < 10 ? 4 : 2) : '—';
  const t1     = p.target_1     != null ? p.target_1.toFixed(p.target_1 < 10 ? 4 : 2) : '—';
  const t2     = p.target_2     != null ? p.target_2.toFixed(p.target_2 < 10 ? 4 : 2) : '—';
  const ptype  = p.pattern_type ? p.pattern_type.toUpperCase().replace(/_/g,' ') : '';
  const tf     = p.timeframe || '';
  const regime = p.regime_at_entry ? p.regime_at_entry.replace(/_/g,' ') : '';
  const note   = escHtml(p.user_note || '');

  return `<div class="jnl-open-card" data-id="${p.id}">
    <div class="jnl-card-header">
      <span class="jnl-ticker ${dirCls}">${dir} ${escHtml(p.ticker)}</span>
      <span class="jnl-badge">${escHtml(ptype)} ${escHtml(tf)}</span>
      <span class="jnl-regime text-muted text-xs">${escHtml(regime)}</span>
      <span class="jnl-hold text-muted text-xs">${escHtml(holdTxt)}</span>
      <span class="jnl-pnl ${pnlCls}" title="Live P&amp;L">${escHtml(pnlTxt)}</span>
      <span class="jnl-r ${rCls}" title="Live R-multiple">${escHtml(rTxt)}</span>
    </div>
    <div class="jnl-card-levels">
      <span class="jnl-level"><span class="text-muted text-xs">Price</span> <strong>${escHtml(price)}</strong></span>
      <span class="jnl-level"><span class="text-muted text-xs">Entry</span> <strong>${escHtml(entry)}</strong></span>
      <span class="jnl-level"><span class="text-muted text-xs">Stop</span> <strong class="red">${escHtml(stop)}</strong></span>
      <span class="jnl-level"><span class="text-muted text-xs">T1</span> <strong class="green">${escHtml(t1)}</strong></span>
      <span class="jnl-level"><span class="text-muted text-xs">T2</span> <strong class="green">${escHtml(t2)}</strong></span>
    </div>
    <div class="jnl-card-actions">
      <button class="btn btn-sm btn-outline" onclick="jnlAction(${p.id},'closed','hit_t1')">✓ Hit T1</button>
      <button class="btn btn-sm btn-outline" onclick="jnlAction(${p.id},'closed','hit_t2')">✓ Hit T2</button>
      <button class="btn btn-sm" style="background:rgba(239,68,68,.15);color:#ef4444;border:1px solid rgba(239,68,68,.3);" onclick="jnlAction(${p.id},'closed','stopped_out')">✗ Stopped</button>
      <button class="btn btn-sm btn-ghost" onclick="jnlAction(${p.id},'hold_t2','manual')">📈 Hold T2</button>
      <button class="btn btn-sm btn-ghost" onclick="jnlOpenPartialModal(${p.id})">⚡ Partial</button>
    </div>
    <textarea class="jnl-note-input" placeholder="Add a note…" data-id="${p.id}" rows="1">${note}</textarea>
  </div>`;
}

async function jnlAction(followupId, action, closeMethod) {
  try {
    const noteEl = document.querySelector(`.jnl-note-input[data-id="${followupId}"]`);
    const note   = noteEl ? noteEl.value.trim() : '';
    const payload = { action, close_method: closeMethod, user_note: note || undefined };
    const r = await apiFetch(`/tips/${followupId}/position-update`, {
      method: 'POST', body: JSON.stringify(payload),
    });
    showToast(r?.message || 'Updated');
    await loadJournalOpen();
  } catch(e) {
    showToast('Error: ' + String(e), 'error');
  }
}

async function saveJournalNote(inp) {
  const id   = parseInt(inp.dataset.id);
  const note = inp.value.trim();
  if (!note) return;
  try {
    await apiFetch(`/tips/${id}/position-update`, {
      method: 'POST',
      body: JSON.stringify({ action: 'override', user_note: note }),
    });
  } catch(_) {}
}

// ── Partial close modal ────────────────────────────────────────────────────────
function jnlOpenPartialModal(followupId) {
  const pos = _jnlOpenData.find(p => p.id === followupId);
  if (!pos) return;
  _jnlPartialTarget = pos;
  document.getElementById('jnl-partial-ticker').textContent = pos.ticker;
  document.getElementById('jnl-partial-slider').value = 50;
  document.getElementById('jnl-partial-pct-input').value = 50;
  document.getElementById('jnl-partial-price').value = '';
  jnlUpdatePartialPreview();
  const modal = document.getElementById('jnl-partial-modal');
  modal.style.display = 'flex';
}

function jnlClosePartialModal() {
  document.getElementById('jnl-partial-modal').style.display = 'none';
  _jnlPartialTarget = null;
}

function jnlUpdatePartialPreview() {
  const pct     = parseInt(document.getElementById('jnl-partial-slider').value) || 50;
  document.getElementById('jnl-partial-pct-input').value = pct;
  _updatePreviewText(pct);
  // Snap to nearest notch: 25/33/50/67/75/100
  const snaps = [25, 33, 50, 67, 75, 100];
  const closest = snaps.reduce((a, b) => Math.abs(b - pct) < Math.abs(a - pct) ? b : a);
  if (Math.abs(pct - closest) <= 3) {
    document.getElementById('jnl-partial-slider').value = closest;
    document.getElementById('jnl-partial-pct-input').value = closest;
  }
}

function jnlSyncSlider() {
  const pct = parseInt(document.getElementById('jnl-partial-pct-input').value) || 50;
  document.getElementById('jnl-partial-slider').value = pct;
  _updatePreviewText(pct);
}

function _updatePreviewText(pct) {
  const pos   = _jnlPartialTarget;
  const el    = document.getElementById('jnl-partial-preview');
  if (!pos || !pos.entry_price || !pos.position_size) { el.textContent = '—'; return; }
  const priceVal = parseFloat(document.getElementById('jnl-partial-price').value) || pos.current_price || pos.entry_price;
  const shares   = pos.position_size * pct / 100;
  const pnl      = (priceVal - pos.entry_price) * shares;
  const pnlPct   = (priceVal - pos.entry_price) / pos.entry_price * 100;
  const cur      = pos.account_currency || 'GBP';
  el.textContent = `${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} ${cur} (${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(1)}%)`;
  el.className   = pnl >= 0 ? '' : 'red';
}

async function jnlConfirmPartial() {
  const pos  = _jnlPartialTarget;
  if (!pos) return;
  const pct  = parseInt(document.getElementById('jnl-partial-pct-input').value) || 50;
  const price = parseFloat(document.getElementById('jnl-partial-price').value) || undefined;
  const noteEl = document.querySelector(`.jnl-note-input[data-id="${pos.id}"]`);
  const note   = noteEl ? noteEl.value.trim() : '';
  try {
    const r = await apiFetch(`/tips/${pos.id}/position-update`, {
      method: 'POST',
      body: JSON.stringify({
        action: 'partial', partial_pct: pct,
        exit_price: price, user_note: note || undefined,
      }),
    });
    showToast(r?.message || `Partial exit (${pct}%) recorded`);
    jnlClosePartialModal();
    await loadJournalOpen();
  } catch(e) {
    showToast('Error: ' + String(e), 'error');
  }
}

// ── Closed trades ──────────────────────────────────────────────────────────────
async function loadJournalClosed() {
  if (!state.userId) return;
  const el = document.getElementById('jnl-closed-list');
  if (!el) return;
  el.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  const days = document.getElementById('jnl-since-days')?.value || 90;
  try {
    const d = await apiFetch(`/users/${state.userId}/journal/closed?since_days=${days}`);
    const trades = d?.trades || [];
    if (!trades.length) {
      el.innerHTML = '<div class="empty text-sm text-muted" style="padding:32px;">No closed trades in this period.</div>';
      return;
    }
    el.innerHTML = `
      <div class="jnl-closed-table">
        <div class="jnl-closed-head">
          <span>Ticker</span><span>Pattern</span><span>Result</span>
          <span>Entry→Exit</span><span>P&amp;L</span><span>R</span>
          <span>Held</span><span>Regime</span><span>Note</span>
        </div>
        ${trades.map(t => renderClosedRow(t)).join('')}
      </div>`;
  } catch(e) {
    el.innerHTML = `<div class="empty text-sm red" style="padding:24px;">Failed to load: ${escHtml(String(e))}</div>`;
  }
}

function renderClosedRow(t) {
  const dir   = t.direction === 'bearish' ? '▼' : '▲';
  const dirCls = t.direction === 'bearish' ? 'red' : 'green';
  const pnl   = t.pnl_pct;
  const pnlTxt = pnl != null ? `${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%` : '—';
  const pnlCls  = pnl == null ? '' : pnl >= 0 ? 'green' : 'red';
  const r     = t.r_multiple;
  const rTxt  = r != null ? `${r >= 0 ? '+' : ''}${r.toFixed(2)}R` : '—';
  const rCls  = r == null ? '' : r >= 0 ? 'green' : 'red';
  const badge = { closed: '✓ WIN', stopped: '✗ STOP', expired: '⏱ EXP' }[t.status] || t.status;
  const badgeCls = { closed: 'green', stopped: 'red', expired: 'muted' }[t.status] || '';
  const holdTxt  = t.holding_hours != null ? (t.holding_hours < 24 ? `${t.holding_hours}h` : `${Math.floor(t.holding_hours/24)}d`) : '—';
  const ptype = (t.pattern_type || '').toUpperCase().replace(/_/g,' ');
  const tf    = t.timeframe || '';
  const entry = t.entry_price != null ? t.entry_price.toFixed(2) : '—';
  const exit  = t.exit_price  != null ? t.exit_price.toFixed(2) : '—';
  const regime = (t.regime_at_entry || '').replace(/_/g,' ');
  const note  = escHtml(t.user_note || '');
  return `<div class="jnl-closed-row">
    <span class="${dirCls}" style="font-weight:600;">${dir} ${escHtml(t.ticker)}</span>
    <span class="text-muted text-xs">${escHtml(ptype)} ${escHtml(tf)}</span>
    <span class="jnl-result-badge ${badgeCls}">${badge}</span>
    <span class="mono-muted text-xs">${escHtml(entry)} → ${escHtml(exit)}</span>
    <span class="${pnlCls}" style="font-weight:600;">${escHtml(pnlTxt)}</span>
    <span class="${rCls}">${escHtml(rTxt)}</span>
    <span class="text-muted text-xs">${escHtml(holdTxt)}</span>
    <span class="text-muted text-xs">${escHtml(regime)}</span>
    <span class="text-muted text-xs jnl-row-note" title="${note}">${note ? note.slice(0,40) + (note.length > 40 ? '…' : '') : ''}</span>
  </div>`;
}

// ── Stats ──────────────────────────────────────────────────────────────────────
async function loadJournalStats() {
  if (!state.userId) return;
  try {
    const [stats, patterns, regimes] = await Promise.all([
      apiFetch(`/users/${state.userId}/journal/stats`),
      apiFetch(`/users/${state.userId}/journal/pattern-breakdown`),
      apiFetch(`/users/${state.userId}/journal/regime-breakdown`),
    ]);
    document.getElementById('jnl-total').textContent    = stats?.total_trades ?? '—';
    document.getElementById('jnl-winrate').textContent  = stats?.win_rate != null ? `${stats.win_rate}%` : '—';
    document.getElementById('jnl-avgr').textContent     = stats?.avg_r    != null ? `${stats.avg_r >= 0 ? '+' : ''}${stats.avg_r}R` : '—';
    document.getElementById('jnl-bestpat').textContent  = stats?.best_pattern  ? stats.best_pattern.replace(/_/g,' ').toUpperCase() : '—';
    document.getElementById('jnl-worstpat').textContent = stats?.worst_pattern ? stats.worst_pattern.replace(/_/g,' ').toUpperCase() : '—';
    document.getElementById('jnl-bestreg').textContent  = stats?.best_regime   ? stats.best_regime.replace(/_/g,' ') : '—';

    // Colour avg R
    const avgREl = document.getElementById('jnl-avgr');
    if (stats?.avg_r != null) {
      avgREl.className = 'jnl-stat-value ' + (stats.avg_r >= 0 ? 'green' : 'red');
    }

    // Pattern bars
    const pbEl = document.getElementById('jnl-pattern-bars');
    const pb   = patterns?.breakdown || [];
    if (!pb.length) {
      pbEl.innerHTML = '<div class="text-muted text-xs" style="padding:10px;">No closed trades with pattern data yet.</div>';
    } else {
      pbEl.innerHTML = pb.map(row => renderBreakdownBar(
        row.pattern_type.toUpperCase().replace(/_/g,' '),
        row.win_rate, row.sample_count, row.avg_r
      )).join('');
    }

    // Regime bars
    const rbEl = document.getElementById('jnl-regime-bars');
    const rb   = regimes?.breakdown || [];
    if (!rb.length) {
      rbEl.innerHTML = '<div class="text-muted text-xs" style="padding:10px;">No closed trades with regime data yet.</div>';
    } else {
      rbEl.innerHTML = rb.map(row => renderBreakdownBar(
        row.regime.replace(/_/g,' '), row.win_rate, row.sample_count, null
      )).join('');
    }
  } catch(e) {
    console.warn('Journal stats failed:', e);
  }
}

function renderBreakdownBar(label, winRate, count, avgR) {
  const pct     = winRate != null ? winRate : 0;
  const barColor = pct >= 60 ? 'var(--green)' : pct >= 45 ? 'var(--amber)' : 'var(--red)';
  const rTxt    = avgR != null ? ` · ${avgR >= 0 ? '+' : ''}${avgR.toFixed(2)}R` : '';
  return `<div class="jnl-bar-row">
    <span class="jnl-bar-label">${escHtml(label)}</span>
    <div class="jnl-bar-track">
      <div class="jnl-bar-fill" style="width:${pct}%;background:${barColor};"></div>
    </div>
    <span class="jnl-bar-pct">${winRate != null ? winRate.toFixed(0) + '%' : '—'}</span>
    <span class="jnl-bar-meta text-muted text-xs">${count}n${escHtml(rTxt)}</span>
  </div>`;
}

// ── Toast helper (falls back to alert if not defined globally) ─────────────────
function showToast(msg, type) {
  if (typeof window.showNotification === 'function') {
    window.showNotification(msg, type === 'error' ? 'error' : 'success');
  } else if (typeof window.showToastMsg === 'function') {
    window.showToastMsg(msg);
  } else {
    console.log('[Journal]', msg);
  }
}
