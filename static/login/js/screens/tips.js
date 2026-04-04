// ── TIPS ──────────────────────────────────────────────────────────────────────
async function loadTipsAccountValue() {
  if (!state.userId) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/portfolio`);
    const holdings = d?.holdings || [];
    if (holdings.length) {
      // Sum all holdings including cash (cash has qty=1, avg_cost=amount)
      const total = holdings.reduce((sum, h) => {
        const v = h.is_cash ? (h.avg_cost || 0) : ((h.quantity || 0) * (h.avg_cost || 0));
        return sum + v;
      }, 0);
      state._tipAccountSize = total > 0 ? Math.round(total) : null;
      // Prefer account_currency from non-cash holdings for display symbol
      const nonCash = holdings.find(h => !h.is_cash);
      const currency = nonCash?.currency || holdings[0]?.currency || 'GBP';
      const sym = _cashSym(currency);
      document.getElementById('tip-account-display').textContent =
        state._tipAccountSize ? `${sym}${state._tipAccountSize.toLocaleString('en-GB')}` : '—';
    } else {
      state._tipAccountSize = null;
      document.getElementById('tip-account-display').textContent = 'No portfolio';
    }
  } catch { /* silent */ }
}

let _mktSectors = [];
let _mktSelected = new Set();
let _mktAllMode = true;

function _mktRenderChips() {
  const wrap = document.getElementById('mkt-chips');
  if (!wrap) return;
  wrap.innerHTML = '';
  _mktSelected.forEach(ticker => {
    const chip = document.createElement('span');
    chip.className = 'mkt-chip';
    chip.innerHTML = escHtml(ticker) + ' <span class="mkt-chip-x">\u00d7</span>';
    chip.addEventListener('click', () => { _mktSelected.delete(ticker); _mktRenderChips(); _mktRenderGroups(); });
    wrap.appendChild(chip);
  });
}

function _mktRenderGroups(filter) {
  const wrap = document.getElementById('mkt-groups');
  if (!wrap) return;
  const q = (filter || '').toUpperCase().trim();
  wrap.innerHTML = '';
  _mktSectors.forEach(sec => {
    const tickers = q ? sec.tickers.filter(t => t.includes(q)) : sec.tickers;
    if (!tickers.length) return;
    const header = document.createElement('div');
    header.className = 'mkt-group-header';
    const allSel = tickers.every(t => _mktSelected.has(t));
    header.innerHTML = '<span>' + escHtml(sec.group) + '</span><span style="font-size:10px;color:var(--accent)">' + (allSel ? '\u2212 Deselect all' : '+ Select all') + '</span>';
    const _filter = filter;
    header.addEventListener('click', () => {
      if (allSel) tickers.forEach(t => _mktSelected.delete(t));
      else tickers.forEach(t => _mktSelected.add(t));
      _mktRenderChips(); _mktRenderGroups(_filter);
    });
    const row = document.createElement('div');
    row.className = 'mkt-group-tickers';
    tickers.forEach(t => {
      const btn = document.createElement('button');
      btn.className = 'mkt-ticker-btn' + (_mktSelected.has(t) ? ' selected' : '');
      btn.textContent = t;
      const _t = t, _f = filter;
      btn.addEventListener('click', () => {
        if (_mktSelected.has(_t)) _mktSelected.delete(_t); else _mktSelected.add(_t);
        _mktRenderChips(); _mktRenderGroups(_f);
      });
      row.appendChild(btn);
    });
    wrap.appendChild(header);
    wrap.appendChild(row);
  });
}

async function _mktInit() {
  if (_mktSectors.length) return;
  try {
    const d = await apiFetch('/markets/tickers');
    _mktSectors = d?.sectors || [];
    _mktRenderGroups();
  } catch(e) { /* silent */ }
}

function _mktSetAllMode(on) {
  _mktAllMode = on;
  const toggle = document.getElementById('mkt-all-toggle');
  const label = document.getElementById('mkt-all-label');
  const pickerWrap = document.getElementById('mkt-picker-wrap');
  if (toggle) toggle.checked = on;
  if (label) label.innerHTML = on
    ? 'All Markets <span class="mkt-count">(default \u2014 tips from any ticker)</span>'
    : 'Custom selection <span class="mkt-count">(' + _mktSelected.size + ' selected)</span>';
  if (pickerWrap) pickerWrap.classList.toggle('mkt-picker-disabled', on);
}

document.getElementById('mkt-all-toggle').addEventListener('change', function() {
  _mktSetAllMode(this.checked);
  if (!this.checked) _mktInit();
});

document.getElementById('mkt-search').addEventListener('input', function() {
  _mktRenderGroups(this.value);
});

async function loadTipConfig() {
  if (!state.userId) return;
  try {
    const d = await apiFetch('/users/' + state.userId + '/tip-config');
    if (!d) return;
    if (d.tip_delivery_time) document.getElementById('tip-time').value = d.tip_delivery_time;
    if (d.tip_delivery_timezone) {
      const sel = document.getElementById('tip-tz');
      for (let i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === d.tip_delivery_timezone) { sel.selectedIndex = i; break; }
      }
    }
    if (d.tier) {
      const sel = document.getElementById('tip-tier');
      for (let i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === d.tier) { sel.selectedIndex = i; break; }
      }
    }
    if (d.tip_markets && d.tip_markets.length > 0) {
      _mktSelected = new Set(d.tip_markets.map(t => t.toUpperCase()));
      await _mktInit();
      _mktSetAllMode(false);
      _mktRenderChips();
      _mktRenderGroups();
    } else {
      _mktSetAllMode(true);
    }
    // Available cash display
    const cashDisplay = document.getElementById('tip-cash-display');
    const cashNudge   = document.getElementById('tip-cash-nudge');
    const currency    = d.account_currency || 'GBP';
    const sym         = currency === 'GBP' ? '\u00a3' : '$';
    state._availableCash     = d.available_cash != null ? d.available_cash : null;
    state._accountCurrency   = currency;
    if (d.available_cash != null) {
      const isNeg = d.available_cash < 0;
      cashDisplay.style.color = isNeg ? 'var(--red)' : 'var(--accent)';
      cashDisplay.textContent = sym + Math.abs(d.available_cash).toLocaleString('en-GB', {maximumFractionDigits:0}) + (isNeg ? ' \u26a0 overcommitted' : '');
      if (cashNudge) cashNudge.textContent = '';
    } else {
      cashDisplay.textContent = '\u2014';
      if (cashNudge) cashNudge.innerHTML = '<a href="#" id="cash-set-nudge" style="color:var(--accent);text-decoration:none;font-size:11px;">Set balance \u2192 (Portfolio tab)</a>';
    }
  } catch(e) { /* silent */ }
}

document.getElementById('tip-config-btn').addEventListener('click', async () => {
  if (!state.userId) { showToast('Sign in first'); return; }
  const msg = document.getElementById('tip-config-msg');
  msg.innerHTML = '<span class="spinner"></span>';
  const tip_markets = _mktAllMode ? null : (_mktSelected.size > 0 ? Array.from(_mktSelected) : null);
  try {
    const d = await apiFetch('/users/' + state.userId + '/tip-config', {
      method: 'POST',
      body: JSON.stringify({
        tip_delivery_time:     document.getElementById('tip-time').value,
        tip_delivery_timezone: document.getElementById('tip-tz').value,
        tip_markets:           tip_markets,
        account_size:          state._tipAccountSize || null,
        tier:                  document.getElementById('tip-tier').value,
      })
    });
    if (!d) return;
    const label = document.getElementById('mkt-all-label');
    if (label && !_mktAllMode) label.innerHTML = 'Custom selection <span class="mkt-count">(' + _mktSelected.size + ' selected)</span>';
    msg.style.color = 'var(--green)'; msg.textContent = '\u2713 Config saved';
    setTimeout(() => { msg.textContent = ''; }, 3000);
  } catch(e) { msg.style.color='var(--red)'; msg.textContent = e.message; }
});

document.getElementById('tip-preview-btn').addEventListener('click', async () => {
  if (!state.userId) { showToast('Sign in first'); return; }
  const wrap = document.getElementById('tip-preview-card');
  wrap.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch(`/users/${state.userId}/tip/preview`);
    if (!d) { wrap.innerHTML = '<div class="text-sm text-muted">No tip available</div>'; return; }
    const t = d.tip;
    if (!t) { wrap.innerHTML = `<div class="text-sm text-muted">${escHtml(d.reason || 'No eligible patterns')}</div>`; return; }
    const _SRC_LABELS = {
      'watchlist':   { icon: '🎯', text: 'Signal from your watchlist' },
      'portfolio':   { icon: '📂', text: 'Your watchlist was quiet — signal from your portfolio' },
      'connected':   { icon: '🔗', text: 'Your portfolio was quiet — signal from a correlated sector ticker' },
      'market-wide': { icon: '🌐', text: 'Market-wide signal — your watchlist and portfolio were quiet today' },
    };
    const srcInfo = d.tip_source ? _SRC_LABELS[d.tip_source] : null;
    const sourceHtml = srcInfo
      ? `<div style="margin-top:10px;padding:7px 10px;border-radius:6px;background:var(--surface);border:1px solid var(--border);font-size:11px;color:var(--muted);display:flex;align-items:center;gap:6px;">${srcInfo.icon} <span>${escHtml(srcInfo.text)}</span></div>`
      : '';

    function _renderSetupCard(t, idx, total, isBatch) {
      const skew = t.skew_warning ? `<div class="skew-warning">⚠ ${escHtml(t.skew_warning)}</div>` : '';
      const patternId = t.pattern_id ?? t.id ?? null;
      const batchHeader = isBatch ? `<div style="font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">Setup ${idx} of ${total}</div>` : '';
      return `<div class="tip-card" style="${isBatch && idx < total ? 'margin-bottom:10px;' : ''}">
        ${batchHeader}
        <div class="tip-card-header">
          <div>
            <div class="tip-card-ticker">${escHtml(t.ticker)}</div>
            <div class="tip-card-type">${escHtml(t.pattern_type||'')} · ${escHtml(t.timeframe||'')} · ${dirBadge(t.direction)}</div>
          </div>
          ${t.quality_score != null ? `<span class="badge badge-open">Q: ${fmt(t.quality_score)}</span>` : ''}
        </div>
        <div class="tip-card-zone">
          <div class="tip-card-zone-label">Entry Zone</div>
          <div class="mono-amber">${fmt(t.zone_low)} – ${fmt(t.zone_high)}</div>
        </div>
        <div class="tip-card-levels">
          <div class="tip-level"><div class="tip-level-label">Entry</div><div class="tip-level-value entry">${fmt(t.entry_price || t.zone_high)}</div></div>
          <div class="tip-level"><div class="tip-level-label">Stop</div><div class="tip-level-value stop">${fmt(t.stop_loss)}</div></div>
          <div class="tip-level"><div class="tip-level-label">T1</div><div class="tip-level-value target">${fmt(t.target_1)}</div></div>
        </div>
        <div class="tip-card-meta">
          ${t.position_units != null ? `<span><span class="meta-key">Units</span><span class="meta-val">${t.position_units}</span></span>` : ''}
          ${t.rr_ratio != null ? `<span><span class="meta-key">R/R</span><span class="meta-val">${fmt(t.rr_ratio)}</span></span>` : ''}
          ${t.kb_conviction ? `<span><span class="meta-key">Conviction</span><span class="meta-val">${escHtml(t.kb_conviction)}</span></span>` : ''}
        </div>
        ${skew}
      </div>`;
    }

    const tips = d.tips && d.tips.length > 0 ? d.tips : [t];
    const isBatch = tips.length > 1;
    const cadence = d.cadence || 'daily';

    let html = '';
    if (isBatch) {
      const cadenceLabel = cadence === 'weekly'
        ? `<div style="font-size:12px;font-weight:600;color:var(--accent);margin-bottom:10px;">📅 Weekly Batch Preview — ${tips.length} setups</div>`
        : '';
      html = cadenceLabel + tips.map((tip, i) => _renderSetupCard(tip, i+1, tips.length, true)).join('');
    } else {
      html = _renderSetupCard(t, 1, 1, false);
    }

    html += sourceHtml;

    // Cash overflow warning
    if (state._availableCash != null && t.position_value != null) {
      const posVal = t.position_value;
      const cash   = state._availableCash;
      const sym    = (state._accountCurrency === 'GBP') ? '£' : '$';
      const fmtVal = v => sym + Math.abs(v).toLocaleString('en-GB', {maximumFractionDigits:0});
      if (posVal > cash) {
        const wouldBeNeg = cash - posVal < 0;
        const warnMsg = wouldBeNeg
          ? `⚠ This trade (${fmtVal(posVal)}) would take your cash balance negative — check your sizing`
          : `⚠ Position size (${fmtVal(posVal)}) exceeds available cash (${fmtVal(cash)}) — you may need to free up capital first`;
        html += `<div style="margin-top:10px;padding:8px 10px;border-radius:6px;background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.3);font-size:11px;color:#f59e0b;">${warnMsg}</div>`;
      }
    } else if (state._availableCash == null) {
      html += `<div style="margin-top:8px;font-size:11px;color:var(--muted);">💡 <a href="#" onclick="navigate('profile');return false;" style="color:var(--accent);text-decoration:none;">Set your cash balance in Profile</a> to see position sizing warnings</div>`;
    }

    const patternId = t.pattern_id ?? t.id ?? null;

    // Accept row — only show if pattern_id is available
    if (patternId) {
      html += `<div class="tip-accept-row" id="tip-accept-row" data-pattern-id="${patternId}">
        <button class="btn btn-primary btn-sm" id="tip-take-btn" style="flex:1;">🎯 Take This Trade</button>
        <button class="btn btn-ghost btn-sm" id="tip-skip-btn">✕ Not for me</button>
      </div>`;
    }

    html += `<div class="feedback-row" id="tip-fb-row">
      <span class="fb-label">Outcome</span>
      <button class="fb-btn" data-v="hit_t1" title="Hit Target 1">T1 ✓</button>
      <button class="fb-btn" data-v="hit_t2" title="Hit Target 2">T2 ✓</button>
      <button class="fb-btn fb-stop" data-v="stopped_out" title="Stopped out">Stop ✗</button>
      <button class="fb-btn" data-v="pending" title="Still open">Open</button>
      <button class="fb-btn" data-v="skipped" title="Skipped">Skip</button>
    </div>`;

    wrap.innerHTML = `<div id="tip-card-main">${html}</div>`;

    // Wire outcome feedback buttons
    wrap.querySelectorAll('#tip-fb-row .fb-btn').forEach(btn => {
      btn.addEventListener('click', async function() {
        const row = document.getElementById('tip-fb-row');
        if (row.dataset.done) return;
        row.dataset.done = '1';
        row.querySelectorAll('.fb-btn').forEach(b => b.classList.remove('fb-selected'));
        this.classList.add('fb-selected');
        row.insertAdjacentHTML('beforeend', '<span class="fb-done">✓ recorded</span>');
        if (state.userId) {
          await apiFetch('/feedback', { method: 'POST', body: JSON.stringify({
            user_id: state.userId,
            outcome: this.dataset.v,
            pattern_id: patternId
          })}).catch(() => {});
        }
      });
    });

    // Wire Take This Trade button
    const takeBtn = wrap.querySelector('#tip-take-btn');
    const skipBtn = wrap.querySelector('#tip-skip-btn');
    const acceptRow = wrap.querySelector('#tip-accept-row');
    if (takeBtn && patternId) {
      takeBtn.addEventListener('click', async function() {
        if (acceptRow.dataset.done) return;
        acceptRow.dataset.done = '1';
        takeBtn.disabled = true; takeBtn.textContent = '…';
        if (skipBtn) skipBtn.disabled = true;
        try {
          const r = await apiFetch(`/tips/${patternId}/feedback`, {
            method: 'POST',
            body: JSON.stringify({ user_id: state.userId, action: 'taking_it', pattern_id: patternId })
          });
          const sym = (state._accountCurrency === 'GBP') ? '£' : '$';
          let confirmHtml = `<div class="tip-confirm" style="margin-top:10px;padding:10px 12px;border-radius:8px;background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.3);">`;
          confirmHtml += `<div style="font-weight:700;color:var(--green);margin-bottom:6px;">✅ Position added to Journal</div>`;
          if (r.entry_price != null) {
            confirmHtml += `<div style="font-size:12px;color:var(--muted);">Entry ${fmt(r.entry_price)} · Stop ${fmt(r.stop_loss)} · T1 ${fmt(r.target_1)}`;
            if (r.position_size) confirmHtml += ` · ${r.position_size} shares`;
            confirmHtml += `</div>`;
          }
          if (r.cash_after != null) {
            confirmHtml += `<div style="font-size:11px;color:var(--muted);margin-top:4px;">Cash remaining: ${sym}${Number(r.cash_after).toLocaleString('en-GB',{maximumFractionDigits:0})}</div>`;
          }
          confirmHtml += `<div style="margin-top:8px;"><a href="#" onclick="navigate('journal');return false;" style="color:var(--accent);font-size:12px;">→ View in Journal</a></div>`;
          confirmHtml += `</div>`;
          acceptRow.insertAdjacentHTML('afterend', confirmHtml);
          acceptRow.style.display = 'none';
        } catch(e) {
          takeBtn.disabled = false; takeBtn.textContent = '🎯 Take This Trade';
          acceptRow.dataset.done = '';
          showToast('Error: ' + (e.message || 'Could not accept tip'), 'error');
        }
      });
    }
    if (skipBtn && patternId) {
      skipBtn.addEventListener('click', async function() {
        if (acceptRow.dataset.done) return;
        acceptRow.dataset.done = '1';
        acceptRow.style.display = 'none';
        await apiFetch(`/tips/${patternId}/feedback`, {
          method: 'POST',
          body: JSON.stringify({ user_id: state.userId, action: 'not_for_me', pattern_id: patternId })
        }).catch(() => {});
      });
    }
  } catch(e) { wrap.innerHTML = `<div class="text-sm" style="color:var(--red)">${escHtml(e.message)}</div>`; }
});

async function loadTipsHistory() {
  if (!state.userId) return;
  const el = document.getElementById('tips-history');
  try {
    const d = await apiFetch(`/users/${state.userId}/delivery-history?limit=90`);
    const rows = d?.history || [];
    if (!rows.length) { el.innerHTML = '<div class="empty text-sm text-muted">No delivery history</div>'; return; }
    el.innerHTML = `<table class="tbl">
      <thead><tr><th>Date</th><th>Regime</th><th>Opportunities</th><th>Success</th><th>Length</th></tr></thead>
      <tbody>${rows.map(r => `<tr>
        <td class="mono-muted">${fmtDate(r.delivered_at || r.created_at)}</td>
        <td><span class="text-sm">${escHtml((r.regime_at_delivery||'').replace(/_/g,' '))||'—'}</span></td>
        <td class="mono-amber">${r.opportunities_count ?? '—'}</td>
        <td>${dot(r.success)}</td>
        <td class="mono-muted text-xs">${r.message_length ? r.message_length+'c' : '—'}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  } catch { el.innerHTML = '<div class="empty text-sm text-muted">History unavailable</div>'; }
}

