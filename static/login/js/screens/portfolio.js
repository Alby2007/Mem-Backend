// ── PORTFOLIO ─────────────────────────────────────────────────────────────────

// ── SIMULATED PORTFOLIO ──────────────────────────────────────────────────────

function _simKey() { return `simulated_${state.userId}`; }

function renderSimBanner(title, description, tipsAlignment, riskTolerance, holdingStyle, sectors) {
  const banner = document.getElementById('p-sim-banner');
  banner.style.display = 'block';
  banner.innerHTML = `
    <div class="sim-banner">
      <span class="sim-badge">⚠ Simulated</span>
      <span>This is a test portfolio — not real positions.</span>
      <span class="sim-clear" id="sim-clear-btn">Clear simulated flag</span>
    </div>
    <div class="sim-profile-card">
      <div class="sim-profile-title">🧪 Test Profile: ${escHtml(title)}</div>
      <div class="sim-profile-row"><span class="sim-profile-key">Description</span><span class="sim-profile-val">${escHtml(description)}</span></div>
      <div class="sim-profile-row"><span class="sim-profile-key">Risk tolerance</span><span class="sim-profile-val" style="text-transform:capitalize">${escHtml(riskTolerance)}</span></div>
      <div class="sim-profile-row"><span class="sim-profile-key">Holding style</span><span class="sim-profile-val" style="text-transform:capitalize">${escHtml(holdingStyle)}</span></div>
      <div class="sim-profile-row"><span class="sim-profile-key">Sectors</span><span class="sim-profile-val">${(sectors||[]).map(s => s.replace(/_/g,' ')).join(' · ')}</span></div>
      <div class="sim-profile-note"><strong>Tips alignment:</strong> ${escHtml(tipsAlignment)}</div>
    </div>`;
  document.getElementById('sim-clear-btn').addEventListener('click', () => {
    try { localStorage.removeItem(_simKey()); } catch { /* storage blocked */ }
    banner.style.display = 'none';
  });
}

function loadSimBannerIfSet() {
  if (!state.userId) return;
  let stored = null;
  try { stored = localStorage.getItem(_simKey()); } catch { /* storage blocked */ }
  if (!stored) return;
  try {
    const d = JSON.parse(stored);
    renderSimBanner(d.title, d.description, d.tips_alignment, d.risk_tolerance, d.holding_style, d.sectors);
  } catch { try { localStorage.removeItem(_simKey()); } catch { /* storage blocked */ } }
}

document.getElementById('p-gen-sim-btn').addEventListener('click', async () => {
  if (!state.userId) { showToast('Sign in first'); return; }
  const btn = document.getElementById('p-gen-sim-btn');
  btn.disabled = true;
  btn.textContent = '⚡ Generating…';
  try {
    const d = await apiFetch(`/users/${state.userId}/portfolio/generate-sim`, { method: 'POST' });
    if (!d) return;
    // Populate holdings in state
    state.holdings = (d.holdings || []).map(h => ({ ticker: h.ticker, quantity: h.quantity, avg_cost: h.avg_cost }));
    renderHoldings();
    // Render model card
    if (d.model) renderModelCard(d.model);
    // Store simulated state per-user in localStorage
    try { localStorage.setItem(_simKey(), JSON.stringify({
      title:          d.title,
      description:    d.description,
      tips_alignment: d.tips_alignment,
      risk_tolerance: d.risk_tolerance,
      holding_style:  d.holding_style,
      sectors:        d.sectors,
    })); } catch { /* storage blocked */ }
    renderSimBanner(d.title, d.description, d.tips_alignment, d.risk_tolerance, d.holding_style, d.sectors);
    showToast(`Test portfolio generated: ${d.title}`, 'ok');
  } catch(e) {
    showToast(e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = '⚡ Generate Test Portfolio';
  }
});

// ── Diverse test portfolio profiles ──────────────────────────────────────────
const _TEST_PROFILES = {
  uk_banks: {
    label: 'UK Banks',
    holdings: [
      { ticker: 'BARC.L',  quantity: 900,  avg_cost: 201.82 },
      { ticker: 'HSBA.L',  quantity: 700,  avg_cost: 747.82 },
      { ticker: 'LLOY.L',  quantity: 2500, avg_cost: 49.85  },
      { ticker: 'NWG.L',   quantity: 800,  avg_cost: 263.88 },
      { ticker: 'STAN.L',  quantity: 400,  avg_cost: 753.92 },
      { ticker: 'LSEG.L',  quantity: 80,   avg_cost: 991.55 },
    ],
  },
  us_tech: {
    label: 'US Tech',
    holdings: [
      { ticker: 'AAPL',  quantity: 50,  avg_cost: 172.50 },
      { ticker: 'MSFT',  quantity: 30,  avg_cost: 380.00 },
      { ticker: 'NVDA',  quantity: 20,  avg_cost: 480.00 },
      { ticker: 'GOOGL', quantity: 25,  avg_cost: 155.00 },
      { ticker: 'META',  quantity: 15,  avg_cost: 490.00 },
      { ticker: 'AMZN',  quantity: 18,  avg_cost: 185.00 },
      { ticker: 'TSLA',  quantity: 40,  avg_cost: 210.00 },
    ],
  },
  global_macro: {
    label: 'Global Macro',
    holdings: [
      { ticker: 'SPY',      quantity: 40,  avg_cost: 480.00 },
      { ticker: 'EEM',      quantity: 80,  avg_cost: 40.00  },
      { ticker: 'GLD',      quantity: 30,  avg_cost: 195.00 },
      { ticker: 'TLT',      quantity: 50,  avg_cost: 92.00  },
      { ticker: 'DX-Y.NYB', quantity: 0,   avg_cost: null   },
      { ticker: 'SHEL.L',   quantity: 200, avg_cost: 2610.00},
      { ticker: 'BP.L',     quantity: 500, avg_cost: 455.00 },
      { ticker: 'VOD.L',    quantity: 1200,avg_cost: 68.00  },
    ],
  },
  dividend_income: {
    label: 'Dividend Income',
    holdings: [
      { ticker: 'ULVR.L', quantity: 150, avg_cost: 3980.00 },
      { ticker: 'BATS.L', quantity: 200, avg_cost: 2680.00 },
      { ticker: 'NG.L',   quantity: 800, avg_cost: 985.00  },
      { ticker: 'TSCO.L', quantity: 600, avg_cost: 295.00  },
      { ticker: 'REL.L',  quantity: 120, avg_cost: 2100.00 },
      { ticker: 'AZN.L',  quantity: 80,  avg_cost: 11200.00},
      { ticker: 'GSK.L',  quantity: 200, avg_cost: 1640.00 },
      { ticker: 'BDEV.L', quantity: 300, avg_cost: 430.00  },
    ],
  },
  crypto_growth: {
    label: 'Crypto & Growth',
    holdings: [
      { ticker: 'MSTR',   quantity: 10,   avg_cost: 340.00  },
      { ticker: 'COIN',   quantity: 20,   avg_cost: 190.00  },
      { ticker: 'NVDA',   quantity: 15,   avg_cost: 480.00  },
      { ticker: 'PLTR',   quantity: 150,  avg_cost: 22.00   },
      { ticker: 'ARKK',   quantity: 100,  avg_cost: 42.00   },
      { ticker: 'SQ',     quantity: 40,   avg_cost: 68.00   },
      { ticker: 'HOOD',   quantity: 200,  avg_cost: 14.00   },
    ],
  },
  commodities: {
    label: 'Commodities & Energy',
    holdings: [
      { ticker: 'RIO.L',  quantity: 150, avg_cost: 5200.00 },
      { ticker: 'GLEN.L', quantity: 800, avg_cost: 420.00  },
      { ticker: 'AAL.L',  quantity: 500, avg_cost: 195.00  },
      { ticker: 'BHP.L',  quantity: 200, avg_cost: 2100.00 },
      { ticker: 'SHEL.L', quantity: 180, avg_cost: 2610.00 },
      { ticker: 'BP.L',   quantity: 400, avg_cost: 455.00  },
      { ticker: 'GLD',    quantity: 25,  avg_cost: 195.00  },
      { ticker: 'SLV',    quantity: 60,  avg_cost: 22.00   },
    ],
  },
};

document.getElementById('p-change-profile-select').addEventListener('change', async function() {
  const key = this.value;
  if (!key) return;
  this.value = ''; // reset dropdown
  if (!state.userId) { showToast('Sign in first'); return; }
  const profile = _TEST_PROFILES[key];
  if (!profile) return;

  // Load holdings into state
  state.holdings = profile.holdings.map(h => ({ ...h }));
  renderHoldings();

  // Auto-submit to backend
  const msg = document.getElementById('p-msg');
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch(`/users/${state.userId}/portfolio`, {
      method: 'POST',
      body: JSON.stringify({ holdings: state.holdings }),
    });
    if (!d) return;
    msg.style.color = 'var(--green)';
    msg.textContent = `✓ ${profile.label} loaded — ${d.count} holdings`;
    try { localStorage.removeItem(`simulated_${state.userId}`); } catch { /* storage blocked */ }
    document.getElementById('p-sim-banner').style.display = 'none';
    loadPortfolioModel();
    showToast(`${profile.label} portfolio loaded`, 'ok');
  } catch(e) {
    msg.style.color = 'var(--red)';
    msg.textContent = e.message;
  }
});

// FTSE sector quick-add
const FTSE_SECTORS = {
  'FTSE Banks':  ['HSBA.L','LLOY.L','BARC.L','NWG.L','STAN.L'],
  'FTSE Energy': ['SHEL.L','BP.L'],
  'FTSE Mining': ['RIO.L','AAL.L','GLEN.L','BHP.L'],
  'FTSE Pharma': ['AZN.L','GSK.L','HIK.L'],
  'FTSE Tech':   ['SAGE.L','AUTO.L','LSEG.L'],
};

// Autocomplete ticker list — seeded from /universe/coverage, topped up with FTSE defaults
let _acTickers = [];
const _FTSE_FALLBACK = [
  'SHEL.L','AZN.L','HSBA.L','ULVR.L','BP.L','GSK.L','RIO.L','BATS.L','VOD.L',
  'LLOY.L','BARC.L','NWG.L','LSEG.L','REL.L','NG.L','BA.L','RR.L','TSCO.L',
  'MKS.L','BDEV.L','PSN.L','AAL.L','GLEN.L','STAN.L','HIK.L','AUTO.L','SAGE.L',
  'GBPUSD=X','EURGBP=X','GLD','^FTSE','^FTMC','^GSPC','^VIX',
];

async function loadTickerList() {
  try {
    const d = await apiFetch('/universe/coverage');
    const remote = (d?.tickers || []).map(t => t.ticker || t).filter(Boolean);
    _acTickers = [...new Set([...remote, ..._FTSE_FALLBACK])];
  } catch {
    _acTickers = [..._FTSE_FALLBACK];
  }
}

// Sector metadata for ptf-sector-btn cards
const FTSE_SECTOR_META = {
  'FTSE Banks':  { icon: '🏦', count: 5 },
  'FTSE Energy': { icon: '⚡', count: 2 },
  'FTSE Mining': { icon: '⛏', count: 4 },
  'FTSE Pharma': { icon: '💊', count: 3 },
  'FTSE Tech':   { icon: '💻', count: 3 },
};

// Build sector buttons
(function buildSectorBtns() {
  const wrap = document.getElementById('p-sector-btns');
  if (!wrap) return;
  Object.keys(FTSE_SECTORS).forEach(label => {
    const meta = FTSE_SECTOR_META[label] || { icon: '📊', count: FTSE_SECTORS[label].length };
    const btn = document.createElement('div');
    btn.className = 'ptf-sector-btn';
    btn.innerHTML = `<span class="ptf-sector-icon">${meta.icon}</span><span class="ptf-sector-name">${label}</span><span class="ptf-sector-count">${meta.count} stocks</span>`;
    btn.addEventListener('click', () => {
      const tickers = FTSE_SECTORS[label];
      const existing = new Set(state.holdings.map(h => h.ticker));
      let added = 0;
      tickers.forEach(t => {
        if (!existing.has(t)) {
          state.holdings.push({ ticker: t, quantity: 0, avg_cost: null });
          added++;
        }
      });
      btn.classList.toggle('selected', true);
      renderHoldings();
      ptfSwitchTab('manual', document.querySelector('.ptf-tab-btn'));
      if (added === 0) showToast(`All ${label} tickers already added`, 'ok');
      else showToast(`${added} ${label} holdings added`, 'ok');
    });
    wrap.appendChild(btn);
  });
})();

// Autocomplete
const _acInput = document.getElementById('p-ticker');
const _acDrop  = document.getElementById('p-ac-dropdown');
let _acIdx = -1;

function _acShow(matches) {
  if (!matches.length) { _acDrop.classList.remove('open'); return; }
  _acDrop.innerHTML = matches.map((t, i) =>
    `<div class="ac-item" data-ticker="${escHtml(t)}">${escHtml(t)}</div>`
  ).join('');
  _acDrop.classList.add('open');
  _acIdx = -1;
  _acDrop.querySelectorAll('.ac-item').forEach(el => {
    el.addEventListener('mousedown', e => {
      e.preventDefault();
      _acInput.value = el.dataset.ticker;
      _acDrop.classList.remove('open');
    });
  });
}

_acInput.addEventListener('input', () => {
  const q = _acInput.value.trim().toUpperCase();
  if (q.length < 2) { _acDrop.classList.remove('open'); return; }
  const matches = _acTickers.filter(t =>
    t.toUpperCase().startsWith(q) || t.toUpperCase().includes(q)
  ).slice(0, 6);
  _acShow(matches);
});

_acInput.addEventListener('keydown', e => {
  const items = _acDrop.querySelectorAll('.ac-item');
  if (!items.length) return;
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    _acIdx = Math.min(_acIdx + 1, items.length - 1);
    items.forEach((el, i) => el.classList.toggle('selected', i === _acIdx));
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    _acIdx = Math.max(_acIdx - 1, 0);
    items.forEach((el, i) => el.classList.toggle('selected', i === _acIdx));
  } else if (e.key === 'Enter' && _acIdx >= 0) {
    e.preventDefault();
    _acInput.value = items[_acIdx].dataset.ticker;
    _acDrop.classList.remove('open');
  } else if (e.key === 'Escape') {
    _acDrop.classList.remove('open');
  }
});

document.addEventListener('click', e => {
  if (!e.target.closest('.autocomplete-wrap')) _acDrop.classList.remove('open');
});

// ── Portfolio tab switcher ──────────────────────────────────────────
function ptfSwitchTab(name, btn) {
  document.querySelectorAll('.ptf-tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.ptf-tab-content').forEach(c => c.classList.remove('active'));
  if (btn) btn.classList.add('active');
  const el = document.getElementById('ptf-tab-' + name);
  if (el) el.classList.add('active');
}

// ── Setup progress helpers ──────────────────────────────────────────
function ptfMarkStep2Done() {
  const num = document.getElementById('ptf-step2-num');
  const name = document.getElementById('ptf-step2-name');
  const action = document.getElementById('ptf-step2-action');
  if (num)    { num.classList.remove('active'); num.classList.add('done'); num.textContent = '✓'; }
  if (name)   { name.classList.add('done'); }
  if (action) { action.className = 'ptf-step-done-mark'; action.textContent = '✓'; }
  const countEl = document.getElementById('ptf-setup-count');
  if (countEl) countEl.textContent = '2 of 4 done';
  const step3 = document.getElementById('ptf-step3-num');
  if (step3) step3.classList.add('active');
  const banner = document.getElementById('ptf-setup-banner');
  if (banner) banner.style.display = 'none';
}

function ptfUpdateHoldingCount() {
  const realHoldings = state.holdings.filter(h => !h.is_cash);
  const countEl = document.getElementById('ptf-holding-count');
  if (countEl) countEl.textContent = realHoldings.length ? `${realHoldings.length} holding${realHoldings.length !== 1 ? 's' : ''}` : '';
}

// ── Portfolio Summary renderer ────────────────────────────────────
function ptfRenderSummary() {
  const body = document.getElementById('ptf-summary-body');
  if (!body) return;
  const realHoldings = state.holdings.filter(h => !h.is_cash);
  const cashRow = state.holdings.find(h => h.is_cash);
  if (!realHoldings.length) {
    body.innerHTML = `<div class="ptf-summary-empty"><span class="pse-icon">📊</span><div class="pse-title">No portfolio yet</div><div class="pse-sub">Submit your holdings to see total value, P&L, and sector exposure.</div></div>`;
    return;
  }
  const count = realHoldings.length;
  const cashAmt = cashRow ? Math.abs(cashRow.avg_cost || 0) : 0;
  const cashCcy = cashRow ? (cashRow.currency || 'GBP') : 'GBP';
  const cashSym = _cashSym(cashCcy);
  body.innerHTML = `
    <div class="ptf-summary-stats">
      <div class="ptf-stat-cell">
        <div class="ptf-stat-label">Holdings</div>
        <div class="ptf-stat-value" style="font-size:22px;">${count}</div>
        <div class="ptf-stat-sub">positions tracked</div>
      </div>
      <div class="ptf-stat-cell">
        <div class="ptf-stat-label">Cash Available</div>
        <div class="ptf-stat-value amber">${cashAmt > 0 ? cashSym + cashAmt.toLocaleString('en-GB', {maximumFractionDigits:0}) : '—'}</div>
        <div class="ptf-stat-sub">${cashAmt > 0 ? cashCcy : 'not set'}</div>
      </div>
    </div>`;
}

// Holdings render
const _CCY_SYM = { GBP:'£', USD:'$', EUR:'\u20ac', CHF:'Fr', JPY:'\u00a5', CAD:'$', AUD:'$', HKD:'$', SGD:'$' };
function _cashSym(ccy) { return _CCY_SYM[ccy] || ccy + ' '; }

function renderHoldings() {
  const tbody = document.getElementById('p-holdings-list');
  if (!tbody) return;
  const allRows = state.holdings;
  ptfUpdateHoldingCount();
  ptfRenderSummary();
  if (!allRows.length) {
    tbody.innerHTML = '<tr class="ptf-empty-row"><td colspan="5">No holdings added yet</td></tr>';
    return;
  }
  tbody.innerHTML = allRows.map((h, i) => {
    if (h.is_cash) {
      const ccy = h.currency || 'GBP';
      const sym = _cashSym(ccy);
      const isNeg = (h.avg_cost || 0) < 0;
      const val = Math.abs(h.avg_cost || 0).toLocaleString('en-GB', {maximumFractionDigits:0});
      return `<tr>
        <td colspan="3"><span class="ptf-h-cash">💰 ${escHtml(ccy)} Cash</span><div class="ptf-h-meta">${isNeg ? '⚠ overcommitted' : 'available'}</div></td>
        <td class="ptf-pnl ${isNeg ? 'neg' : 'pos'}">${isNeg ? '−' : ''}${sym}${val}</td>
        <td><button class="ptf-del-btn" onclick="clearCashHolding()" title="Remove cash">×</button></td>
      </tr>`;
    }
    const sym = _cashSym(h.currency || 'GBP');
    const costStr = h.avg_cost != null ? sym + Number(h.avg_cost).toLocaleString('en-GB', {minimumFractionDigits:2, maximumFractionDigits:2}) : '—';
    return `<tr>
      <td><div class="ptf-h-ticker">${escHtml(h.ticker)}</div></td>
      <td style="color:var(--muted);">${h.quantity != null && h.quantity !== 0 ? h.quantity.toLocaleString() : '—'}</td>
      <td style="color:var(--muted);">${costStr}</td>
      <td><span class="ptf-pnl" style="color:var(--muted);">—</span></td>
      <td><button class="ptf-del-btn" onclick="removeHolding(${i})" title="Remove">×</button></td>
    </tr>`;
  }).join('');
}

window.clearCashHolding = async function() {
  if (!state.userId) return;
  try {
    await apiFetch('/users/' + state.userId + '/cash', {
      method: 'POST', body: JSON.stringify({ available_cash: null, cash_currency: 'GBP' })
    });
  } catch(e) { /* silent */ }
  state.holdings = state.holdings.filter(h => !h.is_cash);
  state._availableCash = null;
  renderHoldings();
  const cashDisplay = document.getElementById('tip-cash-display');
  const cashNudge   = document.getElementById('tip-cash-nudge');
  if (cashDisplay) { cashDisplay.textContent = '\u2014'; cashDisplay.style.color = 'var(--accent)'; }
  if (cashNudge) cashNudge.innerHTML = '<a href="#" id="cash-set-nudge" style="color:var(--accent);text-decoration:none;font-size:11px;">Set balance \u2192 (Portfolio tab)</a>';
};

window.removeHolding = function(i) { state.holdings.splice(i, 1); renderHoldings(); _autoSaveHoldings(); };
window.removeCashHolding = window.clearCashHolding;

// Auto-save debounce: persist holdings to backend 1s after last change
let _autoSaveTimer = null;
function _autoSaveHoldings() {
  if (!state.userId) return;
  clearTimeout(_autoSaveTimer);
  _autoSaveTimer = setTimeout(async () => {
    const holdingsToSubmit = state.holdings.filter(h => !h.is_cash);
    if (!holdingsToSubmit.length) return;
    try {
      await apiFetch(`/users/${state.userId}/portfolio`, {
        method: 'POST',
        body: JSON.stringify({ holdings: holdingsToSubmit }),
      });
    } catch { /* silent — user can still manually submit */ }
  }, 1000);
}

document.getElementById('p-add-btn').addEventListener('click', () => {
  const ticker = (_acInput.value || '').trim().toUpperCase();
  const qty    = parseFloat(document.getElementById('p-qty').value) || 0;
  const cost   = parseFloat(document.getElementById('p-cost').value) || null;
  if (!ticker) { showToast('Enter a ticker'); return; }
  _acDrop.classList.remove('open');
  state.holdings.push({ ticker, quantity: qty, avg_cost: cost });
  _acInput.value = '';
  document.getElementById('p-qty').value = '';
  document.getElementById('p-cost').value = '';
  renderHoldings();
  _autoSaveHoldings();
});

// Screenshot upload
const _dropZone = document.getElementById('p-drop-zone');
const _fileInput = document.getElementById('p-screenshot-input');

['dragenter','dragover'].forEach(ev => _dropZone.addEventListener(ev, e => {
  e.preventDefault(); _dropZone.classList.add('drag-over');
}));
['dragleave','drop'].forEach(ev => _dropZone.addEventListener(ev, e => {
  e.preventDefault(); _dropZone.classList.remove('drag-over');
}));
_dropZone.addEventListener('drop', e => {
  const file = e.dataTransfer?.files?.[0];
  if (file) handleScreenshot(file);
});
_fileInput.addEventListener('change', () => {
  if (_fileInput.files?.[0]) handleScreenshot(_fileInput.files[0]);
});

async function handleScreenshot(file) {
  if (!state.userId) { showToast('Sign in first'); return; }
  const sMsg = document.getElementById('p-screenshot-msg');
  sMsg.innerHTML = '<span class="spinner"></span> Analysing screenshot…';
  sMsg.style.color = 'var(--muted)';

  const formData = new FormData();
  formData.append('file', file);

  try {
    const headers = {};
    if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
    const res = await fetch(`${API}/users/${state.userId}/history/screenshot`, {
      method: 'POST', headers, body: formData,
    });
    const d = await res.json();

    if (!d.vision_available) {
      sMsg.style.color = 'var(--muted)';
      sMsg.textContent = '⚠ Vision model not available — add holdings manually below';
      return;
    }
    if (!d.holdings?.length) {
      sMsg.style.color = 'var(--muted)';
      sMsg.textContent = 'No holdings detected — try a clearer screenshot or add manually';
      return;
    }

    // Merge into state.holdings (skip duplicates)
    const existing = new Set(state.holdings.map(h => h.ticker));
    let added = 0;
    d.holdings.forEach(h => {
      if (!existing.has(h.ticker)) {
        state.holdings.push(h);
        added++;
      }
    });
    renderHoldings();
    sMsg.style.color = 'var(--green)';
    sMsg.textContent = `✓ ${added} holding${added !== 1 ? 's' : ''} extracted — review and submit`;
  } catch(e) {
    sMsg.style.color = 'var(--red)';
    sMsg.textContent = 'Upload failed: ' + e.message;
  }
  // Reset file input so same file can be re-uploaded
  _fileInput.value = '';
}

// ── Portfolio mode toggle (Holdings / Cash) ────────────────────────────────
(function() {
  const btnH = document.getElementById('p-mode-holdings');
  const btnC = document.getElementById('p-mode-cash');
  const panelH = document.getElementById('p-holdings-panel');
  const panelC = document.getElementById('p-cash-panel');
  const submitBtn = document.getElementById('p-submit-btn');
  const holdingsList = document.getElementById('p-holdings-list');

  function _setMode(mode) {
    if (mode === 'cash') {
      panelH.style.display = 'none';
      panelC.style.display = '';
      submitBtn.style.display = 'none';
      holdingsList.style.display = 'none';
      btnC.style.background = 'var(--accent)'; btnC.style.color = '#000';
      btnH.style.background = 'transparent'; btnH.style.color = 'var(--muted)';
      // Pre-fill with current balance and detect currency
      if (state.userId) {
        apiFetch('/users/' + state.userId + '/cash').then(r => {
          if (!r) return;
          const sym = (r.currency === 'GBP') ? '£' : '$';
          document.getElementById('p-cash-currency-symbol').textContent = sym;
          if (r.available_cash != null) {
            document.getElementById('p-cash-input').value = r.available_cash;
          }
        }).catch(() => {});
      }
    } else {
      panelH.style.display = '';
      panelC.style.display = 'none';
      submitBtn.style.display = '';
      holdingsList.style.display = '';
      btnH.style.background = 'var(--accent)'; btnH.style.color = '#000';
      btnC.style.background = 'transparent'; btnC.style.color = 'var(--muted)';
    }
  }

  btnH.addEventListener('click', () => _setMode('holdings'));
  btnC.addEventListener('click', () => _setMode('cash'));

  // Pre-fill cash panel on mode switch to 'cash'
  const origSetMode = _setMode;
  // Override to also pre-fill currency select
  btnC.addEventListener('click', () => {
    if (state.userId) {
      apiFetch('/users/' + state.userId + '/cash').then(r => {
        if (!r) return;
        const sel = document.getElementById('p-cash-currency');
        if (sel && r.cash_currency) {
          for (let i = 0; i < sel.options.length; i++) {
            if (sel.options[i].value === r.cash_currency) { sel.selectedIndex = i; break; }
          }
        }
        if (r.available_cash != null) {
          document.getElementById('p-cash-input').value = r.available_cash;
        }
      }).catch(() => {});
    }
  });
})();

document.getElementById('p-cash-save-btn').addEventListener('click', async () => {
  if (!state.userId) { showToast('Sign in first'); return; }
  const msg = document.getElementById('p-cash-msg');
  const raw = parseFloat(document.getElementById('p-cash-input').value);
  if (isNaN(raw)) { msg.style.color = 'var(--red)'; msg.textContent = 'Enter a valid amount'; return; }
  const ccy = document.getElementById('p-cash-currency').value || 'GBP';
  const sym = _cashSym(ccy);
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch('/users/' + state.userId + '/cash', {
      method: 'POST',
      body: JSON.stringify({ available_cash: raw, cash_currency: ccy })
    });
    if (!d) return;
    state._availableCash = raw;
    state._accountCurrency = ccy;
    msg.style.color = 'var(--green)';
    msg.textContent = `\u2713 Cash balance set to ${sym}${raw.toLocaleString('en-GB', {maximumFractionDigits:0})} ${ccy}`;
    // Update in-memory holdings to reflect cash row
    state.holdings = state.holdings.filter(h => !h.is_cash);
    state.holdings.push({ ticker: 'CASH:' + ccy, quantity: 1, avg_cost: raw, currency: ccy, is_cash: true, value: raw });
    renderHoldings();
    // Refresh tips cash display
    const cashDisplay = document.getElementById('tip-cash-display');
    const cashNudge   = document.getElementById('tip-cash-nudge');
    if (cashDisplay) {
      const isNeg = raw < 0;
      cashDisplay.style.color = isNeg ? 'var(--red)' : 'var(--accent)';
      cashDisplay.textContent = sym + Math.abs(raw).toLocaleString('en-GB', {maximumFractionDigits:0}) + (isNeg ? ' \u26a0 overcommitted' : '');
      if (cashNudge) cashNudge.textContent = '';
    }
    setTimeout(() => { msg.textContent = ''; }, 4000);
  } catch(e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

document.getElementById('p-cash-clear-btn').addEventListener('click', async () => {
  document.getElementById('p-cash-input').value = '';
  await window.clearCashHolding();
  const msg = document.getElementById('p-cash-msg');
  msg.style.color = 'var(--green)'; msg.textContent = '\u2713 Cash balance cleared';
  setTimeout(() => { msg.textContent = ''; }, 3000);
});

document.getElementById('p-submit-btn').addEventListener('click', async () => {
  if (!state.userId) { showToast('Sign in first'); return; }
  const msg = document.getElementById('p-msg');
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    // Exclude cash virtual holdings — cash is managed separately via /cash endpoint
    const holdingsToSubmit = state.holdings.filter(h => !h.is_cash);
    const d = await apiFetch(`/users/${state.userId}/portfolio`, {
      method: 'POST',
      body: JSON.stringify({ holdings: holdingsToSubmit })
    });
    if (!d) return;
    msg.style.color = 'var(--green)';
    msg.textContent = `✓ ${d.count} holdings submitted`;
    // Manual submit clears simulated flag
    try { if (state.userId) localStorage.removeItem(`simulated_${state.userId}`); } catch { /* storage blocked */ }
    document.getElementById('p-sim-banner').style.display = 'none';
    ptfMarkStep2Done();
    loadPortfolioModel();
  } catch(e) { msg.style.color='var(--red)'; msg.textContent = e.message; }
});

function renderModelCard(m) {
  if (!m) return;
  const _val = (v, fallback) => v != null && v !== '' ? String(v) : fallback;
  const _cls = (v, fallback) => (v == null || v === '' || v === fallback) ? 'unknown' : '';
  const fields = [
    ['risk', 'Risk Tolerance', _val(m.risk_tolerance, '—')],
    ['style', 'Holding Style', _val(m.holding_style, '—')],
    ['beta', 'Portfolio Beta', m.portfolio_beta != null ? fmt(m.portfolio_beta) : '—'],
    ['pattern', 'Preferred Pattern', _val(m.preferred_pattern, 'Learning\u2026')],
    ['winrate', 'Avg Win Rate', m.avg_win_rate != null ? fmt(m.avg_win_rate*100)+'%' : 'No history yet'],
    ['sector', 'Sector Affinity', _val((m.sector_affinity||[]).join(', '), '—')],
  ];
  document.getElementById('p-model-card').innerHTML =
    `<div class="ptf-model-grid">${fields.map(([id, label, val]) =>
      `<div><div class="ptf-m-label">${label}</div><div class="ptf-m-value ${_cls(val, ['—','Learning\u2026','No history yet'].includes(val) ? val : null)}" id="ptf-m-${id}">${escHtml(val)}</div></div>`
    ).join('')}</div>`;
}

async function loadPortfolioHoldings() {
  if (!state.userId) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/portfolio`);
    const holdings = d?.holdings || [];
    if (holdings.length) {
      state.holdings = holdings.map(h => h.is_cash
        ? { ticker: h.ticker, quantity: h.quantity, avg_cost: h.avg_cost, currency: h.currency, is_cash: true, value: h.value }
        : { ticker: h.ticker, quantity: h.quantity, avg_cost: h.avg_cost, currency: h.currency, sector: h.sector || '' });
      renderHoldings();
      ptfMarkStep2Done();
    }
  } catch { /* no holdings yet */ }
}

async function loadPortfolioModel() {
  if (!state.userId) return;
  try {
    const m = await apiFetch(`/users/${state.userId}/preferences/inferred`);
    if (m) renderModelCard(m);
  } catch { /* model not ready yet */ }

  // watchlist signals
  const sigEl = document.getElementById('p-signals');
  try {
    const ws = await apiFetch(`/users/${state.userId}/watchlist-signals`);
    const sigs = ws?.signals || [];
    // Cache tickers for feedback widget gating
    state.watchlistTickers = sigs.map(s => s.ticker).filter(Boolean);
    if (!sigs.length) {
      sigEl.innerHTML = `<div class="ptf-signals-empty"><span class="pse-icon">📡</span><div class="pse-title">No signals yet</div><div class="pse-sub">Signals appear here as patterns form on your holdings. Check back after the next KB update.</div></div>`;
      return;
    }
    const rows = sigs.slice(0, 10).map(s => {
      const dir = (s.signal_direction || '').toLowerCase();
      const isBull = dir === 'bullish' || dir === 'long';
      const dirCls = isBull ? 'bullish' : 'bearish';
      const dirLabel = isBull ? 'BULL' : 'BEAR';
      const upside = s.upside_pct != null ? `<div class="ptf-sig-type" style="color:${s.upside_pct>=0?'var(--green)':'var(--red)'}">${s.upside_pct>=0?'+':''}${fmt(s.upside_pct)}%</div>` : '';
      const qScore = s.signal_quality != null ? `<div class="ptf-sig-q">Q ${fmt(s.signal_quality)}</div>` : '';
      return `<div class="ptf-signal-row">
        <div class="ptf-sig-ticker">${escHtml(s.ticker)}</div>
        <div class="ptf-sig-dir ${dirCls}">${dirLabel}</div>
        <div class="ptf-sig-info">${upside}${qScore}</div>
      </div>`;
    }).join('');
    sigEl.innerHTML = rows + `<div class="ptf-signals-footer"><a href="#" onclick="navigate('patterns');return false;">View all patterns for your holdings →</a></div>`;
  } catch {
    if (sigEl) sigEl.innerHTML = `<div class="ptf-signals-empty"><span class="pse-icon">📡</span><div class="pse-title">Signals unavailable</div><div class="pse-sub">Could not load signal data right now.</div></div>`;
  }
}

