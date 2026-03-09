// ── CHAT ─────────────────────────────────────────────────────────────────── v2
let sessionId = `s_${Date.now()}`;

function computeMarketStress(ga) {
  if (!ga || typeof ga !== 'object') return null;
  let score = 0;
  const vol = (ga.volatility_regime || '').toLowerCase();
  const v = vol.includes('extreme') ? 0.40 : vol.includes('high') ? 0.25 : vol.includes('low') ? 0.0 : vol ? 0.10 : 0.15;
  score += v;
  const regime = (ga.price_regime || '').toLowerCase();
  const r = /overbought|near_52w_high|extended/.test(regime) ? 0.20
    : /near_52w_low|breakdown|downtrend/.test(regime) ? 0.15
    : regime.includes('mid_range') ? 0.05 : regime ? 0.08 : 0.10;
  score += r;
  let pcr = 0.5;
  try { pcr = parseFloat(ga.put_call_oi_ratio) || 0.5; } catch(e) {}
  const smart = (ga.smart_money_signal || '').toLowerCase();
  const p = (smart.includes('put_sweep') || pcr > 0.80) ? 0.25
    : (smart.includes('call_sweep') && pcr < 0.40) ? 0.0
    : pcr > 0.65 ? 0.15 : 0.08;
  score += p;
  const composite = Math.min(score, 1.0);
  return {
    composite: Math.round(composite * 1000) / 1000,
    label: composite < 0.30 ? 'LOW' : composite < 0.60 ? 'MED' : 'HIGH',
    vol_regime: ga.volatility_regime || '',
    price_regime: ga.price_regime || '',
    smart_money: ga.smart_money_signal || '',
    put_call_ratio: pcr,
  };
}

function extractKbGrounding(rawAnswer) {
  const m = rawAnswer.match(/\[KB_GROUNDING\]([\s\S]*?)\[\/KB_GROUNDING\]/);
  if (!m) return { prose: rawAnswer, grounding: null };
  const prose = rawAnswer.replace(m[0], '').trim();
  const rows = m[1].trim().split('\n')
    .map(l => l.trim()).filter(Boolean)
    .map(l => {
      const colon = l.indexOf(':');
      return colon > -1 ? { key: l.slice(0, colon).trim(), val: l.slice(colon + 1).trim() } : null;
    }).filter(Boolean);
  return { prose, grounding: rows.length ? rows : null };
}

function renderKbPanel(grounding, groundingAtoms) {
  // Predicate filter — drop LLM hallucinations before merging into KB card
  const _SKIP_TAGS = new Set(['key_finding','quality','reasoning','confidence score','tag','label','ticker']);
  const _validPred = k => k && !k.includes(' ') && k.length <= 40 && !_SKIP_TAGS.has(k.toLowerCase());

  // Merge: LLM rows first (normalise 'regime' -> 'price_regime'), DB wins on conflicts
  const merged = {};
  // Key aliases: normalise LLM-written keys to canonical form
  const _KEY_ALIAS = {
    'regime': 'price_regime',
    'last price': 'last_price', 'last price (usd)': 'last_price', 'last-price': 'last_price',
    'current price': 'last_price', 'current_price': 'last_price',
    'price target': 'price_target', 'target price': 'price_target', 'price-target': 'price_target',
    'upside pct': 'upside_pct', 'upside percent': 'upside_pct', 'upside percentage': 'upside_pct',
    'upside_percentage': 'upside_pct', 'upside_potential': 'upside_pct',
    'invalidation price': 'invalidation_price', 'invalidation-price': 'invalidation_price',
  };
  if (grounding) {
    grounding.filter(r => _validPred(r.key.trim())).forEach(r => {
      const raw = r.key.trim();
      const key = _KEY_ALIAS[raw] || _KEY_ALIAS[raw.replace(/\s+/g, '_')] || raw.replace(/\s+/g, '_');
      if (r.val) merged[key] = r.val;
    });
  }
  if (groundingAtoms && typeof groundingAtoms === 'object') {
    Object.entries(groundingAtoms).forEach(([k, v]) => { if (v && _validPred(k)) merged[k] = v; });
  }
  if (!Object.keys(merged).length) return '';

  const dir = (merged.signal_direction || '').toLowerCase();
  const isBearish = dir === 'bearish';
  const cardClass = isBearish ? 'sig-card bearish' : 'sig-card';
  const arrow = isBearish ? '▼' : '▲';
  const dirWord = isBearish ? 'Bearish' : (dir ? 'Bullish' : '—');

  const conv = (merged.conviction_tier || '').toLowerCase();
  const pillClass = conv === 'high' ? 'conviction-pill high' : (conv === 'low' ? 'conviction-pill low' : 'conviction-pill');
  const pillText = conv === 'high' ? 'HIGH' : (conv === 'low' ? 'LOW' : 'MED');

  const ticker = (groundingAtoms && groundingAtoms.ticker)
    ? `<span class="sig-ticker-badge">${escHtml(groundingAtoms.ticker.toUpperCase())}</span>`
    : '';

  // Price strip — pull from merged atoms (DB wins via merge)
  const _price = (k) => {
    const v = merged[k];
    if (v == null || v === '') return null;
    const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
    return isNaN(n) ? String(v) : n;
  };
  const lastPrice   = _price('last_price');
  const priceTarget = _price('price_target');
  const upsidePct   = _price('upside_pct');
  const invalidation = _price('invalidation_price');
  const hasPriceStrip = lastPrice != null || priceTarget != null;

  const fmt$ = v => v != null ? `$${Number(v).toLocaleString('en-US', {minimumFractionDigits:2,maximumFractionDigits:2})}` : null;
  const fmtPct = v => {
    if (v == null) return null;
    const n = parseFloat(v);
    if (isNaN(n)) return String(v);
    return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
  };
  const upsideSign = upsidePct != null && parseFloat(upsidePct) < 0;
  const upsideCls = upsideSign ? 'sig-price-upside bearish' : 'sig-price-upside';

  const priceStripHtml = hasPriceStrip ? `
    <div class="sig-price-strip">
      ${lastPrice   != null ? `<span class="sig-price-current">${escHtml(fmt$(lastPrice))}</span>` : ''}
      ${(lastPrice != null && priceTarget != null) ? `<span class="sig-price-arrow">→</span>` : ''}
      ${priceTarget != null ? `<span class="sig-price-target">${escHtml(fmt$(priceTarget))}</span>` : ''}
      ${upsidePct   != null ? `<span class="${upsideCls}">${escHtml(fmtPct(upsidePct))}</span>` : ''}
    </div>
    ${invalidation != null ? `<div class="sig-price-invalidation">invalidation ${escHtml(fmt$(invalidation))}</div>` : ''}` : '';

  // Tags: whitelist of semantic keys only
  const _TAG_ORDER = ['price_regime','volatility_regime','smart_money_signal','sector'];
  const _TAG_DISPLAY = {
    price_regime:      v => v.replace(/_/g, ' '),
    volatility_regime: v => v.replace('high_volatility', 'high_vol').replace('extreme_volatility', 'extreme_vol').replace(/_/g, ' '),
    sector:            v => v.replace('financial services', 'fin. services'),
  };
  const tagKeys = _TAG_ORDER.filter(k => k in merged && merged[k]);
  const tags = tagKeys.map(k => {
    const raw = String(merged[k]);
    const display = _TAG_DISPLAY[k] ? _TAG_DISPLAY[k](raw) : raw.replace(/_/g, ' ');
    const volLow = raw.toLowerCase();
    let cls = 'sig-tag';
    if (k === 'volatility_regime' && (volLow.includes('high') || volLow.includes('extreme'))) cls += ' vol-high';
    if (k === 'smart_money_signal') cls += ' smart-money';
    return `<div class="${cls}">${escHtml(display)}</div>`;
  }).join('');

  return `<div class="${cardClass}">
    <div class="sig-header">
      <span class="sig-header-label">KB Grounding</span>
      ${ticker}
    </div>
    <div class="sig-card-inner">
      <div class="sig-direction">
        <div class="sig-arrow">${arrow}</div>
        <div class="sig-word">${escHtml(dirWord)}</div>
        <div class="${pillClass}">${pillText}</div>
      </div>
      <div class="sig-tags">
        ${priceStripHtml}
        <div class="sig-tag-row">${tags}</div>
      </div>
    </div>
  </div>`;
}

function renderCalibrationBadge(cal) {
  if (!cal) return '';
  const t1Pct = cal.hit_rate_t1 != null ? Math.round(cal.hit_rate_t1 * 100) : null;
  const t2Pct = cal.hit_rate_t2 != null ? Math.round(cal.hit_rate_t2 * 100) : null;
  const n = cal.n_total != null ? Number(cal.n_total).toLocaleString() : '—';
  const pat = (cal.pattern_type || '').replace(/_/g, ' ').toUpperCase();
  const tf  = (cal.timeframe || '').toUpperCase();
  const regime = cal.market_regime || 'all regimes';
  const confRaw = (cal.confidence_label || '').toLowerCase();
  const confClass = confRaw === 'moderate' ? 'cal-conf-badge moderate'
    : confRaw === 'low' ? 'cal-conf-badge low'
    : 'cal-conf-badge';
  const confText = cal.confidence_label || '';
  const footerParts = [pat, tf, regime].filter(Boolean).join(' · ');
  return `<div class="cal-panel">
    <div class="cal-header">
      <span class="cal-header-label">Calibration</span>
      <span class="cal-sample">n=${escHtml(n)} setups</span>
    </div>
    <div class="cal-body">
      <div class="cal-row">
        <div class="cal-row-head">
          <span class="cal-row-label">Target 1 hit rate</span>
          <span class="cal-row-pct">${t1Pct != null ? t1Pct + '%' : '—'}</span>
        </div>
        <div class="cal-bar-track"><div class="cal-bar-fill" style="width:0%"></div></div>
      </div>
      <div class="cal-row">
        <div class="cal-row-head">
          <span class="cal-row-label">Target 2 hit rate</span>
          <span class="cal-row-pct t2">${t2Pct != null ? t2Pct + '%' : '—'}</span>
        </div>
        <div class="cal-bar-track"><div class="cal-bar-fill t2" style="width:0%"></div></div>
      </div>
    </div>
    <div class="cal-footer">
      <span class="cal-pattern">${escHtml(footerParts)}</span>
      <span class="${confClass}">${escHtml(confText)}</span>
    </div>
  </div>`;
}

function renderEpistemicFooter(atomsUsed, stress, marketStress) {
  const hasKb = atomsUsed != null || (stress && stress.composite_stress != null);
  const hasMkt = marketStress && marketStress.composite != null;

  if (!hasKb && !hasMkt) return '';

  // KB confidence bar
  let kbBarHtml = '';
  if (hasKb) {
    const s = stress?.composite_stress ?? 0;
    const label = s < 0.30 ? 'LOW' : (s < 0.60 ? 'MED' : 'HIGH');
    const cls = s < 0.30 ? 'low' : (s < 0.60 ? 'med' : 'high');

    const ac = stress?.authority_conflict != null
      ? ` · authority conflict ${stress.authority_conflict.toFixed(2)}` : '';
    const de = stress?.domain_entropy != null
      ? ` · domain entropy ${stress.domain_entropy.toFixed(2)}` : '';
    const atomStr = atomsUsed != null ? `${atomsUsed} atoms` : '';
    const tooltip = `${atomStr} · KB stress ${s.toFixed(2)} ${label}${ac}${de}`;

    kbBarHtml = `
      <div class="stress-bar-wrap" data-tooltip="${escHtml(tooltip)}">
        <div class="stress-bar-row">
          <span class="stress-icon stress-icon-kb">⬡</span>
          <span class="stress-row-label">KB Confidence</span>
          <span class="stress-label ${cls}">${atomsUsed != null ? atomsUsed + ' atoms · ' : ''}${s.toFixed(2)} ${label}</span>
        </div>
        <div class="stress-bar-track">
          <div class="stress-bar-fill ${cls}" style="width:0%" data-width="${Math.round(s * 100)}"></div>
        </div>
      </div>`;
  }

  // Market regime stress bar
  let mktBarHtml = '';
  if (hasMkt) {
    const m = marketStress.composite;
    const label = marketStress.label;
    const cls = m < 0.30 ? 'low' : (m < 0.60 ? 'med' : 'high');

    const volStr = marketStress.vol_regime
      ? `vol: ${marketStress.vol_regime}` : '';
    const regStr = marketStress.price_regime
      ? ` · regime: ${marketStress.price_regime}` : '';
    const smStr = marketStress.smart_money
      ? ` · ${marketStress.smart_money}` : '';
    const tooltip = `Market stress ${m.toFixed(2)} ${label}${volStr ? ' · ' + volStr : ''}${regStr}${smStr}`;

    mktBarHtml = `
      <div class="stress-bar-wrap stress-bar-wrap-market" data-tooltip="${escHtml(tooltip)}">
        <div class="stress-bar-row">
          <span class="stress-icon stress-icon-mkt">◈</span>
          <span class="stress-row-label">Market Regime</span>
          <span class="stress-label ${cls}">${m.toFixed(2)} ${label}</span>
        </div>
        <div class="stress-bar-track">
          <div class="stress-bar-fill stress-bar-fill-market ${cls}" style="width:0%" data-width="${Math.round(m * 100)}"></div>
        </div>
      </div>`;
  }

  return `<div class="epistemic-footer">${kbBarHtml}${mktBarHtml}</div>`;
}

function appendMsg(role, html) {
  const msgs = document.getElementById('chat-messages');
  const el = document.createElement('div');
  el.className = `msg msg-${role}`;
  const fbRow = role === 'assistant'
    ? `<div class="feedback-row">
        <span class="fb-label">Helpful?</span>
        <button class="fb-btn" data-v="hit_t1" title="Yes — useful">👍</button>
        <button class="fb-btn fb-stop" data-v="stopped_out" title="No — not useful">👎</button>
       </div>`
    : '';
  el.innerHTML = `<div class="msg-bubble">${html}</div>${fbRow}<div class="msg-time">${new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</div>`;
  if (role === 'assistant') {
    el.querySelectorAll('.fb-btn').forEach(btn => {
      btn.addEventListener('click', async function() {
        if (el.dataset.fbDone) return;
        el.dataset.fbDone = '1';
        el.querySelectorAll('.fb-btn').forEach(b => b.classList.remove('fb-selected'));
        this.classList.add('fb-selected');
        el.querySelector('.feedback-row').insertAdjacentHTML('beforeend', '<span class="fb-done">✓ recorded</span>');
        if (state.userId) {
          await apiFetch('/feedback', { method: 'POST', body: JSON.stringify({
            user_id: state.userId, outcome: this.dataset.v
          })}).catch(() => {});
        }
      });
    });
  }
  msgs.appendChild(el);
  msgs.scrollTop = msgs.scrollHeight;
  return el;
}

function renderTipCard(tip, tipIdFromLog) {
  if (!tip) return '';
  const p = tip.position || {};
  const patternId = tip.pattern_id;
  const tipId = tipIdFromLog || 'ondemand';
  const dirEmoji = tip.direction === 'bullish' ? '📈' : '📉';
  const patLabel = (tip.pattern_type || '').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
  const tfLabel  = {fvg:'FVG',ifvg:'IFVG',bpr:'BPR'}[tip.pattern_type] || tip.timeframe || '';
  const convBadge = tip.kb_conviction
    ? `<span class="tier-badge" style="background:${{high:'#f59e0b',medium:'#6b7280',low:'#374151'}[tip.kb_conviction]||'#374151'}">${escHtml(tip.kb_conviction.toUpperCase())}</span>`
    : '';
  let posHtml = '';
  if (p.suggested_entry) {
    posHtml = `
      <div class="tip-pos-grid">
        <div><span class="tip-label">Entry</span><span class="mono-amber">${fmt(p.suggested_entry)}</span></div>
        <div><span class="tip-label">Stop</span><span class="mono-red">${fmt(p.stop_loss)}</span></div>
        <div><span class="tip-label">T1</span><span class="mono-green">${fmt(p.target_1)}</span></div>
        <div><span class="tip-label">T2</span><span class="mono-green">${fmt(p.target_2)}</span></div>
        <div><span class="tip-label">Size</span><span class="mono-amber">${p.position_size_units ? Math.round(p.position_size_units)+' shares' : '—'}</span></div>
        <div><span class="tip-label">Value</span><span class="mono-amber">${p.account_currency==='GBP'?'£':'$'}${p.position_value ? p.position_value.toLocaleString('en-GB',{maximumFractionDigits:0}) : '—'}</span></div>
      </div>`;
  }
  const feedbackId = `tip-fb-${Date.now()}`;
  return `<div class="tip-card" id="${feedbackId}">
    <div class="tip-card-header">
      <span class="tip-ticker">${escHtml(tip.ticker)}</span>
      ${convBadge}
      <span class="tip-dir">${dirEmoji} ${escHtml(patLabel)}</span>
      <span class="tip-tf text-muted">${escHtml(tfLabel)}</span>
    </div>
    <div class="tip-zone">Zone: <span class="mono-amber">${fmt(tip.zone_low)} – ${fmt(tip.zone_high)}</span></div>
    ${posHtml}
    <div class="tip-feedback-btns" data-tip-id="${tipId}" data-pattern-id="${patternId||''}">
      <button class="tip-fb-btn tip-fb-take" data-action="taking_it">✅ Taking this trade</button>
      <button class="tip-fb-btn tip-fb-more" data-action="tell_me_more">🤔 Tell me more</button>
      <button class="tip-fb-btn tip-fb-skip" data-action="not_for_me">❌ Not for me</button>
    </div>
  </div>`;
}

function bindTipFeedback(msgEl) {
  const btns = msgEl.querySelectorAll('.tip-feedback-btns');
  btns.forEach(row => {
    row.querySelectorAll('.tip-fb-btn').forEach(btn => {
      btn.addEventListener('click', async function() {
        const tipId  = row.dataset.tipId;
        const patId  = row.dataset.patternId;
        const action = this.dataset.action;
        if (row.dataset.done) return;
        row.dataset.done = '1';
        row.querySelectorAll('.tip-fb-btn').forEach(b => b.disabled = true);
        this.style.opacity = '1';
        try {
          const url = tipId && tipId !== 'ondemand' ? `/tips/${tipId}/feedback` : '/tips/0/feedback';
          const d = await apiFetch(url, { method: 'POST', body: JSON.stringify({
            user_id: state.userId, action, pattern_id: patId ? parseInt(patId) : null
          })});
          if (action === 'taking_it' && d) {
            row.insertAdjacentHTML('afterend',
              `<div class="tip-confirm">✅ ${escHtml(d.message || 'Position added — monitor active')}</div>`);
          } else if (action === 'tell_me_more' && d) {
            row.insertAdjacentHTML('afterend',
              `<div class="tip-confirm">🤔 ${escHtml(d.message || 'Ask me anything about this setup')}</div>`);
            const inp = document.getElementById('chat-input');
            inp.value = 'Tell me more about this setup — what is the risk and what regime is it in?';
            inp.focus();
          } else if (action === 'not_for_me') {
            row.insertAdjacentHTML('afterend',
              `<div class="tip-rejection-btns">
                <span class="tip-label">What put you off?</span>
                <button class="tip-rej-btn" data-r="too_risky">Too risky</button>
                <button class="tip-rej-btn" data-r="wrong_setup">Wrong setup</button>
                <button class="tip-rej-btn" data-r="wrong_timing">Wrong timing</button>
                <button class="tip-rej-btn" data-r="dont_know_stock">Don't know it</button>
                <button class="tip-rej-btn" data-r="prefer_uk">Prefer UK</button>
                <button class="tip-rej-btn" data-r="no_reason">No reason</button>
              </div>`);
            row.nextElementSibling.querySelectorAll('.tip-rej-btn').forEach(rb => {
              rb.addEventListener('click', async function() {
                row.nextElementSibling.querySelectorAll('.tip-rej-btn').forEach(b => b.disabled = true);
                await apiFetch(url || '/tips/0/feedback', { method: 'POST', body: JSON.stringify({
                  user_id: state.userId, action: 'not_for_me',
                  rejection_reason: this.dataset.r, pattern_id: patId ? parseInt(patId) : null
                })}).catch(()=>{});
                this.insertAdjacentHTML('afterend','<span class="fb-done"> ✓</span>');
              });
            });
          }
        } catch(e) {
          showToast('Feedback error: ' + e.message);
        }
      });
    });
  });
}

// ── Trade thesis feedback widget ──────────────────────────────────────────────

async function _ensureCashBalance() {
  if (!state.userId) return;
  if (Date.now() - (state._cashFetchedAt || 0) < 60000) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/cash`);
    state.cashBalance = d?.available_cash ?? 0;
    state._cashFetchedAt = Date.now();
  } catch(e) {}
}

function shouldShowFeedbackWidget(ga) {
  if (!ga || typeof ga !== 'object') return null;
  const ticker     = ga.ticker;
  const direction  = (ga.signal_direction || '').toLowerCase();
  const conviction = (ga.conviction_tier  || '').toLowerCase();
  if (!ticker) return null;
  if (!['long', 'short', 'bullish', 'bearish'].includes(direction)) return null;
  if (!['high', 'medium'].includes(conviction)) return null;
  const t = ticker.toUpperCase();
  const inHoldings  = (state.holdings  || []).some(h => !h.is_cash && h.ticker?.toUpperCase() === t);
  const inWatchlist = (state.watchlistTickers || []).map(s => s.toUpperCase()).includes(t);
  const gaSector    = (ga.sector || '').toLowerCase();
  const sectorMatch = gaSector && (state.holdings || []).some(h => !h.is_cash && (h.sector || '').toLowerCase() === gaSector);
  if (!inHoldings && !inWatchlist && !sectorMatch) return null;
  if ((state.cashBalance || 0) <= 0) return null;
  return { ticker, direction, conviction };
}

function renderFeedbackWidget(thesis, patternId, tipId) {
  const dirWord = ['bearish', 'short'].includes(thesis.direction) ? 'BEARISH' : 'BULLISH';
  const dirCls  = dirWord === 'BEARISH' ? 'trade-fb-dir bearish' : 'trade-fb-dir bullish';
  return `<div class="trade-fb-widget" data-ticker="${escHtml(thesis.ticker)}" data-pattern-id="${patternId || ''}" data-tip-id="${tipId || '0'}">
    <div class="trade-fb-header">
      <span class="trade-fb-ticker">${escHtml(thesis.ticker.toUpperCase())}</span>
      <span class="${dirCls}">${dirWord}</span>
      <span class="trade-fb-conv">${escHtml(thesis.conviction.toUpperCase())}</span>
    </div>
    <div class="trade-fb-btns">
      <button class="trade-fb-take">🎯 Taking it</button>
      <button class="trade-fb-more">💬 More</button>
      <button class="trade-fb-pass">✕ Pass</button>
    </div>
  </div>`;
}

function bindFeedbackWidget(msgEl) {
  const widget = msgEl.querySelector('.trade-fb-widget');
  if (!widget) return;
  const ticker    = widget.dataset.ticker;
  const patternId = widget.dataset.patternId ? parseInt(widget.dataset.patternId) : null;
  const tipId     = widget.dataset.tipId || '0';
  const btnsRow   = widget.querySelector('.trade-fb-btns');

  const _done = () => { widget.dataset.done = '1'; btnsRow.querySelectorAll('button').forEach(b => b.disabled = true); };

  btnsRow.querySelector('.trade-fb-take').addEventListener('click', async function() {
    if (widget.dataset.done) return;
    _done();
    try {
      const d = await apiFetch(`/tips/${tipId}/feedback`, { method: 'POST', body: JSON.stringify({
        user_id: state.userId, action: 'taking_it', pattern_id: patternId
      })});
      let html = '';
      if (d && d.entry_price != null) {
        const sym = (d.cash_after != null) ? '£' : '$';
        const confirmId = `ptf-confirm-${Date.now()}`;
        html = `<div class="trade-fb-confirm" id="${confirmId}">
          <div class="trade-fb-confirm-title">✓ Added to portfolio</div>
          <div class="tip-pos-grid">
            <div><span class="tip-label">Stop</span><span class="mono-red">${fmt(d.stop_loss)}</span></div>
            <div><span class="tip-label">T1</span><span class="mono-green">${fmt(d.target_1)}</span></div>
            <div><span class="tip-label">T2</span><span class="mono-green">${fmt(d.target_2)}</span></div>
            ${d.position_size != null ? `<div><span class="tip-label">Size</span><span class="mono-amber">${Math.round(d.position_size)} shares</span></div>` : ''}
          </div>
          <div class="trade-fb-entry-row">
            <span class="tip-label">Entry price</span>
            <input class="trade-fb-entry-input" type="number" step="0.01" min="0"
              value="${d.entry_price != null ? d.entry_price.toFixed(2) : ''}"
              data-ticker="${escHtml(ticker)}"
              data-size="${d.position_size != null ? Math.round(d.position_size) : ''}" />
            <button class="trade-fb-entry-save">Save</button>
          </div>
          <div class="trade-fb-monitoring">Monitoring active — you'll be alerted when action is needed</div>
          ${d.cash_after != null ? `<div class="trade-fb-cash">Cash remaining: ${sym}${Number(d.cash_after).toLocaleString('en-GB',{minimumFractionDigits:2,maximumFractionDigits:2})}</div>` : ''}
        </div>`;
        state._cashFetchedAt = 0; // invalidate so next sendChat re-fetches portfolio cash
        btnsRow.insertAdjacentHTML('afterend', html);
        btnsRow.style.display = 'none';
        // Bind the save button
        const confirmEl = document.getElementById(confirmId);
        confirmEl.querySelector('.trade-fb-entry-save').addEventListener('click', async function() {
          const inp = confirmEl.querySelector('.trade-fb-entry-input');
          const newEntry = parseFloat(inp.value);
          if (!newEntry || newEntry <= 0) { showToast('Enter a valid price'); return; }
          this.disabled = true;
          try {
            await apiFetch(`/users/${state.userId}/portfolio/holding`, { method: 'POST', body: JSON.stringify({
              ticker: inp.dataset.ticker,
              avg_cost: newEntry,
              quantity: inp.dataset.size ? parseFloat(inp.dataset.size) : null,
            })});
            this.textContent = '✓';
            this.style.color = 'var(--green)';
          } catch(e) { showToast('Could not update entry: ' + e.message); this.disabled = false; }
        });
      } else {
        html = `<div class="trade-fb-confirm"><div class="trade-fb-confirm-title">✓ ${escHtml(d?.message || 'Signal noted — monitoring active')}</div></div>`;
        btnsRow.insertAdjacentHTML('afterend', html);
        btnsRow.style.display = 'none';
      }
    } catch(e) { showToast('Error: ' + e.message); }
  });

  btnsRow.querySelector('.trade-fb-more').addEventListener('click', async function() {
    if (widget.dataset.done) return;
    _done();
    try {
      const d = await apiFetch(`/tips/${tipId}/feedback`, { method: 'POST', body: JSON.stringify({
        user_id: state.userId, action: 'tell_me_more', pattern_id: patternId
      })});
      const questions = d?.suggested_questions || [
        "What's the risk if it breaks below the zone?",
        "How has this pattern performed in this regime?",
        "Does this conflict with my existing positions?",
      ];
      const chips = questions.map(q =>
        `<button class="trade-fb-chip" onclick="document.getElementById('chat-input').value=${JSON.stringify(q)};document.getElementById('chat-input').focus()">${escHtml(q)}</button>`
      ).join('');
      btnsRow.insertAdjacentHTML('afterend', `<div class="trade-fb-more-chips">${chips}</div>`);
      btnsRow.style.display = 'none';
    } catch(e) { showToast('Error: ' + e.message); }
  });

  btnsRow.querySelector('.trade-fb-pass').addEventListener('click', function() {
    if (widget.dataset.done) return;
    widget.dataset.done = '1';
    btnsRow.querySelectorAll('button').forEach(b => b.disabled = true);
    const reasons = [
      ['too_risky', 'Too risky'],
      ['wrong_setup', 'Wrong setup'],
      ['wrong_timing', 'Wrong timing'],
      ['dont_know_stock', "Don't know it"],
      ['no_reason', 'No reason'],
    ];
    const pills = reasons.map(([r, label]) =>
      `<button class="trade-fb-rej-pill" data-r="${r}">${escHtml(label)}</button>`
    ).join('');
    const rejRow = document.createElement('div');
    rejRow.className = 'trade-fb-rej-row';
    rejRow.innerHTML = `<span class="trade-fb-rej-label">What put you off?</span>${pills}`;
    btnsRow.insertAdjacentElement('afterend', rejRow);
    btnsRow.style.display = 'none';
    rejRow.querySelectorAll('.trade-fb-rej-pill').forEach(pill => {
      pill.addEventListener('click', async function() {
        rejRow.querySelectorAll('.trade-fb-rej-pill').forEach(p => p.disabled = true);
        try {
          await apiFetch(`/tips/${tipId}/feedback`, { method: 'POST', body: JSON.stringify({
            user_id: state.userId, action: 'not_for_me',
            rejection_reason: this.dataset.r, pattern_id: patternId
          })});
        } catch(e) {}
        rejRow.innerHTML = '<span class="trade-fb-noted">Noted — improving future signals</span>';
      });
    });
  });
}

function renderOverlay(overlay) {
  if (!overlay || typeof overlay !== 'object') return '';
  const parts = [];
  if (overlay.signals?.length) {
    parts.push(`<div class="overlay-card">
      <div class="overlay-card-title">Signal Summary</div>
      ${overlay.signals.slice(0,4).map(s => `<div class="flex-center gap-8 text-sm mb-8">
        <span class="mono-amber">${escHtml(s.ticker||'')}</span>
        ${tierBadge(s.conviction_tier)}
        <span class="mono-${(s.upside_pct||0)>=0?'green':'red'}">${fmt(s.upside_pct)}%</span>
      </div>`).join('')}
    </div>`);
  }
  if (overlay.causal_chain?.length) {
    parts.push(`<div class="overlay-card">
      <div class="overlay-card-title">Causal Context</div>
      <div class="text-sm text-muted">${overlay.causal_chain.slice(0,3).map(c => escHtml(String(c))).join(' → ')}</div>
    </div>`);
  }
  if (overlay.stress_flag) {
    parts.push(`<div class="overlay-card" style="border-color:var(--red)">
      <div class="overlay-card-title" style="color:var(--red)">⚠ Stress Flag</div>
      <div class="text-sm text-muted">${escHtml(String(overlay.stress_flag))}</div>
    </div>`);
  }
  return parts.join('');
}

async function sendChat() {
  await _ensureCashBalance();
  const inp = document.getElementById('chat-input');
  const msg = inp.value.trim();
  if (!msg) return;
  inp.value = '';
  inp.style.height = 'auto';
  const overlayMode = document.getElementById('overlay-toggle').checked;

  const prompts = document.getElementById('chat-prompts');
  if (prompts) prompts.remove();
  appendMsg('user', renderUserMsg(msg));
  const thinking = appendMsg('assistant', '<span class="spinner"></span>');
  const msgForBackend = expandPatternTag(msg);

  try {
    const d = await apiFetch('/chat', {
      method: 'POST',
      body: JSON.stringify({ message: msgForBackend, session_id: sessionId, overlay_mode: overlayMode, user_id: state.userId || null })
    });
    if (!d) { thinking.querySelector('.msg-bubble').innerHTML = '<span style="color:var(--accent)">⬆ Upgrade required — visit Subscription to unlock this feature.</span>'; return; }
    const rawAnswer = d.response || d.answer || JSON.stringify(d);
    const { prose, grounding } = extractKbGrounding(rawAnswer);
    const answer = mdToHtml(prose);
    const overlayHtml = overlayMode ? renderOverlay(d.overlay) : '';
    const tipCardHtml = d.tip_card ? renderTipCard(d.tip_card, d.tip_card.tip_id) : '';
    const kbPanelHtml = renderKbPanel(grounding, d.grounding_atoms || null);
    const calHtml = renderCalibrationBadge(d.calibration || null);
    // Build merged atoms from LLM grounding block + DB atoms for market stress fallback
    const _mergedAtoms = {};
    if (grounding) grounding.forEach(r => { const k = r.key === 'regime' ? 'price_regime' : r.key; if (r.val) _mergedAtoms[k] = r.val; });
    if (d.grounding_atoms) Object.assign(_mergedAtoms, d.grounding_atoms);
    const mktStress = d.market_stress || computeMarketStress(Object.keys(_mergedAtoms).length ? _mergedAtoms : null);
    const epistemicHtml = renderEpistemicFooter(d.atoms_used, d.stress || null, mktStress);
    const bubble = thinking.querySelector('.msg-bubble');
    bubble.innerHTML = answer + overlayHtml + tipCardHtml + kbPanelHtml + calHtml + epistemicHtml;
    // Animate both stress bars (generic data-width) + calibration bars
    requestAnimationFrame(() => {
      thinking.querySelectorAll('.stress-bar-fill[data-width]').forEach((bar, i) => {
        setTimeout(() => {
          bar.style.width = bar.dataset.width + '%';
        }, i * 150);
      });
      const t1Fill = thinking.querySelector('.cal-bar-fill:not(.t2)');
      if (t1Fill && d.calibration && d.calibration.hit_rate_t1 != null) {
        t1Fill.style.width = (d.calibration.hit_rate_t1 * 100) + '%';
      }
      setTimeout(() => {
        const t2Fill = thinking.querySelector('.cal-bar-fill.t2');
        if (t2Fill && d.calibration && d.calibration.hit_rate_t2 != null) {
          t2Fill.style.width = (d.calibration.hit_rate_t2 * 100) + '%';
        }
      }, 300);
    });
    if (tipCardHtml) bindTipFeedback(thinking);
    const _thesis = shouldShowFeedbackWidget(d.grounding_atoms);
    if (_thesis) {
      const _patId = d.best_pattern?.id || null;
      const _tipId = d.tip_card?.tip_id || '0';
      thinking.querySelector('.msg-bubble').insertAdjacentHTML('beforeend',
        renderFeedbackWidget(_thesis, _patId, _tipId));
      bindFeedbackWidget(thinking);
    }
    if (d.kb_enriched && d.live_fetched?.length) {
      const badge = document.createElement('div');
      badge.className = 'msg-live-badge';
      badge.textContent = `🔴 Live data fetched · ${d.atoms_committed} atom${d.atoms_committed !== 1 ? 's' : ''} committed to KB (${d.live_fetched.join(', ')})`;
      thinking.appendChild(badge);
    }
  } catch(e) {
    thinking.querySelector('.msg-bubble').innerHTML = `<span style="color:var(--red)">${escHtml(e.message)}</span>`;
  }
}

document.getElementById('chat-send-btn').addEventListener('click', sendChat);
document.getElementById('chat-input').addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
});
document.querySelectorAll('.prompt-chip').forEach(btn => {
  btn.addEventListener('click', () => {
    document.getElementById('chat-input').value = btn.dataset.prompt;
    sendChat();
  });
});
document.getElementById('chat-input').addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 120) + 'px';
  _tickerAcUpdate(this);
});

// ── Ticker autocomplete on $ ──────────────────────────────────────────────────
const _AC_FALLBACK = ['AAPL','MSFT','NVDA','GOOGL','AMZN','META','TSLA','AVGO','AMD',
  'JPM','BAC','GS','V','MA','XOM','CVX','JNJ','UNH','WMT','COIN','PLTR','MSTR',
  'BARC.L','HSBA.L','LLOY.L','AZN.L','SHEL.L','BP.L','GSK.L','RIO.L'];

function _tickerAcSources() {
  const keys = Object.keys(window._snapshotPrices || {});
  return keys.length ? keys : _AC_FALLBACK;
}

function _tickerAcUpdate(textarea) {
  const ac = document.getElementById('ticker-autocomplete');
  if (!ac) return;
  const val = textarea.value;
  const pos = textarea.selectionStart;
  // Find last $ before cursor
  const before = val.slice(0, pos);
  const m = before.match(/\$([A-Z0-9.]*)$/i);
  if (!m) { ac.style.display = 'none'; return; }
  const query = m[1].toUpperCase();
  if (!query) { ac.style.display = 'none'; return; }
  const matches = _tickerAcSources()
    .filter(t => t.toUpperCase().startsWith(query))
    .slice(0, 6);
  if (!matches.length) { ac.style.display = 'none'; return; }
  ac.innerHTML = matches.map(t => {
    const hi = `<span class="ticker-ac-match">${escHtml(t.slice(0, query.length))}</span>${escHtml(t.slice(query.length))}`;
    return `<div class="ticker-ac-item" data-ticker="${escHtml(t)}">${hi}</div>`;
  }).join('');
  ac.style.display = 'block';
}

function _tickerAcInsert(ticker) {
  const textarea = document.getElementById('chat-input');
  const ac = document.getElementById('ticker-autocomplete');
  const pos = textarea.selectionStart;
  const val = textarea.value;
  const before = val.slice(0, pos);
  const m = before.match(/\$([A-Z0-9.]*)$/i);
  if (!m) return;
  const start = pos - m[0].length;
  textarea.value = val.slice(0, start) + ticker + val.slice(pos);
  const newPos = start + ticker.length;
  textarea.setSelectionRange(newPos, newPos);
  textarea.focus();
  if (ac) ac.style.display = 'none';
}

document.getElementById('ticker-autocomplete').addEventListener('mousedown', function(e) {
  const item = e.target.closest('.ticker-ac-item');
  if (item) { e.preventDefault(); _tickerAcInsert(item.dataset.ticker); }
});

document.getElementById('chat-input').addEventListener('keydown', function(e) {
  const ac = document.getElementById('ticker-autocomplete');
  if (e.key === 'Escape' && ac && ac.style.display !== 'none') {
    ac.style.display = 'none'; e.preventDefault();
  }
});

document.addEventListener('mousedown', function(e) {
  const ac = document.getElementById('ticker-autocomplete');
  if (ac && !ac.contains(e.target) && e.target.id !== 'chat-input') {
    ac.style.display = 'none';
  }
});

