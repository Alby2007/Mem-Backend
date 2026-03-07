// ── CHAT ─────────────────────────────────────────────────────────────────── v2
let sessionId = `s_${Date.now()}`;

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
  const _LABELS = {
    signal_direction:    'Signal direction',
    conviction_tier:     'Conviction tier',
    price_regime:        'Regime',
    volatility_regime:   'Vol regime',
    sector:              'Sector',
    implied_volatility:  'Implied vol',
    put_call_oi_ratio:   'Put/call OI ratio',
    smart_money_signal:  'Smart money',
    atoms_used:          'Atoms used',
    stress:              'Epistemic stress',
  };
  // Merge: start with LLM-parsed rows (normalise 'regime' -> 'price_regime'),
  // then overwrite/add from authoritative DB atoms (DB wins on conflicts)
  const merged = {};
  if (grounding) {
    grounding.forEach(r => {
      const key = r.key === 'regime' ? 'price_regime' : r.key;
      if (r.val) merged[key] = r.val;
    });
  }
  if (groundingAtoms && typeof groundingAtoms === 'object') {
    Object.entries(groundingAtoms).forEach(([k, v]) => { if (v) merged[k] = v; });
  }
  if (!Object.keys(merged).length) return '';
  // Preferred display order
  const _ORDER = ['signal_direction','conviction_tier','price_regime',
    'volatility_regime','sector','implied_volatility','put_call_oi_ratio',
    'smart_money_signal','atoms_used','stress'];
  const orderedKeys = [
    ..._ORDER.filter(k => k in merged),
    ...Object.keys(merged).filter(k => !_ORDER.includes(k)),
  ];
  const rows = orderedKeys.map(k => {
    const label = _LABELS[k] || k.replace(/_/g, ' ');
    return `<div class="kb-panel-row"><span class="kb-panel-key">${escHtml(label)}</span><span class="kb-panel-val">${escHtml(String(merged[k]))}</span></div>`;
  }).join('');
  return `<div class="kb-panel">
    <div class="kb-panel-header" onclick="this.parentElement.classList.toggle('kb-panel-open')">
      <span class="kb-panel-title">KB GROUNDING</span>
      <span class="kb-panel-toggle">▸</span>
    </div>
    <div class="kb-panel-body">${rows}</div>
  </div>`;
}

function renderCalibrationBadge(cal) {
  if (!cal) return '';
  const patLabel = (cal.pattern_type || '').replace(/_/g, ' ').toUpperCase();
  const tf = (cal.timeframe || '').toUpperCase();
  const t1 = cal.hit_rate_t1 != null ? `${Math.round(cal.hit_rate_t1 * 100)}%` : '—';
  const t2 = cal.hit_rate_t2 != null ? `${Math.round(cal.hit_rate_t2 * 100)}%` : '—';
  const n  = cal.n_total != null ? cal.n_total.toLocaleString() : '—';
  const conf = cal.confidence_label || '';
  return `<div class="kb-panel kb-calibration-panel">
    <div class="kb-panel-header" onclick="this.parentElement.classList.toggle('kb-panel-open')">
      <span class="kb-panel-title">CALIBRATION</span>
      <span class="kb-panel-toggle">▸</span>
    </div>
    <div class="kb-panel-body">
      <div class="kb-panel-row"><span class="kb-panel-key">Pattern</span><span class="kb-panel-val">${escHtml(patLabel)}${tf ? ' · ' + escHtml(tf) : ''}</span></div>
      <div class="kb-panel-row"><span class="kb-panel-key">Hit rate T1 / T2</span><span class="kb-panel-val">${escHtml(t1)} / ${escHtml(t2)} across ${escHtml(n)} setups</span></div>
      <div class="kb-panel-row"><span class="kb-panel-key">Confidence</span><span class="kb-panel-val">${escHtml(conf)}</span></div>
    </div>
  </div>`;
}

function renderEpistemicFooter(atomsUsed, stress) {
  if (atomsUsed == null && !stress) return '';
  const parts = [];
  if (atomsUsed != null) parts.push(`${atomsUsed} atoms`);
  if (stress && stress.composite_stress != null) {
    const s = stress.composite_stress;
    const label = s < 0.30 ? 'LOW' : (s < 0.60 ? 'MED' : 'HIGH');
    parts.push(`stress ${s.toFixed(2)} ${label}`);
  }
  return parts.length
    ? `<div class="epistemic-footer">⬡ ${parts.join(' · ')}</div>`
    : '';
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
    const epistemicHtml = renderEpistemicFooter(d.atoms_used, d.stress || null);
    const bubble = thinking.querySelector('.msg-bubble');
    bubble.innerHTML = answer + overlayHtml + tipCardHtml + kbPanelHtml + calHtml;
    if (epistemicHtml) bubble.insertAdjacentHTML('afterend', epistemicHtml);
    if (tipCardHtml) bindTipFeedback(thinking);
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
});

