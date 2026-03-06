// ── PROFILE ───────────────────────────────────────────────────────────────────
function _prfUpdateDisplay() {
  const first = (document.getElementById('profile-first')?.value || '').trim();
  const last  = (document.getElementById('profile-last')?.value  || '').trim();
  const full  = [first, last].filter(Boolean).join(' ') || '—';
  const initials = [(first[0]||''), (last[0]||'')].join('').toUpperCase() || '?';
  const dn = document.getElementById('profile-display-name');
  const av = document.getElementById('profile-avatar');
  if (dn) dn.textContent = full;
  if (av) av.textContent = initials;
}

async function loadProfile() {
  if (!state.userId) return;
  _profileTgWidgetInjected = false;

  // Populate form + identity block
  try {
    const d = await apiFetch('/auth/me');
    if (!d) return;
    document.getElementById('profile-email').value = d.email || '';
    document.getElementById('profile-first').value = d.first_name || '';
    document.getElementById('profile-last').value  = d.last_name  || '';
    document.getElementById('profile-phone').value = d.phone      || '';
    _prfUpdateDisplay();

    // Username display
    const unEl = document.getElementById('profile-username-display');
    if (unEl) unEl.textContent = state.userId || '';

    // Member since — use created_at if available, else today
    const sinceEl = document.getElementById('profile-member-since');
    if (sinceEl) {
      const raw = d.created_at;
      if (raw) {
        const dt = new Date(raw);
        sinceEl.textContent = 'Member since ' + dt.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
      } else {
        sinceEl.textContent = '';
      }
    }

    // Subscription card
    const tier = (d.tier || 'basic').toLowerCase();
    const tierPrices = { basic: '£9 / month', pro: '£29 / month', premium: '£79 / month' };
    const tierNames  = { basic: 'Basic', pro: 'Pro', premium: 'Premium' };
    const subTierEl  = document.getElementById('profile-sub-tier');
    const subPriceEl = document.getElementById('profile-sub-price');
    if (subTierEl)  subTierEl.textContent  = tierNames[tier]  || tier;
    if (subPriceEl) subPriceEl.textContent = tierPrices[tier] || '';

    const subFeatureSets = {
      basic: [
        { on: true,  text: 'Mon + Wed briefings' },
        { on: true,  text: '2 Monday setups / week' },
        { on: true,  text: 'FVG + IFVG patterns · 1h & 4h' },
        { on: true,  text: 'Zone + thesis alerts' },
        { on: false, text: 'Profit lock + trailing alerts' },
        { on: false, text: 'All patterns · daily TF · 5 setups' },
      ],
      pro: [
        { on: true,  text: 'Mon + Wed briefings' },
        { on: true,  text: '5 Monday setups / week' },
        { on: true,  text: 'All pattern types · 1h, 4h & 1d' },
        { on: true,  text: 'Zone, thesis, profit lock alerts' },
        { on: true,  text: 'Trailing pullback alerts' },
        { on: false, text: 'Daily briefing · unlimited setups' },
      ],
      premium: [
        { on: true, text: 'Daily briefing every morning' },
        { on: true, text: 'Unlimited setups · all TFs (15m–1d)' },
        { on: true, text: 'All pattern types' },
        { on: true, text: 'Zone, thesis, profit lock alerts' },
        { on: true, text: 'Trailing pullback alerts (T1/T2/T3)' },
      ],
    };
    const featuresEl = document.getElementById('profile-sub-features');
    if (featuresEl) {
      const feats = subFeatureSets[tier] || subFeatureSets.basic;
      featuresEl.innerHTML = feats.map(f =>
        `<div style="display:flex;gap:8px;align-items:center;">
          <span style="color:${f.on ? 'var(--green)' : 'var(--border)'};font-size:10px;">${f.on ? '✓' : '–'}</span>
          <span style="${f.on ? '' : 'color:var(--muted);'}">${escHtml(f.text)}</span>
        </div>`
      ).join('');
    }

    const upgradeEl = document.getElementById('profile-sub-upgrade-hint');
    if (upgradeEl) {
      if (tier === 'basic') {
        upgradeEl.innerHTML = 'Upgrade to <span onclick="navigate(\'subscription\')">Pro (£29/mo)</span> to unlock profit lock alerts, all pattern types, and 5 Monday setups.';
      } else if (tier === 'pro') {
        upgradeEl.innerHTML = 'Upgrade to <span onclick="navigate(\'subscription\')">Premium (£79/mo)</span> for daily briefings, unlimited setups and all timeframes.';
      } else {
        upgradeEl.textContent = '';
      }
    }
    // Trading preferences
    const riskSlider = document.getElementById('prf-risk-slider');
    const riskVal    = document.getElementById('prf-risk-val');
    if (riskSlider && d.max_risk_per_trade_pct != null) {
      riskSlider.value = d.max_risk_per_trade_pct;
      if (riskVal) riskVal.textContent = parseFloat(d.max_risk_per_trade_pct).toFixed(1);
    }
    const brokerSel = document.getElementById('prf-broker');
    if (brokerSel && d.preferred_broker) {
      const opt = Array.from(brokerSel.options).find(o => o.value === d.preferred_broker || o.text === d.preferred_broker);
      if (opt) opt.selected = true;
    }
    const expSel = document.getElementById('prf-experience');
    if (expSel && d.trader_level) {
      const opt = Array.from(expSel.options).find(o => o.value === d.trader_level);
      if (opt) opt.selected = true;
    }
    const bioEl = document.getElementById('prf-bio');
    if (bioEl && d.trading_bio) bioEl.value = d.trading_bio;

    // Notification prefs — restore saved state
    if (d.notification_prefs) {
      let prefs = d.notification_prefs;
      if (typeof prefs === 'string') { try { prefs = JSON.parse(prefs); } catch { prefs = {}; } }
      document.querySelectorAll('#screen-profile input[data-pref]').forEach(inp => {
        const key = inp.dataset.pref;
        if (key in prefs) inp.checked = prefs[key];
      });
    }

    // Pro+ toggle gating — disable if basic tier
    if (tier === 'basic') {
      ['profit_lock_alerts', 'trailing_alerts'].forEach(key => {
        const inp = document.querySelector(`#screen-profile input[data-pref="${key}"]`);
        if (inp) inp.disabled = true;
      });
    } else {
      ['profit_lock_alerts', 'trailing_alerts'].forEach(key => {
        const inp = document.querySelector(`#screen-profile input[data-pref="${key}"]`);
        if (inp) inp.disabled = false;
      });
    }
  } catch(e) { /* silent */ }

  // Notification toggle auto-save
  document.querySelectorAll('#screen-profile input[data-pref]').forEach(inp => {
    inp.addEventListener('change', async function() {
      if (this.disabled) return;
      const key = this.dataset.pref;
      const savedEl = document.getElementById('prf-notif-saved');
      try {
        await apiFetch(`/users/${state.userId}/notification-prefs`, {
          method: 'PATCH',
          body: JSON.stringify({ [key]: this.checked })
        });
        if (savedEl) {
          savedEl.textContent = '✓ Saved';
          savedEl.classList.add('show');
          clearTimeout(savedEl._t);
          savedEl._t = setTimeout(() => savedEl.classList.remove('show'), 2000);
        }
      } catch(e) {
        this.checked = !this.checked; // revert on error
        showToast('Failed to save notification preference');
      }
    });
  });

  // Trading prefs save button
  const tradingSaveBtn = document.getElementById('prf-trading-save-btn');
  if (tradingSaveBtn && !tradingSaveBtn._wired) {
    tradingSaveBtn._wired = true;
    tradingSaveBtn.addEventListener('click', async function() {
      const msgEl = document.getElementById('prf-trading-msg');
      const risk  = parseFloat(document.getElementById('prf-risk-slider')?.value);
      const broker = document.getElementById('prf-broker')?.value || '';
      const exp    = document.getElementById('prf-experience')?.value || 'developing';
      const bio    = document.getElementById('prf-bio')?.value || '';
      try {
        await apiFetch(`/users/${state.userId}/trading-prefs`, {
          method: 'PATCH',
          body: JSON.stringify({ max_risk_per_trade_pct: risk, preferred_broker: broker, experience_level: exp, trading_bio: bio })
        });
        await apiFetch(`/users/${state.userId}/trader-level`, {
          method: 'POST',
          body: JSON.stringify({ level: exp })
        });
        if (msgEl) { msgEl.textContent = '\u2713 Preferences saved'; msgEl.style.color = 'var(--green)'; setTimeout(() => { if (msgEl) msgEl.textContent = ''; }, 3000); }
      } catch(e) {
        if (msgEl) { msgEl.textContent = 'Error saving preferences'; msgEl.style.color = 'var(--red)'; }
      }
    });
  }

  // Telegram link status + widget
  try {
    const u = await apiFetch(`/users/${state.userId}/onboarding-status`);
    const tgStatus = document.getElementById('profile-tg-status');
    const tgWidget = document.getElementById('profile-tg-widget');
    const tgRelink = document.getElementById('profile-tg-relink');
    const tgDot    = document.getElementById('prf-tg-dot');
    const tgSub    = document.getElementById('prf-tg-sub');
    const tgSteps  = document.getElementById('prf-tg-steps');
    if (u?.telegram_connected) {
      if (tgDot)   { tgDot.classList.add('linked'); }
      if (tgStatus) tgStatus.textContent = '✓ Linked';
      if (tgSub)   tgSub.textContent = 'Your account is linked. Briefings and alerts are active.';
      if (tgSteps) tgSteps.style.display = 'none';
      if (tgWidget) tgWidget.innerHTML = '';
      if (tgRelink) tgRelink.style.display = 'block';
      document.getElementById('profile-tg-relink-btn')?.addEventListener('click', async () => {
        if (tgRelink) tgRelink.style.display = 'none';
        if (tgStatus) tgStatus.textContent = 'Unlinking…';
        try {
          await apiFetch(`/users/${state.userId}/telegram`, { method: 'DELETE' });
        } catch(e) { /* non-fatal */ }
        if (tgDot)   tgDot.classList.remove('linked');
        if (tgStatus) tgStatus.textContent = 'Not linked';
        if (tgSub)   tgSub.textContent = "Briefings and alerts won't fire until Telegram is connected.";
        if (tgSteps) tgSteps.style.display = '';
        _profileTgWidgetInjected = false;
        _injectProfileTgWidget();
      });
    } else {
      if (tgStatus) tgStatus.textContent = 'Not linked';
      _injectProfileTgWidget();
    }
  } catch(e) {
    const tgStatus = document.getElementById('profile-tg-status');
    if (tgStatus) tgStatus.textContent = 'Not linked';
    _injectProfileTgWidget();
  }
}

let _profileTgWidgetInjected = false;
function _injectProfileTgWidget() {
  if (_profileTgWidgetInjected) return;
  _profileTgWidgetInjected = true;
  const container = document.getElementById('profile-tg-widget');
  if (!container) return;
  container.innerHTML = '';
  const btn = document.createElement('button');
  btn.className = 'btn btn-primary btn-sm';
  btn.style.cssText = 'margin-top:4px;display:flex;align-items:center;gap:8px;';
  btn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.894 8.221-1.97 9.28c-.145.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12L8.32 13.617l-2.96-.924c-.643-.204-.657-.643.136-.953l11.57-4.461c.537-.194 1.006.131.828.942z"/></svg> Link Telegram Account';
  btn.addEventListener('click', function() {
    if (window.Telegram && window.Telegram.Login && typeof window.Telegram.Login.auth === 'function') {
      window.Telegram.Login.auth(
        { bot_id: '8516826412', request_access: true },
        function(tgData) { if (tgData) onProfileTelegramLink(tgData); }
      );
    } else {
      const url = 'https://oauth.telegram.org/auth?bot_id=8516826412&origin=' + encodeURIComponent(location.origin) + '&embed=0&request_access=write';
      const popup = window.open(url, 'tg_oauth', 'width=550,height=470,toolbar=0,menubar=0,location=0');
      const timer = setInterval(function() {
        try {
          if (popup && popup.closed) { clearInterval(timer); loadProfile(); }
        } catch(e) { clearInterval(timer); }
      }, 500);
    }
  });
  container.appendChild(btn);
}


window.onProfileTelegramLink = async function(tgData) {
  const msg = document.getElementById('profile-tg-msg');
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    await apiFetch(`/users/${state.userId}/onboarding`, {
      method: 'POST',
      body: JSON.stringify({ telegram_chat_id: String(tgData.id) })
    });
    const name = tgData.username ? `@${tgData.username}` : (tgData.first_name || String(tgData.id));
    document.getElementById('profile-tg-status').innerHTML =
      `<span style="color:var(--green);">✓ Linked</span> — ${escHtml(name)} (ID: <span style="font-family:var(--mono);color:var(--accent);">${escHtml(String(tgData.id))}</span>)`;
    document.getElementById('profile-tg-widget').innerHTML =
      `<button class="btn btn-ghost btn-sm" id="profile-tg-relink-btn">Re-link Telegram account</button>`;
    document.getElementById('profile-tg-relink-btn').addEventListener('click', () => {
      document.getElementById('profile-tg-widget').innerHTML = '';
      _injectProfileTgWidget();
    });
    msg.style.color = 'var(--green)'; msg.textContent = '✓ Telegram linked successfully';
  } catch(e) {
    msg.style.color = 'var(--red)'; msg.textContent = e.message;
  }
};

document.getElementById('profile-save-btn').addEventListener('click', async () => {
  const msg  = document.getElementById('profile-save-msg');
  const body = {
    first_name: document.getElementById('profile-first').value.trim(),
    last_name:  document.getElementById('profile-last').value.trim(),
    phone:      document.getElementById('profile-phone').value.trim(),
  };
  msg.textContent = '';
  try {
    const d = await apiFetch(`/users/${state.userId}/profile`, { method: 'PATCH', body: JSON.stringify(body) });
    if (!d) return;
    msg.style.color = 'var(--green)'; msg.textContent = '✓ Profile saved';
  } catch(e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

document.getElementById('profile-pw-btn').addEventListener('click', async () => {
  const msg  = document.getElementById('profile-pw-msg');
  const cur  = document.getElementById('profile-pw-current').value;
  const nw   = document.getElementById('profile-pw-new').value;
  const conf = document.getElementById('profile-pw-confirm').value;
  msg.textContent = '';
  if (!cur || !nw) { msg.style.color = 'var(--red)'; msg.textContent = 'All fields are required.'; return; }
  if (nw !== conf) { msg.style.color = 'var(--red)'; msg.textContent = 'New passwords do not match.'; return; }
  if (nw.length < 8) { msg.style.color = 'var(--red)'; msg.textContent = 'New password must be at least 8 characters.'; return; }
  try {
    const d = await apiFetch('/auth/change-password', { method: 'POST', body: JSON.stringify({ current_password: cur, new_password: nw }) });
    if (!d) return;
    msg.style.color = 'var(--green)'; msg.textContent = '✓ Password updated';
    document.getElementById('profile-pw-current').value = '';
    document.getElementById('profile-pw-new').value = '';
    document.getElementById('profile-pw-confirm').value = '';
  } catch(e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

document.getElementById('profile-signout-btn').addEventListener('click', () => signOut());

document.getElementById('profile-delete-btn').addEventListener('click', async () => {
  const msg = document.getElementById('profile-delete-msg');
  if (!confirm('Are you sure you want to permanently delete your account? This cannot be undone.')) return;
  if (!confirm('Last warning: all your data will be deleted forever. Continue?')) return;
  try {
    const d = await apiFetch(`/users/${state.userId}`, { method: 'DELETE' });
    if (!d) return;
    state.token = null; state.userId = null; state.holdings = [];
    try { localStorage.clear(); } catch { /* storage blocked */ }
    navigate('auth', { replace: true });
    showToast('Account deleted.', 'ok');
  } catch(e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

