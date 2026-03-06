// ── SUBSCRIPTION ─────────────────────────────────────────────────────────────
let _subIsAnnual = false;
const _subPrices = {
  basic:   { monthly: '£9',  annual: '£7',  annualTotal: '£86',  saving: '£22' },
  pro:     { monthly: '£29', annual: '£23', annualTotal: '£278', saving: '£70' },
  premium: { monthly: '£79', annual: '£63', annualTotal: '£758', saving: '£190' },
};
const _subMonthlyLabel = { basic: 'Basic — £9/month', pro: 'Pro — £29/month', premium: 'Premium — £79/month' };

function subSetMonthly() {
  _subIsAnnual = false;
  document.getElementById('sub-tog-monthly').classList.add('active');
  document.getElementById('sub-tog-annual').classList.remove('active');
  _subUpdatePrices();
}

function subSetAnnual() {
  _subIsAnnual = true;
  document.getElementById('sub-tog-monthly').classList.remove('active');
  document.getElementById('sub-tog-annual').classList.add('active');
  _subUpdatePrices();
}

function _subUpdatePrices() {
  for (const [tier, p] of Object.entries(_subPrices)) {
    const priceEl  = document.getElementById(`sub-${tier}-price`);
    const annualEl = document.getElementById(`sub-${tier}-annual`);
    if (!priceEl) continue;
    if (_subIsAnnual) {
      priceEl.textContent = p.annual;
      annualEl.innerHTML  = `${p.annualTotal}/yr · <span>save ${p.saving}</span>`;
      annualEl.style.visibility = 'visible';
    } else {
      priceEl.textContent       = p.monthly;
      annualEl.style.visibility = 'hidden';
    }
  }
}

async function loadSubscription() {
  if (!state.userId) return;
  // Reset toggle to monthly each visit
  _subIsAnnual = false;
  document.getElementById('sub-tog-monthly').classList.add('active');
  document.getElementById('sub-tog-annual').classList.remove('active');
  _subUpdatePrices();

  // Fetch user tier from /auth/me (already cached in state if we want, but re-fetch for freshness)
  try {
    const me = await apiFetch('/auth/me');
    const tier = (me?.tier || 'free').toLowerCase();
    state.tier = tier;  // keep state fresh
    // Locked-state banner
    const lockedBanner = document.getElementById('sub-locked-banner');
    if (lockedBanner) lockedBanner.classList.toggle('hidden', tier !== 'free');
    // Update current plan banner
    const planNames = { free: 'Free — no subscription', basic: 'Basic — £9/month', pro: 'Pro — £29/month', premium: 'Premium — £79/month' };
    document.getElementById('sub-plan-name').textContent   = planNames[tier] || tier;
    document.getElementById('sub-plan-renews').textContent = tier === 'free' ? 'No active subscription' : 'Billed monthly';

    // Highlight current card and update CTA buttons
    ['basic', 'pro', 'premium'].forEach(t => {
      const card   = document.getElementById(`sub-card-${t}`);
      const badge  = document.getElementById(`sub-badge-${t}`);
      const cta    = document.getElementById(`sub-cta-${t}`);
      const isC    = t === tier;
      if (!card) return;
      card.classList.toggle('sub-is-current', isC);
      if (badge) badge.style.display = isC ? '' : 'none';
      if (cta) {
        if (isC) {
          cta.className = 'sub-cta-btn sub-cta-current';
          cta.textContent = 'Current Plan';
          cta.onclick = null;
        } else {
          const isUp = ['basic','pro','premium'].indexOf(t) > ['basic','pro','premium'].indexOf(tier);
          cta.className = isUp ? 'sub-cta-btn sub-cta-primary' : 'sub-cta-btn sub-cta-secondary';
          cta.textContent = isUp ? `Upgrade to ${t.charAt(0).toUpperCase()+t.slice(1)} →` : `Switch to ${t.charAt(0).toUpperCase()+t.slice(1)}`;
          cta.onclick = () => startCheckout(t);
        }
      }
    });

    // Pro badge: show MOST POPULAR only if not on pro
    const proBadge = document.getElementById('sub-badge-pro');
    if (proBadge && tier !== 'pro') { proBadge.className = 'sub-badge sub-badge-popular'; proBadge.style.display = ''; proBadge.textContent = 'MOST POPULAR'; }
  } catch(e) { /* silent — banner stays at defaults */ }
}

async function startCheckout(tier) {
  if (!state.userId) { navigate('auth'); return; }
  const annual = document.getElementById('sub-tog-annual')?.classList.contains('active') || false;
  const btn = document.getElementById(`sub-cta-${tier}`);
  if (btn) { btn.disabled = true; btn.textContent = 'Redirecting…'; }
  try {
    const d = await apiFetch('/stripe/checkout', {
      method: 'POST',
      body: JSON.stringify({ tier, annual }),
    });
    if (d?.url) {
      window.location.href = d.url;
    } else {
      showToast('Could not start checkout — please try again', 'error');
      if (btn) { btn.disabled = false; btn.textContent = `Upgrade to ${tier.charAt(0).toUpperCase()+tier.slice(1)} →`; }
    }
  } catch(e) {
    showToast('Checkout error: ' + e.message, 'error');
    if (btn) { btn.disabled = false; btn.textContent = `Upgrade to ${tier.charAt(0).toUpperCase()+tier.slice(1)} →`; }
  }
}

async function openBillingPortal() {
  if (!state.userId) return;
  const btn = document.getElementById('sub-manage-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Opening…'; }
  try {
    const d = await apiFetch('/stripe/portal', { method: 'POST' });
    if (d?.url) {
      window.location.href = d.url;
    } else {
      showToast('Billing portal unavailable — contact support', 'error');
      if (btn) { btn.disabled = false; btn.textContent = 'Manage Billing →'; }
    }
  } catch(e) {
    showToast('Portal error: ' + e.message, 'error');
    if (btn) { btn.disabled = false; btn.textContent = 'Manage Billing →'; }
  }
}

// TEMPORARY: dev/test helper — grants current user premium access instantly
async function devUpgradePremium() {
  const btn = document.getElementById('sub-test-access-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Activating…'; }
  try {
    const d = await apiFetch('/dev/upgrade-premium', {
      method: 'POST',
      body: JSON.stringify({ user_id: state.userId }),
    });
    if (d?.ok) {
      showToast('Premium access activated!', 'success');
      // Re-fetch /auth/me to sync tier from DB (source of truth)
      try {
        const me = await apiFetch('/auth/me');
        state.tier = (me && me.tier) ? me.tier.toLowerCase() : 'premium';
      } catch { state.tier = 'premium'; }
      applyTierGating();
      applySubscriptionGating();
      setTimeout(() => { navigate('dashboard'); }, 800);
    } else {
      showToast(d?.error || 'Upgrade failed — try again', 'error');
      if (btn) { btn.disabled = false; btn.textContent = 'Get Test Access'; }
    }
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
    if (btn) { btn.disabled = false; btn.textContent = 'Get Test Access'; }
  }
}

