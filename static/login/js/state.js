// ── State ────────────────────────────────────────────────────────────────────
const state = {
  token: null,
  userId: null,
  isDev: false,
  tier: 'free',
  chatQueriesUsedToday: 0,
  holdings: [],           // pending holdings before submit
  cashBalance: 0,         // paper account free cash (60s TTL — see _cashFetchedAt)
  _cashFetchedAt: 0,      // epoch ms timestamp of last cashBalance fetch
  watchlistTickers: [],   // tickers from user watchlist (for feedback widget gating)
};

// ── Tier config (mirrors core/tiers.py — kept in sync manually until Stripe) ──
const TIER_CONFIG = {
  free:    { opportunity_scan: false, chat_queries_per_day: 0,    t3_targets: false, live_price_fetch: false },
  basic:   { opportunity_scan: false, chat_queries_per_day: 10,   t3_targets: false, live_price_fetch: false },
  pro:     { opportunity_scan: true,  chat_queries_per_day: null,  t3_targets: false, live_price_fetch: true  },
  premium: { opportunity_scan: true,  chat_queries_per_day: null,  t3_targets: true,  live_price_fetch: true  },
};

const _SUBSCRIPTION_FREE_SCREENS = new Set(['profile', 'subscription', 'login', 'register']);

function _hasSubscription() {
  return state.tier === 'basic' || state.tier === 'pro' || state.tier === 'premium';
}

function applySubscriptionGating() {
  const locked = !_hasSubscription();
  document.querySelectorAll('.nav-item[data-screen], .mnav-item[data-screen]').forEach(el => {
    const screen = el.dataset.screen;
    if (_SUBSCRIPTION_FREE_SCREENS.has(screen)) {
      el.classList.remove('locked');
    } else {
      if (locked) el.classList.add('locked');
      else        el.classList.remove('locked');
    }
  });
  // Always show Subscription nav item — users need to manage/cancel active subscriptions
  const navSub  = document.getElementById('nav-subscription');
  const mnavSub = document.getElementById('mnav-subscription');
  if (navSub)  navSub.style.display  = '';
  if (mnavSub) mnavSub.style.display = '';
}

function checkFeature(feature) {
  return TIER_CONFIG[state.tier]?.[feature] ?? false;
}

function handleUpgradeRequired(resp) {
  showToast('Upgrade required — visit Subscription to unlock this feature.', 'error');
  navigate('subscription');
}

function _renderChatQuotaBadge() {
  const badge = document.getElementById('chat-quota-badge');
  if (!badge) return;
  const limit = TIER_CONFIG[state.tier]?.chat_queries_per_day;
  if (limit === null || limit === undefined || state.tier !== 'basic') {
    badge.style.display = 'none';
    return;
  }
  badge.style.display = '';
  badge.textContent = `${state.chatQueriesUsedToday} / ${limit} queries today`;
}

function applyTierGating() {
  const tier = state.tier;

  // Opportunity scan button — Pro+ only
  const scanBtn = document.getElementById('opportunity-scan-btn');
  if (scanBtn) {
    if (!checkFeature('opportunity_scan')) {
      scanBtn.style.display = 'none';
      let label = document.getElementById('scan-upgrade-label');
      if (!label) {
        label = document.createElement('span');
        label.id = 'scan-upgrade-label';
        label.className = 'text-xs text-muted';
        label.textContent = 'Pro feature — upgrade to scan';
        label.style.cursor = 'pointer';
        label.addEventListener('click', () => { window.location.href = '/subscription'; });
        scanBtn.parentNode && scanBtn.parentNode.appendChild(label);
      }
      label.style.display = '';
    } else {
      scanBtn.style.display = '';
      const label = document.getElementById('scan-upgrade-label');
      if (label) label.style.display = 'none';
    }
  }

  // T3 targets — Premium only
  if (!checkFeature('t3_targets')) {
    document.querySelectorAll('.t3-target').forEach(el => el.style.display = 'none');
  } else {
    document.querySelectorAll('.t3-target').forEach(el => el.style.display = '');
  }

  // Chat quota badge — Basic only
  _renderChatQuotaBadge();
}

function applyDevGating() {
  const dev = state.isDev;
  const subscribed = _hasSubscription();
  // Network: dev-only always
  const navNetwork = document.getElementById('nav-network');
  if (navNetwork) navNetwork.style.display = dev ? '' : 'none';
  const mnavNetwork = document.getElementById('mnav-network');
  if (mnavNetwork) mnavNetwork.style.display = dev ? '' : 'none';
  // Patterns + History: visible to any subscribed user (or dev)
  const showPH = dev || subscribed;
  const navPatterns = document.getElementById('nav-patterns');
  if (navPatterns) navPatterns.style.display = showPH ? '' : 'none';
  const navHistory = document.getElementById('nav-history');
  if (navHistory) navHistory.style.display = showPH ? '' : 'none';
  const mnavPatterns = document.getElementById('mnav-patterns');
  if (mnavPatterns) mnavPatterns.style.display = showPH ? '' : 'none';
  const mnavHistory = document.getElementById('mnav-history');
  if (mnavHistory) mnavHistory.style.display = showPH ? '' : 'none';
  // Topbar KB count
  const kbCount = document.getElementById('kb-count');
  if (kbCount) kbCount.style.display = dev ? '' : 'none';
  // Portfolio: Generate Test Portfolio + Change Portfolio dropdown
  const genBtn = document.getElementById('p-gen-sim-btn');
  if (genBtn) genBtn.style.display = dev ? '' : 'none';
  const profileSel = document.getElementById('p-change-profile-select');
  if (profileSel) profileSel.style.display = dev ? '' : 'none';
  // Tips: Tier row
  const tierRow = document.getElementById('tip-tier-row');
  if (tierRow) tierRow.style.display = dev ? '' : 'none';
  // History: Atoms button
  const atomsBtn = document.getElementById('hist-atoms-btn');
  if (atomsBtn) atomsBtn.style.display = dev ? '' : 'none';
  // Dashboard: adapter panel vs active positions
  const adapterPanel = document.getElementById('dash-adapters-panel');
  const posPanel     = document.getElementById('dash-positions-panel');
  if (adapterPanel) adapterPanel.style.display = dev ? '' : 'none';
  if (posPanel)     posPanel.style.display     = dev ? 'none' : '';
}

