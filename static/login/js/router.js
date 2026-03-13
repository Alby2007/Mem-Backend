// ── Router ────────────────────────────────────────────────────────────────────
const _SCREENS = ['dashboard','portfolio','markets','visualiser','chat','tips','patterns','network','history','journal','paper','private-fleet','subscription','profile'];
const _AUTH_SCREENS = new Set(['login', 'register']);
const _NEXT_RE = /^\/[a-z0-9_-]+\/(dashboard|portfolio|markets|visualiser|chat|tips|patterns|network|history|journal|paper|private-fleet|subscription|profile)$/;

function _screenFromPath(path) {
  // /:username/screen  → screen name
  const parts = path.replace(/^\//, '').split('/');
  if (parts.length === 2 && _SCREENS.includes(parts[1])) return parts[1];
  if (parts[0] === 'login' || path === '/login') return 'login';
  if (parts[0] === 'register' || path === '/register') return 'register';
  return null;
}

function _pathForScreen(name) {
  if (name === 'login') return '/login';
  if (name === 'register') return '/register';
  if (name === 'profile') return `/${state.userId || '_'}/profile`;
  return `/${state.userId || '_'}/${name}`;
}

const _SCREEN_TITLES = {
  dashboard: 'Dashboard', portfolio: 'Portfolio', markets: 'Markets',
  visualiser: 'Visualiser', chat: 'Chat', tips: 'Tips', patterns: 'Patterns',
  network: 'Network', history: 'History', journal: 'Journal',
  paper: 'Paper Trader', 'private-fleet': 'Private Fleet', subscription: 'Subscription', profile: 'Profile',
  login: 'Sign In', register: 'Create Account',
};

function showScreen(name) {
  document.title = (_SCREEN_TITLES[name] ? _SCREEN_TITLES[name] + ' — ' : '') + 'Trading Galaxy';
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.mnav-item').forEach(n => n.classList.remove('active'));
  const sc = document.getElementById(`screen-${name}`);
  const nv = document.getElementById(`nav-${name}`);
  const mn = document.getElementById(`mnav-${name}`);
  if (sc) sc.classList.add('active');
  if (nv) nv.classList.add('active');
  if (mn) mn.classList.add('active');
  const isAuth = _AUTH_SCREENS.has(name);
  // Explicitly set display to override any inline styles set by applyDevGating/pingConnection
  const topbar   = document.getElementById('topbar');
  const sidebar  = document.getElementById('sidebar');
  const mobileNav = document.getElementById('mobile-nav');
  const main     = document.getElementById('main');
  if (topbar)    topbar.style.display    = isAuth ? 'none' : '';
  if (sidebar)   sidebar.style.display   = isAuth ? 'none' : '';
  if (mobileNav) mobileNav.style.display = isAuth ? 'none' : '';
  if (main)      main.style.display      = isAuth ? 'none' : '';
  document.body.classList.toggle('auth-mode', isAuth);
  if (isAuth) {
    document.documentElement.setAttribute('data-auth', name);
  } else {
    document.documentElement.removeAttribute('data-auth');
  }
  if (name === 'chat')      { _updateAlertBadge(0, false); apiFetch('/chat/workflow', { method: 'DELETE' }).catch(() => {}); }
  if (name === 'dashboard') loadDashboard();
  if (name === 'patterns')  loadPatterns();
  if (name === 'network')   loadNetwork();
  if (name === 'history')   loadHistory();
  if (name === 'journal')   loadJournal();
  if (name === 'tips')      { loadTipsHistory(); loadTipsAccountValue(); loadTipConfig(); }
  if (name === 'portfolio') { loadPortfolioModel(); loadPortfolioHoldings(); loadTickerList(); loadSimBannerIfSet(); }
  if (name === 'markets')    initMarketsScreen();
  if (main) main.style.overflow = (name === 'visualiser') ? 'hidden' : '';
  if (name === 'visualiser') loadVisualiser();
  if (name === 'paper')          loadPaperTrader();
  if (name === 'private-fleet') loadPrivateFleet();
  if (name === 'login')         { _injectAuthTgWidget(); _loadLoginStats(); }
  if (name === 'register')      {}
  if (name === 'profile')      loadProfile();
  if (name === 'subscription') loadSubscription();
}

function navigate(name, { replace = false } = {}) {
  if (!_AUTH_SCREENS.has(name) && !state.userId) {
    // Auth guard — hard redirect to standalone /login page
    const intended = _pathForScreen(name);
    const next = _NEXT_RE.test(intended) ? encodeURIComponent(intended) : '';
    window.location.replace('/login' + (next ? `?next=${next}` : ''));
    return;
  }
  // Subscription guard — redirect to /subscription for gated screens
  if (!_SUBSCRIPTION_FREE_SCREENS.has(name) && !_hasSubscription() && state.userId) {
    const path = _pathForScreen('subscription');
    if (replace) window.history.replaceState(null, '', path);
    else         window.history.pushState(null, '', path);
    showScreen('subscription');
    return;
  }
  const path = _pathForScreen(name);
  if (replace) window.history.replaceState(null, '', path);
  else         window.history.pushState(null, '', path);
  showScreen(name);
}

window.addEventListener('popstate', () => {
  const name = _screenFromPath(window.location.pathname);
  if (!name || (!_AUTH_SCREENS.has(name) && !state.userId)) {
    showScreen('login');
    return;
  }
  if (!_SUBSCRIPTION_FREE_SCREENS.has(name) && !_hasSubscription() && state.userId) {
    showScreen('subscription');
    return;
  }
  showScreen(name);
});

document.querySelectorAll('.nav-item, .mnav-item').forEach(el => {
  el.addEventListener('click', () => {
    if (el.dataset.noNav) return;
    const s = el.dataset.screen;
    if (!s) return;
    if (!_AUTH_SCREENS.has(s) && !state.userId) { navigate('login'); return; }
    if (!_SUBSCRIPTION_FREE_SCREENS.has(s) && !_hasSubscription()) {
      navigate('subscription');
      return;
    }
    navigate(s);
  });
});

