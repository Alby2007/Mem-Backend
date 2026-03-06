// ── Router ────────────────────────────────────────────────────────────────────
const _SCREENS = ['dashboard','portfolio','markets','chat','tips','patterns','network','history','paper','subscription','profile'];
const _NEXT_RE = /^\/[a-z0-9_-]+\/(dashboard|portfolio|markets|chat|tips|patterns|network|history|paper|subscription|profile)$/;

function _screenFromPath(path) {
  // /:username/screen  → screen name
  const parts = path.replace(/^\//, '').split('/');
  if (parts.length === 2 && _SCREENS.includes(parts[1])) return parts[1];
  if (parts[0] === 'login' || path === '/login') return 'auth';
  return null;
}

function _pathForScreen(name) {
  if (name === 'auth') return '/login';
  if (name === 'profile') return `/${state.userId || '_'}/profile`;
  return `/${state.userId || '_'}/${name}`;
}

function showScreen(name) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.mnav-item').forEach(n => n.classList.remove('active'));
  const sc = document.getElementById(`screen-${name}`);
  const nv = document.getElementById(`nav-${name}`);
  const mn = document.getElementById(`mnav-${name}`);
  if (sc) sc.classList.add('active');
  if (nv) nv.classList.add('active');
  if (mn) mn.classList.add('active');
  document.getElementById('mobile-nav').style.display = name === 'auth' ? 'none' : '';
  document.body.classList.toggle('auth-mode', name === 'auth');
  if (name === 'dashboard') loadDashboard();
  if (name === 'patterns')  loadPatterns();
  if (name === 'network')   loadNetwork();
  if (name === 'history')   loadHistory();
  if (name === 'tips')      { loadTipsHistory(); loadTipsAccountValue(); loadTipConfig(); }
  if (name === 'portfolio') { loadPortfolioModel(); loadPortfolioHoldings(); loadTickerList(); loadSimBannerIfSet(); }
  if (name === 'markets')   initMarketsScreen();
  if (name === 'paper')     loadPaperTrader();
  if (name === 'auth')         _injectAuthTgWidget();
  if (name === 'profile')      loadProfile();
  if (name === 'subscription') loadSubscription();
}

function navigate(name, { replace = false } = {}) {
  if (name !== 'auth' && !state.userId) {
    // Auth guard — redirect to /login?next=/<username>/<screen>
    const intended = _pathForScreen(name);
    const next = _NEXT_RE.test(intended) ? encodeURIComponent(intended) : '';
    const target = '/login' + (next ? `?next=${next}` : '');
    window.history.replaceState(null, '', target);
    showScreen('auth');
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
  if (!name || (name !== 'auth' && !state.userId)) {
    showScreen('auth');
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
    const s = el.dataset.screen;
    if (s !== 'auth' && !state.userId) { navigate('auth'); return; }
    if (!_SUBSCRIPTION_FREE_SCREENS.has(s) && !_hasSubscription()) {
      navigate('subscription');
      return;
    }
    navigate(s);
  });
});

