// ── Boot ──────────────────────────────────────────────────────────────────────
async function doSignOut() {
  try { await fetch('/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
  window.location.replace('/');
}

function _showLoginFallback() {
  const p = _screenFromPath(window.location.pathname);
  if (p === 'register') { window.location.replace('/register'); return; }
  if (p && !_AUTH_SCREENS.has(p)) {
    window.location.replace('/login?next=' + encodeURIComponent(window.location.pathname));
  } else {
    window.location.replace('/login');
  }
}

(async () => {
  try {
    pingConnection();
    setInterval(pingConnection, 300000);

    const me = await apiFetch('/auth/me');

    if (me && me.user_id) {
      state.userId = me.user_id;
      state.isDev  = !!me.is_dev;
      state.tier   = (me.tier || 'free').toLowerCase();
      state.token  = '__cookie__';
      let storedTgData = null;
      try { storedTgData = localStorage.getItem('tg_user_data'); } catch { /* storage blocked */ }
      const tgData = storedTgData ? JSON.parse(storedTgData) : null;
      _renderTgChip(tgData, me.user_id);
      applyDevGating();
      applyTierGating();
      applySubscriptionGating();

      const _sp = new URLSearchParams(window.location.search);
      if (_sp.get('success') === '1') {
        showToast('Payment successful — your plan has been upgraded!', 'ok');
        window.history.replaceState(null, '', window.location.pathname);
      } else if (_sp.get('cancelled') === '1') {
        showToast('Checkout cancelled — no charge made.', 'ok');
        window.history.replaceState(null, '', window.location.pathname);
      }

      const fromPath = _screenFromPath(window.location.pathname);
      const targetScreen = (fromPath && !_AUTH_SCREENS.has(fromPath)) ? fromPath : 'dashboard';
      navigate(targetScreen, { replace: true });
    } else {
      _showLoginFallback();
    }
  } catch { _showLoginFallback(); }
})();
