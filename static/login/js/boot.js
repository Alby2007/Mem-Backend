// ── Boot ──────────────────────────────────────────────────────────────────────
(async () => {
  await pingConnection();
  setInterval(pingConnection, 300000);

  // Session restore — let the HttpOnly cookie do the work via /auth/me
  try {
    const me = await apiFetch('/auth/me');
    if (me && me.user_id) {
      state.userId = me.user_id;
      state.isDev  = !!me.is_dev;
      state.tier   = (me.tier || 'free').toLowerCase();
      state.token  = '__cookie__'; // sentinel — actual token is in HttpOnly cookie
      let storedTgData = null;
      try { storedTgData = localStorage.getItem('tg_user_data'); } catch { /* storage blocked */ }
      const tgData = storedTgData ? JSON.parse(storedTgData) : null;
      _renderTgChip(tgData, me.user_id);
      applyDevGating();
      applyTierGating();
      applySubscriptionGating();

      // Handle Stripe return params before routing
      const _sp = new URLSearchParams(window.location.search);
      if (_sp.get('success') === '1') {
        showToast('Payment successful — your plan has been upgraded!', 'ok');
        window.history.replaceState(null, '', window.location.pathname);
      } else if (_sp.get('cancelled') === '1') {
        showToast('Checkout cancelled — no charge made.', 'ok');
        window.history.replaceState(null, '', window.location.pathname);
      }

      // Route to the screen from the URL if valid, else dashboard
      const fromPath = _screenFromPath(window.location.pathname);
      const targetScreen = (fromPath && fromPath !== 'auth') ? fromPath : 'dashboard';
      navigate(targetScreen, { replace: true });
      return;
    }
  } catch { /* no cookie or expired — fall through to auth */ }

  // No valid session — if URL had a protected path, preserve as ?next=
  const fromPath = _screenFromPath(window.location.pathname);
  if (fromPath && fromPath !== 'auth') {
    const nextPath = window.location.pathname;
    window.history.replaceState(null, '', '/login?next=' + encodeURIComponent(nextPath));
  } else {
    window.history.replaceState(null, '', '/login');
  }
  showScreen('auth');
})();
