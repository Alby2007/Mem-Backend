// ── Alert badge polling ────────────────────────────────────────────────────────
function _updateAlertBadge(count, hasCritical) {
  const badge  = document.getElementById('chat-alert-badge');
  const mbadge = document.getElementById('mchat-alert-badge');
  const label  = count > 0 ? (count > 9 ? '9+' : String(count)) : '';
  const color  = hasCritical ? '#e53e3e' : '#d97706';
  [badge, mbadge].forEach(el => {
    if (!el) return;
    if (count > 0) {
      el.textContent = label;
      el.style.background = color;
      el.style.display = 'inline-block';
    } else {
      el.style.display = 'none';
    }
  });
}

async function _pollAlertBadge() {
  try {
    const data = await apiFetch('/alerts/pending');
    if (data && typeof data.count === 'number') {
      _updateAlertBadge(data.count, data.critical > 0);
    }
  } catch { /* non-blocking */ }
}

// ── Boot ──────────────────────────────────────────────────────────────────────
async function doSignOut() {
  try { await fetch(API + '/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
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

      // Start alert badge polling (every 60s, immediate first poll)
      _pollAlertBadge();
      setInterval(_pollAlertBadge, 60000);
    } else {
      _showLoginFallback();
    }
  } catch { _showLoginFallback(); }
})();
