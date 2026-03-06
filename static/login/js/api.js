// ── API base ─────────────────────────────────────────────────────────────────
const API = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
    ? 'http://localhost:5050'
    : 'https://api.trading-galaxy.uk';

// ── apiFetch ─────────────────────────────────────────────────────────────────
// credentials:'include' sends HttpOnly cookies cross-origin to api.trading-galaxy.uk
let _refreshing = null; // deduplicate concurrent refresh attempts
async function apiFetch(path, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
  let res;
  try {
    res = await fetch(API + path, { ...opts, headers, credentials: 'include' });
  } catch (e) {
    showToast('Connection error — check your internet');
    return null;
  }
  if (res.status === 401) {
    // Don't try to refresh if this IS the refresh call (avoid infinite loop)
    if (path === '/auth/refresh') { return null; }
    // Attempt silent token refresh using the tg_refresh HttpOnly cookie
    if (!_refreshing) {
      _refreshing = fetch(API + '/auth/refresh', {
        method: 'POST', credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
      }).finally(() => { _refreshing = null; });
    }
    try {
      const ref = await _refreshing;
      if (ref && ref.ok) {
        // New tg_access cookie is now set — retry original request once
        res = await fetch(API + path, { ...opts, headers, credentials: 'include' });
        if (res.ok) {
          return await res.json().catch(() => ({}));
        }
      }
    } catch { /* refresh failed */ }
    // Refresh failed — caller handles no-session state
    return null;
  }
  if (res.status === 429) { showToast('Too many requests — please wait a moment'); return null; }
  const data = await res.json().catch(() => ({}));
  if (res.status === 403 && data.error === 'upgrade_required') { handleUpgradeRequired(data); return null; }
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

let _sessionExpired = false;
function _handleSessionExpired() {
  if (_sessionExpired) return;
  _sessionExpired = true;
  state.token = null; state.userId = null;
  try { localStorage.removeItem('tg_user_id'); localStorage.removeItem('tg_user_data'); } catch { /* blocked */ }
  _renderTgChip(null, null);
  showScreen('login');
  showToast('Session expired — please sign in again');
}

// ── Toast ─────────────────────────────────────────────────────────────────────
let toastTimer = null;
function showToast(msg, type = 'error') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.style.background = type === 'ok' ? 'var(--green)' : 'var(--red)';
  el.style.display = 'block';
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.style.display = 'none'; }, 4000);
}

// ── Inject Telegram widget into auth screen ───────────────────────────────────
let _tgAuthWidgetInjected = false;
function _injectAuthTgWidget() {
  if (_tgAuthWidgetInjected) return;
  _tgAuthWidgetInjected = true;
  const container = document.getElementById('tg-widget-auth');
  if (!container) return;
  const s = document.createElement('script');
  s.src = 'https://telegram.org/js/telegram-widget.js?22';
  s.setAttribute('data-telegram-login', 'TheTelescopeBot');
  s.setAttribute('data-size', 'large');
  s.setAttribute('data-radius', '5');
  s.setAttribute('data-onauth', 'onTelegramAuth(user)');
  s.setAttribute('data-request-access', 'write');
  container.appendChild(s);
}

