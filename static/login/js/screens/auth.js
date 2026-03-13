// ── Guard against SES/MetaMask lockdown unhandled rejections ────────────────
window.addEventListener('unhandledrejection', function(e) {
  const msg = (e.reason && (e.reason.message || String(e.reason))) || '';
  if (
    msg.includes('Access to storage') ||
    msg.includes('intrinsics') ||
    msg.includes('lockdown') ||
    msg.includes('SES') ||
    msg.includes('Compartment')
  ) {
    e.preventDefault(); // suppress — these come from MetaMask/extension, not our code
  }
});

// ── Connection ping ───────────────────────────────────────────────────────────
async function pingConnection() {
  try {
    await apiFetch('/health');
    document.getElementById('conn-dot').className = 'ok';
    document.getElementById('conn-label').textContent = 'CONNECTED';
  } catch {
    document.getElementById('conn-dot').className = 'err';
    document.getElementById('conn-label').textContent = 'DISCONNECTED';
  }
}

// ── AUTH ──────────────────────────────────────────────────────────────────────

// Animated number counter for login stats
function _animateValue(el, target, duration) {
  if (!el || isNaN(target)) { if (el) el.textContent = target; return; }
  const start = 0;
  const startTime = performance.now();
  function tick(now) {
    const elapsed = now - startTime;
    const progress = Math.min(elapsed / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
    el.textContent = Math.round(start + (target - start) * eased).toLocaleString();
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

// Fetch live stats for login page left panel
async function _loadLoginStats() {
  try {
    const health = await fetch(API + '/health').then(r => r.json()).catch(() => null);
    if (health && health.kb_facts) {
      _animateValue(document.getElementById('login-stat-facts'), health.kb_facts, 1200);
    }
  } catch { /* ignore */ }
  try {
    const patterns = await fetch(API + '/patterns/live?limit=1').then(r => r.json()).catch(() => null);
    if (patterns) {
      const total = patterns.total || patterns.count || (patterns.patterns ? patterns.patterns.length : 0);
      if (total) _animateValue(document.getElementById('login-stat-signals'), total, 1000);
      // Top conviction ticker
      const top = patterns.patterns && patterns.patterns[0];
      if (top) {
        const el = document.getElementById('login-stat-ticker');
        if (el) el.textContent = top.ticker || '—';
      }
    }
  } catch { /* ignore */ }
}

// Register form
document.getElementById('reg-btn').addEventListener('click', async () => {
  const email = document.getElementById('reg-email').value.trim();
  const pw    = document.getElementById('reg-pw').value;
  const pw2   = document.getElementById('reg-pw2').value;
  const beta  = document.getElementById('reg-beta').value;
  const msg   = document.getElementById('reg-msg');
  if (!email || !pw) { msg.textContent = 'Email and password required.'; return; }
  if (pw !== pw2)    { msg.textContent = 'Passwords do not match.'; return; }
  if (pw.length < 8) { msg.textContent = 'Password must be at least 8 characters.'; return; }
  if (!beta)         { msg.style.color = 'var(--red)'; msg.textContent = 'Beta access password is required.'; return; }
  const uid = email.split('@')[0].toLowerCase().replace(/[^a-z0-9_-]/g, '_').slice(0, 32)
              + '_' + Math.random().toString(36).slice(2, 7);
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch('/auth/register', { method: 'POST', body: JSON.stringify({ user_id: uid, email, password: pw, beta_password: beta }) });
    if (!d) { msg.textContent = 'Registration failed.'; return; }
    msg.style.color = 'var(--green)';
    msg.textContent = 'Account created — redirecting to sign in…';
    // Pre-fill login email and switch to login screen
    setTimeout(() => {
      document.getElementById('login-email').value = email;
      navigate('login');
    }, 1200);
  } catch (e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

// Login form
document.getElementById('login-btn').addEventListener('click', async () => {
  const email = document.getElementById('login-email').value.trim();
  const pw    = document.getElementById('login-pw').value;
  const msg   = document.getElementById('login-msg');
  if (!email || !pw) { msg.textContent = 'Email and password required.'; return; }
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch('/auth/token', { method: 'POST', body: JSON.stringify({ email, password: pw }) });
    if (!d) { msg.style.color = 'var(--red)'; msg.textContent = 'Sign in failed — check your credentials.'; return; }
    try { await _saveSession(d.access_token || d.token, d.user_id); } catch { /* storage blocked by extension — session still valid via cookie */ }
    const _nextRaw = new URLSearchParams(window.location.search).get('next') || '';
    const _nextScreen = _nextRaw && _NEXT_RE.test(_nextRaw)
      ? _screenFromPath(_nextRaw)
      : 'dashboard';
    navigate(_nextScreen || 'dashboard', { replace: true });
  } catch (e) {
    const errMsg = (e && e.message) || String(e);
    // Ignore SES/lockdown errors from browser extensions — proceed normally
    if (errMsg.includes('storage') || errMsg.includes('intrinsics') || errMsg.includes('lockdown')) {
      navigate('dashboard', { replace: true });
      return;
    }
    msg.style.color = 'var(--red)'; msg.textContent = errMsg;
  }
});

// Enter key submits forms
document.getElementById('login-pw').addEventListener('keydown', e => {
  if (e.key === 'Enter') document.getElementById('login-btn').click();
});
document.getElementById('reg-beta').addEventListener('keydown', e => {
  if (e.key === 'Enter') document.getElementById('reg-btn').click();
});

async function _saveSession(token, userId, tgData) {
  state.token  = token;   // keep in memory for legacy references
  state.userId = userId;
  // Tokens are now stored in HttpOnly cookies set by the backend.
  // Only non-sensitive display data goes in localStorage.
  try {
    if (tgData) localStorage.setItem('tg_user_data', JSON.stringify(tgData));
    localStorage.setItem('tg_user_id', userId);  // display only — not a secret
  } catch { /* storage blocked in some contexts */ }
  _renderTgChip(tgData || null, userId);
  // Fetch is_dev + tier so gating applies immediately after login
  try {
    const me = await apiFetch('/auth/me');
    state.isDev = !!(me && me.is_dev);
    state.tier  = (me && me.tier) ? me.tier.toLowerCase() : 'free';
  } catch { state.isDev = false; state.tier = 'free'; }
  applyDevGating();
  applyTierGating();
  applySubscriptionGating();
}

function _renderTgChip(tgData, userId) {
  const loginBtn = document.getElementById('tg-login-btn');
  const chip     = document.getElementById('tg-user-chip');
  const nameEl   = document.getElementById('tg-name');
  const avatarEl = document.getElementById('tg-avatar');
  if (userId) {
    if (loginBtn) loginBtn.style.display = 'none';
    chip.style.display = 'flex';
    const name = tgData
      ? (tgData.first_name + (tgData.last_name ? ' ' + tgData.last_name : ''))
      : userId;
    nameEl.textContent = name;
    if (tgData?.photo_url) {
      avatarEl.src = tgData.photo_url;
      avatarEl.style.display = 'block';
    } else {
      avatarEl.style.display = 'none';
    }
  } else {
    if (loginBtn) loginBtn.style.display = 'flex';
    chip.style.display = 'none';
  }
}

async function signOut() {
  try { if (state.userId) localStorage.removeItem(`simulated_${state.userId}`); } catch { /* storage blocked */ }
  // Ask backend to clear HttpOnly cookies + revoke refresh token
  try { await fetch(API + '/auth/logout', { method: 'POST', credentials: 'include',
    headers: { 'Content-Type': 'application/json' } }); } catch { /* ignore */ }
  state.token = null; state.userId = null; state.isDev = false; state.tier = 'basic'; state.chatQueriesUsedToday = 0; state.holdings = [];
  try { localStorage.removeItem('tg_user_id'); localStorage.removeItem('tg_user_data'); } catch { /* storage blocked */ }
  _renderTgChip(null, null);
  window.history.replaceState(null, '', '/login');
  showScreen('login');
}

// ── Telegram Login Widget callback ────────────────────────────────────────
window.onTelegramAuth = async function(tgData) {
  try {
    const d = await apiFetch('/auth/telegram', {
      method: 'POST',
      body: JSON.stringify(tgData),
    });
    if (!d || !d.access_token) { showToast('Telegram auth failed'); return; }
    await _saveSession(d.access_token, d.user_id || String(tgData.id), tgData);
    showToast('Signed in as ' + (tgData.first_name || tgData.username), 'ok');
    navigate('dashboard', { replace: true });
  } catch(e) {
    showToast('Sign-in error: ' + e.message);
  }
};


