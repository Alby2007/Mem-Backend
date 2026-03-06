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
  // Derive a stable user_id from the email local part + random suffix
  const uid = email.split('@')[0].toLowerCase().replace(/[^a-z0-9_-]/g, '_').slice(0, 32)
              + '_' + Math.random().toString(36).slice(2, 7);
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch('/auth/register', { method: 'POST', body: JSON.stringify({ user_id: uid, email, password: pw, beta_password: beta }) });
    if (!d) { msg.textContent = 'Failed.'; return; }
    msg.style.color = 'var(--green)';
    msg.textContent = `Account created — sign in as ${email}`;
    document.getElementById('login-email').value = email;
  } catch (e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
});

document.getElementById('login-btn').addEventListener('click', async () => {
  const email = document.getElementById('login-email').value.trim();
  const pw    = document.getElementById('login-pw').value;
  const msg   = document.getElementById('login-msg');
  if (!email || !pw) { msg.textContent = 'Email and password required.'; return; }
  msg.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await apiFetch('/auth/token', { method: 'POST', body: JSON.stringify({ email, password: pw }) });
    if (!d) { msg.textContent = 'Failed.'; return; }
    await _saveSession(d.access_token || d.token, d.user_id);
    // Honour ?next= param if it passes the allowlist, else go to dashboard
    const _nextRaw = new URLSearchParams(window.location.search).get('next') || '';
    const _nextScreen = _nextRaw && _NEXT_RE.test(_nextRaw)
      ? _screenFromPath(_nextRaw)
      : 'dashboard';
    navigate(_nextScreen || 'dashboard', { replace: true });
  } catch (e) { msg.style.color = 'var(--red)'; msg.textContent = e.message; }
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
  showScreen('auth');
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


