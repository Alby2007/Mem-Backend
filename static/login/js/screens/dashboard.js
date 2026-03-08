// ── DASHBOARD ─────────────────────────────────────────────────────────────────
let dashInterval = null;

async function loadDashboard() {
  clearInterval(dashInterval);
  await refreshDashboard();
  loadDashboardPositions();
  loadDashboardBottomRow();
  dashInterval = setInterval(refreshDashboard, 60000);
}

// ── Dashboard live clock ──────────────────────────────────────────
function dshUpdateClock() {
  const el = document.getElementById('dsh-live-time');
  if (!el) return;
  const now = new Date();
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const h = String(now.getHours()).padStart(2,'0');
  const m = String(now.getMinutes()).padStart(2,'0');
  el.textContent = `${h}:${m} · ${days[now.getDay()]} ${now.getDate()} ${months[now.getMonth()]} ${now.getFullYear()}`;
}
dshUpdateClock();
setInterval(dshUpdateClock, 10000);

// ── Regime helper maps ─────────────────────────────────────────────
const _REGIME_LABELS = {
  'risk_on_expansion':    'Risk-On Expansion',
  'risk_off_contraction': 'Risk-Off Contraction',
  'stagflation':          'Stagflation',
  'recovery':             'Recovery',
  'no_data':              'No Data',
};
const _REGIME_DESCS = {
  'risk_on_expansion':    'Broad risk-on conditions. Equities outperforming bonds. KB detecting momentum signals across large caps.',
  'risk_off_contraction': 'Defensive positioning dominant. Bonds, gold and defensive equities leading. Elevated fear indicators.',
  'stagflation':          'Growth slowing with inflation persistent. Mixed signals across asset classes. KB in active reconciliation.',
  'recovery':             'Post-correction bounce. Equities leading recovery. KB detecting earnings momentum and FVG fill signals.',
  'no_data':              'Insufficient KB data to determine current market regime. Check ingest adapter status.',
};
const _REGIME_BIAS = {
  'risk_on_expansion': 'Bullish', 'recovery': 'Bullish',
  'risk_off_contraction': 'Bearish', 'stagflation': 'Neutral', 'no_data': '—',
};

async function refreshDashboard() {
  try {
    const stats = await apiFetch('/stats');
    if (!stats) return;
    const facts = stats.total_facts || 0;
    document.getElementById('s-facts').textContent    = facts.toLocaleString();
    document.getElementById('s-patterns').textContent = (stats.open_patterns || 0).toLocaleString();

    // Epistemic activity (stress ratio)
    const ratio = facts ? (stats.total_conflicts_detected / facts) : 0;
    const pct   = (ratio * 100).toFixed(1) + '%';
    document.getElementById('s-stress').textContent = pct;

    // Regime stat tile + detail card
    const regimeRaw  = stats.market_regime || 'no_data';
    const regimeLabel = _REGIME_LABELS[regimeRaw] || regimeRaw.replace(/_/g,' ');
    document.getElementById('s-regime').textContent = regimeLabel;
    document.getElementById('s-regime-sub').textContent = _REGIME_BIAS[regimeRaw]
      ? `${_REGIME_BIAS[regimeRaw]} bias` : 'from KB';

    // Regime detail card
    const rn = document.getElementById('dsh-regime-name');
    const rd = document.getElementById('dsh-regime-desc');
    const rb = document.getElementById('dsh-regime-bias');
    if (rn) rn.textContent = regimeLabel;
    if (rd) rd.textContent = _REGIME_DESCS[regimeRaw] || '';
    if (rb) { rb.textContent = _REGIME_BIAS[regimeRaw] || '—'; rb.className = 'dsh-regime-ind-val ' + ((_REGIME_BIAS[regimeRaw]||'').toLowerCase() === 'bullish' ? 'amber' : (_REGIME_BIAS[regimeRaw]||'').toLowerCase() === 'bearish' ? 'red' : ''); }
    // Regime detail — volatility, sector lead, KB confidence from /stats response
    const rvEl  = document.getElementById('dsh-regime-vol');
    const rsEl  = document.getElementById('dsh-regime-sector');
    const rcEl  = document.getElementById('dsh-regime-conf');
    if (rvEl)  rvEl.textContent  = stats.regime_volatility  ? String(stats.regime_volatility).replace(/_/g,' ') : '—';
    if (rsEl)  rsEl.textContent  = stats.regime_sector_lead ? String(stats.regime_sector_lead).replace(/_/g,' ') : 'Data pending';
    if (rcEl) { rcEl.textContent = stats.regime_kb_confidence != null ? stats.regime_kb_confidence + '% high-conf' : '—'; }

    document.getElementById('kb-count').textContent = `KB: ${facts.toLocaleString()}`;
  } catch(e) { showToast('Stats load failed: ' + e.message); }

  // conviction tickers — now rendered as dsh-conv-item rows
  try {
    const ps  = await apiFetch('/portfolio/summary');
    const top = (ps?.top_conviction || []).slice(0, 5);
    const conv = document.getElementById('dash-conviction');
    if (!top.length) {
      conv.innerHTML = `<div class="dsh-empty"><span class="dsh-empty-icon">📈</span><div class="dsh-empty-title">No conviction data yet</div><div class="dsh-empty-sub">The KB needs patterns to rank. Check back after the next ingest cycle.</div></div>`;
    } else {
      const pLinkEl = document.getElementById('dsh-patterns-link');
      if (pLinkEl && ps?.total_signals) pLinkEl.textContent = `View all ${ps.total_signals} →`;
      const _tierBadgeCls = t => t === 'high' ? 'dsh-badge-high' : t === 'mid' ? 'dsh-badge-mid' : 'dsh-badge-low';
      conv.innerHTML = top.map((t, i) => {
        const upPct   = t.upside_pct != null ? Math.abs(t.upside_pct) : null;
        const upCls   = (t.upside_pct || 0) < 0 ? 'neg' : '';
        const upStr   = upPct != null ? `${(t.upside_pct || 0) >= 0 ? '▲' : '▼'} ${fmt(upPct)}%` : '—';
        const tier    = (t.conviction_tier || 'low').toLowerCase();
        const pat     = t.pattern_type ? t.pattern_type.replace(/_/g,' ') : '';
        const tf      = t.timeframe || '';
        const qual    = t.signal_quality || '';
        const zoneLo  = t.zone_low  != null ? fmt(t.zone_low)  : null;
        const zoneHi  = t.zone_high != null ? fmt(t.zone_high) : null;
        const zoneStr = (zoneLo && zoneHi) ? `Zone: <span>${zoneLo} – ${zoneHi}</span>` : '';
        const barW    = Math.max(10, 100 - i * 18);
        const barClr  = (t.upside_pct || 0) < 0 ? 'var(--red)' : 'var(--accent)';
        return `<div class="dsh-conv-item" onclick="navigate('patterns')">
          <div class="dsh-conv-rank">#${i+1}</div>
          <div class="dsh-conv-ticker">${escHtml(t.ticker)}</div>
          <div class="dsh-conv-meta">
            <div class="dsh-conv-badges">
              <span class="${_tierBadgeCls(tier)}">${tier.toUpperCase()}</span>
              <span>${escHtml(pat)}${tf ? ' · ' + escHtml(tf) : ''}${qual ? ' · Q ' + escHtml(String(qual)) : ''}</span>
            </div>
            ${zoneStr ? `<div class="dsh-conv-zone">${zoneStr}</div>` : ''}
          </div>
          <div class="dsh-conv-pct ${upCls}">${upStr}</div>
          <div class="dsh-conv-bar" style="width:${barW}%;background:${barClr}"></div>
        </div>`;
      }).join('');
    }
  } catch { document.getElementById('dash-conviction').innerHTML = `<div class="dsh-empty"><span class="dsh-empty-icon">📈</span><div class="dsh-empty-title">Conviction data unavailable</div></div>`; }

  // Next briefing stat tile — load from /auth/me (has tier + delivery prefs)
  if (state.userId) {
    try {
      const me = await apiFetch('/auth/me');
      if (me) {
        const tier      = (me.tier || 'basic').toLowerCase();
        const delivTime = me.tip_delivery_time || me.delivery_time || '07:30';
        const tz        = me.tip_delivery_timezone || me.timezone || 'Europe/London';
        const tierCap   = tier.charAt(0).toUpperCase() + tier.slice(1);
        // Compute next Monday (or Wednesday for pro+)
        const now = new Date();
        const todayDay = now.getDay(); // 0=Sun,1=Mon…
        // Mon=1 delivery for all tiers; Wed=3 also for pro+
        const daysToMon = (1 + 7 - todayDay) % 7 || 7;
        const daysToWed = (3 + 7 - todayDay) % 7 || 7;
        const daysUntil = (tier !== 'basic' && daysToWed < daysToMon) ? daysToWed : daysToMon;
        const nextDay   = daysUntil === daysToWed ? 'Wednesday' : 'Monday';
        const nextDate  = new Date(now); nextDate.setDate(now.getDate() + daysUntil);
        const diffMs    = nextDate - now;
        const diffH     = Math.floor(diffMs / 3600000);
        const dDays     = Math.floor(diffH / 24);
        const dHours    = diffH % 24;
        const countdownStr = dDays > 0 ? `${dDays}d ${dHours}h` : `${dHours}h`;
        const tzCity    = tz.split('/')[1]?.replace(/_/g,' ') || tz;
        document.getElementById('s-next-day').textContent = nextDay;
        document.getElementById('s-next-sub').textContent = `${delivTime} ${tzCity} · ${tierCap}`;
        const cdEl = document.getElementById('dsh-countdown');
        const dtEl = document.getElementById('dsh-next-detail');
        if (cdEl) cdEl.textContent = countdownStr;
        if (dtEl) dtEl.innerHTML = `${nextDay} <span>${delivTime} ${tzCity}</span> · ${escHtml(tierCap)} tier`;
      }
    } catch { /* auth/me not available */ }

    // Checklist — onboarding status
    try {
      const os = await apiFetch(`/users/${state.userId}/onboarding-status`);
      const facts2 = parseInt(document.getElementById('s-facts').textContent.replace(/,/g,'')) || 0;
      const rows = [
        { ok: facts2 > 0,             text: facts2 > 0 ? `KB synced · ${facts2.toLocaleString()} facts` : 'KB not synced yet' },
        { ok: !!os?.portfolio_submitted, text: os?.portfolio_submitted ? 'Portfolio submitted' : 'Portfolio not submitted' },
        { ok: !!os?.telegram_connected,  text: os?.telegram_connected  ? 'Telegram linked'    : 'Telegram not linked' },
      ];
      const cl = document.getElementById('dsh-checklist');
      if (cl) cl.innerHTML = rows.map(r =>
        `<div class="dsh-check-row">
          <span class="${r.ok ? 'dsh-check-ok' : 'dsh-check-warn'}">${r.ok ? '✓' : '!'}</span>
          <span class="dsh-check-text ${r.ok ? '' : 'warn'}">${escHtml(r.text)}</span>
        </div>`).join('');
    } catch { /* silent */ }
  }

  // KB sources (adapters reframed as intelligence sources)
  try {
    const ingest   = await apiFetch('/ingest/status');
    const adapters = Object.values(ingest?.adapters || {});
    const el = document.getElementById('dsh-kb-sources');
    const countEl = document.getElementById('dsh-sources-count');
    if (countEl) countEl.textContent = `${adapters.length} active`;
    if (!adapters.length) { if (el) el.innerHTML = `<div class="dsh-empty" style="padding:16px;"><div class="dsh-empty-sub">No adapter data</div></div>`; }
    else {
      // Sort by atom count desc, show top 6
      const sorted = [...adapters].sort((a,b) => ((b.kb_atoms ?? b.total_atoms ?? 0) - (a.kb_atoms ?? a.total_atoms ?? 0)));
      const top6   = sorted.slice(0, 6);
      const extra  = adapters.length - top6.length;
      const _friendlyName = n => {
        const map = { rss_news_adapter: 'RSS News', signal_enrichment_adapter: 'Signal Enrichment',
          lse_flow_adapter: 'LSE Flow', options_flow_adapter: 'Options Flow',
          fca_short_interest_adapter: 'FCA Short Interest', llm_extraction_adapter: 'LLM Extraction',
          macro_calendar_adapter: 'Macro Calendar', earnings_adapter: 'Earnings',
          dark_pool_adapter: 'Dark Pool', reuters_adapter: 'Reuters', bloomberg_adapter: 'Bloomberg' };
        return map[n] || n.replace(/_adapter$/,'').replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase());
      };
      if (el) el.innerHTML = top6.map(a => {
        const hasError = !!a.last_error;
        const running  = a.is_running;
        const neverRun = !a.last_run_at;
        const dotClr   = running ? 'var(--accent)' : hasError ? 'var(--red)' : neverRun ? 'var(--accent)' : 'var(--green)';
        const atoms    = (a.kb_atoms ?? a.total_atoms ?? 0).toLocaleString();
        const timeStr  = a.last_run_at ? fmtTime(a.last_run_at) : '—';
        return `<div class="dsh-kbh-item">
          <div class="dsh-kbh-dot" style="background:${dotClr}"></div>
          <div class="dsh-kbh-name">${escHtml(_friendlyName(a.name))}</div>
          <div class="dsh-kbh-count">${atoms}</div>
          <div class="dsh-kbh-time">${timeStr}</div>
        </div>`;
      }).join('') + (extra > 0 ? `<div class="dsh-kbh-footer">+${extra} more sources active</div>` : '');
    }
  } catch {
    const el = document.getElementById('dsh-kb-sources');
    if (el) el.innerHTML = `<div class="dsh-empty" style="padding:16px;"><div class="dsh-empty-sub">Sources unavailable</div></div>`;
  }
}

document.getElementById('dash-refresh-btn').addEventListener('click', refreshDashboard);

// Wordmark click — sign out fully and go to landing page
document.getElementById('wordmark-home').addEventListener('click', async () => {
  await signOut();
  window.location.href = 'https://trading-galaxy.uk';
});

// ── DASHBOARD POSITIONS ───────────────────────────────────────────
async function loadDashboardPositions() {
  if (!state.userId) return;
  const el = document.getElementById('dash-positions');
  if (!el) return;
  try {
    const d = await apiFetch(`/users/${state.userId}/positions/open`);
    const positions = d?.positions || d || [];
    if (!positions.length) {
      // Try portfolio holdings as fallback
      const ph = await apiFetch(`/users/${state.userId}/portfolio`).catch(() => null);
      const holdings = ph?.holdings?.filter(h => !h.is_cash) || [];
      if (!holdings.length) {
        el.innerHTML = `<div class="dsh-empty">
          <span class="dsh-empty-icon">📊</span>
          <div class="dsh-empty-title">No positions tracked yet</div>
          <div class="dsh-empty-sub">Add your holdings in Portfolio and the AI will monitor your positions, surface matching signals, and flag when your thesis changes.</div>
          <a class="dsh-empty-cta" onclick="navigate('portfolio')">Set up portfolio →</a>
        </div>`;
        return;
      }
      // Show holdings with KB signal atoms (enriched by backend)
      el.innerHTML = holdings.slice(0, 6).map(h => {
        const sym  = { GBP:'£', USD:'$', EUR:'€' }[h.currency || 'GBP'] || '£';
        const qty  = h.quantity ? h.quantity.toLocaleString() + ' shares' : '';
        const cost = h.avg_cost != null ? `avg ${sym}${Number(h.avg_cost).toFixed(2)}` : '';
        const dir = (h.signal_direction || '').toLowerCase();
        const hasSig = !!h.signal_direction;
        const sigClr = dir === 'bullish' ? 'var(--green)' : dir === 'bearish' ? 'var(--red)' : 'var(--muted)';
        const conv = h.conviction_tier ? ` · ${h.conviction_tier}` : '';
        const sigTxt = hasSig ? (dir.charAt(0).toUpperCase() + dir.slice(1)) + conv : 'No KB signal';
        const price = h.last_price ? `${sym}${parseFloat(h.last_price).toFixed(2)}` : '';
        const upside = h.upside_pct ? ` · ${parseFloat(h.upside_pct) >= 0 ? '+' : ''}${parseFloat(h.upside_pct).toFixed(1)}% upside` : '';
        return `<div class="dsh-pos-item" onclick="navigate('portfolio')">
          <div class="dsh-pos-meta" style="width:72px;flex-shrink:0;">
            <div class="dsh-pos-ticker">${escHtml(h.ticker)}</div>
            ${price ? `<div class="dsh-pos-name" style="font-size:10px;color:var(--muted);">${price}</div>` : ''}
          </div>
          <div class="dsh-pos-meta">
            <div class="dsh-pos-qty">${[qty, cost].filter(Boolean).join(' · ')}</div>
            <div class="dsh-pos-sig"><div class="dsh-pos-sig-dot" style="background:${sigClr}"></div><span style="color:${sigClr}">${escHtml(sigTxt)}${upside}</span></div>
          </div>
          <div class="dsh-pos-ppl">
            <div class="dsh-pos-ppl-val" style="color:var(--muted);">—</div>
          </div>
        </div>`;
      }).join('');
      return;
    }
    el.innerHTML = positions.slice(0, 6).map(p => {
      const dir    = (p.signal_direction || '').toLowerCase();
      const hasSig = !!p.signal_direction;
      const sigClr = dir === 'bullish' ? 'var(--green)' : dir === 'bearish' ? 'var(--red)' : 'var(--accent)';
      const sigTxt = hasSig ? (dir.charAt(0).toUpperCase() + dir.slice(1)) + ' signal' : 'No signal';
      const pnl    = p.unrealized_pnl;
      const pnlPct = p.unrealized_pnl_pct;
      const sym    = { GBP:'£', USD:'$', EUR:'€' }[p.currency || 'GBP'] || '';
      const qty    = p.quantity ? p.quantity.toLocaleString() + ' shares' : '';
      const cost   = p.avg_cost != null ? `avg ${sym}${Number(p.avg_cost).toFixed(2)}` : '';
      return `<div class="dsh-pos-item" onclick="navigate('portfolio')">
        <div class="dsh-pos-meta" style="width:72px;flex-shrink:0;">
          <div class="dsh-pos-ticker">${escHtml(p.ticker||'')}</div>
          ${p.name ? `<div class="dsh-pos-name">${escHtml(p.name)}</div>` : ''}
        </div>
        <div class="dsh-pos-meta">
          <div class="dsh-pos-qty">${[qty, cost].filter(Boolean).join(' · ')}</div>
          ${hasSig ? `<div class="dsh-pos-sig"><div class="dsh-pos-sig-dot" style="background:${sigClr}"></div><span style="color:${sigClr}">${sigTxt}</span></div>` : ''}
        </div>
        <div class="dsh-pos-ppl">
          ${pnl != null ? `<div class="dsh-pos-ppl-val ${pnl >= 0 ? 'pos' : 'neg'}">${pnl >= 0 ? '+' : ''}${sym}${Math.abs(pnl).toLocaleString('en-GB', {maximumFractionDigits:2})}</div>` : '<div class="dsh-pos-ppl-val" style="color:var(--muted);">—</div>'}
          ${pnlPct != null ? `<div class="dsh-pos-ppl-pct">${pnlPct >= 0 ? '+' : ''}${Number(pnlPct).toFixed(2)}%</div>` : ''}
        </div>
      </div>`;
    }).join('');
  } catch {
    el.innerHTML = `<div class="dsh-empty"><span class="dsh-empty-icon">📊</span><div class="dsh-empty-title">Positions unavailable</div></div>`;
  }
}

// ── DASHBOARD BOTTOM ROW ──────────────────────────────────────────
async function loadDashboardBottomRow() {
  // Recent patterns
  try {
    const pf = await apiFetch('/patterns?limit=5&sort=detected_at&order=desc');
    const patterns = pf?.patterns || pf || [];
    const el = document.getElementById('dsh-recent-patterns');
    if (!el) return;
    if (!patterns.length) {
      el.innerHTML = `<div class="dsh-empty" style="padding:20px 16px;"><span class="dsh-empty-icon">📡</span><div class="dsh-empty-title">Market scanning in progress</div><div class="dsh-empty-sub">Pattern signals appear here as the system identifies high-conviction setups from live market data.</div></div>`;
    } else {
      el.innerHTML = patterns.slice(0, 5).map(p => {
        const dir    = (p.signal_direction || '').toLowerCase();
        const isBull = dir === 'bullish' || dir === 'long';
        const dirCls = isBull ? 'dsh-dir-bull' : 'dsh-dir-bear';
        const dirLbl = isBull ? 'BULL' : 'BEAR';
        const pat    = (p.pattern_type || '').replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase());
        const tf     = p.timeframe || '';
        const q      = p.signal_quality != null ? ' · Q ' + fmt(p.signal_quality) : '';
        const dateStr = p.detected_at ? new Date(p.detected_at).toLocaleDateString('en-GB',{day:'numeric',month:'short'}) : '—';
        return `<div class="dsh-sig-item" onclick="navigate('patterns')">
          <div class="dsh-sig-ticker">${escHtml(p.ticker||'')}</div>
          <div class="${dirCls}">${dirLbl}</div>
          <div class="dsh-sig-info">
            <div class="dsh-sig-type">${escHtml(pat)}</div>
            <div class="dsh-sig-tf">${escHtml(tf)}${q}</div>
          </div>
          <div class="dsh-sig-date">${dateStr}</div>
        </div>`;
      }).join('');
    }
  } catch {
    const el = document.getElementById('dsh-recent-patterns');
    if (el) el.innerHTML = `<div class="dsh-empty" style="padding:20px 16px;"><div class="dsh-empty-sub">Patterns unavailable</div></div>`;
  }

  // Market snapshot — fetch from /market/snapshot (KB cached facts, no live calls)
  const MKT_SYMBOLS = [
    { id: 'spx',  sym: '^GSPC',    name: 'S&P 500' },
    { id: 'ftse', sym: '^FTSE',    name: 'FTSE 100' },
    { id: 'ftmc', sym: '^FTMC',    name: 'FTSE 250' },
    { id: 'gld',  sym: 'GLD',      name: 'Gold' },
    { id: 'gbp',  sym: 'GBPUSD=X', name: 'GBP/USD' },
  ];
  try {
    const snap = await apiFetch('/market/snapshot');
    const quotes = snap?.symbols || {};
    MKT_SYMBOLS.forEach(m => {
      const data    = quotes[m.sym] || quotes[m.sym.toUpperCase()] || null;
      const priceEl = document.getElementById(`dsh-${m.id}-price`);
      const chgEl   = document.getElementById(`dsh-${m.id}-chg`);
      if (!priceEl || !chgEl) return;
      if (!data || data.price == null) {
        priceEl.textContent = '—'; chgEl.textContent = '—'; chgEl.className = 'dsh-mkt-chg';
        return;
      }
      const price  = data.price;
      const chgPct = data.return_1m;
      priceEl.textContent = Number(price).toLocaleString('en-GB', {maximumFractionDigits: price > 100 ? 0 : 2});
      if (chgPct != null) {
        const pos = chgPct >= 0;
        chgEl.textContent = (pos ? '+' : '') + Number(chgPct).toFixed(2) + '%';
        chgEl.className = 'dsh-mkt-chg ' + (pos ? 'pos' : 'neg');
      } else {
        chgEl.textContent = '—'; chgEl.className = 'dsh-mkt-chg';
      }
    });
  } catch { /* keep dashes */ }
}

