// ── Helpers ───────────────────────────────────────────────────────────────────
function tierBadge(tier) {
  const t = (tier || '').toLowerCase();
  const cls = { high: 'badge-high', medium: 'badge-medium', low: 'badge-low', avoid: 'badge-avoid' }[t] || 'badge-low';
  return `<span class="badge ${cls}">${tier || '—'}</span>`;
}
function dirBadge(dir) {
  const d = (dir || '').toLowerCase();
  const cls = d === 'bullish' ? 'badge-bullish' : d === 'bearish' ? 'badge-bearish' : 'badge-low';
  return `<span class="badge ${cls}">${dir || '—'}</span>`;
}
function dot(ok) {
  return ok ? `<span class="dot dot-green"></span>` : `<span class="dot dot-red"></span>`;
}
function fmt(v, digits = 2) {
  if (v == null) return '—';
  const n = parseFloat(v);
  return isNaN(n) ? v : n.toFixed(digits);
}
function fmtTime(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
  catch { return iso; }
}
function fmtDate(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleDateString(); }
  catch { return iso; }
}
function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderUserMsg(msg) {
  // Render [PATTERN:TICKER DIR TYPE TF QSCORE ZoneLO-HI] as a styled pill + rest as escaped text
  return escHtml(msg).replace(/\[PATTERN:([^\]]+)\]/g, (_, inner) => {
    const parts = inner.trim().split(/\s+/);
    const ticker = parts[0] || '';
    const dir    = (parts[1] || '').toLowerCase();
    const type   = parts.slice(2, parts.length - 3).join(' ');
    const tf     = parts[parts.length - 3] || '';
    const q      = parts[parts.length - 2] || '';
    const zone   = parts[parts.length - 1] || '';
    const dirClass = dir === 'bullish' ? 'pt-dir-bull' : dir === 'bearish' ? 'pt-dir-bear' : '';
    const dirSpan = `<span class="${dirClass || 'pt-dir-neutral'}">${escHtml(dir.toUpperCase())}</span>`;
    return `<span class="pattern-tag"><span class="pt-icon">⟁</span><span class="pt-ticker">${escHtml(ticker)}</span>${dirSpan}<span class="pt-type">${escHtml(type)}</span><span class="pt-meta">${escHtml(tf)} · ${escHtml(q)} · ${escHtml(zone)}</span></span>`;
  });
}

function expandPatternTag(msg) {
  // Replace [PATTERN:inner] using the same split approach as renderUserMsg (immune to regex misalignment)
  return msg.replace(/\[PATTERN:([^\]]+)\]/g, (_, inner) => {
    const parts = inner.trim().split(/\s+/);
    // parts: [ticker, direction, ...type_words..., timeframe, Qscore, ZoneLO-HI]
    const ticker = parts[0] || '';
    const dir    = parts[1] || '';
    const zone   = parts[parts.length - 1] || '';   // e.g. Zone499.25-509.93
    const q      = parts[parts.length - 2] || '';   // e.g. Q0.75
    const tf     = parts[parts.length - 3] || '';   // e.g. 1d
    const type   = parts.slice(2, parts.length - 3).join(' ').trim(); // everything in between
    const zoneClean = zone.replace(/^Zone/i, '').replace('-', '–');
    const qClean    = q.replace(/^Q/i, '');
    return `[Pattern context: ${ticker} — ${dir} ${type} detected on the ${tf} timeframe. Quality score: ${qClean}/1.0. Key zone: ${zoneClean}. Please discuss this pattern, its implications, and how it may intersect with my portfolio.]`;
  });
}

function mdToHtml(s) {
  // Escape HTML first, then selectively render markdown
  let t = escHtml(String(s || ''));
  // Headers: ### text
  t = t.replace(/^###\s+(.+)$/gm, '<h3>$1</h3>');
  // Bold: **text**
  t = t.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  // Italic: *text* (not preceded/followed by *)
  t = t.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');
  // Inline code: `code`
  t = t.replace(/`([^`]+)`/g, '<code>$1</code>');
  // Numbered list items
  t = t.replace(/^(\d+)\.\s+(.+)$/gm, '<li>$2</li>');
  // Unordered list items: lines starting with * or -
  t = t.replace(/^[\*\-]\s+(.+)$/gm, '<li>$1</li>');
  // Wrap consecutive <li> blocks in <ul>
  t = t.replace(/(<li>.*<\/li>\n?)+/gs, m => `<ul>${m}</ul>`);
  // Paragraphs: blank-line separated blocks not already in a tag
  t = t.split(/\n{2,}/).map(block => {
    block = block.trim();
    if (!block) return '';
    if (/^<[hul]/.test(block)) return block;
    return `<p>${block.replace(/\n/g, ' ')}</p>`;
  }).join('');
  return t;
}

