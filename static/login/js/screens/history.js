// ── HISTORY ───────────────────────────────────────────────────────────────────
async function loadHistory() {
  if (!state.userId) return;
  const tl = document.getElementById('hist-timeline');
  tl.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  document.getElementById('hist-detail').innerHTML =
    '<div class="text-muted text-sm" style="margin-top:40px;text-align:center;">Select a turn to read the full exchange</div>';
  const search = document.getElementById('hist-search').value.trim();
  try {
    const qs = new URLSearchParams({ limit: 80, offset: 0, user_id: state.userId });
    if (search) qs.set('search', search);
    const d = await apiFetch(`/chat/history?${qs}`);
    const entries = d?.entries || [];
    document.getElementById('hist-turn-count').textContent =
      `${d?.total ?? entries.length} turn${d?.total !== 1 ? 's' : ''}`;
    loadHistoryStats();
    if (!entries.length) {
      tl.innerHTML = '<div class="empty text-sm text-muted" style="padding:24px;">No conversation history yet.<br>Start chatting to build your history.</div>';
      return;
    }
    let lastDay = '';
    tl.innerHTML = entries.map(e => {
      let dayHeader = '';
      if (e.day_label && e.day_label !== lastDay) {
        lastDay = e.day_label;
        dayHeader = `<div style="font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);padding:10px 14px 4px;">${escHtml(e.day_label)}</div>`;
      }
      const atomBadge = e.atom_count > 0
        ? `<span title="${e.graduated_count}/${e.atom_count} atoms graduated to KB" style="font-size:10px;color:var(--muted);font-family:var(--mono);">${e.graduated_count > 0 ? `<span style="color:var(--green);">⬆${e.graduated_count}</span>/` : ''}${e.atom_count}⚛</span>`
        : '';
      const ts = e.timestamp ? new Date(e.timestamp).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}) : '';
      return `${dayHeader}<div class="hist-row" data-msg-id="${e.message_id}" style="padding:10px 14px;cursor:pointer;border-bottom:1px solid var(--border);transition:background .12s;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px;">
          <span style="font-size:12px;font-weight:600;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(e.user_preview || '—')}</span>
          <span style="font-size:10px;color:var(--muted);font-family:var(--mono);flex-shrink:0;">${ts}</span>
        </div>
        <div style="display:flex;align-items:center;justify-content:space-between;gap:6px;">
          <span style="font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;">${escHtml(e.assistant_preview || '')}</span>
          ${atomBadge}
        </div>
      </div>`;
    }).join('');

    tl.querySelectorAll('.hist-row').forEach(row => {
      row.addEventListener('mouseenter', () => row.style.background = 'var(--card)');
      row.addEventListener('mouseleave', () => {
        if (!row.classList.contains('hist-selected')) row.style.background = '';
      });
      row.addEventListener('click', () => {
        tl.querySelectorAll('.hist-row').forEach(r => {
          r.classList.remove('hist-selected');
          r.style.background = '';
        });
        row.classList.add('hist-selected');
        row.style.background = 'rgba(245,158,11,0.07)';
        loadHistoryTurn(parseInt(row.dataset.msgId));
      });
    });
  } catch(e) {
    tl.innerHTML = `<div class="empty text-sm" style="color:var(--red);padding:16px;">${escHtml(e.message)}</div>`;
  }
}

async function loadHistoryTurn(messageId) {
  const detail = document.getElementById('hist-detail');
  detail.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  try {
    const d = await apiFetch(`/chat/history/${messageId}`);
    if (!d?.user) { detail.innerHTML = '<div class="text-muted text-sm" style="padding:16px;">Turn not found</div>'; return; }
    const ts = d.user.timestamp ? new Date(d.user.timestamp).toLocaleString() : '';
    detail.innerHTML = `
      <div style="margin-bottom:20px;">
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:8px;">You · ${escHtml(ts)}</div>
        <div style="background:rgba(245,158,11,0.07);border:1px solid rgba(245,158,11,0.18);border-radius:6px;padding:12px;font-size:13px;line-height:1.6;">${mdToHtml(d.user.content || '')}</div>
      </div>
      ${d.assistant ? `<div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:8px;">Trading Galaxy</div>
        <div style="background:var(--card);border:1px solid var(--border);border-radius:6px;padding:14px;font-size:13px;line-height:1.6;">${mdToHtml(d.assistant.content || '')}</div>
        ${d.assistant.metadata?.tickers?.length ? `<div style="margin-top:8px;font-size:11px;color:var(--muted);">Tickers: <span class="mono-amber">${d.assistant.metadata.tickers.join(', ')}</span></div>` : ''}
      </div>` : '<div class="text-muted text-sm" style="margin-top:12px;">No assistant response recorded.</div>'}
    `;
  } catch(e) {
    detail.innerHTML = `<div class="text-sm" style="color:var(--red);padding:16px;">${escHtml(e.message)}</div>`;
  }
}

async function loadHistoryAtoms() {
  if (!state.userId) return;
  const overlay = document.getElementById('hist-atoms-overlay');
  const stats   = document.getElementById('hist-atoms-stats');
  const list    = document.getElementById('hist-atoms-list');
  stats.innerHTML = '<span class="spinner"></span>';
  list.innerHTML  = '';
  overlay.style.display = 'block';
  try {
    const d = await apiFetch(`/chat/atoms?limit=100&user_id=${state.userId}`);
    stats.innerHTML = `<strong>${d.total_atoms ?? 0}</strong> atoms total · <strong style="color:var(--green)">${d.graduated_to_kb ?? 0}</strong> graduated to KB · <strong>${d.pending ?? 0}</strong> pending`;
    const atoms = d.atoms || [];
    if (!atoms.length) { list.innerHTML = '<div class="text-muted text-sm">No atoms extracted yet — chat to populate.</div>'; return; }
    list.innerHTML = `<table class="tbl">
      <thead><tr><th>Subject</th><th>Predicate</th><th>Object</th><th>Type</th><th>Source</th><th>Salience</th><th>KB</th></tr></thead>
      <tbody>${atoms.map(a => `<tr>
        <td class="mono-amber">${escHtml(a.subject)}</td>
        <td class="mono text-xs">${escHtml(a.predicate)}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(a.object)}</td>
        <td><span class="badge badge-${a.atom_type === 'intent' ? 'high' : a.atom_type === 'signal' ? 'medium' : 'low'}">${escHtml(a.atom_type)}</span></td>
        <td class="mono-muted text-xs">${escHtml(a.source)}</td>
        <td class="mono text-xs">${a.effective_salience?.toFixed(3) ?? '—'}</td>
        <td style="text-align:center;">${a.graduated ? '<span style="color:var(--green)">✓</span>' : '<span style="color:var(--muted)">—</span>'}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  } catch(e) {
    stats.innerHTML = `<span style="color:var(--red)">${escHtml(e.message)}</span>`;
  }
}

async function loadHistoryStats() {
  if (!state.userId) return;
  try {
    const d = await apiFetch(`/chat/stats?user_id=${state.userId}`);
    if (!d) return;
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    set('hst-turns',   d.total_turns   ?? '—');
    set('hst-atoms',   d.total_atoms   ?? '—');
    set('hst-grad',    d.graduated     ?? '—');
    set('hst-pending', d.pending       ?? '—');
    set('hst-7d',      d.last_7d       ?? '—');
    const subjEl = document.getElementById('hst-subjects');
    if (subjEl && d.top_subjects?.length) {
      subjEl.innerHTML = 'Top: ' + d.top_subjects.slice(0, 4)
        .map(s => `<span class="mono-amber" style="background:rgba(245,158,11,0.08);padding:1px 5px;border-radius:4px;margin-left:4px;">${escHtml(s.subject)}</span>`)
        .join('');
    }
  } catch { /* stats non-critical, fail silently */ }
}

document.getElementById('hist-search-btn').addEventListener('click', loadHistory);
document.getElementById('hist-search').addEventListener('keydown', e => { if (e.key === 'Enter') loadHistory(); });
document.getElementById('hist-atoms-btn').addEventListener('click', loadHistoryAtoms);
document.getElementById('hist-atoms-close').addEventListener('click', () => {
  document.getElementById('hist-atoms-overlay').style.display = 'none';
});
document.getElementById('hist-atoms-overlay').addEventListener('click', e => {
  if (e.target === e.currentTarget) e.currentTarget.style.display = 'none';
});

