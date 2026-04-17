/* ================================================================
   DPC Advance App — Frontend JS
================================================================ */
'use strict';

/* ── CSRF: Wrap fetch() to include X-Requested-With on all requests ── */
const _origFetch = window.fetch;
window.fetch = function(url, opts = {}) {
  opts.headers = opts.headers || {};
  // Ensure headers is a plain object (not Headers instance) for easy merging
  if (opts.headers instanceof Headers) {
    const h = {};
    opts.headers.forEach((v, k) => { h[k] = v; });
    opts.headers = h;
  }
  if (!opts.headers['X-Requested-With']) {
    opts.headers['X-Requested-With'] = 'XMLHttpRequest';
  }
  return _origFetch.call(window, url, opts);
};

let SHOW_ID = null;
let activeTab = 'advance';
let saveTimer = null;
let _isDirty = false;          // true whenever there are unsaved changes
let _isUploading = false;      // true while any file upload is in progress
let _syncSince = '';           // ISO timestamp cursor for advance-field sync
let _syncInterval = null;      // advance field poll handle (3 s)
let _heartbeatInterval = null; // presence-only poll handle for other tabs (15 s)
let _focusedField = null;      // field_key the current user has focused right now

// Deterministic per-user colour palette (8 colours, cycled by name hash)
const _PRESENCE_COLORS = [
  '#f59e0b','#3b82f6','#10b981','#ef4444',
  '#8b5cf6','#ec4899','#06b6d4','#84cc16'
];
function _userColor(name) {
  let h = 0;
  for (const c of name) h = (h * 31 + c.charCodeAt(0)) & 0xffffffff;
  return _PRESENCE_COLORS[Math.abs(h) % _PRESENCE_COLORS.length];
}
function _initials(name) {
  return name.split(/\s+/).slice(0,2).map(w => w[0]).join('').toUpperCase();
}

/* ── Real-time Sync ─────────────────────────────────────────────── */

/**
 * Poll every 1 s for advance-field changes made by other users.
 * Merges incoming values into any field the current user is NOT focused on.
 * Also sends which field the current user has focused (for others' indicators).
 */
async function _pollAdvanceSync() {
  if (!SHOW_ID) return;
  try {
    const params = new URLSearchParams({
      since: _syncSince,
      tab:   activeTab,
      field: _focusedField || '',
    });
    const resp = await fetch(`/shows/${SHOW_ID}/sync/advance?${params}`);
    if (!resp.ok) return;
    const d = await resp.json();

    if (d.since) _syncSince = d.since;

    // ── Merge field values ───────────────────────────────────────────────────
    const focused = document.activeElement;
    const fields = d.fields || {};
    let mergedCount = 0;
    for (const [key, value] of Object.entries(fields)) {
      const el = document.querySelector(`#advance-form [data-key="${key}"]`);
      if (!el || el === focused) continue;   // never overwrite what you're typing
      if (el.type === 'checkbox') {
        const next = (value === 'true');
        if (el.checked !== next) { el.checked = next; mergedCount++; _flashField(el); }
      } else if (el.value !== value) {
        el.value = value;
        mergedCount++;
        _flashField(el);
        evaluateAllConditionals();
      }
    }
    if (mergedCount > 0) {
      showSaveToast(`↓ ${mergedCount} field${mergedCount > 1 ? 's' : ''} updated`);
    }

    // ── Update presence indicators ───────────────────────────────────────────
    _updatePresenceBadge(d.active_users || []);
    _renderFieldIndicators(d.active_users || []);
  } catch (_) { /* silently ignore network errors */ }
}

/**
 * Heartbeat for schedule / postnotes tabs — presence + "someone saved" notice.
 */
async function _pollHeartbeat() {
  if (!SHOW_ID) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/heartbeat`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ tab: activeTab, focused_field: _focusedField }),
    });
    if (!resp.ok) return;
    const d = await resp.json();
    _updatePresenceBadge(d.active_users || []);
    if (d.other_saved && !_isDirty) _showOtherSavedBanner(d.last_saved_at);
  } catch (_) {}
}

/** Brief amber flash on a field updated by another user. */
function _flashField(el) {
  el.classList.remove('field-synced');
  void el.offsetWidth;  // force reflow to restart animation
  el.classList.add('field-synced');
  setTimeout(() => el.classList.remove('field-synced'), 1400);
}

/**
 * Render or update per-field presence dots.
 * Shows a coloured avatar chip beside any field another user is focused on.
 */
function _renderFieldIndicators(users) {
  // Remove all existing indicators
  document.querySelectorAll('.field-presence-chip').forEach(el => el.remove());

  const focusedUsers = users.filter(u => u.focused_field);
  if (!focusedUsers.length) return;

  // Group by field_key in case multiple users are on the same field
  const byField = {};
  for (const u of focusedUsers) {
    (byField[u.focused_field] = byField[u.focused_field] || []).push(u);
  }

  for (const [key, usrs] of Object.entries(byField)) {
    // Find the field element (input/select/textarea) and its wrapping label
    const fieldEl = document.querySelector(`#advance-form [data-key="${key}"]`);
    if (!fieldEl) continue;

    // Walk up to find the .field-group wrapper, then find its label
    const wrapper = fieldEl.closest('.field-group, .notes-section, .checkbox-label, .adv-field-wrapper');
    if (!wrapper) continue;

    const chipRow = document.createElement('div');
    chipRow.className = 'field-presence-row';

    for (const u of usrs) {
      const color = _userColor(u.name);
      const chip  = document.createElement('span');
      chip.className = 'field-presence-chip';
      chip.title = `${u.name} is here`;
      chip.style.background = color;
      chip.textContent = _initials(u.name);

      // Add typing animation for notes fields
      if (u.focused_field === key) {
        const dots = document.createElement('span');
        dots.className = 'typing-dots';
        dots.innerHTML = '<span></span><span></span><span></span>';
        chip.appendChild(dots);
      }
      chipRow.appendChild(chip);
    }

    wrapper.style.position = 'relative';
    wrapper.appendChild(chipRow);
  }
}

/** Header presence badge showing who else is on this show page. */
function _updatePresenceBadge(users) {
  const el = document.getElementById('presence-indicator');
  if (!el) return;
  if (!users.length) { el.innerHTML = ''; el.hidden = true; return; }

  el.hidden = false;
  el.innerHTML = users.map(u => {
    const color = _userColor(u.name);
    return `<span class="presence-avatar" style="background:${color}" title="${_esc(u.name)} · ${_esc(u.tab)} tab">${_esc(_initials(u.name))}</span>`;
  }).join('') + `<span class="presence-label">${users.length === 1 ? _esc(users[0].name.split(' ')[0]) : users.length + ' people'} also here</span>`;
}

/** Non-blocking "someone else saved" banner for schedule/postnotes tabs. */
function _showOtherSavedBanner(savedAt) {
  if (document.getElementById('other-saved-banner')) return;
  const banner = document.createElement('div');
  banner.id = 'other-saved-banner';
  banner.className = 'other-saved-banner';
  const time = savedAt ? new Date(savedAt + 'Z').toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '';
  banner.innerHTML = `
    <span>⚠ Another user saved this form${time ? ' at ' + time : ''}. Reload to see their changes.</span>
    <div style="display:flex;gap:8px">
      <button class="btn btn-xs btn-primary" onclick="location.reload()">Reload</button>
      <button class="btn btn-xs btn-ghost" onclick="this.closest('.other-saved-banner').remove()">Dismiss</button>
    </div>`;
  (document.getElementById('advance-form') || document.querySelector('.tab-pane.active'))
    ?.insertAdjacentElement('beforebegin', banner);
}

/* ── Tab Switching ─────────────────────────────────────────────── */
function switchTab(name) {
  activeTab = name;
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById('tab-' + name);
  if (pane) pane.classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(b => {
    if (b.getAttribute('onclick') === `switchTab('${name}')`) b.classList.add('active');
  });
  // Sync left-nav sub-items
  document.querySelectorAll('.nav-item.sub').forEach(a => {
    try {
      const p = new URL(a.href).searchParams.get('tab');
      a.classList.toggle('active', p === name);
    } catch(_) {}
  });
  const url = new URL(window.location);
  url.searchParams.set('tab', name);
  history.replaceState({}, '', url);
  // Immediately update presence and load tab-specific data
  if (SHOW_ID) {
    if (name === 'advance') {
      _pollAdvanceSync();
      markAdvanceRead();
    } else {
      _pollHeartbeat();
      if (name === 'comments') loadComments();
      if (name === 'export')   { loadAttachments(); loadReadReceipts(); }
      if (name === 'assets' && typeof loadAssetsTab === 'function') loadAssetsTab();
    }
  }
}

function initShow(showId, initialTab) {
  SHOW_ID = showId;
  switchTab(initialTab || 'advance');
  bindAdvanceForm();
  bindScheduleForm();
  bindPostNotesForm();
  initComments();
  evaluateAllConditionals();
  _bindScheduleTimeParsing();
  _initRowDrag();

  // 30-second safety-net: flush any unsaved changes that the debounce
  // may have missed (e.g. browser regained focus, slow typing bursts).
  setInterval(() => {
    if (_isDirty && ['advance', 'schedule', 'postnotes'].includes(activeTab)) {
      clearTimeout(saveTimer);
      saveActive();
    }
  }, 30000);

  // Start real-time sync polling
  _startSync();

  // Warn before leaving page with unsaved changes or an active upload
  window.addEventListener('beforeunload', e => {
    if (_isDirty || _isUploading) {
      e.preventDefault();
      e.returnValue = '';
    }
  });
}

function _startSync() {
  // Advance tab: 3-second field-level sync poll
  _syncInterval = setInterval(() => {
    if (activeTab === 'advance') _pollAdvanceSync();
  }, 3000);

  // All tabs: 15-second heartbeat for presence + "someone saved" notice
  _heartbeatInterval = setInterval(() => {
    if (activeTab !== 'advance') _pollHeartbeat();
  }, 15000);

  // Immediate first poll to seed the _syncSince cursor and show presence
  _pollAdvanceSync();
}

/* ── Save Status ───────────────────────────────────────────────── */
function setSaveStatus(state, msg) {
  const el = document.getElementById('save-status');
  if (!el) return;
  el.textContent = msg;
  el.className = 'save-status ' + state;
}

function scheduleSave() {
  _isDirty = true;
  setSaveStatus('pending', 'Unsaved changes...');
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => saveActive(), 1500);
}

/* ── Toast Notification ─────────────────────────────────────────── */
function showSaveToast(msg, type) {
  const toast = document.getElementById('save-toast');
  if (!toast) return;
  toast.textContent = msg;
  toast.className = 'save-toast show' + (type === 'error' ? ' toast-error' : '');
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { toast.className = 'save-toast'; }, 2500);
}

function showMsg(el, text, type) {
  if (!el) return;
  el.textContent = text;
  el.className = 'field-msg field-msg-' + (type || 'info');
  el.style.display = text ? 'block' : 'none';
  clearTimeout(el._hideTimer);
  if (text) el._hideTimer = setTimeout(() => { el.style.display = 'none'; }, 4000);
}

function fmtDate(s) {
  if (!s) return '—';
  return String(s).substring(0, 16).replace('T', ' ');
}

function saveActive() {
  clearTimeout(saveTimer);
  if (activeTab === 'advance')   saveAdvance();
  if (activeTab === 'schedule')  saveSchedule();
  if (activeTab === 'postnotes') savePostNotes();
  if (activeTab === 'staffing' && typeof saveLaborAll === 'function') saveLaborAll();
}

/* ── Advance Form ──────────────────────────────────────────────── */
function bindAdvanceForm() {
  const form = document.getElementById('advance-form');
  if (!form) return;
  form.addEventListener('change', () => { evaluateAllConditionals(); scheduleSave(); });
  form.addEventListener('input', () => scheduleSave());

  // Normalize load-in/out time fields to 24-hour HH:MM on blur
  ['load_in_time', 'load_out_time'].forEach(key => {
    const el = form.querySelector(`[data-key="${key}"]`);
    if (el) el.addEventListener('blur', () => {
      const parsed = parseTimeToHHMM(el.value);
      if (parsed !== el.value) {
        el.value = parsed;
        scheduleSave();
      }
    });
  });

  // Yes/No slider toggles
  form.querySelectorAll('.yn-slider').forEach(wrap => {
    if (wrap.dataset.ynBound) return;
    wrap.dataset.ynBound = '1';
    const track = wrap.querySelector('.yn-track');
    const hidden = wrap.querySelector('.adv-field');
    if (!track || !hidden) return;
    track.addEventListener('click', function(e) {
      if (wrap.classList.contains('yn-disabled')) return;
      const rect = track.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const third = rect.width / 3;
      const val = x < third ? 'No' : x < third * 2 ? '-' : 'Yes';
      track.dataset.val = val;
      hidden.value = val;
      scheduleSave();
      evaluateAllConditionals();
    });
  });

  // Multi-select checkbox lists: restore saved selections from JSON
  form.querySelectorAll('.multi-check-list').forEach(list => {
    if (list.dataset.mlBound) return;
    list.dataset.mlBound = '1';
    const hidden = list.querySelector('.adv-field');
    if (!hidden) return;
    // restore
    let vals = [];
    try { vals = JSON.parse(hidden.value || '[]'); } catch(e) { if (hidden.value) vals = [hidden.value]; }
    list.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = vals.includes(cb.value); });
    // update on change
    list.addEventListener('change', () => {
      const selected = Array.from(list.querySelectorAll('input[type=checkbox]:checked')).map(cb => cb.value);
      hidden.value = JSON.stringify(selected);
      scheduleSave();
    });
  });
}

function collectAdvanceData() {
  const data = {};
  const _timeKeys = new Set(['load_in_time', 'load_out_time']);
  document.querySelectorAll('#advance-form .adv-field').forEach(el => {
    const key = el.dataset.key;
    if (!key) return;
    if (el.type === 'checkbox') {
      data[key] = el.checked ? 'true' : 'false';
    } else if (_timeKeys.has(key) && el.value) {
      data[key] = parseTimeToHHMM(el.value);
      el.value = data[key];
    } else {
      data[key] = el.value;
    }
  });
  return data;
}

async function saveAdvance() {
  if (!SHOW_ID) return;
  setSaveStatus('saving', 'Saving...');
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/save/advance`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(collectAdvanceData())
    });
    const d = await resp.json();
    if (d.success) {
      _isDirty = false;
      setSaveStatus('saved', '✓ Saved');
      showSaveToast('✓ Saved');
      setTimeout(() => setSaveStatus('', ''), 3000);
    } else {
      setSaveStatus('error', '✗ Save failed');
      showSaveToast('✗ Save failed', 'error');
    }
  } catch(e) {
    setSaveStatus('error', '✗ Network error');
    showSaveToast('✗ Network error', 'error');
  }
}

/* ── Schedule Form ──────────────────────────────────────────────── */
function bindScheduleForm() {
  const form = document.getElementById('schedule-form');
  if (!form) return;
  form.addEventListener('change', () => scheduleSave());
  form.addEventListener('input',  () => scheduleSave());
}

/* Add a row to the day identified by perfId (null = legacy/single-day) */
function addScheduleRow(perfId) {
  const id    = (perfId !== undefined && perfId !== null) ? perfId : null;
  const tbodyId = id !== null ? `schedule-rows-${id}` : 'schedule-rows-null';
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  const tr = document.createElement('tr');
  tr.className = 'schedule-row';
  tr.innerHTML = `
    <td class="drag-col"><span class="row-drag-handle" title="Drag to reorder">⠿</span></td>
    <td><input type="text" class="sched-cell" placeholder="15:00" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="16:00" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="Description" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="Notes" value=""></td>
    <td><button type="button" class="row-del-btn" onclick="removeRow(this)">×</button></td>
  `;
  tbody.appendChild(tr);
  _bindRowDrag(tr);
  tr.querySelectorAll('.sched-cell')[0].focus();
}

function removeRow(btn) {
  btn.closest('tr').remove();
  scheduleSave();
}

/* ── Sort schedule rows by start time ──────────────────────────── */
function sortSchedRowsByTime(perfId) {
  const tbodyId = (perfId !== null && perfId !== undefined) ? `schedule-rows-${perfId}` : 'schedule-rows-null';
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('.schedule-row'));
  rows.sort((a, b) => {
    const ta = a.querySelectorAll('.sched-cell')[0]?.value || '';
    const tb = b.querySelectorAll('.sched-cell')[0]?.value || '';
    const na = parseTimeToHHMM(ta);
    const nb = parseTimeToHHMM(tb);
    // Empty times sort to the end
    if (!ta && !tb) return 0;
    if (!ta) return 1;
    if (!tb) return -1;
    return na < nb ? -1 : na > nb ? 1 : 0;
  });
  rows.forEach(r => tbody.appendChild(r));
  scheduleSave();
}

/* ── Drag-to-reorder schedule rows ─────────────────────────────── */
function _bindRowDrag(tr) {
  const handle = tr.querySelector('.row-drag-handle');
  if (!handle) return;

  // Only enable dragging while the ⠿ handle is held — all other row buttons stay clickable
  handle.addEventListener('mousedown', () => { tr.draggable = true; });
  handle.addEventListener('mouseup',   () => { tr.draggable = false; });

  tr.addEventListener('dragstart', function(e) {
    _dragSrc = this;
    e.dataTransfer.effectAllowed = 'move';
    this.classList.add('row-dragging');
  });
  tr.addEventListener('dragend', function() {
    this.draggable = false;
    this.classList.remove('row-dragging');
    document.querySelectorAll('.schedule-row.row-drag-over').forEach(r => r.classList.remove('row-drag-over'));
    _dragSrc = null;
    scheduleSave();
  });
  tr.addEventListener('dragover', function(e) {
    if (!_dragSrc || _dragSrc === this) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    document.querySelectorAll('.schedule-row.row-drag-over').forEach(r => r.classList.remove('row-drag-over'));
    this.classList.add('row-drag-over');
  });
  tr.addEventListener('drop', function(e) {
    if (!_dragSrc || _dragSrc === this) return;
    e.preventDefault();
    const tbody = this.parentNode;
    const rows = Array.from(tbody.querySelectorAll('.schedule-row'));
    const srcIdx = rows.indexOf(_dragSrc);
    const tgtIdx = rows.indexOf(this);
    if (srcIdx < tgtIdx) {
      tbody.insertBefore(_dragSrc, this.nextSibling);
    } else {
      tbody.insertBefore(_dragSrc, this);
    }
  });
}

function _initRowDrag() {
  document.querySelectorAll('.schedule-row').forEach(tr => _bindRowDrag(tr));
}

function switchSchedDay(perfId, btn) {
  document.querySelectorAll('.sched-day-tab').forEach(t => {
    t.classList.remove('btn-primary');
    t.classList.add('btn-ghost');
  });
  btn.classList.add('btn-primary');
  btn.classList.remove('btn-ghost');
  document.querySelectorAll('.sched-day-pane').forEach(p => {
    const show = p.dataset.perfId == perfId;
    p.classList.toggle('hidden', !show);
    p.style.display = show ? '' : 'none';
  });
}

/* Copy all rows from sourcePerfId day into targetPerfId day */
function copySchedDay(sourcePerfId, targetPerfId) {
  if (!confirm('Replace this day\'s rows with a copy from the selected day?')) return;
  const src = document.getElementById(`schedule-rows-${sourcePerfId}`);
  const tgt = document.getElementById(`schedule-rows-${targetPerfId}`);
  if (!src || !tgt) return;
  tgt.innerHTML = '';
  src.querySelectorAll('.schedule-row').forEach(row => {
    const cells = row.querySelectorAll('.sched-cell');
    const tr = document.createElement('tr');
    tr.className = 'schedule-row';
    tr.innerHTML = `
      <td class="drag-col"><span class="row-drag-handle" title="Drag to reorder">⠿</span></td>
      <td><input type="text" class="sched-cell" placeholder="15:00" value="${_esc(cells[0]?.value || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="16:00" value="${_esc(cells[1]?.value || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="Description" value="${_esc(cells[2]?.value || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="Notes" value="${_esc(cells[3]?.value || '')}"></td>
      <td><button type="button" class="row-del-btn" onclick="removeRow(this)">×</button></td>
    `;
    tgt.appendChild(tr);
    _bindRowDrag(tr);
  });
  scheduleSave();
}

/* Insert a SHOW START row at the top of the day */
function pullAdvanceTime(perfId, perfTime) {
  const tbody = document.getElementById(`schedule-rows-${perfId}`);
  if (!tbody) return;
  const tr = document.createElement('tr');
  tr.className = 'schedule-row';
  tr.innerHTML = `
    <td class="drag-col"><span class="row-drag-handle" title="Drag to reorder">⠿</span></td>
    <td><input type="text" class="sched-cell" value="${parseTimeToHHMM(perfTime)}"></td>
    <td><input type="text" class="sched-cell" placeholder="16:00" value=""></td>
    <td><input type="text" class="sched-cell" value="SHOW START"></td>
    <td><input type="text" class="sched-cell" placeholder="Notes" value=""></td>
    <td><button type="button" class="row-del-btn" onclick="removeRow(this)">×</button></td>
  `;
  tbody.insertBefore(tr, tbody.firstChild);
  _bindRowDrag(tr);
  scheduleSave();
}

/* Apply a saved template to a day */
async function applySchedTemplate(templateId, perfId) {
  if (!templateId) return;
  const resp = await fetch(`/api/schedule-templates/${templateId}`);
  const d = await resp.json();
  if (!d.rows) return;
  const tbodyId = perfId !== null ? `schedule-rows-${perfId}` : 'schedule-rows-null';
  const tbody   = document.getElementById(tbodyId);
  if (!tbody) return;
  tbody.innerHTML = '';
  d.rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.className = 'schedule-row';
    tr.innerHTML = `
      <td class="drag-col"><span class="row-drag-handle" title="Drag to reorder">⠿</span></td>
      <td><input type="text" class="sched-cell" placeholder="15:00" value="${_esc(r.start_time || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="16:00" value="${_esc(r.end_time || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="Description" value="${_esc(r.description || '')}"></td>
      <td><input type="text" class="sched-cell" placeholder="Notes" value="${_esc(r.notes || '')}"></td>
      <td><button type="button" class="row-del-btn" onclick="removeRow(this)">×</button></td>
    `;
    tbody.appendChild(tr);
    _bindRowDrag(tr);
  });
  scheduleSave();
}

function collectScheduleData() {
  const meta = {};
  document.querySelectorAll('#schedule-form .sched-meta').forEach(el => {
    const key = el.dataset.key;
    if (key) meta[key] = el.value;
  });
  const rows = [];
  // Collect all day panes (multi-day or single legacy)
  document.querySelectorAll('.sched-day-pane').forEach(pane => {
    const rawId  = pane.dataset.perfId;
    const perfId = (rawId && rawId !== '') ? parseInt(rawId, 10) : null;
    pane.querySelectorAll('.schedule-row').forEach(tr => {
      const cells = tr.querySelectorAll('.sched-cell');
      rows.push({
        perf_id:     perfId,
        start_time:  cells[0]?.value || '',
        end_time:    cells[1]?.value || '',
        description: cells[2]?.value || '',
        notes:       cells[3]?.value || '',
      });
    });
  });
  return {meta, rows};
}

async function saveSchedule() {
  if (!SHOW_ID) return;
  setSaveStatus('saving', 'Saving...');
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/save/schedule`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(collectScheduleData())
    });
    const d = await resp.json();
    if (d.success) {
      _isDirty = false;
      setSaveStatus('saved', '✓ Saved');
      showSaveToast('✓ Saved');
      setTimeout(() => setSaveStatus('', ''), 3000);
    } else {
      setSaveStatus('error', '✗ Save failed');
      showSaveToast('✗ Save failed', 'error');
    }
  } catch(e) {
    setSaveStatus('error', '✗ Network error');
    showSaveToast('✗ Network error', 'error');
  }
}

/* ── Post-Show Notes ──────────────────────────────────────────── */
function bindPostNotesForm() {
  const form = document.getElementById('postnotes-form');
  if (!form) return;
  form.addEventListener('change', () => scheduleSave());
  form.addEventListener('input', () => scheduleSave());
}

function collectPostNotesData() {
  const data = {};
  document.querySelectorAll('#postnotes-form .notes-field').forEach(el => {
    const key = el.dataset.key;
    if (key) data[key] = el.value;
  });
  return data;
}

async function savePostNotes() {
  if (!SHOW_ID) return;
  setSaveStatus('saving', 'Saving...');
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/save/postnotes`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(collectPostNotesData())
    });
    const d = await resp.json();
    if (d.success) {
      _isDirty = false;
      setSaveStatus('saved', '✓ Saved');
      showSaveToast('✓ Saved');
      setTimeout(() => setSaveStatus('', ''), 3000);
    } else {
      setSaveStatus('error', '✗ Save failed');
      showSaveToast('✗ Save failed', 'error');
    }
  } catch(e) {
    setSaveStatus('error', '✗ Network error');
    showSaveToast('✗ Network error', 'error');
  }
}

/* ── Conditional Fields ──────────────────────────────────────────── */
function evaluateAllConditionals() {
  document.querySelectorAll('[data-show-when]').forEach(el => {
    const cond  = el.dataset.showWhen;
    const [key, val] = cond.split('=');
    const trigger = document.querySelector(`[data-key="${key}"]`);
    if (!trigger) return;
    const currentVal = trigger.type === 'checkbox' ? (trigger.checked ? 'true' : 'false') : trigger.value;
    el.style.display = (currentVal === val) ? '' : 'none';
  });
}

/* ── Section Collapse ─────────────────────────────────────────── */
function toggleSection(header) {
  const section = header.closest('.form-section');
  section.classList.toggle('collapsed');
  const key = section.dataset.key;
  if (key) {
    localStorage.setItem('adv_sec_' + key,
      section.classList.contains('collapsed') ? 'closed' : 'open');
  }
}

/* ── Notes Toggle ─────────────────────────────────────────────── */
function toggleNotes(btn) {
  const body = btn.nextElementSibling;
  body.classList.toggle('hidden');
  btn.textContent = body.classList.contains('hidden') 
    ? btn.textContent.replace('▾','▸')
    : btn.textContent.replace('▸','▾');
}

/* ── Keyboard shortcut: Ctrl+S / Cmd+S ──────────────────────── */
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 's') {
    e.preventDefault();
    saveActive();
  }
});

/* ── Warn on unload if unsaved ───────────────────────────────── */
window.addEventListener('beforeunload', e => {
  const status = document.getElementById('save-status');
  if (status && status.classList.contains('saving')) {
    e.preventDefault();
    e.returnValue = '';
  }
});

/* ═══════════════════════════════════════════════════════════════
   SETTINGS PAGE — Form Field Editor
═══════════════════════════════════════════════════════════════ */

/* ── Drag-to-reorder for form fields ────────────────────────── */
let _dragSrc = null;

function initFieldDrag() {
  document.querySelectorAll('.field-row[draggable]').forEach(row => {
    row.addEventListener('dragstart', e => {
      _dragSrc = row;
      row.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.stopPropagation();
    });
    row.addEventListener('dragover', e => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (!_dragSrc || _dragSrc === row) return;
      const rect = row.getBoundingClientRect();
      const mid  = rect.top + rect.height / 2;
      row.parentNode.insertBefore(_dragSrc, e.clientY < mid ? row : row.nextSibling);
    });
    row.addEventListener('dragend', () => {
      if (_dragSrc) _dragSrc.classList.remove('dragging');
      _dragSrc = null;
      _saveFieldOrder();
    });
  });
}

async function _saveFieldOrder() {
  const ids = [...document.querySelectorAll('.field-row[draggable]')].map(r => Number(r.dataset.id));
  const sectionId = document.querySelector('.field-row[draggable]')?.closest('[data-section-id]')?.dataset.sectionId;
  const url = sectionId ? `/settings/form-fields/reorder` : `/settings/form-sections/reorder`;
  const body = sectionId ? {field_ids: ids} : {section_ids: ids};
  const resp = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  if ((await resp.json()).success) showSaveToast('✓ Order saved');
}

/* ── Section Drag-to-Reorder ─────────────────────────────────── */
let _dragSectionSrc = null;

function initSectionDrag() {
  document.querySelectorAll('[data-section-id]').forEach(section => {
    const handle = section.querySelector('.section-drag-handle');
    if (!handle) return;

    handle.addEventListener('mousedown', () => { section.draggable = true; });
    handle.addEventListener('mouseup',   () => { section.draggable = false; });

    section.addEventListener('dragstart', e => {
      _dragSectionSrc = section;
      section.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
    });
    section.addEventListener('dragover', e => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (!_dragSectionSrc || _dragSectionSrc === section) return;
      const rect = section.getBoundingClientRect();
      const mid  = rect.top + rect.height / 2;
      section.parentNode.insertBefore(_dragSectionSrc, e.clientY < mid ? section : section.nextSibling);
    });
    section.addEventListener('dragend', () => {
      section.draggable = false;
      if (_dragSectionSrc) _dragSectionSrc.classList.remove('dragging');
      _dragSectionSrc = null;
      _saveSectionOrder();
    });
  });
}

async function _saveSectionOrder() {
  const ids = [...document.querySelectorAll('[data-section-id]')].map(el => Number(el.dataset.sectionId));
  const resp = await fetch('/settings/form-sections/reorder', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({section_ids: ids})
  });
  if ((await resp.json()).success) showSaveToast('✓ Order saved');
}

/* ── Schedule Meta Field Drag-to-Reorder ─────────────────────── */
let _dragSchedSrc = null;

function initSchedMetaDrag() {
  document.querySelectorAll('.sched-meta-row[draggable]').forEach(row => {
    row.addEventListener('dragstart', e => {
      _dragSchedSrc = row;
      row.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.stopPropagation();
    });
    row.addEventListener('dragover', e => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (!_dragSchedSrc || _dragSchedSrc === row) return;
      const rect = row.getBoundingClientRect();
      const mid  = rect.top + rect.height / 2;
      row.parentNode.insertBefore(_dragSchedSrc, e.clientY < mid ? row : row.nextSibling);
    });
    row.addEventListener('dragend', () => {
      if (_dragSchedSrc) _dragSchedSrc.classList.remove('dragging');
      _dragSchedSrc = null;
      _saveSchedMetaOrder();
    });
  });
}

async function _saveSchedMetaOrder() {
  const ids = [...document.querySelectorAll('.sched-meta-row[draggable]')].map(r => Number(r.dataset.id));
  const resp = await fetch('/settings/schedule-meta-fields/reorder', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({field_ids: ids})
  });
  if ((await resp.json()).success) showSaveToast('✓ Order saved');
}

/* ── Schedule Meta Field Add/Edit Modal ──────────────────────── */
let _editSchedFieldId = null;
let _schedAdvFieldsLoaded = false;

async function openSchedFieldModal(id) {
  _editSchedFieldId = id || null;
  const modal = document.getElementById('sched-field-modal');
  if (!modal) return;

  // Reset form
  const form = modal.querySelector('#sched-field-modal-form');
  if (form) {
    form.querySelectorAll('input,select,textarea').forEach(el => {
      if (el.type === 'checkbox') el.checked = el.defaultChecked;
      else el.value = el.defaultValue;
    });
  }

  // Populate advance field dropdown once
  const advSel = document.getElementById('sched-field-advance-ref');
  if (advSel && !_schedAdvFieldsLoaded) {
    try {
      const sections = await fetch('/api/form-fields').then(r => r.json());
      for (const sec of sections) {
        const grp = document.createElement('optgroup');
        grp.label = sec.label;
        for (const f of sec.fields) {
          const opt = document.createElement('option');
          opt.value = f.field_key;
          opt.textContent = `${f.label} (${f.field_key})`;
          grp.appendChild(opt);
        }
        advSel.appendChild(grp);
      }
      _schedAdvFieldsLoaded = true;
    } catch(_) {}
  }

  // Lock field_key input when editing
  const fkEl = modal.querySelector('[name="field_key"]');
  if (fkEl) {
    fkEl.readOnly = !!id;
    fkEl.style.opacity = id ? '0.5' : '';
  }

  if (id) {
    try {
      const fields = await fetch('/api/schedule-meta-fields').then(r => r.json());
      const field = fields.find(f => f.id === id);
      if (field) {
        modal.querySelector('[name="label"]').value           = field.label || '';
        modal.querySelector('[name="field_key"]').value       = field.field_key || '';
        modal.querySelector('[name="field_type"]').value      = field.field_type || 'text';
        modal.querySelector('[name="width_hint"]').value      = field.width_hint || 'half';
        if (advSel) advSel.value = field.advance_field_key || '';
      }
    } catch(_) {}
  }

  modal.style.display = '';
}

function closeSchedFieldModal() {
  const modal = document.getElementById('sched-field-modal');
  if (modal) modal.style.display = 'none';
  _editSchedFieldId = null;
}

async function saveSchedField() {
  const modal = document.getElementById('sched-field-modal');
  if (!modal) return;
  const data = {
    label:             modal.querySelector('[name="label"]').value.trim(),
    field_type:        modal.querySelector('[name="field_type"]').value,
    width_hint:        modal.querySelector('[name="width_hint"]').value,
    advance_field_key: modal.querySelector('[name="advance_field_key"]').value.trim(),
  };
  if (!data.label) { alert('Label is required.'); return; }
  if (!_editSchedFieldId) {
    const fk = modal.querySelector('[name="field_key"]').value.trim();
    if (!fk) { alert('Field key is required for new fields.'); return; }
    data.field_key = fk;
  }
  const url = _editSchedFieldId
    ? `/settings/schedule-meta-fields/${_editSchedFieldId}/edit`
    : `/settings/schedule-meta-fields/add`;
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const d = await resp.json();
    if (d.success) { closeSchedFieldModal(); location.reload(); }
    else alert(d.error || 'Save failed.');
  } catch(_) { alert('Network error.'); }
}

async function deleteSchedField(fid) {
  if (!confirm('Delete this schedule field? Saved values for this field will remain in existing shows but the field will no longer appear on the schedule.')) return;
  const resp = await fetch(`/settings/schedule-meta-fields/${fid}/delete`, {method: 'POST'});
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Delete failed.');
}

/* ── Field Add/Edit Modal ─────────────────────────────────────── */
let _editFieldId = null;

function openFieldModal(fid, sectionId) {
  _editFieldId = fid || null;
  const modal = document.getElementById('field-modal');
  if (!modal) return;
  const form = modal.querySelector('#field-modal-form');
  if (form) {
    if (typeof form.reset === 'function') form.reset();
    else form.querySelectorAll('input,select,textarea').forEach(el => { el.value = el.defaultValue || ''; });
  }

  if (fid) {
    // Load existing field data
    fetch(`/api/form-fields`).then(r => r.json()).then(sections => {
      for (const sec of sections) {
        const field = sec.fields.find(f => f.id === fid);
        if (field) { _populateFieldModal(field); break; }
      }
    });
  } else {
    if (sectionId) {
      const sel = modal.querySelector('[name="section_id"]');
      if (sel) sel.value = sectionId;
    }
  }
  modal.style.display = '';
  _toggleFieldTypeOptions(modal.querySelector('[name="field_type"]')?.value);
}

function _populateFieldModal(field) {
  const modal = document.getElementById('field-modal');
  if (!modal) return;
  for (const [k, v] of Object.entries(field)) {
    const el = modal.querySelector(`[name="${k}"]`);
    if (!el) continue;
    if (el.type === 'checkbox') el.checked = !!v;
    else el.value = v ?? '';
  }
  if (field.options && field.options.length) {
    const optEl = modal.querySelector('[name="options_text"]');
    if (optEl) optEl.value = field.options.join('\n');
  }
  _toggleFieldTypeOptions(field.field_type);
}

function _toggleFieldTypeOptions(type) {
  const modal = document.getElementById('field-modal');
  if (!modal) return;
  const optGroup = modal.querySelector('.options-group');
  const deptGroup = modal.querySelector('.contact-dept-group');
  const ynGroup = modal.querySelector('.yes-no-display-group');
  if (optGroup) optGroup.style.display = (type === 'select') ? '' : 'none';
  if (deptGroup) deptGroup.style.display = (type === 'contact_dropdown') ? '' : 'none';
  if (ynGroup) ynGroup.style.display = (type === 'yes_no') ? '' : 'none';
}

function closeFieldModal() {
  const modal = document.getElementById('field-modal');
  if (modal) modal.style.display = 'none';
  _editFieldId = null;
}

async function saveField() {
  const modal = document.getElementById('field-modal');
  if (!modal) return;
  const data = {};
  modal.querySelectorAll('[name]').forEach(el => {
    if (el.name === 'options_text') return;
    if (el.type === 'checkbox') data[el.name] = el.checked;
    else data[el.name] = el.value;
  });
  // Parse options
  const optText = modal.querySelector('[name="options_text"]')?.value || '';
  data.options = optText.split('\n').map(s => s.trim()).filter(Boolean);
  if (data.section_id) data.section_id = Number(data.section_id);

  const url = _editFieldId
    ? `/settings/form-fields/${_editFieldId}/edit`
    : `/settings/form-fields/add`;
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const d = await resp.json();
    if (d.success) { closeFieldModal(); location.reload(); }
    else { alert(d.error || 'Save failed.'); }
  } catch(e) { alert('Network error.'); }
}

async function deleteField(fid) {
  if (!confirm('Delete this field? Saved values for this field will remain in existing shows but the field will no longer appear in the form.')) return;
  const resp = await fetch(`/settings/form-fields/${fid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Delete failed.');
}

/* ── Section Add/Edit Modal ──────────────────────────────────── */
let _editSectionId = null;

function openSectionModal(sid) {
  _editSectionId = sid || null;
  const modal = document.getElementById('section-modal');
  if (!modal) return;
  if (sid) {
    const row = document.querySelector(`.section-row[data-id="${sid}"]`);
    if (row) {
      modal.querySelector('[name="label"]').value = row.dataset.label || '';
      modal.querySelector('[name="icon"]').value  = row.dataset.icon  || '◈';
      const collEl = modal.querySelector('[name="collapsible"]');
      if (collEl) collEl.checked = (row.dataset.collapsible === '1');
      const defOpen = row.dataset.defaultOpen !== '0';
      modal.querySelectorAll('[name="default_open"]').forEach(r => {
        r.checked = (r.value === (defOpen ? '1' : '0'));
      });
    }
  } else {
    const secForm = modal.querySelector('#section-modal-form');
    if (secForm) {
      if (typeof secForm.reset === 'function') secForm.reset();
      else secForm.querySelectorAll('input,select,textarea').forEach(el => {
        if (el.type === 'checkbox') el.checked = el.defaultChecked;
        else el.value = el.defaultValue;
      });
    }
  }
  modal.style.display = '';
}

function closeSectionModal() {
  const modal = document.getElementById('section-modal');
  if (modal) modal.style.display = 'none';
  _editSectionId = null;
}

async function saveSection() {
  const modal = document.getElementById('section-modal');
  if (!modal) return;
  const data = {};
  modal.querySelectorAll('[name]').forEach(el => {
    if (el.type === 'checkbox') data[el.name] = el.checked;
    else if (el.type === 'radio') { if (el.checked) data[el.name] = el.value; }
    else data[el.name] = el.value;
  });
  const url = _editSectionId
    ? `/settings/form-sections/${_editSectionId}/edit`
    : `/settings/form-sections/add`;
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const d = await resp.json();
    if (d.success) { closeSectionModal(); location.reload(); }
    else alert(d.error || 'Save failed.');
  } catch(e) { alert('Network error.'); }
}

async function deleteSection(sid) {
  if (!confirm('Delete this section and ALL its fields? This cannot be undone.')) return;
  const resp = await fetch(`/settings/form-sections/${sid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Delete failed.');
}

/* ── Schedule Template Editor (Settings) ─────────────────────── */
let _editSchedTmplId = null;

function openSchedTemplateModal(tid) {
  _editSchedTmplId = tid;
  const modal = document.getElementById('sched-template-modal');
  if (!modal) return;
  document.getElementById('sched-tmpl-name').value = '';
  const tbody = document.getElementById('sched-tmpl-rows');
  tbody.innerHTML = `<tr class="schedule-row">
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="8:00am"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="10:00am"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="Load In"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="Notes"></td>
    <td><button type="button" class="row-del-btn" onclick="this.closest('tr').remove()">×</button></td>
  </tr>`;
  if (tid) {
    fetch(`/api/schedule-templates/${tid}`).then(r => r.json()).then(d => {
      document.getElementById('sched-tmpl-name').value = d.name || '';
      tbody.innerHTML = '';
      (d.rows || []).forEach(r => _appendTmplRow(tbody, r));
      if (!d.rows || d.rows.length === 0) _appendTmplRow(tbody, {});
    });
  }
  modal.style.display = '';
}

function _appendTmplRow(tbody, r) {
  const tr = document.createElement('tr');
  tr.className = 'schedule-row';
  tr.innerHTML = `
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="8:00am" value="${_esc(r.start_time || '')}"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="10:00am" value="${_esc(r.end_time || '')}"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="Description" value="${_esc(r.description || '')}"></td>
    <td><input type="text" class="sched-cell tmpl-cell" placeholder="Notes" value="${_esc(r.notes || '')}"></td>
    <td><button type="button" class="row-del-btn" onclick="this.closest('tr').remove()">×</button></td>
  `;
  tbody.appendChild(tr);
}

function addSchedTmplRow() {
  _appendTmplRow(document.getElementById('sched-tmpl-rows'), {});
}

function closeSchedTemplateModal() {
  const modal = document.getElementById('sched-template-modal');
  if (modal) modal.style.display = 'none';
  _editSchedTmplId = null;
}

async function saveSchedTemplate() {
  const name = document.getElementById('sched-tmpl-name').value.trim();
  if (!name) { alert('Template name is required.'); return; }
  const rows = [];
  document.querySelectorAll('#sched-tmpl-rows .schedule-row').forEach(tr => {
    const cells = tr.querySelectorAll('.tmpl-cell');
    rows.push({
      start_time:  cells[0]?.value || '',
      end_time:    cells[1]?.value || '',
      description: cells[2]?.value || '',
      notes:       cells[3]?.value || '',
    });
  });
  const url = _editSchedTmplId
    ? `/settings/schedule-templates/${_editSchedTmplId}/edit`
    : '/settings/schedule-templates/add';
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name, rows})
    });
    const d = await resp.json();
    if (d.success) { closeSchedTemplateModal(); location.reload(); }
    else alert(d.error || 'Save failed.');
  } catch(e) { alert('Network error.'); }
}

async function deleteSchedTemplate(tid) {
  if (!confirm('Delete this template?')) return;
  const resp = await fetch(`/settings/schedule-templates/${tid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Delete failed.');
}

/* ── Group Management ────────────────────────────────────────── */
async function addGroupMember(gid, userId) {
  if (!userId) return;
  const resp = await fetch(`/settings/groups/${gid}/members/add`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user_id: Number(userId)})
  });
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Failed to add member.');
}

async function removeGroupMember(gid, userId) {
  const resp = await fetch(`/settings/groups/${gid}/members/remove`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user_id: userId})
  });
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Failed to remove member.');
}

async function addGroupShowAccess(showId, groupId) {
  const resp = await fetch(`/shows/${showId}/access/add`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({group_id: Number(groupId)})
  });
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Failed.');
}

async function removeGroupShowAccess(showId, groupId) {
  const resp = await fetch(`/shows/${showId}/access/remove`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({group_id: groupId})
  });
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Failed.');
}

async function deleteGroup(gid) {
  if (!confirm('Delete this group? Members will lose their restricted access assignments.')) return;
  const resp = await fetch(`/settings/groups/${gid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) location.reload();
  else alert(d.error || 'Delete failed.');
}

/* ── Server Settings (port change + live restart) ────────────── */
async function saveServerSettings(form) {
  const newPort = parseInt(form.querySelector('[name="app_port"]').value, 10);
  const msg = form.querySelector('.server-save-msg');

  const resp = await fetch('/settings/server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ app_port: newPort })
  });
  const d = await resp.json();

  if (!d.success) {
    if (msg) {
      msg.textContent = 'Error: ' + (d.error || 'Unknown');
      msg.className = 'field-msg field-msg-error';
    }
    return;
  }

  if (d.restarting) {
    // Service is restarting on new port — poll until it responds, then redirect
    if (msg) {
      msg.textContent = '⟳ Restarting on port ' + d.new_port + '...';
      msg.className = 'field-msg field-msg-warning';
    }
    const newOrigin = location.protocol + '//' + location.hostname + ':' + d.new_port;
    let attempts = 0;
    const poll = setInterval(async () => {
      attempts++;
      if (attempts > 40) {  // 40 s timeout
        clearInterval(poll);
        if (msg) {
          msg.textContent = '⚠ Timeout. Navigate manually to port ' + d.new_port;
          msg.className = 'field-msg field-msg-error';
        }
        return;
      }
      try {
        // no-cors fetch just to see if the server is up
        await fetch(newOrigin + '/login', {
          mode: 'no-cors',
          signal: AbortSignal.timeout(2000)
        });
        clearInterval(poll);
        if (msg) msg.textContent = '✓ Restarted! Redirecting...';
        setTimeout(() => { location.href = newOrigin + location.pathname; }, 800);
      } catch (_) { /* still restarting */ }
    }, 1000);
  } else {
    // Not a systemd service — just show confirmation
    if (msg) {
      msg.textContent = '✓ ' + (d.message || 'Saved. Restart the service to apply.');
      msg.className = 'field-msg field-msg-success';
      setTimeout(() => { msg.textContent = ''; msg.className = 'field-msg'; }, 6000);
    }
    showSaveToast('✓ Server settings saved');
  }
}

/* ── Syslog Settings ─────────────────────────────────────────── */
async function saveSyslogSettings(form) {
  const fd = new FormData(form);
  const data = Object.fromEntries(fd.entries());
  data.syslog_enabled = form.querySelector('[name="syslog_enabled"]')?.checked ? '1' : '0';
  const resp = await fetch('/settings/syslog', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data)
  });
  const d = await resp.json();
  const msg = form.querySelector('.syslog-save-msg');
  if (msg) {
    msg.textContent = d.success ? '✓ Saved' : ('Error: ' + (d.error||'Unknown'));
    msg.className = 'field-msg ' + (d.success ? 'field-msg-success' : 'field-msg-error');
    setTimeout(() => { msg.textContent=''; msg.className='field-msg'; }, 3000);
  }
  if (d.success) showSaveToast('✓ Syslog settings saved');
}

/* ── Backup Controls ─────────────────────────────────────────── */
async function runManualBackup(btn) {
  btn.disabled = true;
  btn.textContent = 'Running...';
  const resp = await fetch('/settings/backups/run', {method:'POST'});
  const d = await resp.json();
  btn.disabled = false;
  btn.textContent = 'Run Backup Now';
  if (d.success) {
    loadBackupStatus();
    showSaveToast('✓ Backup created');
  } else {
    showSaveToast('✗ Backup failed: ' + (d.error||'Unknown error'), 'error');
  }
}

async function loadBackupStatus() {
  const resp = await fetch('/settings/backups');
  const data = await resp.json();
  for (const kind of ['hourly', 'daily']) {
    const container = document.getElementById(`backup-list-${kind}`);
    if (!container) continue;
    if (!data[kind] || !data[kind].length) {
      container.innerHTML = '<p class="text-dim" style="padding:10px">No backups yet.</p>';
      continue;
    }
    container.innerHTML = data[kind].map(f => `
      <div class="settings-info-row">
        <span class="backup-filename">${_esc(f.filename)}</span>
        <span class="backup-size">${_esc(String(f.size_kb))} KB</span>
        <span class="backup-mtime">${_esc(f.mtime)}</span>
        <a href="/settings/backups/download/${_esc(kind)}/${_esc(f.filename)}"
           class="btn btn-xs btn-ghost" download style="margin-left:auto">↓ Download</a>
      </div>
    `).join('');
  }
}

/* ═══════════════════════════════════════════════════════════════
   DARK / LIGHT MODE TOGGLE
═══════════════════════════════════════════════════════════════ */
const _SUN_SVG  = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
const _MOON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 12.79A9 9 0 1111.21 3a7 7 0 009.79 9.79z"/></svg>`;

function toggleTheme() {
  const html    = document.documentElement;
  const current = html.getAttribute('data-theme') || 'dark';
  const next    = current === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  const btn = document.getElementById('theme-toggle-btn');
  if (btn) btn.innerHTML = next === 'dark' ? _SUN_SVG : _MOON_SVG;
  fetch('/account/theme', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({theme: next}),
  }).catch(() => {});
}

/* ═══════════════════════════════════════════════════════════════
   COMMENTS
═══════════════════════════════════════════════════════════════ */

function initComments() {
  const input = document.getElementById('comment-input');
  if (!input) return;
  input.addEventListener('input',   handleCommentInput);
  input.addEventListener('keydown', handleCommentKeydown);
  // Close mention dropdown when clicking outside
  document.addEventListener('click', e => {
    if (!e.target.closest('#mention-dropdown') && !e.target.closest('#comment-input')) {
      hideMentionDropdown();
    }
  });
}

async function loadComments() {
  if (!SHOW_ID) return;
  const list = document.getElementById('comments-list');
  if (!list) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/comments`);
    const comments = await resp.json();
    renderComments(comments);
    const badge = document.getElementById('comments-count-badge');
    if (badge) {
      badge.textContent = comments.length;
      badge.hidden = comments.length === 0;
    }
  } catch(_) {}
}

function renderComments(comments) {
  const list = document.getElementById('comments-list');
  if (!list) return;
  const visible = comments.filter(c => !c.deleted_at);
  const deleted = comments.filter(c => c.deleted_at);
  if (!visible.length && !deleted.length) {
    list.innerHTML = '<p class="text-dim" style="padding:28px;text-align:center;margin:0">No comments yet. Start the conversation!</p>';
    return;
  }
  const isAdmin = typeof IS_ADMIN !== 'undefined' && IS_ADMIN;
  const items = [...visible, ...deleted].sort((a, b) => (a.created_at || '').localeCompare(b.created_at || ''));
  list.innerHTML = items.map(c => {
    const color = _userColor(c.author);
    const dt = c.created_at
      ? new Date((c.created_at.includes('T') ? c.created_at : c.created_at + 'Z'))
          .toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})
      : '';
    const isDeleted = !!c.deleted_at;
    const bodyHtml = isDeleted
      ? `<span style="text-decoration:line-through;opacity:.5">${_renderCommentBody(c.body)}</span> <em class="text-dim" style="font-size:.8em">deleted by ${_esc(c.deleted_by || 'user')}</em>`
      : _renderCommentBody(c.body);
    const editedIndicator = c.edited_at && !isDeleted ? ` <span class="text-dim" style="font-size:.75em">(edited)</span>` : '';
    const actions = [];
    if (!isDeleted && (c.is_own || isAdmin)) {
      actions.push(`<button class="comment-delete-btn" onclick="deleteComment(${c.id})" title="Delete">×</button>`);
    }
    if (!isDeleted && (c.is_own || isAdmin)) {
      actions.push(`<button class="comment-edit-btn" onclick="startEditComment(${c.id})" title="Edit" style="background:none;border:none;cursor:pointer;color:var(--text-dim);font-size:.85em;padding:0 4px">✏</button>`);
    }
    if (isDeleted && isAdmin) {
      actions.push(`<button class="btn btn-ghost" style="font-size:.75em;padding:2px 6px" onclick="restoreComment(${c.id})">Restore</button>`);
    }
    if (isAdmin) {
      actions.push(`<button class="btn btn-ghost" style="font-size:.75em;padding:2px 6px" onclick="showCommentVersions(${c.id})" title="Version history">History</button>`);
    }
    return `
      <div class="comment-item ${isDeleted ? 'comment-deleted' : ''}" data-id="${c.id}">
        <div class="comment-avatar" style="background:${color};${isDeleted ? 'opacity:.4' : ''}">${c.initials}</div>
        <div class="comment-bubble" style="${isDeleted ? 'opacity:.6' : ''}">
          <div class="comment-header">
            <strong>${_esc(c.author)}</strong>
            <span class="comment-time">${dt}${editedIndicator}</span>
            <span style="margin-left:auto;display:flex;gap:4px;align-items:center">${actions.join('')}</span>
          </div>
          <div class="comment-body" id="comment-body-${c.id}">${bodyHtml}</div>
          <div class="comment-edit-form" id="comment-edit-${c.id}" style="display:none;margin-top:.5rem">
            <textarea class="field-input field-textarea" rows="2" id="comment-edit-input-${c.id}" style="font-size:.875rem">${_esc(c.body)}</textarea>
            <div style="display:flex;gap:.5rem;margin-top:.4rem">
              <button class="btn btn-primary btn-sm" onclick="saveEditComment(${c.id})">Save</button>
              <button class="btn btn-ghost btn-sm" onclick="cancelEditComment(${c.id})">Cancel</button>
            </div>
          </div>
        </div>
      </div>`;
  }).join('');
  list.scrollTop = list.scrollHeight;
}

function startEditComment(cid) {
  document.getElementById('comment-body-' + cid).style.display = 'none';
  const editForm = document.getElementById('comment-edit-' + cid);
  editForm.style.display = '';
  editForm.querySelector('textarea').focus();
}

function cancelEditComment(cid) {
  document.getElementById('comment-body-' + cid).style.display = '';
  document.getElementById('comment-edit-' + cid).style.display = 'none';
}

async function saveEditComment(cid) {
  const input = document.getElementById('comment-edit-input-' + cid);
  const body = input.value.trim();
  if (!body) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/comments/${cid}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({body}),
    });
    const d = await resp.json();
    if (d.success) {
      await loadComments();
    } else {
      alert(d.error || 'Edit failed.');
    }
  } catch(_) {
    alert('Network error.');
  }
}

async function restoreComment(cid) {
  if (!confirm('Restore this deleted comment?')) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/comments/${cid}/restore`, {method: 'POST'});
    const d = await resp.json();
    if (d.success) await loadComments();
    else alert(d.error || 'Restore failed.');
  } catch(_) {
    alert('Network error.');
  }
}

async function showCommentVersions(cid) {
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/comments/${cid}/versions`);
    const versions = await resp.json();
    if (!versions.length) { alert('No edit history for this comment.'); return; }
    const lines = versions.map((v, i) =>
      `[${i+1}] ${fmtDate(v.edited_at)} by ${v.edited_by}:\n${v.body}`
    ).join('\n\n---\n\n');
    const choice = prompt(`Comment edit history (${versions.length} versions):\n\n${lines}\n\nEnter version number to restore (or cancel):`);
    if (!choice) return;
    const idx = parseInt(choice) - 1;
    if (isNaN(idx) || idx < 0 || idx >= versions.length) { alert('Invalid version number.'); return; }
    if (!confirm(`Restore to version ${idx+1}?`)) return;
    const resp2 = await fetch(`/shows/${SHOW_ID}/comments/${cid}/versions/${versions[idx].id}/restore`, {method:'POST'});
    const d = await resp2.json();
    if (d.success) await loadComments();
    else alert(d.error || 'Version restore failed.');
  } catch(_) {
    alert('Network error.');
  }
}

function _esc(str) {
  return String(str)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;');
}

function _renderCommentBody(text) {
  const escaped = _esc(text).replace(/\n/g,'<br>');
  // Highlight @mentions
  return escaped.replace(/@([\w][\w .'-]*?)(?=\s|$|<br>)/g,
    '<span class="comment-mention">@$1</span>');
}

async function submitComment() {
  const input = document.getElementById('comment-input');
  if (!input) return;
  const body = input.value.trim();
  if (!body) return;
  const btn = document.querySelector('.comment-input-area .btn-primary');
  if (btn) { btn.disabled = true; btn.textContent = 'Posting…'; }
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/comments`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({body}),
    });
    const d = await resp.json();
    if (d.success) {
      input.value = '';
      hideMentionDropdown();
      await loadComments();
    } else {
      alert(d.error || 'Failed to post comment.');
    }
  } catch(_) {
    alert('Network error.');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Post'; }
  }
}

async function deleteComment(cid) {
  if (!confirm('Delete this comment?')) return;
  const resp = await fetch(`/shows/${SHOW_ID}/comments/${cid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) loadComments();
  else alert(d.error || 'Delete failed.');
}

/* @mention autocomplete ───────────────────────────────────────── */
function handleCommentInput(e) {
  const input = e.target;
  const text  = input.value.substring(0, input.selectionStart);
  const match = text.match(/@(\w*)$/);
  if (match) {
    showMentionDropdown(match[1].toLowerCase());
  } else {
    hideMentionDropdown();
  }
}

function handleCommentKeydown(e) {
  const dropdown = document.getElementById('mention-dropdown');
  if (dropdown && !dropdown.hidden) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const items = dropdown.querySelectorAll('.mention-item');
      const focused = dropdown.querySelector('.mention-item.focused');
      const next = focused ? focused.nextElementSibling : items[0];
      focused?.classList.remove('focused');
      if (next) next.classList.add('focused');
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      const items = dropdown.querySelectorAll('.mention-item');
      const focused = dropdown.querySelector('.mention-item.focused');
      const prev = focused ? focused.previousElementSibling : items[items.length - 1];
      focused?.classList.remove('focused');
      if (prev) prev.classList.add('focused');
      return;
    }
    if (e.key === 'Enter' || e.key === 'Tab') {
      const focused = dropdown.querySelector('.mention-item.focused') || dropdown.querySelector('.mention-item');
      if (focused) { e.preventDefault(); focused.click(); return; }
    }
    if (e.key === 'Escape') { hideMentionDropdown(); return; }
  }
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    submitComment();
  }
}

function showMentionDropdown(query) {
  const dropdown = document.getElementById('mention-dropdown');
  if (!dropdown) return;
  const all = typeof ALL_USERS !== 'undefined' ? ALL_USERS : [];
  const matches = all.filter(u => {
    const name  = (u.display_name || '').toLowerCase();
    const uname = (u.username || '').toLowerCase();
    return name.startsWith(query) || uname.startsWith(query) ||
           name.includes(query)   || uname.includes(query);
  }).slice(0, 6);
  if (!matches.length) { hideMentionDropdown(); return; }
  dropdown.innerHTML = matches.map(u => {
    const color = _userColor(u.display_name || u.username);
    return `<div class="mention-item" onclick="insertMention('${_esc(u.display_name || u.username)}')">
      <span class="mention-avatar" style="background:${color}">${_initials(u.display_name || u.username)}</span>
      <span>${_esc(u.display_name || u.username)}</span>
    </div>`;
  }).join('');
  dropdown.hidden = false;
}

function hideMentionDropdown() {
  const d = document.getElementById('mention-dropdown');
  if (d) d.hidden = true;
}

function insertMention(name) {
  const input = document.getElementById('comment-input');
  if (!input) return;
  const pos    = input.selectionStart;
  const before = input.value.substring(0, pos).replace(/@\w*$/, '@' + name + ' ');
  const after  = input.value.substring(pos);
  input.value  = before + after;
  input.setSelectionRange(before.length, before.length);
  input.focus();
  hideMentionDropdown();
}

/* ═══════════════════════════════════════════════════════════════
   FILE ATTACHMENTS
═══════════════════════════════════════════════════════════════ */

async function loadAttachments() {
  if (!SHOW_ID) return;
  const list = document.getElementById('attachments-list');
  if (!list) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/attachments`);
    const files = await resp.json();
    renderAttachments(files);
  } catch(_) {}
}

function renderAttachments(files) {
  const list = document.getElementById('attachments-list');
  if (!list) return;
  if (!files.length) {
    list.innerHTML = '<p class="text-dim" style="font-size:12px;margin:0">No files attached yet.</p>';
    return;
  }
  list.innerHTML = files.map(f => {
    const size = f.file_size > 1048576
      ? (f.file_size / 1048576).toFixed(1) + ' MB'
      : f.file_size > 1024
      ? Math.round(f.file_size / 1024) + ' KB'
      : f.file_size + ' B';
    const time = fmtDate(f.created_at);
    return `
      <div class="attachment-item">
        <div class="attachment-icon">${_fileIcon(f.mime_type)}</div>
        <div class="attachment-info">
          <a href="/shows/${SHOW_ID}/attachments/${f.id}/download" class="attachment-name">${_esc(f.filename)}</a>
          <span class="attachment-meta">${size} · ${_esc(f.uploader)} · ${time}</span>
        </div>
        <button class="btn btn-xs btn-danger-ghost" onclick="deleteAttachment(${f.id})" title="Remove">×</button>
      </div>`;
  }).join('');
}

function _fileIcon(mime) {
  if (!mime) return '📎';
  if (mime.includes('pdf'))                             return '📄';
  if (mime.includes('image'))                           return '🖼';
  if (mime.includes('audio'))                           return '🎵';
  if (mime.includes('video'))                           return '🎬';
  if (mime.includes('word') || mime.includes('document')) return '📝';
  if (mime.includes('sheet') || mime.includes('excel')) return '📊';
  if (mime.includes('zip') || mime.includes('compressed')) return '🗜';
  return '📎';
}

function handleFileSelect(input) {
  const file = input.files[0];
  if (file) uploadFile(file);
  input.value = '';  // reset so same file can be re-selected
}

function handleFileDrop(e) {
  e.preventDefault();
  e.currentTarget.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) uploadFile(file);
}

function handleFileDragOver(e) {
  e.preventDefault();
  e.currentTarget.classList.add('drag-over');
}

function handleFileDragLeave(e) {
  e.currentTarget.classList.remove('drag-over');
}

function uploadFile(file) {
  const zone = document.getElementById('upload-zone');
  const progressWrap = document.getElementById('upload-progress-wrap');
  const progressBar  = document.getElementById('upload-progress-bar');
  const progressLabel = document.getElementById('upload-progress-label');

  const formData = new FormData();
  formData.append('file', file);

  _isUploading = true;
  if (zone) zone.classList.add('uploading');
  if (progressWrap) progressWrap.style.display = '';
  if (progressBar)  progressBar.style.width = '0%';
  if (progressLabel) progressLabel.textContent = '0%';

  const xhr = new XMLHttpRequest();
  xhr.upload.addEventListener('progress', e => {
    if (e.lengthComputable && progressBar && progressLabel) {
      const pct = Math.round(e.loaded / e.total * 100);
      progressBar.style.width = pct + '%';
      progressLabel.textContent = pct + '%';
    }
  });
  xhr.addEventListener('load', () => {
    _isUploading = false;
    if (zone) zone.classList.remove('uploading');
    if (progressWrap) progressWrap.style.display = 'none';
    try {
      const d = JSON.parse(xhr.responseText);
      if (d.success) {
        loadAttachments();
        showSaveToast('✓ File attached');
      } else {
        alert(d.error || 'Upload failed.');
      }
    } catch(_) {
      alert('Upload failed.');
    }
  });
  xhr.addEventListener('error', () => {
    _isUploading = false;
    if (zone) zone.classList.remove('uploading');
    if (progressWrap) progressWrap.style.display = 'none';
    alert('Network error during upload.');
  });
  xhr.open('POST', `/shows/${SHOW_ID}/attachments`);
  xhr.send(formData);
}

async function deleteAttachment(aid) {
  if (!confirm('Remove this attachment?')) return;
  const resp = await fetch(`/shows/${SHOW_ID}/attachments/${aid}/delete`, {method:'POST'});
  const d = await resp.json();
  if (d.success) loadAttachments();
  else alert(d.error || 'Delete failed.');
}

/* ═══════════════════════════════════════════════════════════════
   TIME PARSING
═══════════════════════════════════════════════════════════════ */

/**
 * parseTimeToHHMM(str) — normalise various time formats to "HH:MM" (24hr).
 * Accepts: "4pm", "4 PM", "4P.M.", "1600", "16:00", "4:00 PM", "4:00pm", "4"
 * Returns: "HH:MM" string, or the original string if unparseable.
 */
function parseTimeToHHMM(str) {
  if (!str || !str.toString().trim()) return str;
  let s = str.toString().trim();

  // Remove dots from AM/PM indicators (e.g. "4P.M." → "4PM")
  s = s.replace(/\b([AaPp])\.([Mm])\./g, '$1$2');

  // Extract AM/PM suffix
  const ampmMatch = s.match(/([AaPp][Mm])\s*$/);
  const ampm = ampmMatch ? ampmMatch[1].toUpperCase() : null;
  if (ampm) s = s.slice(0, s.length - ampmMatch[0].length).trim();

  // Parse HH:MM or HHMM or H
  let hours, minutes;
  const colonMatch = s.match(/^(\d{1,2}):(\d{2})$/);
  const compactMatch = s.match(/^(\d{3,4})$/);
  const simpleMatch = s.match(/^(\d{1,2})$/);

  if (colonMatch) {
    hours = parseInt(colonMatch[1]);
    minutes = parseInt(colonMatch[2]);
  } else if (compactMatch) {
    const n = compactMatch[1];
    if (n.length <= 2) {
      hours = parseInt(n);
      minutes = 0;
    } else {
      hours = parseInt(n.slice(0, n.length - 2));
      minutes = parseInt(n.slice(-2));
    }
  } else if (simpleMatch) {
    hours = parseInt(simpleMatch[1]);
    minutes = 0;
  } else {
    return str; // unparseable
  }

  if (isNaN(hours) || isNaN(minutes)) return str;

  // Apply AM/PM
  if (ampm === 'AM') {
    if (hours === 12) hours = 0;
  } else if (ampm === 'PM') {
    if (hours !== 12) hours += 12;
  }

  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return str;
  return String(hours).padStart(2, '0') + ':' + String(minutes).padStart(2, '0');
}

// Bind time normalisation to all schedule time cells on blur
function _bindScheduleTimeParsing() {
  const form = document.getElementById('schedule-form');
  if (!form) return;
  form.addEventListener('blur', function(e) {
    const td = e.target.closest('td');
    if (!td) return;
    const tr = td.closest('tr.schedule-row');
    if (!tr) return;
    const cells = tr.querySelectorAll('.sched-cell');
    if (e.target === cells[0] || e.target === cells[1]) {
      const parsed = parseTimeToHHMM(e.target.value);
      if (parsed !== e.target.value) {
        e.target.value = parsed;
        e.target.dispatchEvent(new Event('input', {bubbles: true}));
      }
    }
  }, true);
}

/* ═══════════════════════════════════════════════════════════════
   READ RECEIPTS
═══════════════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════
   FIELD KEY AUTO-POPULATE + AVAILABILITY CHECK
═══════════════════════════════════════════════════════════════ */

(function initFieldKeyAutoPopulate() {
  // Wait for DOM to be ready
  function setup() {
    const modal = document.getElementById('field-modal');
    if (!modal) return;

    const labelInput = modal.querySelector('[name="label"]');
    const keyInput   = modal.querySelector('[name="field_key"]');
    if (!labelInput || !keyInput) return;

    let _manuallyEdited = false;
    let _debounceTimer  = null;

    function slugify(str) {
      return str.toLowerCase()
        .replace(/[^a-z0-9\s_]/g, '')
        .trim()
        .replace(/\s+/g, '_');
    }

    function checkAvailability(key) {
      const statusEl = document.getElementById('field-key-status');
      if (!statusEl || !key) { if (statusEl) statusEl.style.display = 'none'; return; }
      const excludeId = _editFieldId || '';
      const url = `/settings/form-fields/check-key?key=${encodeURIComponent(key)}${excludeId ? '&exclude_id='+excludeId : ''}`;
      fetch(url).then(r => r.json()).then(d => {
        statusEl.style.display = '';
        if (d.available) {
          statusEl.textContent = '✓ field_key is available';
          statusEl.style.color = 'var(--success, #4caf50)';
        } else {
          statusEl.textContent = `✗ field_key already used by: "${d.conflict || 'another field'}"`;
          statusEl.style.color = 'var(--danger, #f44336)';
        }
      }).catch(() => {});
    }

    labelInput.addEventListener('input', function() {
      if (!_manuallyEdited && !_editFieldId) {
        const generated = slugify(labelInput.value);
        keyInput.value = generated;
        clearTimeout(_debounceTimer);
        _debounceTimer = setTimeout(() => checkAvailability(generated), 300);
      }
    });

    keyInput.addEventListener('input', function() {
      _manuallyEdited = true;
      clearTimeout(_debounceTimer);
      _debounceTimer = setTimeout(() => checkAvailability(keyInput.value), 300);
    });

    // Reset manuallyEdited when modal opens fresh (new field)
    const origOpen = window.openFieldModal;
    window.openFieldModal = function(fid, sectionId) {
      _manuallyEdited = !!fid;  // editing = already has key, don't auto-fill
      const statusEl = document.getElementById('field-key-status');
      if (statusEl) statusEl.style.display = 'none';
      if (origOpen) origOpen(fid, sectionId);
      // If editing, check current key availability after modal loads
      if (fid) {
        setTimeout(() => checkAvailability(keyInput.value), 500);
      }
    };
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setup);
  } else {
    setup();
  }
})();


/* ═══════════════════════════════════════════════════════════════
   AI DOCUMENT EXTRACTION
═══════════════════════════════════════════════════════════════ */

function openAiExtract() {
  const modal = document.getElementById('ai-extract-modal');
  if (!modal) return;
  resetAiExtract();
  // Load attachments for dropdown
  if (SHOW_ID) {
    fetch(`/shows/${SHOW_ID}/attachments`).then(r => r.json()).then(attachments => {
      const sel = document.getElementById('ai-attachment-select');
      if (!sel) return;
      sel.innerHTML = '<option value="">— Select attachment —</option>';
      (Array.isArray(attachments) ? attachments : []).forEach(a => {
        if (a.mime_type === 'application/pdf' || a.filename.toLowerCase().endsWith('.pdf') ||
            a.mime_type === 'text/plain' || a.filename.toLowerCase().endsWith('.txt')) {
          const opt = document.createElement('option');
          opt.value = a.id;
          opt.textContent = a.filename;
          sel.appendChild(opt);
        }
      });
    }).catch(() => {});
  }
  modal.style.display = '';
}

function closeAiExtract() {
  const modal = document.getElementById('ai-extract-modal');
  if (modal) modal.style.display = 'none';
}

function resetAiExtract() {
  document.getElementById('ai-upload-section').style.display = '';
  document.getElementById('ai-results-section').style.display = 'none';
  const fileInput = document.getElementById('ai-file-input');
  if (fileInput) fileInput.value = '';
  const sel = document.getElementById('ai-attachment-select');
  if (sel) sel.value = '';
  const msg = document.getElementById('ai-upload-msg');
  if (msg) { msg.style.display = 'none'; msg.textContent = ''; }
  const log = document.getElementById('ai-progress-log');
  if (log) { log.style.display = 'none'; log.innerHTML = ''; }
  _aiDino.stop();
}

// ── AI modal Dino game ────────────────────────────────────────────────────────
function aiDinoJump() { _aiDino.jump(); }

const _aiDino = (() => {
  const GROUND = 78;
  let canvas, ctx, raf = null;
  let dino, obstacles, frame, score, running, dead, speed;
  let _keyBound = false;

  function _reset() {
    dino = { x: 40, y: GROUND - 22, w: 18, h: 22, vy: 0, onGround: true };
    obstacles = []; frame = 0; score = 0; dead = false; speed = 3; running = false;
    if (raf) { cancelAnimationFrame(raf); raf = null; }
    _setMsg('Click or Space to play');
    _setScore('');
    if (ctx && canvas) ctx.clearRect(0, 0, canvas.width, canvas.height);
  }

  function _setMsg(t) {
    const el = document.getElementById('ai-dino-msg');
    if (el) { el.textContent = t; el.style.display = t ? 'block' : 'none'; }
  }
  function _setScore(t) {
    const el = document.getElementById('ai-dino-score');
    if (el) el.textContent = t;
  }

  function _loop() {
    if (!running || dead) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.strokeStyle = 'rgba(245,127,32,0.3)';
    ctx.beginPath(); ctx.moveTo(0, GROUND); ctx.lineTo(canvas.width, GROUND); ctx.stroke();

    speed = 3 + score * 0.15;
    frame++;
    if (frame % Math.max(40, 80 - score * 3) === 0) {
      const h = 18 + Math.random() * 14;
      obstacles.push({ x: canvas.width + 20, y: GROUND - h, w: 12, h });
    }

    dino.vy += 0.55; dino.y += dino.vy;
    if (dino.y >= GROUND - dino.h) { dino.y = GROUND - dino.h; dino.vy = 0; dino.onGround = true; }

    ctx.fillStyle = '#F57F20';
    ctx.fillRect(dino.x, dino.y, dino.w, dino.h);
    ctx.fillRect(dino.x + 4, dino.y - 8, 14, 10);
    ctx.fillStyle = '#000';
    ctx.fillRect(dino.x + 14, dino.y - 7, 3, 3);

    ctx.fillStyle = 'rgba(245,127,32,0.7)';
    for (let i = obstacles.length - 1; i >= 0; i--) {
      const o = obstacles[i];
      o.x -= speed;
      ctx.fillRect(o.x, o.y, o.w, o.h);
      if (o.x + o.w < 0) { obstacles.splice(i, 1); score++; _setScore(`Score: ${score}`); }
      if (dino.x + 3 < o.x + o.w && dino.x + dino.w - 3 > o.x &&
          dino.y + 3 < o.y + o.h && dino.y + dino.h - 3 > o.y) {
        dead = true; running = false;
        ctx.fillStyle = 'rgba(239,68,68,0.15)'; ctx.fillRect(0, 0, canvas.width, canvas.height);
        _setMsg('Click to try again');
        return;
      }
    }

    ctx.fillStyle = 'var(--text-dim,#888)'; ctx.font = '11px monospace';
    ctx.fillText(score, canvas.width - 36, 14);
    raf = requestAnimationFrame(_loop);
  }

  return {
    start() {
      canvas = document.getElementById('ai-dino-canvas');
      ctx = canvas ? canvas.getContext('2d') : null;
      if (!canvas) return;
      if (!_keyBound) {
        const c = document.getElementById('ai-dino-container');
        if (c) c.addEventListener('keydown', e => { if (e.key === ' ' || e.key === 'ArrowUp') { e.preventDefault(); this.jump(); } });
        _keyBound = true;
      }
      _reset();
      const section = document.getElementById('ai-game-section');
      if (section) section.style.display = '';
      // auto-start after a brief moment so user sees the canvas
      setTimeout(() => { running = true; _setMsg(''); _loop(); }, 400);
    },
    stop() {
      running = false;
      if (raf) { cancelAnimationFrame(raf); raf = null; }
      const section = document.getElementById('ai-game-section');
      if (section) section.style.display = 'none';
    },
    jump() {
      if (!canvas) return;
      if (dead) { _reset(); return; }
      if (!running) {
        running = true; _setMsg(''); _loop(); return;
      }
      if (dino.onGround) { dino.vy = -9; dino.onGround = false; }
    }
  };
})();

async function runAiExtract() {
  const fileInput = document.getElementById('ai-file-input');
  const attachSel = document.getElementById('ai-attachment-select');
  const msgEl     = document.getElementById('ai-upload-msg');
  const logEl     = document.getElementById('ai-progress-log');

  const hasFile   = fileInput && fileInput.files && fileInput.files.length > 0;
  const attachId  = attachSel ? attachSel.value : '';

  if (!hasFile && !attachId) {
    msgEl.style.display = '';
    msgEl.textContent = 'Please upload a document or select an existing attachment.';
    msgEl.style.color = 'var(--danger, #f44336)';
    return;
  }

  // Progress log helpers
  const _start = Date.now();
  function _elapsed() {
    return '[' + String(Math.floor((Date.now() - _start) / 1000)).padStart(3) + 's]';
  }
  function _log(msg, color) {
    if (!logEl) return;
    logEl.style.display = '';
    const line = document.createElement('div');
    line.style.color = color || '';
    line.textContent = _elapsed() + ' ' + msg;
    logEl.appendChild(line);
    logEl.scrollTop = logEl.scrollHeight;
  }

  const btn = document.querySelector('#ai-upload-section .btn-primary');
  if (btn) { btn.disabled = true; btn.textContent = 'Extracting…'; }

  msgEl.style.display = 'none';
  if (logEl) { logEl.style.display = ''; logEl.innerHTML = ''; }
  _aiDino.start();

  const fname = hasFile ? fileInput.files[0].name : ('attachment #' + attachId);
  _log('Reading document: ' + fname);

  const formData = new FormData();
  if (hasFile) {
    formData.append('document', fileInput.files[0]);
  } else {
    formData.append('attachment_id', attachId);
  }

  function _resetBtn() {
    if (btn) { btn.disabled = false; btn.textContent = 'Extract Fields'; }
  }

  // Animated "waiting" line while fetch is in-flight
  let _waitLine = null;
  let _waitDots = 0;
  const _waitTimer = setInterval(() => {
    if (!_waitLine) {
      _waitLine = document.createElement('div');
      logEl.appendChild(_waitLine);
    }
    _waitDots = (_waitDots % 3) + 1;
    _waitLine.textContent = _elapsed() + ' AI model is processing' + '.'.repeat(_waitDots);
    logEl.scrollTop = logEl.scrollHeight;
  }, 800);

  try {
    _log('Sending to Ollama...');
    const resp = await fetch(`/shows/${SHOW_ID}/ai-extract`, {
      method: 'POST',
      body: formData,
    });
    clearInterval(_waitTimer);
    if (_waitLine) _waitLine.remove();
    _resetBtn();

    const data = await resp.json();

    if (!data.success) {
      _log('Error: ' + (data.error || 'Unknown error'), 'var(--red, #f44336)');
      return;
    }

    const n = Object.keys(data.suggestions || {}).length;
    _log(`Done — ${n} field${n !== 1 ? 's' : ''} found in ${data.model}`, 'var(--green, #22c55e)');
    setTimeout(() => _showAiSuggestions(data), 600);
  } catch(e) {
    clearInterval(_waitTimer);
    if (_waitLine) _waitLine.remove();
    _resetBtn();
    _log('Network error: ' + e.message, 'var(--red, #f44336)');
  }
}

function _showAiSuggestions(data) {
  _aiDino.stop();
  document.getElementById('ai-upload-section').style.display = 'none';
  document.getElementById('ai-results-section').style.display = '';

  const header = document.getElementById('ai-results-header');
  const list   = document.getElementById('ai-suggestions-list');
  const applyBtn = document.getElementById('ai-apply-btn');

  const count = Object.keys(data.suggestions || {}).length;
  header.textContent = `Document: ${data.document}  ·  Model: ${data.model}  ·  ${count} field${count !== 1 ? 's' : ''} found`;

  if (!count) {
    list.innerHTML = '<p style="font-size:13px;color:var(--text-dim)">No fields could be extracted from this document. Make sure fields have AI hints configured in Settings → Form Fields.</p>';
    if (applyBtn) applyBtn.disabled = true;
    return;
  }

  if (applyBtn) applyBtn.disabled = false;

  list.innerHTML = Object.entries(data.suggestions).map(([key, s]) => `
    <div style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)">
      <input type="checkbox" class="ai-suggestion-check" data-key="${_esc(key)}" data-value="${_esc(s.value)}" checked style="margin-top:3px;flex-shrink:0">
      <div style="flex:1;min-width:0">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-dim)">${_esc(s.label)}</div>
        <div style="font-size:13px;margin-top:2px;word-break:break-word">${_esc(s.value)}</div>
      </div>
    </div>
  `).join('');
}

async function applyAiSuggestions() {
  const checks = document.querySelectorAll('.ai-suggestion-check:checked');
  if (!checks.length) { alert('No fields selected.'); return; }

  const applyBtn = document.getElementById('ai-apply-btn');
  if (applyBtn) applyBtn.disabled = true;

  // Build payload: {field_key: value, ...}
  const payload = {};
  checks.forEach(cb => { payload[cb.dataset.key] = cb.dataset.value; });

  try {
    const resp = await fetch(`/shows/${SHOW_ID}/save/advance`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await resp.json();
    if (d.success) {
      // Update fields in the UI
      Object.entries(payload).forEach(([key, value]) => {
        const fieldEl = document.querySelector(`[data-key="${key}"]`);
        if (fieldEl) {
          if (fieldEl.tagName === 'INPUT' || fieldEl.tagName === 'TEXTAREA') {
            fieldEl.value = value;
          } else if (fieldEl.tagName === 'SELECT') {
            fieldEl.value = value;
          }
        }
      });
      closeAiExtract();
      if (typeof syncAdvanceSectionVisibility === 'function') syncAdvanceSectionVisibility();
      const statusEl = document.getElementById('save-status');
      if (statusEl) {
        const count = Object.keys(payload).length;
        statusEl.textContent = `AI applied ${count} field${count !== 1 ? 's' : ''}`;
        setTimeout(() => { if (statusEl.textContent.startsWith('AI')) statusEl.textContent = ''; }, 3000);
      }
    } else {
      alert('Apply failed: ' + (d.error || 'Unknown error'));
      if (applyBtn) applyBtn.disabled = false;
    }
  } catch(e) {
    alert('Network error: ' + e.message);
    if (applyBtn) applyBtn.disabled = false;
  }
}

function markAdvanceRead() {
  if (!SHOW_ID) return;
  fetch(`/shows/${SHOW_ID}/read`, {method:'POST'}).catch(() => {});
}

async function loadReadReceipts() {
  if (!SHOW_ID) return;
  const container = document.getElementById('read-receipts-list');
  if (!container) return;
  try {
    const resp = await fetch(`/shows/${SHOW_ID}/reads`);
    const reads = await resp.json();
    if (!reads.length) {
      container.innerHTML = '<span class="text-dim" style="font-size:12px">No one has opened the advance sheet yet.</span>';
      return;
    }
    container.innerHTML = reads.map(r => {
      const color = _userColor(r.author);
      const time  = fmtDate(r.read_at);
      return `<span class="read-receipt-chip"
                    style="border-color:${color}33;background:${color}11"
                    title="${_esc(r.author)} · v${r.version_read} · ${time}">
        <span class="read-receipt-avatar" style="background:${color}">${r.initials}</span>
        ${_esc(r.author.split(' ')[0])} <span class="read-version">v${r.version_read}</span>
      </span>`;
    }).join('');
  } catch(_) {}
}
