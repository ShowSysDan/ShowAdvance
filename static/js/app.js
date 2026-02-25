/* ================================================================
   DPC Advance App — Frontend JS
================================================================ */
'use strict';

let SHOW_ID = null;
let activeTab = 'advance';
let saveTimer = null;
let _isDirty = false;          // true whenever there are unsaved changes
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
    return `<span class="presence-avatar" style="background:${color}" title="${u.name} · ${u.tab} tab">${_initials(u.name)}</span>`;
  }).join('') + `<span class="presence-label">${users.length === 1 ? users[0].name.split(' ')[0] : users.length + ' people'} also here</span>`;
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
  const url = new URL(window.location);
  url.searchParams.set('tab', name);
  history.replaceState({}, '', url);
  // Immediately update presence when the user switches tabs
  if (SHOW_ID) {
    if (name === 'advance') _pollAdvanceSync();
    else _pollHeartbeat();
  }
}

function initShow(showId, initialTab) {
  SHOW_ID = showId;
  switchTab(initialTab || 'advance');
  bindAdvanceForm();
  bindScheduleForm();
  bindPostNotesForm();
  evaluateAllConditionals();

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

  // Warn before leaving page with unsaved changes
  window.addEventListener('beforeunload', e => {
    if (_isDirty) {
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

function saveActive() {
  clearTimeout(saveTimer);
  if (activeTab === 'advance')   saveAdvance();
  if (activeTab === 'schedule')  saveSchedule();
  if (activeTab === 'postnotes') savePostNotes();
}

/* ── Advance Form ──────────────────────────────────────────────── */
function bindAdvanceForm() {
  const form = document.getElementById('advance-form');
  if (!form) return;
  form.addEventListener('change', () => { evaluateAllConditionals(); scheduleSave(); });
  form.addEventListener('input', () => scheduleSave());
}

function collectAdvanceData() {
  const data = {};
  document.querySelectorAll('#advance-form .adv-field').forEach(el => {
    const key = el.dataset.key;
    if (!key) return;
    if (el.type === 'checkbox') {
      data[key] = el.checked ? 'true' : 'false';
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
  form.addEventListener('input', () => scheduleSave());
}

function addScheduleRow() {
  const tbody = document.getElementById('schedule-rows');
  if (!tbody) return;
  const tr = document.createElement('tr');
  tr.className = 'schedule-row';
  tr.innerHTML = `
    <td><input type="text" class="sched-cell" placeholder="3:00pm" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="4:00pm" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="Description" value=""></td>
    <td><input type="text" class="sched-cell" placeholder="Notes" value=""></td>
    <td><button type="button" class="row-del-btn" onclick="removeRow(this)">×</button></td>
  `;
  tbody.appendChild(tr);
  tr.querySelector('.sched-cell').focus();
  // Bind changes
  tr.querySelectorAll('.sched-cell').forEach(inp => {
    inp.addEventListener('input', () => scheduleSave());
  });
}

function removeRow(btn) {
  const row = btn.closest('tr');
  row.remove();
  scheduleSave();
}

function collectScheduleData() {
  const meta = {};
  document.querySelectorAll('#schedule-form .sched-meta').forEach(el => {
    const key = el.dataset.key;
    if (key) meta[key] = el.value;
  });
  const rows = [];
  document.querySelectorAll('#schedule-rows .schedule-row').forEach(tr => {
    const cells = tr.querySelectorAll('.sched-cell');
    rows.push({
      start_time:  cells[0] ? cells[0].value : '',
      end_time:    cells[1] ? cells[1].value : '',
      description: cells[2] ? cells[2].value : '',
      notes:       cells[3] ? cells[3].value : '',
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
  header.closest('.form-section').classList.toggle('collapsed');
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
  await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
}

/* ── Field Add/Edit Modal ─────────────────────────────────────── */
let _editFieldId = null;

function openFieldModal(fid, sectionId) {
  _editFieldId = fid || null;
  const modal = document.getElementById('field-modal');
  if (!modal) return;
  const form = modal.querySelector('#field-modal-form');
  if (form) form.reset();

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
  if (optGroup) optGroup.style.display = (type === 'select') ? '' : 'none';
  if (deptGroup) deptGroup.style.display = (type === 'contact_dropdown') ? '' : 'none';
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
    }
  } else {
    modal.querySelector('#section-modal-form')?.reset();
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
    alert('Backup created successfully.');
  } else {
    alert('Backup failed: ' + (d.error||'Unknown error'));
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
        <span class="backup-filename">${f.filename}</span>
        <span class="backup-size">${f.size_kb} KB</span>
        <span class="backup-mtime">${f.mtime}</span>
      </div>
    `).join('');
  }
}
