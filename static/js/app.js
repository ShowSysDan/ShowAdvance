/* ================================================================
   DPC Advance App — Frontend JS
================================================================ */
'use strict';

let SHOW_ID = null;
let activeTab = 'advance';
let saveTimer = null;

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
}

function initShow(showId, initialTab) {
  SHOW_ID = showId;
  switchTab(initialTab || 'advance');
  bindAdvanceForm();
  bindScheduleForm();
  bindPostNotesForm();
  evaluateAllConditionals();
}

/* ── Save Status ───────────────────────────────────────────────── */
function setSaveStatus(state, msg) {
  const el = document.getElementById('save-status');
  if (!el) return;
  el.textContent = msg;
  el.className = 'save-status ' + state;
}

function scheduleSave() {
  setSaveStatus('saving', 'Unsaved changes...');
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => saveActive(), 1500);
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
    if (d.success) { setSaveStatus('saved', '✓ Saved'); setTimeout(() => setSaveStatus('', ''), 3000); }
    else setSaveStatus('error', '✗ Save failed');
  } catch(e) { setSaveStatus('error', '✗ Network error'); }
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
    if (d.success) { setSaveStatus('saved', '✓ Saved'); setTimeout(() => setSaveStatus('', ''), 3000); }
    else setSaveStatus('error', '✗ Save failed');
  } catch(e) { setSaveStatus('error', '✗ Network error'); }
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
    if (d.success) { setSaveStatus('saved', '✓ Saved'); setTimeout(() => setSaveStatus('', ''), 3000); }
    else setSaveStatus('error', '✗ Network error'); 
  } catch(e) { setSaveStatus('error', '✗ Network error'); }
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
