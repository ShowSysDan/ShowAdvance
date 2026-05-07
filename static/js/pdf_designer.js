/* PDF Designer — admin-only layout editor for the 5 PDF exports.
 * Mirrors the vanilla HTML5 drag-drop pattern used in static/js/app.js for
 * the form-fields settings page. Loads catalog + saved layout from the
 * server, renders an editable section list, posts the full layout JSON on
 * Save, then refreshes the preview iframe.
 */
(function () {
  'use strict';

  const SHOW_KEY = 'pdfDesignerShowId';
  const TYPE_KEY = 'pdfDesignerType';

  let currentType = null;
  let currentCatalog = null;     // [{key, default_label, required, fields:[{key,default_label}]}, ...]
  let currentLayout = null;      // mutable layout dict {version,type,sections:[...]}
  let dragSrcKey = null;
  let dragSrcFieldRow = null;

  const elTabs = document.getElementById('pd-type-tabs');
  const elEdit = document.getElementById('pd-edit-pane');
  const elStatus = document.getElementById('pd-status');
  const elPreview = document.getElementById('pd-preview-iframe');
  const elShowSelect = document.getElementById('pd-show-select');
  const elSaveBtn = document.getElementById('pd-save-btn');
  const elResetBtn = document.getElementById('pd-reset-btn');

  // ── helpers ────────────────────────────────────────────────────
  function setStatus(msg, kind) {
    elStatus.textContent = msg || '';
    elStatus.className = 'pd-status' + (kind ? ' ' + kind : '');
    if (kind === 'success') {
      setTimeout(() => { if (elStatus.textContent === msg) setStatus(''); }, 2500);
    }
  }

  function fontSizeOptions(selected) {
    return window.PD_FONT_CHOICES.map(v => {
      const sel = (selected == null && v == null) || (selected != null && Number(selected) === v);
      const label = v == null ? 'default' : (v + 'pt');
      const val   = v == null ? '' : String(v);
      return `<option value="${val}"${sel ? ' selected' : ''}>${label}</option>`;
    }).join('');
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => (
      {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  function findCatalogSection(key) {
    return currentCatalog.find(s => s.key === key) || null;
  }

  function findCatalogField(secKey, fieldKey) {
    const s = findCatalogSection(secKey);
    if (!s) return null;
    return (s.fields || []).find(f => f.key === fieldKey) || null;
  }

  // ── render ─────────────────────────────────────────────────────
  function render() {
    if (!currentLayout || !currentCatalog) {
      elEdit.innerHTML = '<div class="pd-empty-msg">Loading…</div>';
      return;
    }
    elEdit.innerHTML = currentLayout.sections.map(sec => renderSection(sec)).join('');
    bindDragHandlers();
    bindControlHandlers();
  }

  function renderSection(sec) {
    const cat = findCatalogSection(sec.key);
    if (!cat) return '';
    const required = !!cat.required;
    const visibleAttr = required || sec.visible !== false ? 'checked' : '';
    const disabledVisAttr = required ? 'disabled title="Required — cannot be hidden"' : '';
    const labelDefault = cat.default_label || sec.key;
    const labelVal = sec.label || '';
    const fontHtml = fontSizeOptions(sec.font_size_pt);
    const fieldsHtml = (cat.fields || []).map(f => renderField(sec, f)).join('');
    const hasFields = (cat.fields || []).length > 0;

    return `
      <div class="pd-section-row" data-key="${escapeHtml(sec.key)}" draggable="true">
        <div class="pd-section-head">
          <span class="drag-handle" title="Drag to reorder">⠿</span>
          <input type="checkbox" class="pd-vis" ${visibleAttr} ${disabledVisAttr} title="Visible">
          <input type="text" class="pd-label" placeholder="${escapeHtml(labelDefault)}" value="${escapeHtml(labelVal)}">
          <select class="pd-font" title="Font size">${fontHtml}</select>
          ${required ? '<span class="pd-required-tag">required</span>' : ''}
          ${hasFields ? '<button type="button" class="pd-toggle-fields" title="Show/hide fields">▸</button>' : ''}
        </div>
        <div class="pd-section-controls">
          <label><input type="checkbox" class="pd-pagebreak" ${sec.page_break_before ? 'checked' : ''}> page-break before</label>
          <span style="color:var(--text-muted);font-family:monospace;font-size:10.5px">${escapeHtml(sec.key)}</span>
        </div>
        ${hasFields ? `<div class="pd-fields">${fieldsHtml}</div>` : ''}
      </div>`;
  }

  function renderField(sec, fcat) {
    const saved = (sec.fields || []).find(f => f.key === fcat.key) || {visible: true, label: ''};
    const visAttr = saved.visible !== false ? 'checked' : '';
    const labelVal = saved.label || '';
    return `
      <div class="pd-field-row" draggable="true" data-key="${escapeHtml(fcat.key)}">
        <span class="drag-handle" title="Drag to reorder field">⠿</span>
        <input type="checkbox" class="pd-field-vis" ${visAttr}>
        <span class="pd-field-key">${escapeHtml(fcat.key)}</span>
        <input type="text" class="pd-field-label" placeholder="${escapeHtml(fcat.default_label || fcat.key)}" value="${escapeHtml(labelVal)}">
      </div>`;
  }

  // ── drag-drop (vanilla HTML5, mirrors app.js form-fields pattern) ──
  function bindDragHandlers() {
    elEdit.querySelectorAll('.pd-section-row').forEach(row => {
      row.addEventListener('dragstart', e => {
        if (!e.target.closest('.drag-handle') && e.target !== row) {
          // Only drag from handle or row itself, not from input/select
        }
        dragSrcKey = row.dataset.key;
        row.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
      });
      row.addEventListener('dragover', e => {
        e.preventDefault();
        if (dragSrcFieldRow) return;
        if (dragSrcKey && dragSrcKey !== row.dataset.key) {
          row.classList.add('dragover');
        }
      });
      row.addEventListener('dragleave', () => row.classList.remove('dragover'));
      row.addEventListener('drop', e => {
        e.preventDefault();
        row.classList.remove('dragover');
        if (dragSrcFieldRow) return;
        if (!dragSrcKey || dragSrcKey === row.dataset.key) return;
        reorderSections(dragSrcKey, row.dataset.key);
      });
      row.addEventListener('dragend', () => {
        row.classList.remove('dragging');
        dragSrcKey = null;
      });

      // Field drag inside this section.
      row.querySelectorAll('.pd-field-row').forEach(fr => {
        fr.addEventListener('dragstart', e => {
          e.stopPropagation();
          dragSrcFieldRow = { sectionKey: row.dataset.key, fieldKey: fr.dataset.key };
          fr.classList.add('dragging');
          e.dataTransfer.effectAllowed = 'move';
        });
        fr.addEventListener('dragover', e => {
          if (!dragSrcFieldRow) return;
          if (dragSrcFieldRow.sectionKey !== row.dataset.key) return;
          e.preventDefault();
          e.stopPropagation();
          if (dragSrcFieldRow.fieldKey !== fr.dataset.key) fr.classList.add('dragover');
        });
        fr.addEventListener('dragleave', () => fr.classList.remove('dragover'));
        fr.addEventListener('drop', e => {
          e.stopPropagation();
          fr.classList.remove('dragover');
          if (!dragSrcFieldRow) return;
          if (dragSrcFieldRow.sectionKey !== row.dataset.key) return;
          if (dragSrcFieldRow.fieldKey === fr.dataset.key) return;
          e.preventDefault();
          reorderFields(row.dataset.key, dragSrcFieldRow.fieldKey, fr.dataset.key);
        });
        fr.addEventListener('dragend', () => {
          fr.classList.remove('dragging');
          dragSrcFieldRow = null;
        });
      });
    });
  }

  function bindControlHandlers() {
    elEdit.querySelectorAll('.pd-section-row').forEach(row => {
      const key = row.dataset.key;
      const sec = currentLayout.sections.find(s => s.key === key);
      if (!sec) return;
      row.querySelector('.pd-vis').addEventListener('change', e => { sec.visible = e.target.checked; });
      row.querySelector('.pd-label').addEventListener('input', e => { sec.label = e.target.value; });
      row.querySelector('.pd-font').addEventListener('change', e => {
        const v = e.target.value;
        sec.font_size_pt = v === '' ? null : Number(v);
      });
      row.querySelector('.pd-pagebreak').addEventListener('change', e => { sec.page_break_before = e.target.checked; });
      const toggleBtn = row.querySelector('.pd-toggle-fields');
      if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
          const fields = row.querySelector('.pd-fields');
          if (!fields) return;
          fields.classList.toggle('open');
          toggleBtn.textContent = fields.classList.contains('open') ? '▾' : '▸';
        });
      }
      row.querySelectorAll('.pd-field-row').forEach(fr => {
        const fkey = fr.dataset.key;
        const f = (sec.fields || []).find(x => x.key === fkey);
        if (!f) return;
        fr.querySelector('.pd-field-vis').addEventListener('change', e => { f.visible = e.target.checked; });
        fr.querySelector('.pd-field-label').addEventListener('input', e => { f.label = e.target.value; });
      });
    });
  }

  function reorderSections(srcKey, destKey) {
    const sects = currentLayout.sections;
    const srcIdx = sects.findIndex(s => s.key === srcKey);
    const destIdx = sects.findIndex(s => s.key === destKey);
    if (srcIdx < 0 || destIdx < 0) return;
    const [moved] = sects.splice(srcIdx, 1);
    sects.splice(destIdx, 0, moved);
    render();
  }

  function reorderFields(secKey, srcFieldKey, destFieldKey) {
    const sec = currentLayout.sections.find(s => s.key === secKey);
    if (!sec) return;
    const srcIdx = sec.fields.findIndex(f => f.key === srcFieldKey);
    const destIdx = sec.fields.findIndex(f => f.key === destFieldKey);
    if (srcIdx < 0 || destIdx < 0) return;
    const [moved] = sec.fields.splice(srcIdx, 1);
    sec.fields.splice(destIdx, 0, moved);
    render();
  }

  // ── api ────────────────────────────────────────────────────────
  async function loadType(pdfType) {
    if (!window.PD_ENABLED_TYPES.includes(pdfType)) {
      elEdit.innerHTML = '<div class="pd-empty-msg">This PDF type is not yet enabled.</div>';
      return;
    }
    currentType = pdfType;
    localStorage.setItem(TYPE_KEY, pdfType);
    setStatus('Loading…');
    try {
      const res = await fetch(`/admin/pdf-designer/${pdfType}/layout.json`, { credentials: 'same-origin' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      currentCatalog = data.catalog;
      currentLayout = data.layout;
      render();
      refreshPreview();
      setStatus('');
    } catch (e) {
      setStatus('Failed to load: ' + e.message, 'error');
    }
  }

  async function saveLayout() {
    if (!currentType || !currentLayout) return;
    setStatus('Saving…');
    try {
      const res = await fetch(`/admin/pdf-designer/${currentType}/layout.json`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(currentLayout),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      currentLayout = data.layout || currentLayout;
      render();
      refreshPreview();
      setStatus('Saved', 'success');
    } catch (e) {
      setStatus('Save failed: ' + e.message, 'error');
    }
  }

  async function resetLayout() {
    if (!currentType) return;
    if (!confirm(`Reset the ${currentType} layout to defaults? This cannot be undone (audit logged).`)) return;
    setStatus('Resetting…');
    try {
      const res = await fetch(`/admin/pdf-designer/${currentType}/reset`, {
        method: 'POST',
        credentials: 'same-origin',
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      await loadType(currentType);
      setStatus('Reset', 'success');
    } catch (e) {
      setStatus('Reset failed: ' + e.message, 'error');
    }
  }

  function refreshPreview() {
    if (!currentType) return;
    const showId = elShowSelect.value;
    if (!showId) return;
    elPreview.src = `/admin/pdf-designer/${currentType}/preview.pdf?show_id=${encodeURIComponent(showId)}&t=${Date.now()}`;
  }

  // ── init ───────────────────────────────────────────────────────
  function init() {
    // Restore last show
    const savedShow = localStorage.getItem(SHOW_KEY);
    if (savedShow && [...elShowSelect.options].some(o => o.value === savedShow)) {
      elShowSelect.value = savedShow;
    }
    elShowSelect.addEventListener('change', () => {
      localStorage.setItem(SHOW_KEY, elShowSelect.value);
      refreshPreview();
    });

    elTabs.querySelectorAll('.pd-type-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        if (tab.disabled) return;
        elTabs.querySelectorAll('.pd-type-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        loadType(tab.dataset.type);
      });
    });

    elSaveBtn.addEventListener('click', saveLayout);
    elResetBtn.addEventListener('click', resetLayout);

    // Initial type: last used (if enabled) or first enabled tab.
    const lastType = localStorage.getItem(TYPE_KEY);
    let initial = window.PD_ENABLED_TYPES.includes(lastType) ? lastType : window.PD_ENABLED_TYPES[0];
    if (initial) {
      const tab = elTabs.querySelector(`.pd-type-tab[data-type="${initial}"]`);
      if (tab) tab.click();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
