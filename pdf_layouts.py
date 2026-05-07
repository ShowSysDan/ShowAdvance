"""
PDF layout configuration — admin-customizable section ordering, visibility,
labels, and font sizing for the 5 PDF exports (advance, schedule, postnotes,
asset_invoice, post_show_invoice).

Layouts are persisted as JSON in the existing `app_settings` table under the
key `pdf_layout_<type>`. The catalog (what *can* be configured) lives here in
code; the saved layout (what *is* configured) lives in the DB.
"""

import json
import logging

logger = logging.getLogger(__name__)

PDF_TYPES = ('advance', 'schedule', 'postnotes', 'asset_invoice', 'post_show_invoice')

# Font-size choices exposed in the editor (in points). `None` = inherit template CSS.
FONT_SIZE_CHOICES = (None, 6, 7, 8, 9, 10, 11, 12)


# ── Static catalogs ──────────────────────────────────────────────────────────
# Each entry: {key, default_label, required, fields: [{key, default_label}, ...]}
# `header` is required on every type; `total_bar` is required on the two
# invoices to prevent admins from accidentally hiding the grand-total.

LAYOUT_CATALOG = {
    'schedule': [
        {'key': 'header',         'default_label': 'Header',          'required': True,  'fields': []},
        {'key': 'day_banner',     'default_label': 'Day Banner',      'required': False, 'fields': []},
        {'key': 'info_bar',       'default_label': 'Info Bar',        'required': False, 'fields': []},
        {'key': 'wifi_box',       'default_label': 'WiFi Info',       'required': False, 'fields': []},
        {'key': 'crew_call',      'default_label': 'Crew Call Times', 'required': False, 'fields': []},
        {'key': 'sched_table',    'default_label': 'Schedule Table',  'required': False, 'fields': []},
        {'key': 'contacts_grid',  'default_label': 'Contacts',        'required': False, 'fields': []},
    ],
    'postnotes': [
        {'key': 'header',   'default_label': 'Header',   'required': True,  'fields': []},
        {'key': 'info_bar', 'default_label': 'Info Bar', 'required': False, 'fields': []},
        {'key': 'post_show_notes', 'default_label': 'Post-Show Notes', 'required': False, 'fields': [
            {'key': 'show_notes_tour',  'default_label': 'Show Notes (Tour/Client)'},
            {'key': 'house_notes',      'default_label': 'House Notes'},
            {'key': 'equipment_issues', 'default_label': 'Equipment / Maintenance Issues'},
            {'key': 'miscellaneous',    'default_label': 'Miscellaneous'},
        ]},
        {'key': 'production_schedule', 'default_label': 'Production Schedule Timeline', 'required': False, 'fields': []},
    ],
    'asset_invoice': [
        {'key': 'header',           'default_label': 'Header',           'required': True,  'fields': []},
        {'key': 'show_block',       'default_label': 'Show Info',        'required': False, 'fields': []},
        {'key': 'asset_rentals',    'default_label': 'Asset Rentals',    'required': False, 'fields': []},
        {'key': 'external_rentals', 'default_label': 'External Rentals', 'required': False, 'fields': []},
        {'key': 'total_bar',        'default_label': 'Grand Total',      'required': True,  'fields': []},
        {'key': 'footer',           'default_label': 'Footer',           'required': False, 'fields': []},
    ],
    'post_show_invoice': [
        {'key': 'header',           'default_label': 'Header',           'required': True,  'fields': []},
        {'key': 'show_block',       'default_label': 'Show Info',        'required': False, 'fields': []},
        {'key': 'asset_rentals',    'default_label': 'Asset Rentals',    'required': False, 'fields': []},
        {'key': 'external_rentals', 'default_label': 'External Rentals', 'required': False, 'fields': []},
        {'key': 'labor',            'default_label': 'Labor',            'required': False, 'fields': []},
        {'key': 'total_bar',        'default_label': 'Grand Total',      'required': True,  'fields': []},
        {'key': 'footer',           'default_label': 'Footer',           'required': False, 'fields': []},
    ],
}


def build_advance_catalog(form_sections):
    """
    Build the advance catalog at runtime from `form_sections` (output of
    get_form_fields_for_template()).  The advance schema is itself DB-driven
    so its catalog must be derived per-request.

    Returns a list with the synthetic `header` section first (always required),
    followed by one entry per form_section, each with `fields` populated from
    form_fields.
    """
    catalog = [
        {'key': 'header', 'default_label': 'Header', 'required': True, 'fields': []},
    ]
    for s in form_sections or []:
        section_key = s.get('section_key')
        if not section_key:
            continue
        catalog.append({
            'key': section_key,
            'default_label': s.get('label') or section_key,
            'required': False,
            'fields': [
                {'key': f['field_key'], 'default_label': f.get('label') or f['field_key']}
                for f in s.get('fields', [])
            ],
        })
    return catalog


def get_catalog(pdf_type, form_sections=None):
    """Return the section catalog for a pdf_type. For 'advance', requires
    form_sections (from get_form_fields_for_template())."""
    if pdf_type == 'advance':
        return build_advance_catalog(form_sections)
    return list(LAYOUT_CATALOG.get(pdf_type, []))


def default_layout(pdf_type, form_sections=None):
    """Return the default (no-overrides) layout JSON dict for a pdf_type."""
    catalog = get_catalog(pdf_type, form_sections=form_sections)
    return {
        'version': 1,
        'type': pdf_type,
        'sections': [
            {
                'key': s['key'],
                'visible': True,
                'label': '',
                'font_size_pt': None,
                'page_break_before': False,
                'fields': [
                    {'key': f['key'], 'visible': True, 'label': ''}
                    for f in s.get('fields', [])
                ],
            }
            for s in catalog
        ],
    }


def _parse_or_default(raw, catalog, pdf_type):
    """Parse saved JSON; on any failure return defaults derived from catalog.
    Drops unknown section/field keys silently and forces required-section visibility."""
    if not raw:
        return _layout_from_catalog(catalog, pdf_type)
    try:
        data = json.loads(raw)
        if not isinstance(data, dict) or 'sections' not in data:
            raise ValueError('layout JSON missing sections')
    except Exception as e:
        logger.warning('pdf_layout %s: parse failed (%s) — using defaults', pdf_type, e)
        return _layout_from_catalog(catalog, pdf_type)

    cat_section_keys = {s['key'] for s in catalog}
    cat_field_keys = {s['key']: {f['key'] for f in s.get('fields', [])} for s in catalog}
    required_keys = {s['key'] for s in catalog if s.get('required')}

    saved_sections = []
    seen = set()
    for sec in data.get('sections') or []:
        if not isinstance(sec, dict):
            continue
        skey = sec.get('key')
        if skey not in cat_section_keys or skey in seen:
            continue
        seen.add(skey)
        # Filter fields to known keys.
        valid_field_keys = cat_field_keys.get(skey, set())
        saved_fields = []
        seen_f = set()
        for f in sec.get('fields') or []:
            if not isinstance(f, dict):
                continue
            fkey = f.get('key')
            if fkey not in valid_field_keys or fkey in seen_f:
                continue
            seen_f.add(fkey)
            saved_fields.append({
                'key': fkey,
                'visible': bool(f.get('visible', True)),
                'label': str(f.get('label') or ''),
            })
        # Append catalog fields that weren't in saved layout (default-visible).
        for fkey in valid_field_keys - seen_f:
            saved_fields.append({'key': fkey, 'visible': True, 'label': ''})

        saved_sections.append({
            'key': skey,
            'visible': True if skey in required_keys else bool(sec.get('visible', True)),
            'label': str(sec.get('label') or ''),
            'font_size_pt': _coerce_font_size(sec.get('font_size_pt')),
            'page_break_before': bool(sec.get('page_break_before', False)),
            'fields': saved_fields,
        })

    # Append catalog sections that were missing from the saved layout.
    for s in catalog:
        if s['key'] not in seen:
            saved_sections.append({
                'key': s['key'],
                'visible': True,
                'label': '',
                'font_size_pt': None,
                'page_break_before': False,
                'fields': [
                    {'key': f['key'], 'visible': True, 'label': ''}
                    for f in s.get('fields', [])
                ],
            })

    return {'version': 1, 'type': pdf_type, 'sections': saved_sections}


def _layout_from_catalog(catalog, pdf_type):
    """Build a default layout dict directly from a catalog list."""
    return {
        'version': 1,
        'type': pdf_type,
        'sections': [
            {
                'key': s['key'],
                'visible': True,
                'label': '',
                'font_size_pt': None,
                'page_break_before': False,
                'fields': [
                    {'key': f['key'], 'visible': True, 'label': ''}
                    for f in s.get('fields', [])
                ],
            }
            for s in catalog
        ],
    }


def _coerce_font_size(v):
    if v is None or v == '':
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n in FONT_SIZE_CHOICES else None


def validate_payload(payload, catalog, pdf_type):
    """Validate a layout payload from the editor before saving.
    Returns (cleaned_layout_dict, error_message_or_None).
    Rejects payloads that hide a required section.
    """
    if not isinstance(payload, dict):
        return None, 'Payload must be an object.'
    cat_section_keys = {s['key'] for s in catalog}
    required_keys = {s['key'] for s in catalog if s.get('required')}
    for sec in payload.get('sections') or []:
        if not isinstance(sec, dict):
            continue
        if sec.get('key') in required_keys and sec.get('visible') is False:
            return None, f"Section '{sec.get('key')}' is required and cannot be hidden."
    # Reuse _parse_or_default to drop unknown keys & normalize structure.
    cleaned = _parse_or_default(json.dumps(payload), catalog, pdf_type)
    return cleaned, None


# ── Helper class consumed by Jinja templates ────────────────────────────────

class PdfLayout:
    """
    Templates instantiate this and call methods like layout.visible(...) /
    layout.label(...) / layout.iter_sections(...).  Reads saved JSON once;
    falls back silently to defaults on any error.
    """

    def __init__(self, pdf_type, get_setting, form_sections=None):
        """
        pdf_type:       one of PDF_TYPES
        get_setting:    a callable like app.get_app_setting (key -> str)
        form_sections:  required for pdf_type='advance'; ignored otherwise
        """
        self.pdf_type = pdf_type
        self.catalog = get_catalog(pdf_type, form_sections=form_sections)
        self._catalog_required = {s['key'] for s in self.catalog if s.get('required')}
        try:
            raw = get_setting(f'pdf_layout_{pdf_type}', '')
        except Exception as e:
            logger.warning('pdf_layout %s: get_setting failed (%s) — using defaults', pdf_type, e)
            raw = ''
        self.config = _parse_or_default(raw, self.catalog, pdf_type)
        self._sections_by_key = {s['key']: s for s in self.config['sections']}

    # ── section-level helpers ──
    def visible(self, section_key):
        if section_key in self._catalog_required:
            return True
        sec = self._sections_by_key.get(section_key)
        return True if sec is None else bool(sec.get('visible', True))

    def label(self, section_key, default):
        sec = self._sections_by_key.get(section_key)
        if sec:
            v = sec.get('label')
            if v:
                return v
        return default

    def font_size(self, section_key, default):
        """Return a CSS font-size string. `default` is a CSS string like '8.5pt'."""
        sec = self._sections_by_key.get(section_key)
        if sec and sec.get('font_size_pt'):
            return f"{sec['font_size_pt']}pt"
        return default

    def page_break_before(self, section_key):
        sec = self._sections_by_key.get(section_key)
        return bool(sec and sec.get('page_break_before'))

    # ── field-level helpers ──
    def field_visible(self, section_key, field_key):
        sec = self._sections_by_key.get(section_key)
        if not sec:
            return True
        for f in sec.get('fields') or []:
            if f.get('key') == field_key:
                return bool(f.get('visible', True))
        return True  # unknown field → default visible

    def field_label(self, section_key, field_key, default):
        sec = self._sections_by_key.get(section_key)
        if not sec:
            return default
        for f in sec.get('fields') or []:
            if f.get('key') == field_key:
                v = f.get('label')
                if v:
                    return v
                return default
        return default

    # ── ordering ──
    def iter_sections(self, default_keys):
        """
        Return section_keys in the user's saved order.  Saved keys not in
        `default_keys` are dropped; keys in `default_keys` not yet in the
        saved layout are appended in their default order.
        """
        default_set = set(default_keys)
        saved_order = [s['key'] for s in self.config.get('sections', []) if s['key'] in default_set]
        seen = set(saved_order)
        for k in default_keys:
            if k not in seen:
                saved_order.append(k)
                seen.add(k)
        return saved_order
