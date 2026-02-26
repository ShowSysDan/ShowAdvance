"""
DPC Advance Sheet App — Flask Backend
Run: python app.py  (after running init_db.py first)
"""
import os
import sqlite3
import json
import shutil
import logging
import logging.handlers
import atexit
import subprocess
import threading
from datetime import datetime, date
from functools import wraps
from io import BytesIO

from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, jsonify, make_response, abort)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dpc-advance-secret-change-in-production')

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'advance.db')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')

# Flask-Limiter for login rate limiting
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=[],
        storage_uri="memory://",
    )
    _limiter_available = True
except ImportError:
    limiter = None
    _limiter_available = False


def _get_upload_max():
    """Read upload_max_mb from app_settings, default 20."""
    if not os.path.exists(DATABASE):
        return 20 * 1024 * 1024
    try:
        db = get_db()
        row = db.execute("SELECT value FROM app_settings WHERE key='upload_max_mb'").fetchone()
        db.close()
        mb = int(row['value']) if row and row['value'] else 20
        return mb * 1024 * 1024
    except Exception:
        return 20 * 1024 * 1024

DEPARTMENTS = ['Production', 'Programming', 'Event Manager', 'Education Team',
               'Hospitality', 'Guest Services', 'Security', 'Runners']

# ─── Syslog ───────────────────────────────────────────────────────────────────

syslog_logger = logging.getLogger('showadvance')
syslog_logger.setLevel(logging.INFO)
syslog_logger.addHandler(logging.NullHandler())
_syslog_handler = None


def reload_syslog_handler():
    """Read syslog settings from DB and reconfigure the handler."""
    global _syslog_handler
    if not os.path.exists(DATABASE):
        return
    try:
        db = get_db()
        rows = db.execute(
            "SELECT key, value FROM app_settings WHERE key LIKE 'syslog_%'"
        ).fetchall()
        db.close()
    except Exception:
        return
    settings = {r['key']: r['value'] for r in rows}

    if _syslog_handler:
        syslog_logger.removeHandler(_syslog_handler)
        _syslog_handler.close()
        _syslog_handler = None

    if settings.get('syslog_enabled') != '1':
        return

    host = settings.get('syslog_host', '127.0.0.1')
    try:
        port = int(settings.get('syslog_port', 514))
    except ValueError:
        port = 514
    facility_name = settings.get('syslog_facility', 'LOG_LOCAL0')
    facility = getattr(logging.handlers.SysLogHandler, facility_name,
                       logging.handlers.SysLogHandler.LOG_LOCAL0)
    try:
        _syslog_handler = logging.handlers.SysLogHandler(
            address=(host, port), facility=facility
        )
        _syslog_handler.setFormatter(
            logging.Formatter('showadvance: %(levelname)s %(message)s')
        )
        syslog_logger.addHandler(_syslog_handler)
    except Exception as e:
        app.logger.error(f'Failed to configure syslog: {e}')


# ─── App Settings Helper ──────────────────────────────────────────────────────

def get_app_setting(key, default=''):
    """Fetch a single app_setting value from the database."""
    if not os.path.exists(DATABASE):
        return default
    try:
        db = get_db()
        row = db.execute('SELECT value FROM app_settings WHERE key=?', (key,)).fetchone()
        db.close()
        return row['value'] if row and row['value'] is not None else default
    except Exception:
        return default


# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")   # allows concurrent reads during writes
    return conn


# ─── Auth Decorators ──────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated


def content_admin_required(f):
    """Allow system admins AND users in an 'admin_group' type group."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') == 'admin':
            return f(*args, **kwargs)
        if session.get('is_content_admin'):
            return f(*args, **kwargs)
        abort(403)
    return decorated


def get_current_user():
    if 'user_id' in session:
        return {
            'id': session['user_id'],
            'username': session['username'],
            'display_name': session.get('display_name', session['username']),
            'role': session.get('user_role', 'user'),
            'theme': session.get('theme', 'dark'),
            'is_restricted': session.get('is_restricted', False),
            'is_content_admin': session.get('is_content_admin', False),
        }
    return None


# ─── Access Control Helpers ───────────────────────────────────────────────────

def _get_user_group_types(user_id):
    """Returns a list of group_type strings for the user's groups."""
    db = get_db()
    rows = db.execute("""
        SELECT ug.group_type FROM user_groups ug
        JOIN user_group_members ugm ON ug.id = ugm.group_id
        WHERE ugm.user_id = ?
    """, (user_id,)).fetchall()
    db.close()
    return [r['group_type'] for r in rows]


def is_content_admin(user_id):
    """True if the user is a system admin OR is in an 'admin_group' type group."""
    db = get_db()
    user = db.execute('SELECT role FROM users WHERE id=?', (user_id,)).fetchone()
    db.close()
    if not user:
        return False
    if user['role'] == 'admin':
        return True
    return 'admin_group' in _get_user_group_types(user_id)


def get_accessible_shows(user_id):
    """Returns None (all shows) or a list of accessible show IDs."""
    db = get_db()
    user = db.execute('SELECT role FROM users WHERE id=?', (user_id,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return None

    group_types = _get_user_group_types(user_id)

    # admin_group and all_access both get unrestricted show access
    if not group_types or any(t in ('all_access', 'admin_group') for t in group_types):
        return None

    rows = db.execute("""
        SELECT DISTINCT sga.show_id
        FROM show_group_access sga
        JOIN user_group_members ugm ON sga.group_id = ugm.group_id
        WHERE ugm.user_id = ?
    """, (user_id,)).fetchall()
    db.close()
    return [r['show_id'] for r in rows]


def can_access_show(user_id, show_id):
    accessible = get_accessible_shows(user_id)
    if accessible is None:
        return True
    return show_id in accessible


def is_restricted_user(user_id):
    """True if the user is ONLY in restricted groups (read-only, assigned shows only)."""
    db = get_db()
    user = db.execute('SELECT role FROM users WHERE id=?', (user_id,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return False
    group_types = _get_user_group_types(user_id)
    if not group_types:
        return False
    # Restricted only if ALL groups are 'restricted' (no all_access or admin_group)
    return all(t == 'restricted' for t in group_types)


# ─── Form Fields Helper ───────────────────────────────────────────────────────

def get_form_fields_for_template():
    """Returns ordered list of sections, each with a .fields list."""
    db = get_db()
    sections = db.execute(
        'SELECT * FROM form_sections ORDER BY sort_order'
    ).fetchall()
    fields = db.execute(
        'SELECT * FROM form_fields ORDER BY section_id, sort_order'
    ).fetchall()
    db.close()

    field_map = {}
    for f in fields:
        fd = dict(f)
        if fd['options_json']:
            try:
                fd['options'] = json.loads(fd['options_json'])
            except (json.JSONDecodeError, TypeError):
                fd['options'] = []
        else:
            fd['options'] = []
        field_map.setdefault(fd['section_id'], []).append(fd)

    result = []
    for s in sections:
        sd = dict(s)
        sd['fields'] = field_map.get(s['id'], [])
        result.append(sd)
    return result


def get_schedule_meta_fields():
    """Returns ordered list of schedule meta field templates."""
    db = get_db()
    rows = db.execute(
        'SELECT * FROM schedule_meta_fields ORDER BY sort_order, id'
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ─── Backup Scheduler ─────────────────────────────────────────────────────────

def _ensure_backup_dirs():
    os.makedirs(os.path.join(BACKUP_DIR, 'hourly'), exist_ok=True)
    os.makedirs(os.path.join(BACKUP_DIR, 'daily'), exist_ok=True)


def run_hourly_backup():
    _ensure_backup_dirs()
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    dest = os.path.join(BACKUP_DIR, 'hourly', f'advance_{ts}.db')
    shutil.copy2(DATABASE, dest)
    syslog_logger.info(f'BACKUP_CREATED type=hourly file={dest}')
    hourly_dir = os.path.join(BACKUP_DIR, 'hourly')
    files = sorted(
        [f for f in os.listdir(hourly_dir) if f.endswith('.db')],
        reverse=True
    )
    for old in files[24:]:
        os.remove(os.path.join(hourly_dir, old))


def run_daily_backup():
    _ensure_backup_dirs()
    ts = datetime.now().strftime('%Y%m%d')
    dest = os.path.join(BACKUP_DIR, 'daily', f'advance_{ts}.db')
    shutil.copy2(DATABASE, dest)
    syslog_logger.info(f'BACKUP_CREATED type=daily file={dest}')
    daily_dir = os.path.join(BACKUP_DIR, 'daily')
    files = sorted(
        [f for f in os.listdir(daily_dir) if f.endswith('.db')],
        reverse=True
    )
    for old in files[30:]:
        os.remove(os.path.join(daily_dir, old))


def start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(run_hourly_backup, 'interval', hours=1, id='hourly_backup')
        scheduler.add_job(run_daily_backup, 'cron', hour=0, minute=0, id='daily_backup')
        scheduler.start()
        return scheduler
    except ImportError:
        app.logger.warning('APScheduler not installed — backups disabled.')
        return None


# ─── General Helpers ──────────────────────────────────────────────────────────

def auto_archive_past_shows():
    """Move shows whose last performance date has passed into 'archived' status."""
    db = get_db()
    today = date.today().isoformat()
    db.execute("""
        UPDATE shows SET status = 'archived'
        WHERE status = 'active'
          AND (
            -- Has performances: archive only when ALL have passed
            (id IN (SELECT DISTINCT show_id FROM show_performances)
             AND id NOT IN (
               SELECT DISTINCT show_id FROM show_performances
               WHERE perf_date IS NULL OR perf_date >= ?
             ))
            OR
            -- No performances: use legacy show_date field
            (id NOT IN (SELECT DISTINCT show_id FROM show_performances)
             AND show_date IS NOT NULL
             AND show_date < ?)
          )
    """, (today, today))
    db.commit()
    db.close()


def _sync_show_primary_date(db, show_id):
    """Keep shows.show_date/show_time in sync with the earliest performance."""
    first = db.execute("""
        SELECT perf_date, perf_time FROM show_performances
        WHERE show_id = ?
        ORDER BY CASE WHEN perf_date IS NULL THEN 1 ELSE 0 END, perf_date, id
        LIMIT 1
    """, (show_id,)).fetchone()
    if first:
        db.execute("""
            UPDATE shows SET show_date=?, show_time=?, updated_at=CURRENT_TIMESTAMP WHERE id=?
        """, (first['perf_date'], first['perf_time'], show_id))
        for key, val in [('show_date', first['perf_date'] or ''),
                         ('show_time', first['perf_time'] or '')]:
            db.execute("""
                INSERT OR REPLACE INTO advance_data (show_id, field_key, field_value, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (show_id, key, val))
    else:
        db.execute("""
            UPDATE shows SET show_date=NULL, show_time='', updated_at=CURRENT_TIMESTAMP WHERE id=?
        """, (show_id,))


def get_show_or_404(show_id):
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id = ?', (show_id,)).fetchone()
    db.close()
    if not show:
        abort(404)
    return show


def get_contacts_by_dept():
    db = get_db()
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    db.close()
    by_dept = {}
    for c in contacts:
        dept = c['department'] or 'Other'
        by_dept.setdefault(dept, []).append(dict(c))
    return by_dept


def _snapshot_form_history(db, show_id, form_type, snapshot_data):
    """Insert a history snapshot and prune to 50 entries."""
    db.execute("""
        INSERT INTO form_history (show_id, form_type, saved_by, snapshot_json)
        VALUES (?, ?, ?, ?)
    """, (show_id, form_type, session.get('user_id'), json.dumps(snapshot_data)))
    db.execute("""
        DELETE FROM form_history
        WHERE show_id = ? AND form_type = ?
          AND id NOT IN (
            SELECT id FROM form_history
            WHERE show_id = ? AND form_type = ?
            ORDER BY saved_at DESC LIMIT 50
          )
    """, (show_id, form_type, show_id, form_type))


# ─── Auth Routes ──────────────────────────────────────────────────────────────

def _login_route():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        if user and check_password_hash(user['password_hash'], password):
            # Update last_login timestamp
            try:
                db.execute('UPDATE users SET last_login=CURRENT_TIMESTAMP WHERE id=?', (user['id'],))
                db.commit()
            except Exception:
                pass
            db.close()
            session['user_id']        = user['id']
            session['username']       = user['username']
            session['display_name']   = user['display_name'] or user['username']
            session['user_role']      = user['role']
            session['theme']          = user['theme'] or 'dark'
            session['is_restricted']  = is_restricted_user(user['id'])
            session['is_content_admin'] = is_content_admin(user['id'])
            syslog_logger.info(f"LOGIN user={username} ip={request.remote_addr}")
            next_url = request.form.get('next') or url_for('dashboard')
            return redirect(next_url)
        db.close()
        flash('Invalid username or password.', 'error')

    return render_template('login.html', next=request.args.get('next', ''))


if _limiter_available and limiter:
    @app.route('/login', methods=['GET', 'POST'])
    @limiter.limit("15 per minute", methods=["POST"])
    def login():
        return _login_route()
else:
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        return _login_route()


@app.route('/logout')
def logout():
    syslog_logger.info(f"LOGOUT user={session.get('username')}")
    session.clear()
    return redirect(url_for('login'))


# ─── Dashboard ────────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return redirect(url_for('dashboard'))


@app.route('/dashboard')
@login_required
def dashboard():
    auto_archive_past_shows()
    accessible = get_accessible_shows(session['user_id'])
    db = get_db()

    if accessible is None:
        active = db.execute("""
            SELECT s.*, u.display_name as creator,
              (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count
            FROM shows s LEFT JOIN users u ON s.created_by = u.id
            WHERE s.status = 'active'
            ORDER BY s.show_date ASC NULLS LAST
        """).fetchall()
        archived = db.execute("""
            SELECT s.*, u.display_name as creator,
              (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count
            FROM shows s LEFT JOIN users u ON s.created_by = u.id
            WHERE s.status = 'archived'
            ORDER BY s.show_date DESC
            LIMIT 30
        """).fetchall()
    else:
        if accessible:
            placeholders = ','.join('?' * len(accessible))
            active = db.execute(f"""
                SELECT s.*, u.display_name as creator,
                  (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count
                FROM shows s LEFT JOIN users u ON s.created_by = u.id
                WHERE s.status = 'active' AND s.id IN ({placeholders})
                ORDER BY s.show_date ASC NULLS LAST
            """, accessible).fetchall()
            archived = db.execute(f"""
                SELECT s.*, u.display_name as creator,
                  (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count
                FROM shows s LEFT JOIN users u ON s.created_by = u.id
                WHERE s.status = 'archived' AND s.id IN ({placeholders})
                ORDER BY s.show_date DESC LIMIT 30
            """, accessible).fetchall()
        else:
            active = []
            archived = []

    db.close()
    restricted = session.get('is_restricted', False)

    # Group active shows by venue for column layout
    _venue_map = {}
    for s in active:
        v = (s['venue'] or '').strip() or 'Unassigned'
        _venue_map.setdefault(v, []).append(s)
    _names = sorted([v for v in _venue_map if v != 'Unassigned'], key=str.lower)
    if 'Unassigned' in _venue_map:
        _names.append('Unassigned')
    for v in _names:
        _venue_map[v].sort(key=lambda s: (s['show_date'] is None, s['show_date'] or ''))
    venue_groups = [(v, _venue_map[v]) for v in _names]

    return render_template('dashboard.html',
                           active_shows=active,
                           archived_shows=archived,
                           venue_groups=venue_groups,
                           restricted=restricted,
                           user=get_current_user())


# ─── New Show ─────────────────────────────────────────────────────────────────

@app.route('/shows/new', methods=['GET', 'POST'])
@login_required
def new_show():
    if session.get('is_restricted'):
        abort(403)

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if not name:
            flash('Show name is required.', 'error')
            return render_template('new_show.html', user=get_current_user())

        show_date = request.form.get('show_date') or None
        show_time = request.form.get('show_time', '')
        venue     = request.form.get('venue', "Judson's Live")

        db = get_db()
        cur = db.execute("""
            INSERT INTO shows (name, show_date, show_time, venue, created_by)
            VALUES (?, ?, ?, ?, ?)
        """, (name, show_date, show_time, venue, session['user_id']))
        show_id = cur.lastrowid

        for key, val in [('show_name', name), ('show_date', show_date or ''),
                         ('show_time', show_time), ('venue', venue)]:
            if val:
                db.execute("""
                    INSERT OR REPLACE INTO advance_data (show_id, field_key, field_value)
                    VALUES (?, ?, ?)
                """, (show_id, key, val))

        if show_date:
            db.execute("""
                INSERT INTO show_performances (show_id, perf_date, perf_time, sort_order)
                VALUES (?, ?, ?, 0)
            """, (show_id, show_date, show_time))

        db.commit()
        db.close()
        syslog_logger.info(f"SHOW_CREATE show_id={show_id} name={name} by={session.get('username')}")
        return redirect(url_for('show_page', show_id=show_id, tab='advance'))

    return render_template('new_show.html', user=get_current_user())


# ─── Show Page (all tabs) ─────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>')
@login_required
def show_page(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)

    tab = request.args.get('tab', 'advance')
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id = ?', (show_id,)).fetchone()
    if not show:
        abort(404)

    # Last-saved-by info
    last_saved_display_name = None
    last_saved_at = None
    try:
        _last_saved_by_id = show['last_saved_by']
    except (IndexError, KeyError):
        _last_saved_by_id = None
    if _last_saved_by_id:
        saver = db.execute(
            'SELECT display_name, username FROM users WHERE id=?',
            (_last_saved_by_id,)
        ).fetchone()
        if saver:
            last_saved_display_name = saver['display_name'] or saver['username']
        try:
            last_saved_at = show['last_saved_at']
        except (IndexError, KeyError):
            last_saved_at = None

    # Advance data
    adv_rows = db.execute(
        'SELECT field_key, field_value FROM advance_data WHERE show_id = ?', (show_id,)
    ).fetchall()
    advance_data = {r['field_key']: r['field_value'] for r in adv_rows}

    # Production schedule
    sched_rows = db.execute("""
        SELECT * FROM schedule_rows WHERE show_id = ?
        ORDER BY sort_order, id
    """, (show_id,)).fetchall()
    meta_rows = db.execute(
        'SELECT field_key, field_value FROM schedule_meta WHERE show_id = ?', (show_id,)
    ).fetchall()
    schedule_meta = {r['field_key']: r['field_value'] for r in meta_rows}

    # Post-show notes
    note_rows = db.execute(
        'SELECT field_key, field_value FROM post_show_notes WHERE show_id = ?', (show_id,)
    ).fetchall()
    notes_data = {r['field_key']: r['field_value'] for r in note_rows}

    # Performances
    performances = db.execute("""
        SELECT * FROM show_performances WHERE show_id = ?
        ORDER BY CASE WHEN perf_date IS NULL THEN 1 ELSE 0 END, perf_date, id
    """, (show_id,)).fetchall()

    # Export log
    exports = db.execute("""
        SELECT e.*, u.display_name as exporter
        FROM export_log e LEFT JOIN users u ON e.exported_by = u.id
        WHERE e.show_id = ?
        ORDER BY e.exported_at DESC
        LIMIT 10
    """, (show_id,)).fetchall()

    # Contacts
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    contacts_by_dept = {}
    for c in contacts:
        dept = c['department'] or 'Other'
        contacts_by_dept.setdefault(dept, []).append(dict(c))

    # All users — for @mention autocomplete in comments
    all_users_rows = db.execute(
        'SELECT id, username, display_name FROM users ORDER BY display_name'
    ).fetchall()
    all_users = [{'id': u['id'], 'username': u['username'],
                  'display_name': u['display_name'] or u['username']} for u in all_users_rows]

    db.close()

    form_sections = get_form_fields_for_template()
    restricted = session.get('is_restricted', False)

    try:
        _vl = get_app_setting('venue_list', '[]')
        venue_list = json.loads(_vl) if _vl else []
    except Exception:
        venue_list = []
    try:
        _rl = get_app_setting('radio_channel_list', '[]')
        radio_channel_list = json.loads(_rl) if _rl else []
    except Exception:
        radio_channel_list = []

    return render_template('show.html',
                           show=show,
                           tab=tab,
                           advance_data=advance_data,
                           performances=[dict(p) for p in performances],
                           schedule_rows=[dict(r) for r in sched_rows],
                           schedule_meta=schedule_meta,
                           sched_meta_fields=get_schedule_meta_fields(),
                           notes_data=notes_data,
                           exports=exports,
                           contacts_by_dept=contacts_by_dept,
                           departments=DEPARTMENTS,
                           form_sections=form_sections,
                           last_saved_display_name=last_saved_display_name,
                           last_saved_at=last_saved_at,
                           restricted=restricted,
                           all_users=all_users,
                           venue_list=venue_list,
                           radio_channel_list=radio_channel_list,
                           user=get_current_user())


# ─── Save Endpoints (AJAX) ────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/save/advance', methods=['POST'])
@login_required
def save_advance(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403

    get_show_or_404(show_id)
    data = request.get_json(force=True) or {}
    db = get_db()
    for key, value in data.items():
        db.execute("""
            INSERT OR REPLACE INTO advance_data (show_id, field_key, field_value, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, (show_id, key, str(value) if value is not None else ''))

    # Sync core show fields
    if 'show_name' in data and data['show_name']:
        db.execute('UPDATE shows SET name=?, updated_at=CURRENT_TIMESTAMP WHERE id=?',
                   (data['show_name'], show_id))
    if 'show_date' in data:
        db.execute('UPDATE shows SET show_date=?, updated_at=CURRENT_TIMESTAMP WHERE id=?',
                   (data['show_date'] or None, show_id))
    if 'show_time' in data:
        db.execute('UPDATE shows SET show_time=?, updated_at=CURRENT_TIMESTAMP WHERE id=?',
                   (data['show_time'], show_id))
    if 'venue' in data:
        db.execute('UPDATE shows SET venue=?, updated_at=CURRENT_TIMESTAMP WHERE id=?',
                   (data['venue'], show_id))

    # Track last saved
    db.execute("""
        UPDATE shows SET last_saved_by=?, last_saved_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session['user_id'], show_id))

    # Version snapshot
    _snapshot_form_history(db, show_id, 'advance', {'advance_data': data})

    db.commit()
    db.close()
    syslog_logger.info(f"FORM_SAVE show_id={show_id} type=advance by={session.get('username')}")
    return jsonify({'success': True})


# ─── Performances (multiple dates/times per show) ─────────────────────────────

@app.route('/shows/<int:show_id>/performances', methods=['POST'])
@login_required
def add_performance(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    get_show_or_404(show_id)
    data = request.get_json(force=True) or {}
    perf_date = data.get('perf_date') or None
    perf_time = data.get('perf_time', '')
    db = get_db()
    cur = db.execute("""
        INSERT INTO show_performances (show_id, perf_date, perf_time, sort_order)
        VALUES (?, ?, ?,
          (SELECT COALESCE(MAX(sort_order)+1, 0) FROM show_performances WHERE show_id=?))
    """, (show_id, perf_date, perf_time, show_id))
    perf_id = cur.lastrowid
    _sync_show_primary_date(db, show_id)
    db.commit()
    perf = db.execute('SELECT * FROM show_performances WHERE id=?', (perf_id,)).fetchone()
    db.close()
    return jsonify({'success': True, 'performance': dict(perf)})


@app.route('/shows/<int:show_id>/performances/<int:perf_id>', methods=['PUT'])
@login_required
def update_performance(show_id, perf_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    db = get_db()
    perf = db.execute(
        'SELECT * FROM show_performances WHERE id=? AND show_id=?', (perf_id, show_id)
    ).fetchone()
    if not perf:
        db.close()
        return jsonify({'success': False, 'error': 'Not found.'}), 404
    data = request.get_json(force=True) or {}
    db.execute("""
        UPDATE show_performances SET perf_date=?, perf_time=? WHERE id=?
    """, (data.get('perf_date') or None, data.get('perf_time', ''), perf_id))
    _sync_show_primary_date(db, show_id)
    db.commit()
    perf = db.execute('SELECT * FROM show_performances WHERE id=?', (perf_id,)).fetchone()
    db.close()
    return jsonify({'success': True, 'performance': dict(perf)})


@app.route('/shows/<int:show_id>/performances/<int:perf_id>', methods=['DELETE'])
@login_required
def delete_performance(show_id, perf_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    db = get_db()
    perf = db.execute(
        'SELECT * FROM show_performances WHERE id=? AND show_id=?', (perf_id, show_id)
    ).fetchone()
    if not perf:
        db.close()
        return jsonify({'success': False, 'error': 'Not found.'}), 404
    db.execute('DELETE FROM show_performances WHERE id=?', (perf_id,))
    _sync_show_primary_date(db, show_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/save/schedule', methods=['POST'])
@login_required
def save_schedule(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403

    get_show_or_404(show_id)
    data = request.get_json(force=True) or {}
    db = get_db()
    if 'meta' in data:
        for key, val in data['meta'].items():
            db.execute("""
                INSERT OR REPLACE INTO schedule_meta (show_id, field_key, field_value)
                VALUES (?, ?, ?)
            """, (show_id, key, val or ''))
    if 'rows' in data:
        db.execute('DELETE FROM schedule_rows WHERE show_id = ?', (show_id,))
        for i, row in enumerate(data['rows']):
            db.execute("""
                INSERT INTO schedule_rows (show_id, sort_order, start_time, end_time, description, notes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (show_id, i,
                  row.get('start_time', ''), row.get('end_time', ''),
                  row.get('description', ''), row.get('notes', '')))

    db.execute('UPDATE shows SET updated_at=CURRENT_TIMESTAMP WHERE id=?', (show_id,))
    db.execute("""
        UPDATE shows SET last_saved_by=?, last_saved_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session['user_id'], show_id))

    _snapshot_form_history(db, show_id, 'schedule', data)

    db.commit()
    db.close()
    syslog_logger.info(f"FORM_SAVE show_id={show_id} type=schedule by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/save/postnotes', methods=['POST'])
@login_required
def save_postnotes(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403

    get_show_or_404(show_id)
    data = request.get_json(force=True) or {}
    db = get_db()
    for key, val in data.items():
        db.execute("""
            INSERT OR REPLACE INTO post_show_notes (show_id, field_key, field_value)
            VALUES (?, ?, ?)
        """, (show_id, key, val or ''))
    db.execute('UPDATE shows SET updated_at=CURRENT_TIMESTAMP WHERE id=?', (show_id,))
    db.execute("""
        UPDATE shows SET last_saved_by=?, last_saved_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session['user_id'], show_id))

    _snapshot_form_history(db, show_id, 'postnotes', {'notes_data': data})

    db.commit()
    db.close()
    syslog_logger.info(f"FORM_SAVE show_id={show_id} type=postnotes by={session.get('username')}")
    return jsonify({'success': True})


# ─── Version History ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/history/<form_type>')
@login_required
def form_history_list(show_id, form_type):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    entries = db.execute("""
        SELECT fh.id, fh.saved_at, fh.form_type,
               u.display_name as saved_by_name, u.username as saved_by_username
        FROM form_history fh
        LEFT JOIN users u ON fh.saved_by = u.id
        WHERE fh.show_id = ? AND fh.form_type = ?
        ORDER BY fh.saved_at DESC
        LIMIT 50
    """, (show_id, form_type)).fetchall()
    db.close()
    return jsonify([{
        'id': e['id'],
        'saved_at': e['saved_at'],
        'form_type': e['form_type'],
        'saved_by_name': e['saved_by_name'] or e['saved_by_username'] or 'Unknown',
    } for e in entries])


@app.route('/shows/<int:show_id>/history/<int:hist_id>/snapshot')
@login_required
def history_snapshot(show_id, hist_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    entry = db.execute(
        'SELECT * FROM form_history WHERE id=? AND show_id=?', (hist_id, show_id)
    ).fetchone()
    db.close()
    if not entry:
        abort(404)
    return jsonify({'id': entry['id'], 'form_type': entry['form_type'],
                    'saved_at': entry['saved_at'],
                    'data': json.loads(entry['snapshot_json'])})


@app.route('/shows/<int:show_id>/history/<int:hist_id>/restore', methods=['POST'])
@login_required
def restore_history(show_id, hist_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403

    db = get_db()
    entry = db.execute(
        'SELECT * FROM form_history WHERE id=? AND show_id=?', (hist_id, show_id)
    ).fetchone()
    if not entry:
        db.close()
        abort(404)

    snapshot = json.loads(entry['snapshot_json'])
    form_type = entry['form_type']

    if form_type == 'advance':
        adv = snapshot.get('advance_data', {})
        for key, val in adv.items():
            db.execute("""
                INSERT OR REPLACE INTO advance_data (show_id, field_key, field_value, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (show_id, key, str(val) if val is not None else ''))

    elif form_type == 'schedule':
        if 'meta' in snapshot:
            for key, val in snapshot['meta'].items():
                db.execute("""
                    INSERT OR REPLACE INTO schedule_meta (show_id, field_key, field_value)
                    VALUES (?, ?, ?)
                """, (show_id, key, val or ''))
        if 'rows' in snapshot:
            db.execute('DELETE FROM schedule_rows WHERE show_id=?', (show_id,))
            for i, row in enumerate(snapshot['rows']):
                db.execute("""
                    INSERT INTO schedule_rows (show_id, sort_order, start_time, end_time, description, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (show_id, i, row.get('start_time',''), row.get('end_time',''),
                      row.get('description',''), row.get('notes','')))

    elif form_type == 'postnotes':
        notes = snapshot.get('notes_data', {})
        for key, val in notes.items():
            db.execute("""
                INSERT OR REPLACE INTO post_show_notes (show_id, field_key, field_value)
                VALUES (?, ?, ?)
            """, (show_id, key, val or ''))

    db.execute("""
        UPDATE shows SET last_saved_by=?, last_saved_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session['user_id'], show_id))
    db.commit()
    db.close()
    syslog_logger.info(
        f"HISTORY_RESTORE show_id={show_id} hist_id={hist_id} type={form_type} by={session.get('username')}"
    )
    return jsonify({'success': True})


# ─── Comments ─────────────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/comments', methods=['GET'])
@login_required
def get_comments(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    rows = db.execute("""
        SELECT sc.id, sc.body, sc.created_at,
               u.display_name, u.username, u.id as uid
        FROM show_comments sc
        JOIN users u ON sc.user_id = u.id
        WHERE sc.show_id = ?
        ORDER BY sc.created_at ASC
    """, (show_id,)).fetchall()
    db.close()
    return jsonify([{
        'id':        r['id'],
        'body':      r['body'],
        'created_at': r['created_at'],
        'author':    r['display_name'] or r['username'],
        'author_id': r['uid'],
        'initials':  ''.join(w[0].upper() for w in (r['display_name'] or r['username']).split()[:2]),
        'is_own':    r['uid'] == session['user_id'],
    } for r in rows])


@app.route('/shows/<int:show_id>/comments', methods=['POST'])
@login_required
def post_comment(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    data = request.get_json(force=True) or {}
    body = data.get('body', '').strip()
    if not body:
        return jsonify({'success': False, 'error': 'Comment cannot be empty.'}), 400
    if len(body) > 2000:
        return jsonify({'success': False, 'error': 'Comment too long (max 2000 chars).'}), 400
    db = get_db()
    cur = db.execute(
        'INSERT INTO show_comments (show_id, user_id, body) VALUES (?, ?, ?)',
        (show_id, session['user_id'], body)
    )
    cid = cur.lastrowid
    db.commit()
    row = db.execute("""
        SELECT sc.id, sc.body, sc.created_at,
               u.display_name, u.username, u.id as uid
        FROM show_comments sc JOIN users u ON sc.user_id = u.id
        WHERE sc.id = ?
    """, (cid,)).fetchone()
    db.close()
    syslog_logger.info(f"COMMENT_POST show_id={show_id} by={session.get('username')}")
    return jsonify({
        'success': True,
        'comment': {
            'id':        row['id'],
            'body':      row['body'],
            'created_at': row['created_at'],
            'author':    row['display_name'] or row['username'],
            'author_id': row['uid'],
            'initials':  ''.join(w[0].upper() for w in (row['display_name'] or row['username']).split()[:2]),
            'is_own':    True,
        }
    })


@app.route('/shows/<int:show_id>/comments/<int:cid>/delete', methods=['POST'])
@login_required
def delete_comment(show_id, cid):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    comment = db.execute(
        'SELECT * FROM show_comments WHERE id=? AND show_id=?', (cid, show_id)
    ).fetchone()
    if not comment:
        db.close()
        abort(404)
    if comment['user_id'] != session['user_id'] and session.get('user_role') != 'admin':
        db.close()
        abort(403)
    db.execute('DELETE FROM show_comments WHERE id=?', (cid,))
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── File Attachments ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/attachments', methods=['GET'])
@login_required
def get_attachments(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    rows = db.execute("""
        SELECT sa.id, sa.filename, sa.mime_type, sa.file_size, sa.created_at,
               u.display_name, u.username
        FROM show_attachments sa
        LEFT JOIN users u ON sa.uploaded_by = u.id
        WHERE sa.show_id = ?
        ORDER BY sa.created_at ASC
    """, (show_id,)).fetchall()
    db.close()
    return jsonify([{
        'id':         r['id'],
        'filename':   r['filename'],
        'mime_type':  r['mime_type'],
        'file_size':  r['file_size'],
        'created_at': r['created_at'],
        'uploader':   r['display_name'] or r['username'] or 'Unknown',
    } for r in rows])


@app.route('/shows/<int:show_id>/attachments', methods=['POST'])
@login_required
def upload_attachment(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'success': False, 'error': 'No file provided.'}), 400
    data = f.read()
    max_size = _get_upload_max()
    max_mb = max_size // (1024 * 1024)
    if len(data) > max_size:
        return jsonify({'success': False, 'error': f'File too large (max {max_mb} MB).'}), 413
    filename  = secure_filename(f.filename) or 'file'
    mime_type = f.content_type or 'application/octet-stream'
    db = get_db()
    cur = db.execute("""
        INSERT INTO show_attachments (show_id, uploaded_by, filename, mime_type, file_data, file_size)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (show_id, session['user_id'], filename, mime_type, data, len(data)))
    aid = cur.lastrowid
    db.commit()
    row = db.execute("""
        SELECT sa.id, sa.filename, sa.mime_type, sa.file_size, sa.created_at,
               u.display_name, u.username
        FROM show_attachments sa LEFT JOIN users u ON sa.uploaded_by = u.id
        WHERE sa.id = ?
    """, (aid,)).fetchone()
    db.close()
    syslog_logger.info(f"FILE_UPLOAD show_id={show_id} filename={filename} by={session.get('username')}")
    return jsonify({
        'success': True,
        'attachment': {
            'id':         row['id'],
            'filename':   row['filename'],
            'mime_type':  row['mime_type'],
            'file_size':  row['file_size'],
            'created_at': row['created_at'],
            'uploader':   row['display_name'] or row['username'] or 'Unknown',
        }
    })


@app.route('/shows/<int:show_id>/attachments/<int:aid>/download')
@login_required
def download_attachment(show_id, aid):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    row = db.execute(
        'SELECT * FROM show_attachments WHERE id=? AND show_id=?', (aid, show_id)
    ).fetchone()
    db.close()
    if not row:
        abort(404)
    resp = make_response(bytes(row['file_data']))
    resp.headers['Content-Type'] = row['mime_type']
    resp.headers['Content-Disposition'] = f'attachment; filename="{row["filename"]}"'
    return resp


@app.route('/shows/<int:show_id>/attachments/<int:aid>/delete', methods=['POST'])
@login_required
def delete_attachment(show_id, aid):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    row = db.execute(
        'SELECT * FROM show_attachments WHERE id=? AND show_id=?', (aid, show_id)
    ).fetchone()
    if not row:
        db.close()
        abort(404)
    if row['uploaded_by'] != session['user_id'] and session.get('user_role') != 'admin':
        db.close()
        abort(403)
    db.execute('DELETE FROM show_attachments WHERE id=?', (aid,))
    db.commit()
    db.close()
    syslog_logger.info(f"FILE_DELETE show_id={show_id} aid={aid} by={session.get('username')}")
    return jsonify({'success': True})


# ─── Read Receipts ─────────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/read', methods=['POST'])
@login_required
def mark_advance_read(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    show = db.execute('SELECT advance_version FROM shows WHERE id=?', (show_id,)).fetchone()
    if not show:
        db.close()
        abort(404)
    version = show['advance_version'] or 0
    db.execute("""
        INSERT INTO advance_reads (show_id, user_id, version_read, read_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(show_id, user_id) DO UPDATE SET
            version_read = excluded.version_read,
            read_at      = excluded.read_at
    """, (show_id, session['user_id'], version))
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/reads')
@login_required
def get_advance_reads(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    rows = db.execute("""
        SELECT ar.version_read, ar.read_at,
               u.display_name, u.username, u.id as uid
        FROM advance_reads ar
        JOIN users u ON ar.user_id = u.id
        WHERE ar.show_id = ?
        ORDER BY ar.read_at DESC
    """, (show_id,)).fetchall()
    db.close()
    return jsonify([{
        'version_read':     r['version_read'],
        'read_at':          r['read_at'],
        'author':           r['display_name'] or r['username'],
        'initials':         ''.join(w[0].upper() for w in (r['display_name'] or r['username']).split()[:2]),
        'is_current_user':  r['uid'] == session['user_id'],
    } for r in rows])


# ─── Real-time Sync ───────────────────────────────────────────────────────────

def _upsert_active_session(db, user_id, show_id, tab, focused_field=None):
    """Record that a user is actively on a show page and prune stale sessions."""
    db.execute("""
        INSERT INTO active_sessions (user_id, show_id, tab, focused_field, last_seen)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, show_id) DO UPDATE SET
            tab=excluded.tab,
            focused_field=excluded.focused_field,
            last_seen=excluded.last_seen
    """, (user_id, show_id, tab, focused_field or None))
    # Prune sessions idle > 60 s
    db.execute("DELETE FROM active_sessions WHERE last_seen < datetime('now', '-60 seconds')")


def _get_other_active_users(db, user_id, show_id):
    """Return list of other users active on this show in the last 45 s."""
    rows = db.execute("""
        SELECT u.display_name, u.username, acs.tab, acs.focused_field
        FROM active_sessions acs
        JOIN users u ON acs.user_id = u.id
        WHERE acs.show_id = ?
          AND acs.user_id != ?
          AND acs.last_seen > datetime('now', '-45 seconds')
        ORDER BY acs.last_seen DESC
    """, (show_id, user_id)).fetchall()
    return [{
        'name':          r['display_name'] or r['username'],
        'initials':      ''.join(w[0].upper() for w in (r['display_name'] or r['username']).split()[:2]),
        'tab':           r['tab'],
        'focused_field': r['focused_field'],
    } for r in rows]


@app.route('/shows/<int:show_id>/sync/advance')
@login_required
def sync_advance(show_id):
    """
    Lightweight poll endpoint for real-time field sync.
    Query param:  since=<YYYY-MM-DD HH:MM:SS>  — last sync timestamp
                  tab=<advance|schedule|postnotes>  — caller's current tab
    Returns changed advance_data fields + active-user presence list.
    """
    if not can_access_show(session['user_id'], show_id):
        abort(403)

    since         = request.args.get('since', '')
    tab           = request.args.get('tab', 'advance')
    focused_field = request.args.get('field') or None

    db = get_db()

    # Fields changed since last poll (exclude the current user's own saves so
    # we don't echo back what they just wrote)
    if since:
        changed_rows = db.execute("""
            SELECT ad.field_key, ad.field_value
            FROM advance_data ad
            WHERE ad.show_id = ?
              AND ad.updated_at > ?
              AND (
                SELECT last_saved_by FROM shows WHERE id = ad.show_id
              ) != ?
        """, (show_id, since, session['user_id'])).fetchall()
    else:
        changed_rows = []

    # New "since" cursor = latest updated_at across the whole show's advance data
    ts_row = db.execute(
        "SELECT MAX(updated_at) FROM advance_data WHERE show_id = ?", (show_id,)
    ).fetchone()
    new_since = ts_row[0] if ts_row and ts_row[0] else since

    # Update presence (including which field is focused) and get other active users
    _upsert_active_session(db, session['user_id'], show_id, tab, focused_field)
    others = _get_other_active_users(db, session['user_id'], show_id)

    db.commit()
    db.close()

    return jsonify({
        'since':        new_since,
        'fields':       {r['field_key']: r['field_value'] for r in changed_rows},
        'active_users': others,
    })


@app.route('/shows/<int:show_id>/heartbeat', methods=['POST'])
@login_required
def show_heartbeat(show_id):
    """
    Thin presence-only update for schedule/postnotes tabs where the advance
    sync poll isn't running. Also detects when another user saved non-advance
    data so the client can show a "someone else saved" notice.
    """
    if not can_access_show(session['user_id'], show_id):
        abort(403)

    data          = request.get_json(force=True) or {}
    tab           = data.get('tab', 'advance')
    focused_field = data.get('focused_field') or None

    db = get_db()
    _upsert_active_session(db, session['user_id'], show_id, tab, focused_field)
    others = _get_other_active_users(db, session['user_id'], show_id)

    # For schedule / postnotes: tell the client if someone else saved recently
    # so it can show a "reload?" banner without fetching the full dataset.
    show = db.execute('SELECT last_saved_by, last_saved_at FROM shows WHERE id=?',
                      (show_id,)).fetchone()
    other_saved = (show and show['last_saved_by'] and
                   show['last_saved_by'] != session['user_id'])

    db.commit()
    db.close()

    return jsonify({
        'active_users': others,
        'other_saved':  other_saved,
        'last_saved_at': show['last_saved_at'] if show else None,
    })


# ─── PDF Export ───────────────────────────────────────────────────────────────

def _build_advance_pdf(show_id):
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id = ?', (show_id,)).fetchone()
    adv_rows = db.execute(
        'SELECT field_key, field_value FROM advance_data WHERE show_id = ?', (show_id,)
    ).fetchall()
    advance_data = {r['field_key']: r['field_value'] for r in adv_rows}
    contacts = db.execute('SELECT * FROM contacts ORDER BY name').fetchall()
    contact_map = {c['id']: dict(c) for c in contacts}

    logo_data = get_app_setting('logo_data', '')

    new_v = (show['advance_version'] or 0) + 1
    db.execute('UPDATE shows SET advance_version=? WHERE id=?', (new_v, show_id))
    log_cur = db.execute("""INSERT INTO export_log (show_id, export_type, version, exported_by)
                  VALUES (?, 'advance', ?, ?)""", (show_id, new_v, session['user_id']))
    log_id = log_cur.lastrowid
    db.commit()
    db.close()

    # Fetch form sections; if fetch fails, PDF falls back to generic rendering
    try:
        form_sections = get_form_fields_for_template()
    except Exception as e:
        app.logger.warning(f'Could not load form_sections for PDF: {e}')
        form_sections = []

    try:
        html = render_template('pdf/advance_pdf.html',
                               show=show, advance_data=advance_data,
                               contact_map=contact_map,
                               form_sections=form_sections,
                               logo_data=logo_data,
                               version=new_v,
                               export_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'))
    except Exception as e:
        app.logger.error(f'advance_pdf template error for show {show_id}: {e}')
        html = render_template('pdf/advance_pdf.html',
                               show=show, advance_data=advance_data,
                               contact_map=contact_map,
                               form_sections=[],
                               logo_data=logo_data,
                               version=new_v,
                               export_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'))

    # Store PDF bytes in export_log for re-download
    try:
        from weasyprint import HTML as WP_HTML
        pdf_bytes = WP_HTML(string=html, base_url=request.url_root).write_pdf()
        db2 = get_db()
        db2.execute('UPDATE export_log SET pdf_data=? WHERE id=?', (pdf_bytes, log_id))
        db2.commit()
        db2.close()
    except Exception:
        pdf_bytes = None

    syslog_logger.info(
        f"PDF_EXPORT show_id={show_id} type=advance v={new_v} by={session.get('username')}"
    )
    return html, new_v, dict(show), pdf_bytes


def _build_schedule_pdf(show_id):
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id = ?', (show_id,)).fetchone()
    sched_rows = db.execute(
        'SELECT * FROM schedule_rows WHERE show_id=? ORDER BY sort_order,id', (show_id,)
    ).fetchall()
    meta_rows = db.execute(
        'SELECT field_key, field_value FROM schedule_meta WHERE show_id=?', (show_id,)
    ).fetchall()
    schedule_meta = {r['field_key']: r['field_value'] for r in meta_rows}
    adv_rows = db.execute(
        'SELECT field_key, field_value FROM advance_data WHERE show_id=?', (show_id,)
    ).fetchall()
    advance_data = {r['field_key']: r['field_value'] for r in adv_rows}
    contacts = db.execute('SELECT * FROM contacts ORDER BY name').fetchall()
    contact_map = {c['id']: dict(c) for c in contacts}

    logo_data = get_app_setting('logo_data', '')

    # WiFi QR Code generation
    wifi_qr_b64 = None
    wifi_ssid = schedule_meta.get('wifi_network') or get_app_setting('wifi_network', '')
    wifi_pass = schedule_meta.get('wifi_code') or get_app_setting('wifi_password', '')
    if wifi_ssid:
        try:
            import qrcode, io, base64
            qr = qrcode.make(f"WIFI:S:{wifi_ssid};T:WPA;P:{wifi_pass};;")
            buf = io.BytesIO()
            qr.save(buf, format='PNG')
            wifi_qr_b64 = base64.b64encode(buf.getvalue()).decode()
        except Exception:
            pass

    new_v = (show['schedule_version'] or 0) + 1
    db.execute('UPDATE shows SET schedule_version=? WHERE id=?', (new_v, show_id))
    log_cur = db.execute("""INSERT INTO export_log (show_id, export_type, version, exported_by)
                  VALUES (?, 'schedule', ?, ?)""", (show_id, new_v, session['user_id']))
    log_id = log_cur.lastrowid
    db.commit()
    db.close()

    html = render_template('pdf/schedule_pdf.html',
                           show=show, schedule_rows=sched_rows,
                           schedule_meta=schedule_meta,
                           sched_meta_fields=get_schedule_meta_fields(),
                           advance_data=advance_data,
                           contact_map=contact_map,
                           logo_data=logo_data,
                           wifi_qr_b64=wifi_qr_b64,
                           version=new_v,
                           export_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'))

    # Store PDF bytes in export_log for re-download
    try:
        from weasyprint import HTML as WP_HTML
        pdf_bytes = WP_HTML(string=html, base_url=request.url_root).write_pdf()
        db2 = get_db()
        db2.execute('UPDATE export_log SET pdf_data=? WHERE id=?', (pdf_bytes, log_id))
        db2.commit()
        db2.close()
    except Exception:
        pdf_bytes = None

    syslog_logger.info(
        f"PDF_EXPORT show_id={show_id} type=schedule v={new_v} by={session.get('username')}"
    )
    return html, new_v, dict(show), pdf_bytes


@app.route('/shows/<int:show_id>/export/advance')
@login_required
def export_advance(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    get_show_or_404(show_id)
    html, version, show, pdf_bytes = _build_advance_pdf(show_id)
    safe_name = show['name'].replace(' ', '_').replace('/', '-')
    filename  = f"Advance_{safe_name}_{show.get('show_date','nodate')}_v{version}.pdf"
    if pdf_bytes:
        resp = make_response(pdf_bytes)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return resp
    # Fallback to HTML if weasyprint failed
    try:
        from weasyprint import HTML
        pdf = HTML(string=html, base_url=request.url_root).write_pdf()
        resp = make_response(pdf)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return resp
    except Exception:
        resp = make_response(html)
        resp.headers['Content-Type'] = 'text/html'
        return resp


@app.route('/shows/<int:show_id>/export/schedule')
@login_required
def export_schedule(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    get_show_or_404(show_id)
    html, version, show, pdf_bytes = _build_schedule_pdf(show_id)
    safe_name = show['name'].replace(' ', '_').replace('/', '-')
    filename  = f"Schedule_{safe_name}_{show.get('show_date','nodate')}_v{version}.pdf"
    if pdf_bytes:
        resp = make_response(pdf_bytes)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return resp
    try:
        from weasyprint import HTML
        pdf = HTML(string=html, base_url=request.url_root).write_pdf()
        resp = make_response(pdf)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return resp
    except Exception:
        resp = make_response(html)
        resp.headers['Content-Type'] = 'text/html'
        return resp


@app.route('/shows/<int:show_id>/export/history/<int:log_id>/download')
@login_required
def download_export_history(show_id, log_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    row = db.execute(
        'SELECT * FROM export_log WHERE id=? AND show_id=?', (log_id, show_id)
    ).fetchone()
    db.close()
    if not row or not row['pdf_data']:
        abort(404)
    filename = f"{row['export_type'].capitalize()}_v{row['version']}.pdf"
    resp = make_response(bytes(row['pdf_data']))
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return resp


@app.route('/shows/<int:show_id>/export/postnotes')
@login_required
def export_postnotes(show_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id=?', (show_id,)).fetchone()
    if not show:
        abort(404)
    note_rows = db.execute(
        'SELECT field_key, field_value FROM post_show_notes WHERE show_id=?', (show_id,)
    ).fetchall()
    notes_data = {r['field_key']: r['field_value'] for r in note_rows}
    adv_rows = db.execute(
        'SELECT field_key, field_value FROM advance_data WHERE show_id=?', (show_id,)
    ).fetchall()
    advance_data = {r['field_key']: r['field_value'] for r in adv_rows}
    sched_rows = db.execute(
        'SELECT * FROM schedule_rows WHERE show_id=? ORDER BY sort_order,id', (show_id,)
    ).fetchall()
    logo_data = get_app_setting('logo_data', '')
    db.close()

    html = render_template('pdf/postnotes_pdf.html',
                           show=show,
                           notes_data=notes_data,
                           advance_data=advance_data,
                           schedule_rows=sched_rows,
                           logo_data=logo_data,
                           export_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'))
    safe_name = show['name'].replace(' ', '_').replace('/', '-')
    filename = f"PostNotes_{safe_name}_{show['show_date'] or 'nodate'}.pdf"
    try:
        from weasyprint import HTML
        pdf = HTML(string=html, base_url=request.url_root).write_pdf()
        resp = make_response(pdf)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return resp
    except Exception:
        resp = make_response(html)
        resp.headers['Content-Type'] = 'text/html'
        return resp


# ─── Show Management ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/archive', methods=['POST'])
@login_required
def archive_show(show_id):
    if session.get('is_restricted'):
        abort(403)
    db = get_db()
    db.execute("UPDATE shows SET status='archived' WHERE id=?", (show_id,))
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_ARCHIVE show_id={show_id} by={session.get('username')}")
    flash('Show archived.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/shows/<int:show_id>/restore', methods=['POST'])
@login_required
def restore_show(show_id):
    if session.get('is_restricted'):
        abort(403)
    db = get_db()
    db.execute("UPDATE shows SET status='active' WHERE id=?", (show_id,))
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_RESTORE show_id={show_id} by={session.get('username')}")
    flash('Show restored to active.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/shows/<int:show_id>/delete', methods=['POST'])
@admin_required
def delete_show(show_id):
    db = get_db()
    for tbl in ['advance_data', 'schedule_rows', 'schedule_meta',
                'post_show_notes', 'export_log', 'form_history', 'show_group_access']:
        db.execute(f'DELETE FROM {tbl} WHERE show_id=?', (show_id,))
    db.execute('DELETE FROM shows WHERE id=?', (show_id,))
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_DELETE show_id={show_id} by={session.get('username')}")
    flash('Show permanently deleted.', 'success')
    return redirect(url_for('dashboard'))


# ─── Show Access (Groups ↔ Shows) ─────────────────────────────────────────────

@app.route('/shows/<int:show_id>/access/add', methods=['POST'])
@admin_required
def add_show_access(show_id):
    data = request.get_json(force=True) or {}
    group_id = data.get('group_id')
    if not group_id:
        return jsonify({'success': False, 'error': 'group_id required'}), 400
    db = get_db()
    db.execute('INSERT OR IGNORE INTO show_group_access (show_id, group_id) VALUES (?,?)',
               (show_id, group_id))
    db.commit(); db.close()
    syslog_logger.info(
        f"SHOW_ACCESS_ADD show_id={show_id} group_id={group_id} by={session.get('username')}"
    )
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/access/remove', methods=['POST'])
@admin_required
def remove_show_access(show_id):
    data = request.get_json(force=True) or {}
    group_id = data.get('group_id')
    if not group_id:
        return jsonify({'success': False, 'error': 'group_id required'}), 400
    db = get_db()
    db.execute('DELETE FROM show_group_access WHERE show_id=? AND group_id=?',
               (show_id, group_id))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/api/shows/<int:show_id>/access')
@admin_required
def get_show_access(show_id):
    db = get_db()
    rows = db.execute("""
        SELECT ug.id, ug.name, ug.group_type
        FROM show_group_access sga
        JOIN user_groups ug ON sga.group_id = ug.id
        WHERE sga.show_id = ?
    """, (show_id,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


# ─── Settings ─────────────────────────────────────────────────────────────────

@app.route('/settings')
@login_required
def settings():
    db = get_db()
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    users    = db.execute(
        'SELECT id, username, display_name, role, created_at FROM users ORDER BY display_name'
    ).fetchall()
    groups   = db.execute('SELECT * FROM user_groups ORDER BY name').fetchall()

    # Attach members and shows to each group
    groups_data = []
    for g in groups:
        members = db.execute("""
            SELECT u.id, u.display_name, u.username FROM user_group_members ugm
            JOIN users u ON ugm.user_id = u.id
            WHERE ugm.group_id = ?
        """, (g['id'],)).fetchall()
        shows = db.execute("""
            SELECT s.id, s.name, s.show_date FROM show_group_access sga
            JOIN shows s ON sga.show_id = s.id
            WHERE sga.group_id = ?
            ORDER BY s.show_date DESC
        """, (g['id'],)).fetchall()
        gd = dict(g)
        gd['members'] = [dict(m) for m in members]
        gd['shows'] = [dict(s) for s in shows]
        groups_data.append(gd)

    all_settings = {r['key']: r['value'] for r in
                    db.execute("SELECT key, value FROM app_settings").fetchall()}

    db.close()
    _is_ca = session.get('is_content_admin', False) or session.get('user_role') == 'admin'
    form_sections = get_form_fields_for_template() if _is_ca else []
    sched_meta_fields = get_schedule_meta_fields() if _is_ca else []

    try:
        venue_list = json.loads(all_settings.get('venue_list', '[]'))
    except Exception:
        venue_list = []
    try:
        radio_channel_list = json.loads(all_settings.get('radio_channel_list', '[]'))
    except Exception:
        radio_channel_list = []

    return render_template('settings.html',
                           contacts=contacts,
                           users=users,
                           groups=groups_data,
                           form_sections=form_sections,
                           sched_meta_fields=sched_meta_fields,
                           syslog_settings=all_settings,
                           departments=DEPARTMENTS,
                           is_content_admin=_is_ca,
                           venue_list=venue_list,
                           radio_channel_list=radio_channel_list,
                           wifi_network=all_settings.get('wifi_network', ''),
                           wifi_password=all_settings.get('wifi_password', ''),
                           upload_max_mb=all_settings.get('upload_max_mb', '20'),
                           logo_data=all_settings.get('logo_data', ''),
                           user=get_current_user())


@app.route('/settings/contacts/add', methods=['POST'])
@content_admin_required
def add_contact():
    db = get_db()
    db.execute("""
        INSERT INTO contacts (name, title, department, phone, email)
        VALUES (?, ?, ?, ?, ?)
    """, (request.form.get('name','').strip(),
          request.form.get('title','').strip(),
          request.form.get('department','').strip(),
          request.form.get('phone','').strip(),
          request.form.get('email','').strip()))
    db.commit(); db.close()
    flash('Contact added.', 'success')
    return redirect(url_for('settings') + '#contacts')


@app.route('/settings/contacts/<int:cid>/edit', methods=['POST'])
@content_admin_required
def edit_contact(cid):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE contacts SET name=?, title=?, department=?, phone=?, email=?
        WHERE id=?
    """, (data.get('name',''), data.get('title',''), data.get('department',''),
          data.get('phone',''), data.get('email',''), cid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/contacts/<int:cid>/delete', methods=['POST'])
@content_admin_required
def delete_contact(cid):
    db = get_db()
    db.execute('DELETE FROM contacts WHERE id=?', (cid,))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/users/add', methods=['POST'])
@admin_required
def add_user():
    username = request.form.get('username','').strip()
    password = request.form.get('password','')
    display  = request.form.get('display_name','').strip() or username
    role     = request.form.get('role','user')
    if not username or not password:
        flash('Username and password are required.', 'error')
        return redirect(url_for('settings') + '#users')
    db = get_db()
    try:
        db.execute("""INSERT INTO users (username, password_hash, display_name, role)
                      VALUES (?, ?, ?, ?)""",
                   (username, generate_password_hash(password), display, role))
        db.commit()
        flash(f'User "{username}" created.', 'success')
        syslog_logger.info(f"USER_CREATE username={username} by={session.get('username')}")
    except sqlite3.IntegrityError:
        flash('Username already exists.', 'error')
    db.close()
    return redirect(url_for('settings') + '#users')


@app.route('/settings/users/<int:uid>/delete', methods=['POST'])
@admin_required
def delete_user(uid):
    if uid == session['user_id']:
        return jsonify({'success': False, 'error': "You can't delete your own account."})
    db = get_db()
    db.execute('DELETE FROM users WHERE id=?', (uid,))
    db.commit(); db.close()
    syslog_logger.info(f"USER_DELETE user_id={uid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/users/<int:uid>/reset_password', methods=['POST'])
@admin_required
def reset_password(uid):
    data = request.get_json(force=True) or {}
    pw = data.get('password','')
    if not pw:
        return jsonify({'success': False, 'error': 'Password required.'})
    db = get_db()
    db.execute('UPDATE users SET password_hash=? WHERE id=?', (generate_password_hash(pw), uid))
    db.commit(); db.close()
    syslog_logger.info(f"PASSWORD_CHANGE user_id={uid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/account/theme', methods=['POST'])
@login_required
def set_theme():
    data = request.get_json(force=True) or {}
    theme = data.get('theme', 'dark')
    if theme not in ('dark', 'light'):
        theme = 'dark'
    db = get_db()
    db.execute('UPDATE users SET theme=? WHERE id=?', (theme, session['user_id']))
    db.commit()
    db.close()
    session['theme'] = theme
    return jsonify({'success': True})


@app.route('/account/change_password', methods=['POST'])
@login_required
def change_own_password():
    data = request.get_json(force=True) or {}
    current = data.get('current_password','')
    new_pw  = data.get('new_password','')
    db = get_db()
    user = db.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    if not check_password_hash(user['password_hash'], current):
        db.close()
        return jsonify({'success': False, 'error': 'Current password incorrect.'})
    db.execute('UPDATE users SET password_hash=? WHERE id=?',
               (generate_password_hash(new_pw), session['user_id']))
    db.commit(); db.close()
    syslog_logger.info(f"PASSWORD_CHANGE user_id={session['user_id']} by={session.get('username')} (self)")
    return jsonify({'success': True})


# ─── User Groups Management ───────────────────────────────────────────────────

@app.route('/settings/groups/add', methods=['POST'])
@admin_required
def add_group():
    data = request.get_json(force=True) or {}
    name = data.get('name','').strip()
    group_type = data.get('group_type','all_access')
    desc = data.get('description','').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required.'}), 400
    db = get_db()
    try:
        cur = db.execute(
            'INSERT INTO user_groups (name, group_type, description) VALUES (?,?,?)',
            (name, group_type, desc)
        )
        gid = cur.lastrowid
        db.commit()
        syslog_logger.info(f"GROUP_CREATE name={name} by={session.get('username')}")
        return jsonify({'success': True, 'id': gid})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Group name already exists.'}), 400
    finally:
        db.close()


@app.route('/settings/groups/<int:gid>/edit', methods=['POST'])
@admin_required
def edit_group(gid):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE user_groups SET name=?, group_type=?, description=? WHERE id=?
    """, (data.get('name',''), data.get('group_type','all_access'),
          data.get('description',''), gid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/groups/<int:gid>/delete', methods=['POST'])
@admin_required
def delete_group(gid):
    db = get_db()
    db.execute('DELETE FROM user_groups WHERE id=?', (gid,))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/groups/<int:gid>/members/add', methods=['POST'])
@admin_required
def add_group_member(gid):
    data = request.get_json(force=True) or {}
    uid = data.get('user_id')
    if not uid:
        return jsonify({'success': False, 'error': 'user_id required'}), 400
    db = get_db()
    db.execute('INSERT OR IGNORE INTO user_group_members (user_id, group_id) VALUES (?,?)',
               (uid, gid))
    db.commit()

    # Refresh the affected user's is_restricted in their session (best effort)
    # Session is server-side cookie; we can't update other sessions directly.
    # User will see updated access on next login.

    db.close()
    syslog_logger.info(
        f"GROUP_ASSIGN user_id={uid} group_id={gid} by={session.get('username')}"
    )
    return jsonify({'success': True})


@app.route('/settings/groups/<int:gid>/members/remove', methods=['POST'])
@admin_required
def remove_group_member(gid):
    data = request.get_json(force=True) or {}
    uid = data.get('user_id')
    if not uid:
        return jsonify({'success': False, 'error': 'user_id required'}), 400
    db = get_db()
    db.execute('DELETE FROM user_group_members WHERE user_id=? AND group_id=?', (uid, gid))
    db.commit(); db.close()
    syslog_logger.info(
        f"GROUP_REMOVE user_id={uid} group_id={gid} by={session.get('username')}"
    )
    return jsonify({'success': True})


@app.route('/api/groups')
@login_required
def api_groups():
    db = get_db()
    groups = db.execute('SELECT * FROM user_groups ORDER BY name').fetchall()
    db.close()
    return jsonify([dict(g) for g in groups])


# ─── Form Field Editor ────────────────────────────────────────────────────────

@app.route('/settings/form-fields')
@content_admin_required
def form_fields_settings():
    form_sections = get_form_fields_for_template()
    db = get_db()
    users = db.execute(
        'SELECT id, username, display_name, role, created_at FROM users ORDER BY display_name'
    ).fetchall()
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    groups = db.execute('SELECT * FROM user_groups ORDER BY name').fetchall()

    groups_data = []
    for g in groups:
        members = db.execute("""
            SELECT u.id, u.display_name, u.username FROM user_group_members ugm
            JOIN users u ON ugm.user_id = u.id
            WHERE ugm.group_id = ?
        """, (g['id'],)).fetchall()
        shows = db.execute("""
            SELECT s.id, s.name, s.show_date FROM show_group_access sga
            JOIN shows s ON sga.show_id = s.id
            WHERE sga.group_id = ?
            ORDER BY s.show_date DESC
        """, (g['id'],)).fetchall()
        gd = dict(g)
        gd['members'] = [dict(m) for m in members]
        gd['shows'] = [dict(s) for s in shows]
        groups_data.append(gd)

    all_settings = {r['key']: r['value'] for r in
                    db.execute("SELECT key, value FROM app_settings").fetchall()}
    db.close()

    _is_ca = session.get('is_content_admin', False) or session.get('user_role') == 'admin'
    try:
        venue_list = json.loads(all_settings.get('venue_list', '[]'))
    except Exception:
        venue_list = []
    try:
        radio_channel_list = json.loads(all_settings.get('radio_channel_list', '[]'))
    except Exception:
        radio_channel_list = []

    return render_template('settings.html',
                           contacts=contacts,
                           users=users,
                           groups=groups_data,
                           form_sections=form_sections,
                           sched_meta_fields=get_schedule_meta_fields(),
                           syslog_settings=all_settings,
                           departments=DEPARTMENTS,
                           active_tab='fields',
                           is_content_admin=_is_ca,
                           venue_list=venue_list,
                           radio_channel_list=radio_channel_list,
                           wifi_network=all_settings.get('wifi_network', ''),
                           wifi_password=all_settings.get('wifi_password', ''),
                           upload_max_mb=all_settings.get('upload_max_mb', '20'),
                           logo_data=all_settings.get('logo_data', ''),
                           user=get_current_user())


@app.route('/settings/form-fields/add', methods=['POST'])
@content_admin_required
def add_form_field():
    data = request.get_json(force=True) or {}
    section_id = data.get('section_id')
    field_key  = data.get('field_key','').strip().lower().replace(' ','_')
    label      = data.get('label','').strip()
    if not section_id or not field_key or not label:
        return jsonify({'success': False, 'error': 'section_id, field_key, and label required.'}), 400

    options = data.get('options', [])
    options_json = json.dumps(options) if options else None

    db = get_db()
    # Put it at the end of the section
    max_order = db.execute(
        'SELECT MAX(sort_order) FROM form_fields WHERE section_id=?', (section_id,)
    ).fetchone()[0] or 0
    try:
        cur = db.execute("""
            INSERT INTO form_fields
            (section_id, field_key, label, field_type, sort_order,
             options_json, contact_dept, conditional_show_when,
             help_text, placeholder, width_hint, is_notes_field)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (section_id, field_key, label,
              data.get('field_type','text'), max_order + 10,
              options_json,
              data.get('contact_dept'),
              data.get('conditional_show_when'),
              data.get('help_text'),
              data.get('placeholder',''),
              data.get('width_hint','full'),
              1 if data.get('is_notes_field') else 0))
        fid = cur.lastrowid
        db.commit()
        syslog_logger.info(f"FIELD_ADD key={field_key} by={session.get('username')}")
        return jsonify({'success': True, 'id': fid})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': f'field_key "{field_key}" already exists.'}), 400
    finally:
        db.close()


@app.route('/settings/form-fields/<int:fid>/edit', methods=['POST'])
@content_admin_required
def edit_form_field(fid):
    data = request.get_json(force=True) or {}
    options = data.get('options', [])
    options_json = json.dumps(options) if options else None
    db = get_db()
    db.execute("""
        UPDATE form_fields SET
            section_id=?, label=?, field_type=?,
            options_json=?, contact_dept=?, conditional_show_when=?,
            help_text=?, placeholder=?, width_hint=?, is_notes_field=?
        WHERE id=?
    """, (data.get('section_id'), data.get('label',''),
          data.get('field_type','text'), options_json,
          data.get('contact_dept'), data.get('conditional_show_when'),
          data.get('help_text'), data.get('placeholder',''),
          data.get('width_hint','full'),
          1 if data.get('is_notes_field') else 0,
          fid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/form-fields/<int:fid>/delete', methods=['POST'])
@content_admin_required
def delete_form_field(fid):
    db = get_db()
    db.execute('DELETE FROM form_fields WHERE id=?', (fid,))
    db.commit(); db.close()
    syslog_logger.info(f"FIELD_DELETE id={fid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/form-fields/reorder', methods=['POST'])
@content_admin_required
def reorder_form_fields():
    data = request.get_json(force=True) or {}
    field_ids = data.get('field_ids', [])
    db = get_db()
    for i, fid in enumerate(field_ids):
        db.execute('UPDATE form_fields SET sort_order=? WHERE id=?', (i * 10, fid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/form-sections/add', methods=['POST'])
@content_admin_required
def add_form_section():
    data = request.get_json(force=True) or {}
    section_key = data.get('section_key','').strip().lower().replace(' ','_')
    label = data.get('label','').strip()
    if not section_key or not label:
        return jsonify({'success': False, 'error': 'section_key and label required.'}), 400
    db = get_db()
    max_order = db.execute('SELECT MAX(sort_order) FROM form_sections').fetchone()[0] or 0
    try:
        cur = db.execute("""
            INSERT INTO form_sections (section_key, label, sort_order, collapsible, icon)
            VALUES (?,?,?,?,?)
        """, (section_key, label, max_order + 10,
              1 if data.get('collapsible', True) else 0,
              data.get('icon', '◈')))
        sid = cur.lastrowid
        db.commit()
        return jsonify({'success': True, 'id': sid})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': f'section_key "{section_key}" already exists.'}), 400
    finally:
        db.close()


@app.route('/settings/form-sections/<int:sid>/edit', methods=['POST'])
@content_admin_required
def edit_form_section(sid):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE form_sections SET label=?, collapsible=?, icon=? WHERE id=?
    """, (data.get('label',''),
          1 if data.get('collapsible', True) else 0,
          data.get('icon','◈'), sid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/form-sections/<int:sid>/delete', methods=['POST'])
@content_admin_required
def delete_form_section(sid):
    db = get_db()
    db.execute('DELETE FROM form_sections WHERE id=?', (sid,))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/form-sections/reorder', methods=['POST'])
@content_admin_required
def reorder_form_sections():
    data = request.get_json(force=True) or {}
    section_ids = data.get('section_ids', [])
    db = get_db()
    for i, sid in enumerate(section_ids):
        db.execute('UPDATE form_sections SET sort_order=? WHERE id=?', (i * 10, sid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/api/form-fields')
@login_required
def api_form_fields():
    return jsonify(get_form_fields_for_template())


# ─── Schedule Meta Field Editor ───────────────────────────────────────────────

@app.route('/api/schedule-meta-fields')
@login_required
def api_schedule_meta_fields():
    return jsonify(get_schedule_meta_fields())


@app.route('/settings/schedule-meta-fields/add', methods=['POST'])
@content_admin_required
def add_sched_meta_field():
    data = request.get_json(force=True) or {}
    field_key = data.get('field_key', '').strip().lower().replace(' ', '_')
    label = data.get('label', '').strip()
    if not field_key or not label:
        return jsonify({'success': False, 'error': 'field_key and label required.'}), 400
    db = get_db()
    max_order = db.execute('SELECT MAX(sort_order) FROM schedule_meta_fields').fetchone()[0] or 0
    try:
        cur = db.execute("""
            INSERT INTO schedule_meta_fields
              (field_key, label, field_type, advance_field_key, sort_order, width_hint)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (field_key, label,
              data.get('field_type', 'text'),
              data.get('advance_field_key', '').strip() or None,
              max_order + 10,
              data.get('width_hint', 'half')))
        fid = cur.lastrowid
        db.commit()
        return jsonify({'success': True, 'id': fid})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': f'field_key "{field_key}" already exists.'}), 400
    finally:
        db.close()


@app.route('/settings/schedule-meta-fields/<int:fid>/edit', methods=['POST'])
@content_admin_required
def edit_sched_meta_field(fid):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE schedule_meta_fields
        SET label=?, field_type=?, advance_field_key=?, width_hint=?
        WHERE id=?
    """, (data.get('label', ''),
          data.get('field_type', 'text'),
          data.get('advance_field_key', '').strip() or None,
          data.get('width_hint', 'half'),
          fid))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/schedule-meta-fields/<int:fid>/delete', methods=['POST'])
@content_admin_required
def delete_sched_meta_field(fid):
    db = get_db()
    db.execute('DELETE FROM schedule_meta_fields WHERE id=?', (fid,))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/schedule-meta-fields/reorder', methods=['POST'])
@content_admin_required
def reorder_sched_meta_fields():
    data = request.get_json(force=True) or {}
    field_ids = data.get('field_ids', [])
    db = get_db()
    for i, fid in enumerate(field_ids):
        db.execute('UPDATE schedule_meta_fields SET sort_order=? WHERE id=?', (i * 10, fid))
    db.commit(); db.close()
    return jsonify({'success': True})


# ─── Syslog Settings ──────────────────────────────────────────────────────────

@app.route('/settings/server', methods=['POST'])
@admin_required
def save_server_settings():
    data = request.get_json(force=True) or {}
    port_str = str(data.get('app_port', '5400')).strip()
    try:
        port_val = int(port_str)
        if not (1024 <= port_val <= 65535):
            return jsonify({'success': False, 'error': 'Port must be between 1024 and 65535.'}), 400
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid port number.'}), 400

    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('app_port', str(port_val)))
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE key=app_port value={port_val} by={session.get('username')}")

    # Check if running under the showadvance systemd service
    svc_active = False
    try:
        result = subprocess.run(
            ['systemctl', 'is-active', 'showadvance'],
            capture_output=True, text=True, timeout=3
        )
        svc_active = result.stdout.strip() == 'active'
    except Exception:
        pass

    if svc_active:
        # Schedule restart after response is sent (1 s delay so the JSON
        # response reaches the browser before the process is killed)
        def _do_restart():
            import time as _time
            _time.sleep(1.0)
            try:
                subprocess.run(
                    ['sudo', 'systemctl', 'restart', 'showadvance'],
                    timeout=10
                )
            except Exception as exc:
                app.logger.error(f'Service restart failed: {exc}')
        threading.Thread(target=_do_restart, daemon=True).start()

    return jsonify({
        'success': True,
        'new_port': port_val,
        'restarting': svc_active,
        'message': (
            f'Port changed to {port_val}. Service is restarting...'
            if svc_active else
            f'Port set to {port_val}. Restart the service to apply.'
        )
    })


@app.route('/settings/syslog', methods=['POST'])
@admin_required
def save_syslog_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    for key in ('syslog_host', 'syslog_port', 'syslog_facility', 'syslog_enabled'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    db.commit(); db.close()
    reload_syslog_handler()
    syslog_logger.info(f"SETTINGS_CHANGE key=syslog by={session.get('username')}")
    return jsonify({'success': True})


# ─── Backup Management ────────────────────────────────────────────────────────

@app.route('/settings/backups')
@admin_required
def backup_status():
    result = {'hourly': [], 'daily': []}
    for kind in ('hourly', 'daily'):
        d = os.path.join(BACKUP_DIR, kind)
        if os.path.isdir(d):
            files = sorted(
                [f for f in os.listdir(d) if f.endswith('.db')],
                reverse=True
            )
            result[kind] = [{
                'filename': f,
                'size_kb': round(os.path.getsize(os.path.join(d, f)) / 1024, 1),
                'mtime': datetime.fromtimestamp(
                    os.path.getmtime(os.path.join(d, f))
                ).strftime('%Y-%m-%d %H:%M')
            } for f in files[:10]]
    return jsonify(result)


@app.route('/settings/backups/run', methods=['POST'])
@admin_required
def manual_backup():
    try:
        run_hourly_backup()
        return jsonify({'success': True, 'message': 'Backup created successfully.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── API ──────────────────────────────────────────────────────────────────────

@app.route('/api/contacts')
@login_required
def api_contacts():
    db = get_db()
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    db.close()
    return jsonify([dict(c) for c in contacts])


@app.route('/api/users')
@admin_required
def api_users():
    db = get_db()
    users = db.execute(
        'SELECT id, username, display_name, role FROM users ORDER BY display_name'
    ).fetchall()
    db.close()
    return jsonify([dict(u) for u in users])


@app.route('/api/shows')
@admin_required
def api_shows():
    db = get_db()
    shows = db.execute(
        "SELECT id, name, show_date, status FROM shows ORDER BY show_date DESC"
    ).fetchall()
    db.close()
    return jsonify([dict(s) for s in shows])


# ─── API Time ─────────────────────────────────────────────────────────────────

@app.route('/api/time')
@login_required
def api_time():
    return jsonify({
        'utc': datetime.utcnow().isoformat(),
        'local': datetime.now().isoformat()
    })


# ─── God Mode (Admin) ─────────────────────────────────────────────────────────

@app.route('/api/admin/god-mode')
@admin_required
def api_god_mode():
    db = get_db()
    sessions = db.execute("""
        SELECT u.display_name, u.username, acs.tab, acs.last_seen,
               s.name as show_name, s.id as show_id
        FROM active_sessions acs
        JOIN users u ON acs.user_id = u.id
        JOIN shows s ON acs.show_id = s.id
        WHERE acs.last_seen > datetime('now', '-5 minutes')
        ORDER BY acs.last_seen DESC
    """).fetchall()
    users = db.execute("""
        SELECT id, display_name, username, role, last_login
        FROM users ORDER BY display_name
    """).fetchall()
    db.close()
    return jsonify({
        'sessions': [{
            'user': r['display_name'] or r['username'],
            'show': r['show_name'],
            'show_id': r['show_id'],
            'tab': r['tab'],
            'last_seen': r['last_seen'],
        } for r in sessions],
        'users': [{
            'id': u['id'],
            'name': u['display_name'] or u['username'],
            'username': u['username'],
            'role': u['role'],
            'last_login': u['last_login'] or '—',
        } for u in users],
    })


# ─── File Manager (Admin) ─────────────────────────────────────────────────────

@app.route('/api/admin/files')
@admin_required
def api_file_manager():
    db = get_db()
    rows = db.execute("""
        SELECT sa.id, sa.filename, sa.mime_type, sa.file_size, sa.created_at,
               s.id as show_id, s.name as show_name,
               u.display_name, u.username
        FROM show_attachments sa
        JOIN shows s ON sa.show_id = s.id
        LEFT JOIN users u ON sa.uploaded_by = u.id
        ORDER BY sa.created_at DESC
    """).fetchall()
    total_bytes = db.execute('SELECT SUM(file_size) FROM show_attachments').fetchone()[0] or 0
    db.close()
    return jsonify({
        'files': [{
            'id':         r['id'],
            'show_id':    r['show_id'],
            'show_name':  r['show_name'],
            'filename':   r['filename'],
            'mime_type':  r['mime_type'],
            'file_size':  r['file_size'],
            'created_at': r['created_at'],
            'uploader':   r['display_name'] or r['username'] or 'Unknown',
        } for r in rows],
        'total_bytes': total_bytes,
    })


# ─── Venue / Radio / WiFi / Logo Settings ────────────────────────────────────

@app.route('/settings/venues', methods=['POST'])
@admin_required
def save_venues():
    data = request.get_json(force=True) or {}
    venue_list = data.get('venue_list', [])
    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('venue_list', json.dumps(venue_list)))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/radio-channels', methods=['POST'])
@admin_required
def save_radio_channels():
    data = request.get_json(force=True) or {}
    channel_list = data.get('channel_list', [])
    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('radio_channel_list', json.dumps(channel_list)))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/wifi', methods=['POST'])
@admin_required
def save_wifi_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    for key in ('wifi_network', 'wifi_password'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, data[key]))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/logo', methods=['POST'])
@admin_required
def save_logo():
    f = request.files.get('logo')
    if f and f.filename:
        import base64
        data = f.read()
        if len(data) > 2 * 1024 * 1024:
            return jsonify({'success': False, 'error': 'Logo too large (max 2 MB).'}), 413
        mime = f.content_type or 'image/png'
        b64 = base64.b64encode(data).decode()
        logo_data = f'data:{mime};base64,{b64}'
    else:
        data_uri = (request.get_json(force=True) or {}).get('logo_data', '')
        logo_data = data_uri

    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('logo_data', logo_data))
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/logo/delete', methods=['POST'])
@admin_required
def delete_logo():
    db = get_db()
    db.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('logo_data', '')")
    db.commit(); db.close()
    return jsonify({'success': True})


@app.route('/settings/upload-size', methods=['POST'])
@admin_required
def save_upload_size():
    data = request.get_json(force=True) or {}
    try:
        mb = int(data.get('upload_max_mb', 20))
        mb = max(1, min(mb, 500))
    except (ValueError, TypeError):
        mb = 20
    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('upload_max_mb', str(mb)))
    db.commit(); db.close()
    return jsonify({'success': True, 'upload_max_mb': mb})


# ─── Public Show Page ─────────────────────────────────────────────────────────

@app.route('/public')
def public_shows():
    db = get_db()
    shows = db.execute("""
        SELECT id, name, show_date, show_time, venue, advance_version, schedule_version
        FROM shows WHERE status='active'
        ORDER BY show_date ASC NULLS LAST
    """).fetchall()
    db.close()
    return render_template('public.html', shows=shows)


@app.route('/public/shows/<int:show_id>/advance')
def public_advance_pdf(show_id):
    db = get_db()
    row = db.execute("""
        SELECT pdf_data FROM export_log
        WHERE show_id=? AND export_type='advance'
        ORDER BY exported_at DESC LIMIT 1
    """, (show_id,)).fetchone()
    show = db.execute('SELECT * FROM shows WHERE id=? AND status="active"', (show_id,)).fetchone()
    db.close()
    if not show:
        abort(404)
    if row and row['pdf_data']:
        resp = make_response(bytes(row['pdf_data']))
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'inline; filename="Advance_{show_id}.pdf"'
        return resp
    abort(404)


@app.route('/public/shows/<int:show_id>/schedule')
def public_schedule_pdf(show_id):
    db = get_db()
    row = db.execute("""
        SELECT pdf_data FROM export_log
        WHERE show_id=? AND export_type='schedule'
        ORDER BY exported_at DESC LIMIT 1
    """, (show_id,)).fetchone()
    show = db.execute('SELECT * FROM shows WHERE id=? AND status="active"', (show_id,)).fetchone()
    db.close()
    if not show:
        abort(404)
    if row and row['pdf_data']:
        resp = make_response(bytes(row['pdf_data']))
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = f'inline; filename="Schedule_{show_id}.pdf"'
        return resp
    abort(404)


# ─── Error Handlers ───────────────────────────────────────────────────────────

@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html', code=403,
                           message="You don't have permission to do that.",
                           user=get_current_user()), 403


@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', code=404, message="Page not found.",
                           user=get_current_user()), 404


# ─── Run ──────────────────────────────────────────────────────────────────────

# Initialize syslog at import time (for Gunicorn)
if os.path.exists(DATABASE):
    reload_syslog_handler()
    # Ensure backup dirs exist (fixes PermissionError if dirs were missing)
    try:
        _ensure_backup_dirs()
    except Exception:
        pass
    # Auto-run DB migrations on startup (idempotent — safe to run every time)
    try:
        from init_db import migrate_db
        migrate_db()
    except Exception as _mig_err:
        print(f"[startup] Migration warning: {_mig_err}")

# Start backup scheduler (guarded against Flask reloader double-start)
_scheduler = None
if not (os.environ.get('WERKZEUG_RUN_MAIN') == 'false'):
    _scheduler = start_scheduler()
    if _scheduler:
        atexit.register(lambda: _scheduler.shutdown(wait=False))

if __name__ == '__main__':
    if not os.path.exists(DATABASE):
        print("Database not found. Run: python init_db.py")
        run_port = 5400
    else:
        try:
            _db = get_db()
            _row = _db.execute(
                "SELECT value FROM app_settings WHERE key='app_port'"
            ).fetchone()
            _db.close()
            run_port = int(_row['value']) if _row else 5400
        except Exception:
            run_port = 5400
    app.run(debug=True, port=run_port)
