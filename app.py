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
import secrets
import re
import html as _html_mod
from datetime import datetime, date, timedelta
from functools import wraps
from io import BytesIO

import db_adapter
from db_adapter import DBIntegrityError

from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, jsonify, make_response, abort)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename


def _safe_content_disposition(filename):
    """Build a safe Content-Disposition header value, stripping injection chars."""
    safe = secure_filename(filename) or 'download'
    return f'attachment; filename="{safe}"'


# Allowed HTML tags/attributes for user-supplied rich text (message body)
_MSG_ALLOWED_TAGS  = frozenset(['p','br','b','strong','i','em','a','ul','ol','li','span','h3','h4','hr','div'])
_MSG_ALLOWED_ATTRS = {'a': ['href', 'title']}

def _sanitize_html(raw):
    """Strip disallowed HTML tags, keeping a safe subset. Prevents stored XSS."""
    from html.parser import HTMLParser
    class _S(HTMLParser):
        def __init__(self):
            super().__init__()
            self.out = []
        def handle_starttag(self, tag, attrs):
            t = tag.lower()
            if t not in _MSG_ALLOWED_TAGS:
                return
            allowed = _MSG_ALLOWED_ATTRS.get(t, [])
            safe_attrs = ''
            for k, v in attrs:
                k = k.lower()
                if k not in allowed or not v:
                    continue
                # Reject javascript: and data: URIs
                if re.match(r'\s*(javascript|data|vbscript)\s*:', v, re.I):
                    continue
                safe_attrs += f' {_html_mod.escape(k)}="{_html_mod.escape(v)}"'
            self.out.append(f'<{t}{safe_attrs}>')
        def handle_endtag(self, tag):
            t = tag.lower()
            if t in _MSG_ALLOWED_TAGS:
                self.out.append(f'</{t}>')
        def handle_data(self, data):
            self.out.append(_html_mod.escape(data))
    s = _S()
    s.feed(raw or '')
    return ''.join(s.out)

app = Flask(__name__)

# ── SECRET_KEY — generate and persist if not provided via environment ─────────
_secret = os.environ.get('SECRET_KEY', '')
if not _secret:
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    # Try to read from .env file
    if os.path.exists(_env_path):
        with open(_env_path) as _ef:
            for _line in _ef:
                if _line.strip().startswith('SECRET_KEY='):
                    _secret = _line.strip().split('=', 1)[1]
                    break
    # Auto-generate if still missing (first run / dev mode)
    if not _secret:
        import secrets as _secrets_mod
        _secret = _secrets_mod.token_hex(32)
        app.logger.warning(
            'SECRET_KEY not set — generated an ephemeral key. '
            'Run install.sh or set SECRET_KEY in .env for persistent sessions.'
        )
app.secret_key = _secret

# ── Session cookie security ───────────────────────────────────────────────────
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# SESSION_COOKIE_SECURE intentionally omitted (app typically runs on LAN over HTTP)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)


@app.after_request
def _set_security_headers(response):
    """Add defense-in-depth security headers to every response."""
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    return response


# ── CSRF Protection ───────────────────────────────────────────────────────────
# For AJAX: require X-Requested-With header (cannot be set cross-origin without CORS)
# For form POSTs: validate Origin/Referer header matches our host
_CSRF_SAFE_METHODS = frozenset(('GET', 'HEAD', 'OPTIONS'))
_CSRF_EXEMPT_ENDPOINTS = frozenset(('login', 'static'))


@app.before_request
def _csrf_protect():
    """Block cross-site state-changing requests."""
    if request.method in _CSRF_SAFE_METHODS:
        return
    if request.endpoint in _CSRF_EXEMPT_ENDPOINTS:
        return
    if not session.get('user_id'):
        return  # Not logged in — auth decorators will handle it

    # AJAX requests: require X-Requested-With header
    # (Browsers block cross-origin custom headers without CORS preflight)
    if request.is_json or request.content_type == 'application/json':
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return  # Valid AJAX request
        # Also accept if Origin matches (for fetch() without custom header)
        if _origin_matches():
            return
        abort(403)

    # Form POSTs: validate Origin or Referer
    if _origin_matches():
        return

    app.logger.warning(
        f'CSRF blocked: endpoint={request.endpoint} '
        f'origin={request.headers.get("Origin")} '
        f'referer={request.headers.get("Referer")} '
        f'user={session.get("username")}'
    )
    abort(403)


def _origin_matches():
    """Check that Origin or Referer header matches our server."""
    from urllib.parse import urlparse
    # Check Origin header first (most reliable)
    origin = request.headers.get('Origin')
    if origin:
        parsed = urlparse(origin)
        return parsed.hostname == request.host.split(':')[0]
    # Fall back to Referer
    referer = request.headers.get('Referer')
    if referer:
        parsed = urlparse(referer)
        return parsed.hostname == request.host.split(':')[0]
    # No Origin or Referer — could be a direct form submission from same host
    # (some privacy extensions strip Referer). Allow only if SameSite=Lax is set.
    return True


@app.context_processor
def inject_version():
    return {'app_version': APP_VERSION}


@app.template_filter('pretty_json')
def pretty_json_filter(value):
    """Pretty-print a JSON string in templates."""
    try:
        return json.dumps(json.loads(value), indent=2, ensure_ascii=False)
    except Exception:
        return value or ''


DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'advance.db')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')

# ── Application Version ───────────────────────────────────────────────────────
# Format: MAJOR.MINOR.PATCH
#   MAJOR — breaking schema or architectural changes
#   MINOR — new feature sets (e.g. asset manager, user enhancements)
#   PATCH — bug fixes, small improvements, security patches
APP_VERSION = '2.5.1'

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
    import warnings
    warnings.warn(
        'flask-limiter is not installed — login rate limiting is DISABLED. '
        'Install it: pip install flask-limiter',
        stacklevel=1,
    )


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
    """
    Fetch a single app_setting value.
    Always reads from the SQLite bootstrap file so this is safe to call
    at startup before the active DB connection type is resolved.
    """
    if not os.path.exists(DATABASE):
        return default
    try:
        _conn = sqlite3.connect(DATABASE)
        _conn.row_factory = sqlite3.Row
        row = _conn.execute('SELECT value FROM app_settings WHERE key=?', (key,)).fetchone()
        _conn.close()
        return row['value'] if row and row['value'] is not None else default
    except Exception:
        return default


# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    """Return a normalized DB connection (SQLite or PostgreSQL based on settings)."""
    settings = db_adapter.read_db_settings(DATABASE)
    return db_adapter.connect(DATABASE, settings)


# ─── Auth Decorators ──────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


def readonly_blocked(f):
    """Block read-only users from mutating actions."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('is_readonly'):
            if request.is_json:
                return jsonify({'error': 'Read-only access'}), 403
            abort(403)
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


def staff_or_admin_required(f):
    """Allow staff and admin roles (but not plain users) to access a route."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') not in ('admin', 'staff'):
            abort(403)
        return f(*args, **kwargs)
    return decorated


@app.before_request
def _refresh_session_roles():
    """Re-check user role/permissions from DB every 5 minutes to catch demotions."""
    if 'user_id' not in session:
        return
    last_check = session.get('_role_checked_at', 0)
    now = datetime.utcnow().timestamp()
    if now - last_check < 300:  # 5 minutes
        return
    try:
        db = get_db()
        user = db.execute('SELECT id, role, display_name FROM users WHERE id=?',
                          (session['user_id'],)).fetchone()
        db.close()
        if not user:
            session.clear()
            return redirect(url_for('login'))
        session['user_role'] = user['role']
        session['display_name'] = user['display_name'] or session.get('username', '')
        session['is_restricted'] = is_restricted_user(user['id'])
        session['is_content_admin'] = is_content_admin(user['id'])
        session['is_readonly'] = bool(user.get('is_readonly', 0))
        session['_role_checked_at'] = now
    except Exception:
        pass


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
    """True if the user is a system admin, a staff user, or is in an 'admin_group' type group."""
    db = get_db()
    user = db.execute('SELECT role FROM users WHERE id=?', (user_id,)).fetchone()
    db.close()
    if not user:
        return False
    if user['role'] in ('admin', 'staff'):
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


def _get_smtp_settings():
    """Return a dict of SMTP config from app_settings."""
    keys = ('smtp_host', 'smtp_port', 'smtp_user', 'smtp_pass',
            'smtp_from', 'smtp_tls')
    return {k: get_app_setting(k, '') for k in keys}


def _build_mime_message(subject, from_addr, recipients, body_text=None,
                        body_html=None, attachments=None):
    """
    Build a MIME email message.

    Args:
        subject (str): Email subject
        from_addr (str): Sender address
        recipients (list[str]): Recipient addresses
        body_text (str|None): Plain text body
        body_html (str|None): HTML body
        attachments (list[dict]|None): Each dict: {'filename', 'data' (bytes), 'mimetype'}

    Returns:
        email.mime.multipart.MIMEMultipart
    """
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders as email_encoders

    # Sanitize email headers to prevent header injection
    def _clean_header(v):
        return v.replace('\r', '').replace('\n', '') if v else v

    msg = MIMEMultipart()
    msg['From'] = _clean_header(from_addr)
    msg['To'] = _clean_header(', '.join(recipients))
    msg['Subject'] = _clean_header(subject)

    if body_text and body_html:
        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(body_text, 'plain'))
        alt.attach(MIMEText(body_html, 'html'))
        msg.attach(alt)
    elif body_html:
        msg.attach(MIMEText(body_html, 'html'))
    elif body_text:
        msg.attach(MIMEText(body_text, 'plain'))

    for att in (attachments or []):
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(att['data'])
        email_encoders.encode_base64(part)
        mime = att.get('mimetype', 'application/octet-stream')
        part.add_header('Content-Type', mime)
        part.add_header('Content-Disposition',
                        f'attachment; filename="{att["filename"]}"')
        msg.attach(part)

    return msg


def _send_email_smtp(subject, recipients, body_text=None, body_html=None,
                     attachments=None, from_address=None):
    """Send email via configured SMTP relay. Returns (success, message)."""
    import smtplib

    smtp_cfg = _get_smtp_settings()
    if not smtp_cfg.get('smtp_host'):
        return False, 'SMTP not configured.'

    from_addr = from_address or smtp_cfg.get('smtp_from') or smtp_cfg.get('smtp_user', '')
    msg = _build_mime_message(subject, from_addr, recipients, body_text,
                              body_html, attachments)

    try:
        port = int(smtp_cfg.get('smtp_port') or 587)
        use_tls = smtp_cfg.get('smtp_tls', '1') not in ('0', 'false', 'False', '')
        if use_tls:
            server = smtplib.SMTP(smtp_cfg['smtp_host'], port, timeout=15)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP_SSL(smtp_cfg['smtp_host'], port, timeout=15)
        if smtp_cfg.get('smtp_user') and smtp_cfg.get('smtp_pass'):
            server.login(smtp_cfg['smtp_user'], smtp_cfg['smtp_pass'])
        server.sendmail(from_addr, recipients, msg.as_string())
        server.quit()
    except Exception as e:
        app.logger.error(f'SMTP send failed: {e}')
        return False, f'SMTP error: {e}'

    return True, f'Sent to {len(recipients)} recipient(s).'


def _send_email_direct(subject, recipients, body_text=None, body_html=None,
                       attachments=None, from_address=None):
    """Send email directly via MX lookup (no relay). Returns (success, message)."""
    import smtplib
    import dns.resolver

    smtp_cfg = _get_smtp_settings()
    from_addr = from_address or smtp_cfg.get('smtp_from') or 'noreply@localhost'
    ehlo_hostname = get_app_setting('direct_ehlo_hostname', '').strip() or None
    display_name = get_app_setting('direct_display_name', '').strip()

    # Wrap from address with display name if configured
    if display_name:
        from email.utils import formataddr
        from_addr_header = formataddr((display_name, from_addr))
    else:
        from_addr_header = from_addr

    msg = _build_mime_message(subject, from_addr_header, recipients, body_text,
                              body_html, attachments)
    msg_str = msg.as_string()

    # Group recipients by domain
    from collections import defaultdict
    by_domain = defaultdict(list)
    for addr in recipients:
        if '@' in addr:
            by_domain[addr.split('@')[1].lower()].append(addr)

    errors = []
    sent_count = 0

    for domain, addrs in by_domain.items():
        # Resolve MX records
        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            mx_hosts = sorted(mx_records, key=lambda r: r.preference)
        except Exception as e:
            errors.append(f'MX lookup failed for {domain}: {e}')
            continue

        # Try each MX host in priority order
        delivered = False
        last_error = None
        for mx in mx_hosts:
            mx_host = str(mx.exchange).rstrip('.')
            try:
                server = smtplib.SMTP(mx_host, 25, timeout=15,
                                      local_hostname=ehlo_hostname)
                server.ehlo(ehlo_hostname)
                try:
                    server.starttls()
                    server.ehlo(ehlo_hostname)
                except smtplib.SMTPNotSupportedError:
                    pass  # Server doesn't support STARTTLS, continue unencrypted
                server.sendmail(from_addr, addrs, msg_str)
                server.quit()
                sent_count += len(addrs)
                delivered = True
                break
            except Exception as e:
                last_error = f'{mx_host}: {e}'
                app.logger.warning(f'Direct send to MX {mx_host} for {domain} failed: {e}')
                continue

        if not delivered:
            detail = f' ({last_error})' if last_error else ''
            errors.append(f'All MX hosts failed for {domain}{detail}')

    if errors and sent_count == 0:
        return False, '; '.join(errors)
    elif errors:
        return True, f'Sent to {sent_count} recipient(s). Failures: {"; ".join(errors)}'
    return True, f'Sent to {sent_count} recipient(s).'


def _send_email(subject, recipients, body_text=None, body_html=None,
                attachments=None, from_address=None):
    """
    General-purpose email sender. Dispatches to SMTP relay or direct MX
    based on the email_provider setting.

    Args:
        subject (str): Email subject
        recipients (list[str]): Recipient email addresses
        body_text (str|None): Plain text body
        body_html (str|None): HTML body
        attachments (list[dict]|None): Each dict: {'filename', 'data' (bytes), 'mimetype'}
        from_address (str|None): Override the configured from address

    Returns:
        (bool, str): (success, message)
    """
    provider = get_app_setting('email_provider', 'smtp')
    if provider == 'direct':
        return _send_email_direct(subject, recipients, body_text, body_html,
                                  attachments, from_address)
    return _send_email_smtp(subject, recipients, body_text, body_html,
                            attachments, from_address)


def _send_pdf_email(show_id, pdf_type, triggered_by, exported_by_id=None, days_before=None):
    """
    Build a PDF (advance or schedule) and email it to all report recipients.

    triggered_by : display string for the email body ('username' or 'system')
    exported_by_id : user.id to log in export_log (None for scheduled sends)
    days_before : int used for dedup key in email_send_log

    Returns (success: bool, message: str, recipient_count: int)
    """
    provider = get_app_setting('email_provider', 'smtp')
    if provider == 'smtp':
        smtp_cfg = _get_smtp_settings()
        if not smtp_cfg.get('smtp_host'):
            return False, 'SMTP not configured.', 0

    # Fetch recipients
    db = get_db()
    recipients = [
        r['email'] for r in
        db.execute(
            "SELECT email FROM contacts WHERE report_recipient=1 AND email != '' ORDER BY name"
        ).fetchall()
    ]
    db.close()

    if not recipients:
        return False, 'No report recipients configured.', 0

    # Build PDF bytes — run inside app context, no request context needed
    try:
        with app.app_context():
            if pdf_type == 'advance':
                _, _, show_dict, pdf_bytes = _build_advance_pdf(
                    show_id, exported_by_id=exported_by_id, base_url='/'
                )
            else:
                _, _, show_dict, pdf_bytes = _build_schedule_pdf(
                    show_id, exported_by_id=exported_by_id, base_url='/'
                )
    except Exception as e:
        app.logger.error(f'PDF build failed for email show={show_id} type={pdf_type}: {e}')
        return False, f'PDF generation failed: {e}', 0

    if not pdf_bytes:
        return False, 'PDF generation produced no output.', 0

    # Build email subject
    show_name  = show_dict.get('name', 'Show')
    show_date  = show_dict.get('show_date', '')
    venue      = show_dict.get('venue', '')
    pm_name    = show_dict.get('production_manager', '')
    if not pm_name:
        # Try pulling from advance_data
        try:
            _db2 = get_db()
            _row = _db2.execute(
                "SELECT field_value FROM advance_data WHERE show_id=? AND field_key='production_manager'",
                (show_id,)
            ).fetchone()
            _db2.close()
            pm_name = _row['field_value'] if _row else ''
        except Exception:
            pm_name = ''

    type_label  = 'Advance Sheet' if pdf_type == 'advance' else 'Production Schedule'
    subject_parts = ['3·2·1→Theater', type_label, show_name]
    if show_date:
        subject_parts.append(show_date)
    if venue:
        subject_parts.append(venue)
    if pm_name:
        subject_parts.append(f'PM: {pm_name}')
    subject = ' | '.join(subject_parts)

    # Email body
    if triggered_by == 'system':
        body_line = (f'This {type_label} was automatically generated and sent by 3·2·1→Theater '
                     f'on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}.')
    else:
        body_line = (f'This {type_label} was generated and sent by {triggered_by} '
                     f'on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}.')

    safe_show = show_name.replace(' ', '_').replace('/', '-')
    filename   = f"{type_label.replace(' ','_')}_{safe_show}_{show_date}.pdf"

    # Send email via configured provider (SMTP relay or direct MX)
    attachments = [{'filename': filename, 'data': pdf_bytes, 'mimetype': 'application/pdf'}]
    success, send_message = _send_email(
        subject=subject, recipients=recipients,
        body_text=body_line, attachments=attachments
    )
    if not success:
        app.logger.error(f'Email send failed show={show_id} type={pdf_type}: {send_message}')
        return False, send_message, 0

    # Log the send
    try:
        _db3 = get_db()
        _db3.execute("""
            INSERT INTO email_send_log
              (show_id, pdf_type, trigger_type, days_before, sent_by, recipient_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (show_id, pdf_type,
              'scheduled' if triggered_by == 'system' else 'manual',
              days_before, triggered_by, len(recipients)))
        _db3.commit()
        _db3.close()
    except Exception as e:
        app.logger.warning(f'email_send_log write failed: {e}')

    syslog_logger.info(
        f"PDF_EMAIL show_id={show_id} type={pdf_type} "
        f"recipients={len(recipients)} by={triggered_by}"
    )
    return True, f'Sent to {len(recipients)} recipient(s).', len(recipients)


def run_scheduled_pdf_emails():
    """
    APScheduler job: hourly tick.  Sends PDFs when the configured send hour
    matches the current hour and no send has been recorded for this show/type/day.
    """
    send_hour = int(get_app_setting('pdf_email_send_hour', '6'))
    if datetime.now().hour != send_hour:
        return

    today = date.today()
    today_str = today.isoformat()

    db = get_db()
    shows = db.execute(
        "SELECT id, name, show_date FROM shows WHERE status='active'"
    ).fetchall()

    # Pre-load first perf date per show
    perfs = db.execute(
        "SELECT show_id, MIN(perf_date) as first_perf FROM show_performances GROUP BY show_id"
    ).fetchall()
    first_perf = {r['show_id']: r['first_perf'] for r in perfs}

    # Already sent today (to avoid duplicate sends within the same day)
    sent_today = set()
    for r in db.execute(
        "SELECT show_id, pdf_type, days_before FROM email_send_log "
        "WHERE trigger_type='scheduled' AND DATE(sent_at)=?", (today_str,)
    ).fetchall():
        sent_today.add((r['show_id'], r['pdf_type'], r['days_before']))

    db.close()

    configs = [
        ('advance',  'advance_email_enabled',     'advance_email_days_before',  None),
        ('schedule', 'schedule_email_enabled_1',  'schedule_email_days_1',      None),
        ('schedule', 'schedule_email_enabled_2',  'schedule_email_days_2',      None),
    ]

    for show_row in shows:
        show_id   = show_row['id']
        perf_date_str = first_perf.get(show_id) or show_row['show_date']
        if not perf_date_str:
            continue
        try:
            perf_date = date.fromisoformat(perf_date_str)
        except ValueError:
            continue
        days_until = (perf_date - today).days

        for pdf_type, enabled_key, days_key, _ in configs:
            if get_app_setting(enabled_key, '0') not in ('1', 'true'):
                continue
            try:
                trigger_days = int(get_app_setting(days_key, '0'))
            except ValueError:
                continue
            if trigger_days <= 0:
                continue
            if days_until != trigger_days:
                continue
            if (show_id, pdf_type, trigger_days) in sent_today:
                continue

            ok, msg, _ = _send_pdf_email(
                show_id, pdf_type, 'system', days_before=trigger_days
            )
            app.logger.info(
                f'Scheduled PDF email show={show_id} type={pdf_type} '
                f'days_before={trigger_days}: {msg}'
            )


def start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(run_hourly_backup, 'interval', hours=1, id='hourly_backup')
        scheduler.add_job(run_daily_backup, 'cron', hour=0, minute=0, id='daily_backup')
        scheduler.add_job(run_scheduled_pdf_emails, 'interval', hours=1, id='pdf_email_check')
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


def log_audit(db, action, entity_type, entity_id=None, show_id=None,
              before=None, after=None, detail=None):
    """Write one row to audit_log. Never raises — audit failure must not block normal flow."""
    try:
        db.execute("""
            INSERT INTO audit_log
              (user_id, username, action, entity_type, entity_id,
               show_id, before_json, after_json, ip_address, detail)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            session.get('user_id'),
            session.get('username', ''),
            action,
            entity_type,
            str(entity_id) if entity_id is not None else None,
            show_id,
            json.dumps(before) if before is not None else None,
            json.dumps(after)  if after  is not None else None,
            request.remote_addr,
            detail,
        ))
    except Exception:
        pass


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
            # Regenerate session to prevent session fixation
            next_url = request.form.get('next') or url_for('dashboard')
            session.clear()
            session['user_id']        = user['id']
            session['username']       = user['username']
            session['display_name']   = user['display_name'] or user['username']
            session['user_role']      = user['role']
            session['theme']          = user['theme'] or 'dark'
            session['is_restricted']  = is_restricted_user(user['id'])
            session['is_content_admin'] = is_content_admin(user['id'])
            session['is_readonly'] = bool(user.get('is_readonly', 0))
            session['_role_checked_at'] = datetime.utcnow().timestamp()
            log_audit(db, 'LOGIN', 'user', user['id'], detail=username)
            db.commit()
            db.close()
            session.permanent = True
            syslog_logger.info(f"LOGIN user={username} ip={request.remote_addr}")
            # Prevent open redirect — only allow relative paths
            if not next_url or not next_url.startswith('/') or next_url.startswith('//'):
                next_url = url_for('dashboard')
            # Force password change if still using default
            try:
                _must_change = user['must_change_password']
            except (KeyError, IndexError):
                _must_change = False
            if _must_change:
                session['must_change_password'] = True
                return redirect(url_for('force_change_password'))
            return redirect(next_url)
        else:
            # Constant-time failure: always hash something to prevent user enumeration
            if not user:
                check_password_hash(
                    'scrypt:32768:8:1$dummy$0000000000000000000000000000000000000000000000000000000000000000',
                    password,
                )
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
    if session.get('user_id'):
        db = get_db()
        log_audit(db, 'LOGOUT', 'user', session['user_id'])
        db.commit()
        db.close()
    session.clear()
    return redirect(url_for('login'))


@app.route('/admin/view-as', methods=['POST'])
@login_required
def admin_view_as():
    """Admin-only: temporarily view the site as a different role."""
    if session.get('role') != 'admin' and not session.get('_real_role'):
        return jsonify({'error': 'Forbidden'}), 403
    # If already in view-as mode, restore real role first before switching
    real_role = session.get('_real_role', session.get('role'))
    if real_role != 'admin':
        return jsonify({'error': 'Forbidden'}), 403
    data = request.get_json() or {}
    view_as = data.get('role', '')
    if view_as not in ('user', 'readonly', 'content_admin'):
        return jsonify({'error': 'Invalid role'}), 400
    # Save real values if not already saved
    if '_real_role' not in session:
        session['_real_role'] = session.get('role')
        session['_real_is_readonly'] = session.get('is_readonly', False)
        session['_real_is_content_admin'] = session.get('is_content_admin', False)
    session['_view_as'] = view_as
    syslog_logger.info(f"ADMIN_VIEW_AS view_as={view_as} by={session.get('username')}")
    if view_as == 'readonly':
        session['role'] = 'user'
        session['is_readonly'] = True
        session['is_content_admin'] = False
    elif view_as == 'user':
        session['role'] = 'user'
        session['is_readonly'] = False
        session['is_content_admin'] = False
    elif view_as == 'content_admin':
        session['role'] = 'user'
        session['is_readonly'] = False
        session['is_content_admin'] = True
    return jsonify({'success': True, 'view_as': view_as})


@app.route('/admin/view-as/reset', methods=['POST'])
@login_required
def admin_view_as_reset():
    """Restore the admin's real role after view-as preview."""
    if '_real_role' not in session:
        return jsonify({'success': True})
    session['role'] = session.pop('_real_role')
    session['is_readonly'] = session.pop('_real_is_readonly', False)
    session['is_content_admin'] = session.pop('_real_is_content_admin', False)
    session.pop('_view_as', None)
    syslog_logger.info(f"ADMIN_VIEW_AS_RESET by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def force_change_password():
    """Force password change screen (shown after login when must_change_password is set)."""
    if not session.get('must_change_password'):
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        new_pw = request.form.get('new_password', '')
        confirm = request.form.get('confirm_password', '')
        if new_pw != confirm:
            flash('Passwords do not match.', 'error')
            return render_template('force_change_password.html', user=get_current_user())
        pw_err = _validate_password(new_pw)
        if pw_err:
            flash(pw_err, 'error')
            return render_template('force_change_password.html', user=get_current_user())
        db = get_db()
        db.execute('UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?',
                   (generate_password_hash(new_pw), session['user_id']))
        db.commit()
        db.close()
        session.pop('must_change_password', None)
        syslog_logger.info(f"FORCED_PASSWORD_CHANGE user_id={session['user_id']}")
        flash('Password changed successfully.', 'success')
        return redirect(url_for('dashboard'))
    return render_template('force_change_password.html', user=get_current_user())


@app.before_request
def _enforce_password_change():
    """Block all routes except logout/change-password if must_change_password is set."""
    if session.get('must_change_password'):
        allowed = ('force_change_password', 'logout', 'static')
        if request.endpoint not in allowed:
            return redirect(url_for('force_change_password'))


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

    _eff_date = """COALESCE(s.show_date,
        (SELECT MIN(perf_date) FROM show_performances
         WHERE show_id=s.id AND perf_date IS NOT NULL))"""

    if accessible is None:
        active = db.execute(f"""
            SELECT s.*, u.display_name as creator,
              (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count,
              {_eff_date} as show_date
            FROM shows s LEFT JOIN users u ON s.created_by = u.id
            WHERE s.status = 'active'
            ORDER BY {_eff_date} ASC NULLS LAST
        """).fetchall()
        archived = db.execute(f"""
            SELECT s.*, u.display_name as creator,
              (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count,
              {_eff_date} as show_date
            FROM shows s LEFT JOIN users u ON s.created_by = u.id
            WHERE s.status = 'archived'
            ORDER BY {_eff_date} DESC
            LIMIT 30
        """).fetchall()
    else:
        if accessible:
            placeholders = ','.join('?' * len(accessible))
            active = db.execute(f"""
                SELECT s.*, u.display_name as creator,
                  (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count,
                  {_eff_date} as show_date
                FROM shows s LEFT JOIN users u ON s.created_by = u.id
                WHERE s.status = 'active' AND s.id IN ({placeholders})
                ORDER BY {_eff_date} ASC NULLS LAST
            """, accessible).fetchall()
            archived = db.execute(f"""
                SELECT s.*, u.display_name as creator,
                  (SELECT COUNT(*) FROM show_performances WHERE show_id=s.id) as perf_count,
                  {_eff_date} as show_date
                FROM shows s LEFT JOIN users u ON s.created_by = u.id
                WHERE s.status = 'archived' AND s.id IN ({placeholders})
                ORDER BY {_eff_date} DESC LIMIT 30
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
                           motd_messages=get_active_messages(session.get('user_id'), 'motd'),
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

        log_audit(db, 'SHOW_CREATE', 'show', show_id, show_id=show_id,
                  after={'name': name, 'show_date': show_date, 'venue': venue})
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

    # Global WiFi for schedule display (no longer a per-show editable field)
    global_wifi_network  = get_app_setting('wifi_network', '')
    global_wifi_password = get_app_setting('wifi_password', '')

    # Schedule templates for the schedule tab
    db2 = get_db()
    sched_templates = [dict(r) for r in db2.execute(
        'SELECT id, name FROM schedule_templates ORDER BY sort_order, name'
    ).fetchall()]

    # Labor requests for this show
    labor_rows = db2.execute("""
        SELECT lr.*, jp.name as position_name
        FROM labor_requests lr
        LEFT JOIN job_positions jp ON lr.position_id = jp.id
        WHERE lr.show_id = ?
        ORDER BY lr.sort_order, lr.id
    """, (show_id,)).fetchall()
    labor_requests_data = [dict(r) for r in labor_rows]

    # Asset categories (for the Assets tab)
    asset_cats = db2.execute('SELECT * FROM asset_categories ORDER BY sort_order, name').fetchall()
    asset_categories_for_tab = [dict(c) for c in asset_cats]
    db2.close()

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
                           global_wifi_network=global_wifi_network,
                           global_wifi_password=global_wifi_password,
                           sched_templates=sched_templates,
                           labor_requests_data=labor_requests_data,
                           asset_categories=asset_categories_for_tab,
                           is_content_admin_user=session.get('is_content_admin', False),
                           ollama_enabled=get_app_setting('ollama_enabled', '0') == '1',
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
    log_audit(db, 'FORM_SAVE', 'form', show_id, show_id=show_id, detail='type=advance')

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
            perf_id = row.get('perf_id')  # None for single-day / first day
            db.execute("""
                INSERT INTO schedule_rows (show_id, perf_id, sort_order, start_time, end_time, description, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (show_id, perf_id, i,
                  row.get('start_time', ''), row.get('end_time', ''),
                  row.get('description', ''), row.get('notes', '')))

    db.execute('UPDATE shows SET updated_at=CURRENT_TIMESTAMP WHERE id=?', (show_id,))
    db.execute("""
        UPDATE shows SET last_saved_by=?, last_saved_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session['user_id'], show_id))

    _snapshot_form_history(db, show_id, 'schedule', data)
    log_audit(db, 'FORM_SAVE', 'form', show_id, show_id=show_id, detail='type=schedule')

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
    log_audit(db, 'FORM_SAVE', 'form', show_id, show_id=show_id, detail='type=postnotes')

    db.commit()
    db.close()
    syslog_logger.info(f"FORM_SAVE show_id={show_id} type=postnotes by={session.get('username')}")
    return jsonify({'success': True})


# ─── Version History ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/history/<form_type>')
@login_required
def form_history_list(show_id, form_type):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    db = get_db()
    entry = db.execute(
        'SELECT * FROM form_history WHERE id=? AND show_id=?', (hist_id, show_id)
    ).fetchone()
    db.close()
    if not entry:
        return jsonify({'success': False, 'error': 'Snapshot not found.'}), 404
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
        return jsonify({'success': False, 'error': 'Snapshot not found.'}), 404

    snapshot = json.loads(entry['snapshot_json'])
    form_type = entry['form_type']

    # Check for newer snapshots — warn the user before overwriting
    force = request.args.get('force') == '1'
    if not force:
        newer = db.execute("""
            SELECT fh.id, fh.saved_at, u.username, u.display_name, fh.snapshot_json
            FROM form_history fh
            LEFT JOIN users u ON fh.saved_by = u.id
            WHERE fh.show_id=? AND fh.form_type=? AND fh.id > ?
            ORDER BY fh.saved_at DESC LIMIT 1
        """, (show_id, form_type, hist_id)).fetchone()
        if newer:
            db.close()
            return jsonify({
                'conflict':          True,
                'newer_saved_at':    newer['saved_at'],
                'newer_saved_by':    newer['display_name'] or newer['username'] or 'Unknown',
                'restoring_snapshot': snapshot,
                'current_snapshot':  json.loads(newer['snapshot_json']),
            }), 409

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
    log_audit(db, 'HISTORY_RESTORE', 'form', hist_id, show_id=show_id,
              detail=f'type={form_type}')
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    is_admin = session.get('user_role') == 'admin'
    db = get_db()
    rows = db.execute("""
        SELECT sc.id, sc.body, sc.created_at, sc.edited_at, sc.deleted_at,
               u.display_name, u.username, u.id as uid,
               du.display_name as deleted_by_name, du.username as deleted_by_username
        FROM show_comments sc
        JOIN users u ON sc.user_id = u.id
        LEFT JOIN users du ON sc.deleted_by = du.id
        WHERE sc.show_id = ? AND (sc.deleted_at IS NULL OR ?)
        ORDER BY sc.created_at ASC
    """, (show_id, is_admin)).fetchall()
    db.close()
    result = []
    for r in rows:
        author = r['display_name'] or r['username']
        entry = {
            'id':          r['id'],
            'body':        r['body'],
            'created_at':  r['created_at'],
            'edited_at':   r['edited_at'],
            'deleted_at':  r['deleted_at'],
            'author':      author,
            'author_id':   r['uid'],
            'initials':    ''.join(w[0].upper() for w in author.split()[:2]),
            'is_own':      r['uid'] == session['user_id'],
        }
        if is_admin and r['deleted_at']:
            entry['deleted_by'] = r['deleted_by_name'] or r['deleted_by_username'] or 'Unknown'
        result.append(entry)
    return jsonify(result)


@app.route('/shows/<int:show_id>/comments', methods=['POST'])
@login_required
def post_comment(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
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
    log_audit(db, 'COMMENT_POST', 'comment', cid, show_id=show_id,
              after={'body': body})
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    db = get_db()
    comment = db.execute(
        'SELECT * FROM show_comments WHERE id=? AND show_id=? AND deleted_at IS NULL',
        (cid, show_id)
    ).fetchone()
    if not comment:
        db.close()
        return jsonify({'success': False, 'error': 'Comment not found.'}), 404
    if comment['user_id'] != session['user_id'] and session.get('user_role') != 'admin':
        db.close()
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    log_audit(db, 'COMMENT_DELETE', 'comment', cid, show_id=show_id,
              before={'body': comment['body']})
    db.execute(
        'UPDATE show_comments SET deleted_at=CURRENT_TIMESTAMP, deleted_by=? WHERE id=?',
        (session['user_id'], cid)
    )
    db.commit()
    db.close()
    syslog_logger.info(f"COMMENT_DELETE show_id={show_id} comment_id={cid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/comments/<int:cid>', methods=['PUT'])
@login_required
def edit_comment(show_id, cid):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    data = request.get_json(force=True) or {}
    new_body = data.get('body', '').strip()
    if not new_body:
        return jsonify({'success': False, 'error': 'Comment cannot be empty.'}), 400
    if len(new_body) > 2000:
        return jsonify({'success': False, 'error': 'Comment too long (max 2000 chars).'}), 400
    db = get_db()
    comment = db.execute(
        'SELECT * FROM show_comments WHERE id=? AND show_id=? AND deleted_at IS NULL',
        (cid, show_id)
    ).fetchone()
    if not comment:
        db.close()
        return jsonify({'success': False, 'error': 'Comment not found.'}), 404
    if comment['user_id'] != session['user_id'] and session.get('user_role') != 'admin':
        db.close()
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    old_body = comment['body']
    # Save previous version
    db.execute(
        'INSERT INTO comment_versions (comment_id, body, edited_by) VALUES (?,?,?)',
        (cid, old_body, session['user_id'])
    )
    db.execute(
        'UPDATE show_comments SET body=?, edited_at=CURRENT_TIMESTAMP WHERE id=?',
        (new_body, cid)
    )
    log_audit(db, 'COMMENT_EDIT', 'comment', cid, show_id=show_id,
              before={'body': old_body}, after={'body': new_body})
    db.commit()
    db.close()
    syslog_logger.info(f"COMMENT_EDIT show_id={show_id} comment_id={cid} by={session.get('username')}")
    return jsonify({'success': True, 'body': new_body})


@app.route('/shows/<int:show_id>/comments/<int:cid>/restore', methods=['POST'])
@admin_required
def restore_comment(show_id, cid):
    db = get_db()
    comment = db.execute(
        'SELECT * FROM show_comments WHERE id=? AND show_id=? AND deleted_at IS NOT NULL',
        (cid, show_id)
    ).fetchone()
    if not comment:
        db.close()
        return jsonify({'success': False, 'error': 'Comment not found.'}), 404
    db.execute(
        'UPDATE show_comments SET deleted_at=NULL, deleted_by=NULL WHERE id=?', (cid,)
    )
    log_audit(db, 'COMMENT_RESTORE', 'comment', cid, show_id=show_id)
    db.commit()
    db.close()
    syslog_logger.info(f"COMMENT_RESTORE show_id={show_id} comment_id={cid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/comments/<int:cid>/versions', methods=['GET'])
@admin_required
def comment_versions_list(show_id, cid):
    db = get_db()
    rows = db.execute("""
        SELECT cv.id, cv.body, cv.edited_at,
               u.display_name, u.username
        FROM comment_versions cv
        LEFT JOIN users u ON cv.edited_by = u.id
        WHERE cv.comment_id = ?
        ORDER BY cv.edited_at DESC
    """, (cid,)).fetchall()
    db.close()
    return jsonify([{
        'id':        r['id'],
        'body':      r['body'],
        'edited_at': r['edited_at'],
        'edited_by': r['display_name'] or r['username'] or 'Unknown',
    } for r in rows])


@app.route('/shows/<int:show_id>/comments/<int:cid>/versions/<int:vid>/restore', methods=['POST'])
@admin_required
def comment_version_restore(show_id, cid, vid):
    db = get_db()
    version = db.execute(
        'SELECT * FROM comment_versions WHERE id=? AND comment_id=?', (vid, cid)
    ).fetchone()
    if not version:
        db.close()
        return jsonify({'success': False, 'error': 'Version not found.'}), 404
    comment = db.execute(
        'SELECT body FROM show_comments WHERE id=? AND show_id=?', (cid, show_id)
    ).fetchone()
    if not comment:
        db.close()
        return jsonify({'success': False, 'error': 'Comment not found.'}), 404
    old_body = comment['body']
    # Save current as a version before restoring
    db.execute(
        'INSERT INTO comment_versions (comment_id, body, edited_by) VALUES (?,?,?)',
        (cid, old_body, session['user_id'])
    )
    db.execute(
        'UPDATE show_comments SET body=?, edited_at=CURRENT_TIMESTAMP WHERE id=?',
        (version['body'], cid)
    )
    log_audit(db, 'COMMENT_VERSION_RESTORE', 'comment', cid, show_id=show_id,
              before={'body': old_body}, after={'body': version['body']},
              detail=f'version_id={vid}')
    db.commit()
    db.close()
    syslog_logger.info(f"COMMENT_VERSION_RESTORE show_id={show_id} comment_id={cid} version_id={vid} by={session.get('username')}")
    return jsonify({'success': True, 'body': version['body']})


# ─── File Attachments ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/attachments', methods=['GET'])
@login_required
def get_attachments(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
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
    log_audit(db, 'FILE_UPLOAD', 'attachment', aid, show_id=show_id, detail=filename)
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
    resp.headers['Content-Disposition'] = _safe_content_disposition(row['filename'])
    return resp


@app.route('/shows/<int:show_id>/attachments/<int:aid>/delete', methods=['POST'])
@login_required
def delete_attachment(show_id, aid):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    db = get_db()
    row = db.execute(
        'SELECT * FROM show_attachments WHERE id=? AND show_id=?', (aid, show_id)
    ).fetchone()
    if not row:
        db.close()
        return jsonify({'success': False, 'error': 'File not found.'}), 404
    if row['uploaded_by'] != session['user_id'] and session.get('user_role') != 'admin':
        db.close()
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    log_audit(db, 'FILE_DELETE', 'attachment', aid, show_id=show_id,
              detail=row['filename'] if row else str(aid))
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    db = get_db()
    show = db.execute('SELECT advance_version FROM shows WHERE id=?', (show_id,)).fetchone()
    if not show:
        db.close()
        return jsonify({'success': False, 'error': 'Show not found.'}), 404
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403

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
        return jsonify({'success': False, 'error': 'Access denied.'}), 403

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

def _build_advance_pdf(show_id, exported_by_id=None, base_url=None):
    """
    Build the advance PDF.  Works both in a request context (base_url=None reads
    from request.url_root) and in a background context (pass base_url='/').
    exported_by_id defaults to session['user_id'] when not supplied.
    """
    if exported_by_id is None:
        exported_by_id = session.get('user_id')
    if base_url is None:
        base_url = request.url_root

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
                  VALUES (?, 'advance', ?, ?)""", (show_id, new_v, exported_by_id))
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
        pdf_bytes = WP_HTML(string=html, base_url=base_url).write_pdf()
        db2 = get_db()
        db2.execute('UPDATE export_log SET pdf_data=? WHERE id=?', (pdf_bytes, log_id))
        db2.commit()
        db2.close()
    except Exception:
        pdf_bytes = None

    syslog_logger.info(
        f"PDF_EXPORT show_id={show_id} type=advance v={new_v} by={exported_by_id}"
    )
    return html, new_v, dict(show), pdf_bytes


def _build_schedule_pdf(show_id, exported_by_id=None, base_url=None):
    """
    Build the schedule PDF.  Works both in a request context (base_url=None reads
    from request.url_root) and in a background context (pass base_url='/').
    exported_by_id defaults to session['user_id'] when not supplied.
    """
    if exported_by_id is None:
        exported_by_id = session.get('user_id')
    if base_url is None:
        base_url = request.url_root

    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id = ?', (show_id,)).fetchone()
    all_sched_rows = db.execute(
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
    performances = [dict(p) for p in db.execute(
        'SELECT * FROM show_performances WHERE show_id=? ORDER BY sort_order, perf_date, id', (show_id,)
    ).fetchall()]
    contacts = db.execute('SELECT * FROM contacts ORDER BY name').fetchall()
    contact_map = {c['id']: dict(c) for c in contacts}

    logo_data = get_app_setting('logo_data', '')

    # WiFi always from global settings (not per-show)
    wifi_ssid = get_app_setting('wifi_network', '')
    wifi_pass  = get_app_setting('wifi_password', '')
    wifi_qr_b64 = None
    if wifi_ssid:
        try:
            import qrcode, io, base64
            qr = qrcode.make(f"WIFI:S:{wifi_ssid};T:WPA;P:{wifi_pass};;")
            buf = io.BytesIO()
            qr.save(buf, format='PNG')
            wifi_qr_b64 = base64.b64encode(buf.getvalue()).decode()
        except Exception:
            pass

    # Group schedule rows by perf_id; NULL rows go to the first performance
    rows_by_perf = {}
    for row in all_sched_rows:
        pid = row['perf_id']
        rows_by_perf.setdefault(pid, []).append(dict(row))

    # Build per-day data for the PDF template
    schedule_days = []
    for i, p in enumerate(performances):
        day_rows = rows_by_perf.get(p['id'], [])
        if i == 0:  # First day absorbs any legacy NULL-keyed rows
            day_rows = rows_by_perf.get(None, []) + day_rows
        schedule_days.append({'perf': p, 'rows': day_rows, 'day_num': i + 1})

    if not schedule_days:  # Fallback: show with no performances recorded
        schedule_days = [{'perf': {'perf_date': show['show_date'], 'perf_time': show['show_time']},
                          'rows': rows_by_perf.get(None, []), 'day_num': 1}]

    # Crew call times — unique in_times from labor_requests, sorted
    labor_in_times = db.execute(
        "SELECT in_time FROM labor_requests WHERE show_id=? AND in_time != '' ORDER BY in_time",
        (show_id,)
    ).fetchall()
    crew_call_times = sorted(set(r['in_time'] for r in labor_in_times if r['in_time']))

    new_v = (show['schedule_version'] or 0) + 1
    db.execute('UPDATE shows SET schedule_version=? WHERE id=?', (new_v, show_id))
    log_cur = db.execute("""INSERT INTO export_log (show_id, export_type, version, exported_by)
                  VALUES (?, 'schedule', ?, ?)""", (show_id, new_v, exported_by_id))
    log_id = log_cur.lastrowid
    db.commit()
    db.close()

    html = render_template('pdf/schedule_pdf.html',
                           show=show,
                           schedule_days=schedule_days,
                           schedule_meta=schedule_meta,
                           sched_meta_fields=get_schedule_meta_fields(),
                           advance_data=advance_data,
                           contact_map=contact_map,
                           logo_data=logo_data,
                           wifi_ssid=wifi_ssid,
                           wifi_pass=wifi_pass,
                           wifi_qr_b64=wifi_qr_b64,
                           crew_call_times=crew_call_times,
                           version=new_v,
                           export_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'))

    # Store PDF bytes in export_log for re-download
    try:
        from weasyprint import HTML as WP_HTML
        pdf_bytes = WP_HTML(string=html, base_url=base_url).write_pdf()
        db2 = get_db()
        db2.execute('UPDATE export_log SET pdf_data=? WHERE id=?', (pdf_bytes, log_id))
        db2.commit()
        db2.close()
    except Exception:
        pdf_bytes = None

    syslog_logger.info(
        f"PDF_EXPORT show_id={show_id} type=schedule v={new_v} by={exported_by_id}"
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
        resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
        return resp
    # Fallback to HTML if weasyprint failed
    try:
        from weasyprint import HTML
        pdf = HTML(string=html, base_url=request.url_root).write_pdf()
        resp = make_response(pdf)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
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
        resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
        return resp
    try:
        from weasyprint import HTML
        pdf = HTML(string=html, base_url=request.url_root).write_pdf()
        resp = make_response(pdf)
        resp.headers['Content-Type'] = 'application/pdf'
        resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
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
    resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
    return resp


@app.route('/shows/<int:show_id>/email/<pdf_type>', methods=['POST'])
@login_required
def email_pdf(show_id, pdf_type):
    """Manually trigger a PDF email for advance or schedule."""
    if pdf_type not in ('advance', 'schedule'):
        return jsonify({'success': False, 'error': 'Invalid PDF type.'}), 400
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    get_show_or_404(show_id)
    triggered_by = session.get('username') or session.get('user_display') or 'user'
    ok, msg, count = _send_pdf_email(
        show_id, pdf_type, triggered_by,
        exported_by_id=session.get('user_id')
    )
    if ok:
        _adb = get_db()
        log_audit(_adb, f'PDF_EMAIL_{pdf_type.upper()}', 'show', show_id,
                  show_id=show_id, detail=f'Manual email to {count} recipient(s)')
        _adb.commit()
        _adb.close()
    return jsonify({'success': ok, 'message': msg, 'recipients': count})


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
        resp.headers['Content-Disposition'] = _safe_content_disposition(filename)
        return resp
    except Exception:
        resp = make_response(html)
        resp.headers['Content-Type'] = 'text/html'
        return resp


# ─── Show Management ──────────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/archive', methods=['POST'])
@staff_or_admin_required
def archive_show(show_id):
    if session.get('user_role') != 'admin' and not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    db.execute("UPDATE shows SET status='archived' WHERE id=?", (show_id,))
    log_audit(db, 'SHOW_ARCHIVE', 'show', show_id, show_id=show_id)
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_ARCHIVE show_id={show_id} by={session.get('username')}")
    flash('Show archived.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/shows/<int:show_id>/restore', methods=['POST'])
@staff_or_admin_required
def restore_show(show_id):
    if session.get('user_role') != 'admin' and not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    db.execute("UPDATE shows SET status='active' WHERE id=?", (show_id,))
    log_audit(db, 'SHOW_RESTORE', 'show', show_id, show_id=show_id)
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_RESTORE show_id={show_id} by={session.get('username')}")
    flash('Show restored to active.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/shows/<int:show_id>/delete', methods=['POST'])
@admin_required
def delete_show(show_id):
    db = get_db()
    show = db.execute('SELECT name FROM shows WHERE id=?', (show_id,)).fetchone()
    show_name = show['name'] if show else str(show_id)
    for tbl in ['advance_data', 'schedule_rows', 'schedule_meta',
                'post_show_notes', 'export_log', 'form_history', 'show_group_access']:
        db.execute(f'DELETE FROM {tbl} WHERE show_id=?', (show_id,))
    db.execute('DELETE FROM shows WHERE id=?', (show_id,))
    log_audit(db, 'SHOW_DELETE', 'show', show_id, detail=show_name)
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
    log_audit(db, 'SHOW_ACCESS_ADD', 'show', show_id, show_id=show_id,
              detail=f'group_id={group_id}')
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
    log_audit(db, 'SHOW_ACCESS_REMOVE', 'show', show_id, show_id=show_id,
              detail=f'group_id={group_id}')
    db.commit(); db.close()
    syslog_logger.info(f"SHOW_ACCESS_REMOVE show_id={show_id} group_id={group_id} by={session.get('username')}")
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


# ─── Audit Log ────────────────────────────────────────────────────────────────

@app.route('/admin/audit')
@admin_required
def audit_log_view():
    db = get_db()
    page = max(1, int(request.args.get('page', 1)))
    per_page = 50
    offset = (page - 1) * per_page

    filters = []
    params = []
    if request.args.get('user_id'):
        try:
            filters.append('al.user_id = ?')
            params.append(int(request.args['user_id']))
        except ValueError:
            pass
    if request.args.get('show_id'):
        try:
            filters.append('al.show_id = ?')
            params.append(int(request.args['show_id']))
        except ValueError:
            pass
    if request.args.get('action'):
        filters.append('al.action LIKE ?')
        params.append(f"%{request.args['action'].upper()}%")
    if request.args.get('date_from'):
        filters.append('al.timestamp >= ?')
        params.append(request.args['date_from'])
    if request.args.get('date_to'):
        filters.append('al.timestamp <= ?')
        params.append(request.args['date_to'] + ' 23:59:59')

    where = ('WHERE ' + ' AND '.join(filters)) if filters else ''

    total_row = db.execute(
        f'SELECT COUNT(*) FROM audit_log al {where}', params
    ).fetchone()
    total = total_row[0] if total_row else 0

    rows = db.execute(f"""
        SELECT al.id, al.timestamp, al.username, al.action, al.entity_type,
               al.entity_id, al.show_id, al.before_json, al.after_json,
               al.ip_address, al.detail,
               s.name as show_name,
               u.display_name as display_name
        FROM audit_log al
        LEFT JOIN shows s ON al.show_id = s.id
        LEFT JOIN users u ON al.user_id = u.id
        {where}
        ORDER BY al.timestamp DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, offset]).fetchall()

    users = db.execute(
        'SELECT id, username, display_name FROM users ORDER BY username'
    ).fetchall()
    shows = db.execute(
        'SELECT id, name FROM shows ORDER BY name'
    ).fetchall()
    db.close()

    entries = []
    for r in rows:
        entry = dict(r)
        entry['display_name'] = r['display_name'] or r['username']
        entries.append(entry)

    return render_template('audit_log.html',
        entries=entries,
        users=users,
        shows=shows,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=max(1, (total + per_page - 1) // per_page),
        filters=request.args,
        user=get_current_user(),
    )


# ─── Settings ─────────────────────────────────────────────────────────────────

@app.route('/settings')
@login_required
def settings():
    db = get_db()
    contacts = db.execute('SELECT * FROM contacts ORDER BY department, name').fetchall()
    users    = db.execute(
        'SELECT id, username, display_name, role, created_at, is_readonly FROM users ORDER BY display_name'
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

    db3 = get_db()
    sched_templates = [dict(t) for t in db3.execute(
        'SELECT id, name FROM schedule_templates ORDER BY sort_order, name'
    ).fetchall()] if _is_ca else []

    # Job positions data for settings tab
    position_categories = [dict(c) for c in db3.execute(
        'SELECT * FROM position_categories ORDER BY sort_order, id'
    ).fetchall()] if _is_ca else []
    positions_raw = db3.execute(
        'SELECT jp.*, pc.name as category_name FROM job_positions jp LEFT JOIN position_categories pc ON jp.category_id = pc.id ORDER BY pc.sort_order, jp.sort_order, jp.id'
    ).fetchall() if _is_ca else []
    job_positions = [dict(p) for p in positions_raw]

    # Crew members
    crew_members_list = [dict(m) for m in db3.execute(
        'SELECT * FROM crew_members ORDER BY sort_order, name'
    ).fetchall()] if _is_ca else []
    db3.close()

    db_settings = {
        'db_type':   all_settings.get('db_type', 'sqlite'),
        'pg_host':   all_settings.get('pg_host', 'localhost'),
        'pg_port':   all_settings.get('pg_port', '5432'),
        'pg_dbname': all_settings.get('pg_dbname', 'showadvance'),
        'pg_user':   all_settings.get('pg_user', ''),
        'pg_schema': all_settings.get('pg_schema', 'showadvance'),
    }
    ai_settings = {
        'ollama_enabled': all_settings.get('ollama_enabled', '0'),
        'ollama_url':     all_settings.get('ollama_url', 'http://localhost:11434'),
        'ollama_model':   all_settings.get('ollama_model', 'llama3.2'),
    }
    _is_admin = session.get('user_role') == 'admin'
    smtp_settings = {
        'smtp_host':  all_settings.get('smtp_host', ''),
        'smtp_port':  all_settings.get('smtp_port', '587'),
        'smtp_user':  all_settings.get('smtp_user', ''),
        'smtp_pass':  all_settings.get('smtp_pass', '') if _is_admin else '',
        'smtp_from':  all_settings.get('smtp_from', ''),
        'smtp_tls':   all_settings.get('smtp_tls', '1'),
    }
    email_provider_settings = {
        'email_provider':        all_settings.get('email_provider', 'smtp'),
        'direct_ehlo_hostname':  all_settings.get('direct_ehlo_hostname', ''),
        'direct_display_name':   all_settings.get('direct_display_name', ''),
    }
    pdf_email_settings = {
        'pdf_email_send_hour':       all_settings.get('pdf_email_send_hour', '6'),
        'advance_email_enabled':     all_settings.get('advance_email_enabled', '0'),
        'advance_email_days_before': all_settings.get('advance_email_days_before', '7'),
        'schedule_email_enabled_1':  all_settings.get('schedule_email_enabled_1', '0'),
        'schedule_email_days_1':     all_settings.get('schedule_email_days_1', '10'),
        'schedule_email_enabled_2':  all_settings.get('schedule_email_enabled_2', '0'),
        'schedule_email_days_2':     all_settings.get('schedule_email_days_2', '1'),
    }

    # Strip sensitive keys from syslog_settings for non-admin users
    safe_settings = all_settings if _is_admin else {
        k: v for k, v in all_settings.items()
        if k not in ('smtp_pass', 'pg_password', 'wifi_password')
    }

    # Pending registrations for admin approval panel
    pending_regs = []
    if _is_admin:
        db4 = get_db()
        pending_regs = [dict(r) for r in db4.execute("""
            SELECT id, username, display_name, email, created_at, email_confirmed
            FROM user_pending_registration
            ORDER BY created_at
        """).fetchall()]
        db4.close()

    return render_template('settings.html',
                           contacts=contacts,
                           users=users,
                           groups=groups_data,
                           form_sections=form_sections,
                           sched_meta_fields=sched_meta_fields,
                           syslog_settings=safe_settings,
                           db_settings=db_settings if _is_admin else {},
                           ai_settings=ai_settings,
                           departments=DEPARTMENTS,
                           is_content_admin=_is_ca,
                           sched_templates=sched_templates,
                           position_categories=position_categories,
                           job_positions=job_positions,
                           crew_members_list=crew_members_list,
                           wifi_network=all_settings.get('wifi_network', ''),
                           wifi_password=all_settings.get('wifi_password', ''),
                           upload_max_mb=all_settings.get('upload_max_mb', '20'),
                           logo_data=all_settings.get('logo_data', ''),
                           smtp_settings=smtp_settings,
                           email_provider_settings=email_provider_settings,
                           pdf_email_settings=pdf_email_settings,
                           pending_regs=pending_regs,
                           ai_max_sessions=all_settings.get('ai_max_sessions', '2'),
                           user=get_current_user())


@app.route('/settings/contacts/add', methods=['POST'])
@content_admin_required
def add_contact():
    db = get_db()
    name = request.form.get('name','').strip()
    cur = db.execute("""
        INSERT INTO contacts (name, title, department, phone, email, report_recipient)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name,
          request.form.get('title','').strip(),
          request.form.get('department','').strip(),
          request.form.get('phone','').strip(),
          request.form.get('email','').strip(),
          1 if request.form.get('report_recipient') else 0))
    cid_new = cur.lastrowid
    log_audit(db, 'CONTACT_ADD', 'contact', cid_new, detail=name)
    db.commit(); db.close()
    syslog_logger.info(f"CONTACT_ADD id={cid_new} name={name!r} by={session.get('username')}")
    flash('Contact added.', 'success')
    return redirect(url_for('settings') + '#contacts')


@app.route('/settings/contacts/<int:cid>/edit', methods=['POST'])
@content_admin_required
def edit_contact(cid):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE contacts SET name=?, title=?, department=?, phone=?, email=?,
                            report_recipient=?
        WHERE id=?
    """, (data.get('name',''), data.get('title',''), data.get('department',''),
          data.get('phone',''), data.get('email',''),
          1 if data.get('report_recipient') else 0, cid))
    log_audit(db, 'CONTACT_EDIT', 'contact', cid, detail=data.get('name',''))
    db.commit(); db.close()
    syslog_logger.info(f"CONTACT_EDIT id={cid} name={data.get('name','')!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/contacts/<int:cid>/delete', methods=['POST'])
@content_admin_required
def delete_contact(cid):
    db = get_db()
    row = db.execute('SELECT name FROM contacts WHERE id=?', (cid,)).fetchone()
    log_audit(db, 'CONTACT_DELETE', 'contact', cid, detail=row['name'] if row else str(cid))
    db.execute('DELETE FROM contacts WHERE id=?', (cid,))
    db.commit(); db.close()
    syslog_logger.info(f"CONTACT_DELETE id={cid} by={session.get('username')}")
    return jsonify({'success': True})


_MIN_PASSWORD_LENGTH = 8


def _validate_password(pw):
    """Return None if password is acceptable, else an error string."""
    if not pw or len(pw) < _MIN_PASSWORD_LENGTH:
        return f'Password must be at least {_MIN_PASSWORD_LENGTH} characters.'
    return None


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
    pw_err = _validate_password(password)
    if pw_err:
        flash(pw_err, 'error')
        return redirect(url_for('settings') + '#users')
    email    = request.form.get('email','').strip()
    is_readonly = 1 if request.form.get('is_readonly') else 0
    db = get_db()
    try:
        cur = db.execute("""INSERT INTO users (username, password_hash, display_name, role, email, is_readonly)
                      VALUES (?, ?, ?, ?, ?, ?)""",
                   (username, generate_password_hash(password), display, role, email, is_readonly))
        log_audit(db, 'USER_CREATE', 'user', cur.lastrowid, detail=f'{username} role={role}')
        db.commit()
        flash(f'User "{username}" created.', 'success')
        syslog_logger.info(f"USER_CREATE username={username} role={role} by={session.get('username')}")
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
    row = db.execute('SELECT username FROM users WHERE id=?', (uid,)).fetchone()
    log_audit(db, 'USER_DELETE', 'user', uid, detail=row['username'] if row else str(uid))
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
    pw_err = _validate_password(pw)
    if pw_err:
        return jsonify({'success': False, 'error': pw_err})
    db = get_db()
    db.execute('UPDATE users SET password_hash=? WHERE id=?', (generate_password_hash(pw), uid))
    log_audit(db, 'USER_PASSWORD_RESET', 'user', uid, detail=f'reset by {session.get("username")}')
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
    pw_err = _validate_password(new_pw)
    if pw_err:
        db.close()
        return jsonify({'success': False, 'error': pw_err})
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
        log_audit(db, 'GROUP_CREATE', 'group', gid, detail=name)
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
    log_audit(db, 'GROUP_EDIT', 'group', gid, detail=data.get('name',''))
    db.commit(); db.close()
    syslog_logger.info(f"GROUP_EDIT id={gid} name={data.get('name','')!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/groups/<int:gid>/delete', methods=['POST'])
@admin_required
def delete_group(gid):
    db = get_db()
    row = db.execute('SELECT name FROM user_groups WHERE id=?', (gid,)).fetchone()
    log_audit(db, 'GROUP_DELETE', 'group', gid, detail=row['name'] if row else str(gid))
    db.execute('DELETE FROM user_groups WHERE id=?', (gid,))
    db.commit(); db.close()
    syslog_logger.info(f"GROUP_DELETE id={gid} by={session.get('username')}")
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
    log_audit(db, 'GROUP_MEMBER_ADD', 'group', gid, detail=f'user_id={uid}')

    # Refresh the affected user's is_restricted in their session (best effort)
    # Session is server-side cookie; we can't update other sessions directly.
    # User will see updated access on next login.

    db.commit()
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
    log_audit(db, 'GROUP_MEMBER_REMOVE', 'group', gid, detail=f'user_id={uid}')
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

    db4 = get_db()
    sched_templates2 = [dict(t) for t in db4.execute(
        'SELECT id, name FROM schedule_templates ORDER BY sort_order, name'
    ).fetchall()] if _is_ca else []
    db4.close()

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
                           sched_templates=sched_templates2,
                           wifi_network=all_settings.get('wifi_network', ''),
                           wifi_password=all_settings.get('wifi_password', ''),
                           upload_max_mb=all_settings.get('upload_max_mb', '20'),
                           logo_data=all_settings.get('logo_data', ''),
                           db_settings=all_settings,
                           ai_settings=all_settings,
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
             help_text, placeholder, width_hint, is_notes_field, ai_hint)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (section_id, field_key, label,
              data.get('field_type','text'), max_order + 10,
              options_json,
              data.get('contact_dept'),
              data.get('conditional_show_when'),
              data.get('help_text'),
              data.get('placeholder',''),
              data.get('width_hint','full'),
              1 if data.get('is_notes_field') else 0,
              data.get('ai_hint') or None))
        fid = cur.lastrowid
        log_audit(db, 'FIELD_ADD', 'form_field', fid, detail=field_key)
        db.commit()
        syslog_logger.info(f"FIELD_ADD key={field_key} by={session.get('username')}")
        return jsonify({'success': True, 'id': fid})
    except (sqlite3.IntegrityError, DBIntegrityError):
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
            help_text=?, placeholder=?, width_hint=?, is_notes_field=?, ai_hint=?
        WHERE id=?
    """, (data.get('section_id'), data.get('label',''),
          data.get('field_type','text'), options_json,
          data.get('contact_dept'), data.get('conditional_show_when'),
          data.get('help_text'), data.get('placeholder',''),
          data.get('width_hint','full'),
          1 if data.get('is_notes_field') else 0,
          data.get('ai_hint') or None,
          fid))
    log_audit(db, 'FIELD_EDIT', 'form_field', fid, detail=data.get('label',''))
    db.commit(); db.close()
    syslog_logger.info(f"FIELD_EDIT id={fid} label={data.get('label','')!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/form-fields/<int:fid>/delete', methods=['POST'])
@content_admin_required
def delete_form_field(fid):
    db = get_db()
    log_audit(db, 'FIELD_DELETE', 'form_field', fid)
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
            INSERT INTO form_sections (section_key, label, sort_order, collapsible, icon, default_open)
            VALUES (?,?,?,?,?,?)
        """, (section_key, label, max_order + 10,
              1 if data.get('collapsible', True) else 0,
              data.get('icon', '◈'),
              0 if str(data.get('default_open', '1')) == '0' else 1))
        sid = cur.lastrowid
        log_audit(db, 'SECTION_ADD', 'form_section', sid, detail=label)
        db.commit()
        syslog_logger.info(f"SECTION_ADD id={sid} label={label!r} by={session.get('username')}")
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
        UPDATE form_sections SET label=?, collapsible=?, icon=?, default_open=? WHERE id=?
    """, (data.get('label',''),
          1 if data.get('collapsible', True) else 0,
          data.get('icon','◈'),
          0 if str(data.get('default_open', '1')) == '0' else 1,
          sid))
    log_audit(db, 'SECTION_EDIT', 'form_section', sid, detail=data.get('label',''))
    db.commit(); db.close()
    syslog_logger.info(f"SECTION_EDIT id={sid} label={data.get('label','')!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/form-sections/<int:sid>/delete', methods=['POST'])
@content_admin_required
def delete_form_section(sid):
    db = get_db()
    log_audit(db, 'SECTION_DELETE', 'form_section', sid)
    db.execute('DELETE FROM form_sections WHERE id=?', (sid,))
    db.commit(); db.close()
    syslog_logger.info(f"SECTION_DELETE id={sid} by={session.get('username')}")
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

_last_port_change = 0  # timestamp of last port change — rate limiter

@app.route('/settings/server', methods=['POST'])
@admin_required
def save_server_settings():
    global _last_port_change
    import time as _time_mod
    now = _time_mod.time()
    if now - _last_port_change < 30:
        return jsonify({'success': False, 'error': 'Port was changed recently. Wait 30 seconds.'}), 429
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
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail=f'app_port={port_val}')
    db.commit(); db.close()
    _last_port_change = now
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
    # Validate syslog host — block metadata/link-local to prevent exfiltration
    syslog_host = data.get('syslog_host', '')
    if syslog_host and _is_blocked_host(syslog_host):
        return jsonify({'success': False, 'error': 'Invalid syslog host.'}), 400
    # Validate port range
    try:
        syslog_port = int(data.get('syslog_port', 514))
        if not (1 <= syslog_port <= 65535):
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'Invalid syslog port.'}), 400
    # Validate facility against known values
    valid_facilities = [f'LOG_LOCAL{i}' for i in range(8)] + [
        'LOG_USER', 'LOG_DAEMON', 'LOG_SYSLOG', 'LOG_AUTH']
    if data.get('syslog_facility') and data['syslog_facility'] not in valid_facilities:
        return jsonify({'success': False, 'error': 'Invalid syslog facility.'}), 400
    db = get_db()
    for key in ('syslog_host', 'syslog_port', 'syslog_facility', 'syslog_enabled'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail='syslog')
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
        app.logger.error(f'Backup failed: {e}')
        return jsonify({'success': False, 'error': 'Backup failed. Check server logs.'}), 500


# ─── API ──────────────────────────────────────────────────────────────────────

_gs_rate_limit = limiter.limit("60 per minute") if (_limiter_available and limiter) else (lambda f: f)


@app.route('/api/search')
@login_required
@_gs_rate_limit
def global_search():
    """Universal search across shows, contacts, asset types, and asset items."""
    q = (request.args.get('q') or '').strip()
    if len(q) < 2 or len(q) > 255:
        return jsonify([])

    db = get_db()
    results = []
    like = f'%{q}%'
    is_admin = session.get('role') == 'admin'

    # ── Shows ────────────────────────────────────────────────────────────────
    accessible = get_accessible_shows(session['user_id'])  # None=all, []=none, list=ids
    if accessible != []:
        if accessible is None:
            show_rows = db.execute("""
                SELECT id, name, show_date, venue, performance_company, status
                FROM shows
                WHERE name LIKE ? OR venue LIKE ? OR performance_company LIKE ? OR show_date LIKE ?
                ORDER BY show_date DESC LIMIT 6
            """, (like, like, like, like)).fetchall()
        else:
            placeholders = ','.join('?' * len(accessible))
            show_rows = db.execute(f"""
                SELECT id, name, show_date, venue, performance_company, status
                FROM shows
                WHERE id IN ({placeholders})
                  AND (name LIKE ? OR venue LIKE ? OR performance_company LIKE ? OR show_date LIKE ?)
                ORDER BY show_date DESC LIMIT 6
            """, (*accessible, like, like, like, like)).fetchall()
        for r in show_rows:
            sub_parts = [p for p in [r['show_date'], r['venue'], r['performance_company']] if p]
            results.append({
                'type': 'show',
                'icon': '🎭',
                'label': r['name'],
                'sub': '  ·  '.join(sub_parts),
                'url': f"/shows/{r['id']}",
                'status': r['status'],
            })

    # ── Contacts ────────────────────────────────────────────────────────────
    contact_rows = db.execute("""
        SELECT id, name, title, department, email, phone
        FROM contacts
        WHERE name LIKE ? OR department LIKE ? OR email LIKE ? OR phone LIKE ? OR title LIKE ?
        ORDER BY department, name LIMIT 5
    """, (like, like, like, like, like)).fetchall()
    for r in contact_rows:
        sub_parts = [p for p in [r['department'], r['title'], r['email']] if p]
        results.append({
            'type': 'contact',
            'icon': '👤',
            'label': r['name'],
            'sub': '  ·  '.join(sub_parts),
            'url': None,  # contacts don't have their own page; sub-label carries the info
        })

    # ── Asset Types (admin only) ─────────────────────────────────────────────
    if is_admin:
        type_rows = db.execute("""
            SELECT at.id, at.name, at.manufacturer, at.model, ac.name as cat_name,
                   at.storage_location, at.is_retired
            FROM asset_types at
            JOIN asset_categories ac ON ac.id = at.category_id
            WHERE at.name LIKE ? OR at.manufacturer LIKE ? OR at.model LIKE ?
            ORDER BY at.is_retired, at.name LIMIT 5
        """, (like, like, like)).fetchall()
        for r in type_rows:
            label_parts = [p for p in [r['manufacturer'], r['model']] if p]
            sub_parts = [r['cat_name']] + ([r['storage_location']] if r['storage_location'] else [])
            results.append({
                'type': 'asset_type',
                'icon': '◈',
                'label': r['name'] + (f" — {' '.join(label_parts)}" if label_parts else ''),
                'sub': '  ·  '.join(sub_parts) + ('  ·  RETIRED' if r['is_retired'] else ''),
                'url': '/assets',
                'retired': bool(r['is_retired']),
            })

        # ── Asset Items / Barcodes (leading-zero tolerant) ──────────────────
        # Strip leading zeros from stored barcodes and compare with stripped query
        norm_q = q.lstrip('0') or '0'
        item_rows = db.execute("""
            SELECT ai.id, ai.barcode, ai.status, ai.condition,
                   at.name as type_name, at.id as type_id, ac.name as cat_name
            FROM asset_items ai
            JOIN asset_types at ON at.id = ai.asset_type_id
            JOIN asset_categories ac ON ac.id = at.category_id
            WHERE ai.barcode LIKE ?
               OR ltrim(ai.barcode, '0') = ?
               OR ai.barcode = ?
            ORDER BY ai.status, ai.id LIMIT 5
        """, (like, norm_q, q)).fetchall()
        for r in item_rows:
            results.append({
                'type': 'asset_item',
                'icon': '🔖',
                'label': f"Unit #{r['id']}" + (f" — {r['barcode']}" if r['barcode'] else ''),
                'sub': f"{r['type_name']}  ·  {r['cat_name']}  ·  {r['status']}",
                'url': '/assets',
                'status': r['status'],
            })

    db.close()
    return jsonify(results)


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


# ─── Schedule Templates ───────────────────────────────────────────────────────

@app.route('/api/schedule-templates')
@login_required
def api_schedule_templates():
    db = get_db()
    templates = [dict(t) for t in db.execute(
        'SELECT * FROM schedule_templates ORDER BY sort_order, name'
    ).fetchall()]
    for t in templates:
        t['rows'] = [dict(r) for r in db.execute(
            'SELECT * FROM schedule_template_rows WHERE template_id=? ORDER BY sort_order',
            (t['id'],)
        ).fetchall()]
    db.close()
    return jsonify(templates)


@app.route('/api/schedule-templates/<int:tid>')
@login_required
def api_schedule_template(tid):
    db = get_db()
    t = db.execute('SELECT * FROM schedule_templates WHERE id=?', (tid,)).fetchone()
    if not t:
        db.close(); return jsonify({'error': 'Not found'}), 404
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM schedule_template_rows WHERE template_id=? ORDER BY sort_order', (tid,)
    ).fetchall()]
    db.close()
    return jsonify({'id': t['id'], 'name': t['name'], 'rows': rows})


@app.route('/settings/schedule-templates/add', methods=['POST'])
@content_admin_required
def add_schedule_template():
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required.'}), 400
    db = get_db()
    max_o = db.execute('SELECT MAX(sort_order) FROM schedule_templates').fetchone()[0] or 0
    cur = db.execute('INSERT INTO schedule_templates (name, sort_order) VALUES (?,?)',
                     (name, max_o + 10))
    tid = cur.lastrowid
    for i, row in enumerate(data.get('rows', [])):
        db.execute("""INSERT INTO schedule_template_rows
                      (template_id, sort_order, start_time, end_time, description, notes)
                      VALUES (?,?,?,?,?,?)""",
                   (tid, i, row.get('start_time',''), row.get('end_time',''),
                    row.get('description',''), row.get('notes','')))
    log_audit(db, 'TEMPLATE_ADD', 'schedule_template', tid, detail=name)
    db.commit(); db.close()
    syslog_logger.info(f"TEMPLATE_ADD id={tid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True, 'id': tid})


@app.route('/settings/schedule-templates/<int:tid>/edit', methods=['POST'])
@content_admin_required
def edit_schedule_template(tid):
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required.'}), 400
    db = get_db()
    db.execute('UPDATE schedule_templates SET name=? WHERE id=?', (name, tid))
    db.execute('DELETE FROM schedule_template_rows WHERE template_id=?', (tid,))
    for i, row in enumerate(data.get('rows', [])):
        db.execute("""INSERT INTO schedule_template_rows
                      (template_id, sort_order, start_time, end_time, description, notes)
                      VALUES (?,?,?,?,?,?)""",
                   (tid, i, row.get('start_time',''), row.get('end_time',''),
                    row.get('description',''), row.get('notes','')))
    log_audit(db, 'TEMPLATE_EDIT', 'schedule_template', tid, detail=name)
    db.commit(); db.close()
    syslog_logger.info(f"TEMPLATE_EDIT id={tid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/schedule-templates/<int:tid>/delete', methods=['POST'])
@content_admin_required
def delete_schedule_template(tid):
    db = get_db()
    row = db.execute('SELECT name FROM schedule_templates WHERE id=?', (tid,)).fetchone()
    log_audit(db, 'TEMPLATE_DELETE', 'schedule_template', tid,
              detail=row['name'] if row else str(tid))
    db.execute('DELETE FROM schedule_templates WHERE id=?', (tid,))
    db.commit(); db.close()
    syslog_logger.info(f"TEMPLATE_DELETE id={tid} by={session.get('username')}")
    return jsonify({'success': True})


# ─── WiFi / Logo Settings ─────────────────────────────────────────────────────

@app.route('/settings/wifi', methods=['POST'])
@admin_required
def save_wifi_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    for key in ('wifi_network', 'wifi_password'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, data[key]))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail='wifi')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE detail=wifi by={session.get('username')}")
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
        # Only allow safe image MIME types (reject SVG — can contain JavaScript)
        _allowed_logo_mimes = ('image/png', 'image/jpeg', 'image/gif', 'image/webp')
        if mime not in _allowed_logo_mimes:
            return jsonify({'success': False,
                            'error': f'Unsupported image type. Allowed: PNG, JPEG, GIF, WebP.'}), 400
        b64 = base64.b64encode(data).decode()
        logo_data = f'data:{mime};base64,{b64}'
    else:
        data_uri = (request.get_json(force=True) or {}).get('logo_data', '')
        logo_data = data_uri

    db = get_db()
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('logo_data', logo_data))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail='logo_upload')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE detail=logo_upload by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/logo/delete', methods=['POST'])
@admin_required
def delete_logo():
    db = get_db()
    db.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('logo_data', '')")
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail='logo_delete')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE detail=logo_delete by={session.get('username')}")
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
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, detail=f'upload_max_mb={mb}')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE detail=upload_max_mb={mb} by={session.get('username')}")
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


# ─── Field Key Availability Check ─────────────────────────────────────────────

@app.route('/settings/form-fields/check-key')
@content_admin_required
def check_field_key():
    key = request.args.get('key', '').strip().lower().replace(' ', '_')
    exclude_id = request.args.get('exclude_id', type=int)
    if not key:
        return jsonify({'available': False, 'conflict': None})
    db = get_db()
    if exclude_id:
        row = db.execute(
            'SELECT id, label FROM form_fields WHERE field_key=? AND id!=?', (key, exclude_id)
        ).fetchone()
    else:
        row = db.execute(
            'SELECT id, label FROM form_fields WHERE field_key=?', (key,)
        ).fetchone()
    db.close()
    if row:
        return jsonify({'available': False, 'conflict': row['label']})
    return jsonify({'available': True})


# ─── Database Settings ─────────────────────────────────────────────────────────

@app.route('/settings/database', methods=['POST'])
@admin_required
def save_database_settings():
    data = request.get_json(force=True) or {}
    db_type = data.get('db_type', 'sqlite')
    db = get_db()
    settings_to_save = {
        'db_type': db_type,
        'pg_host': data.get('pg_host', 'localhost'),
        'pg_port': str(data.get('pg_port', '5432')),
        'pg_dbname': data.get('pg_dbname', 'showadvance'),
        'pg_user': data.get('pg_user', ''),
        'pg_schema': data.get('pg_schema', 'showadvance'),
    }
    # Only update password if provided (non-empty)
    if data.get('pg_password'):
        settings_to_save['pg_password'] = data['pg_password']

    for key, value in settings_to_save.items():
        db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)', (key, value))
    db.commit(); db.close()

    # Also write to SQLite bootstrap (in case active DB is PostgreSQL)
    _sqlite_conn = sqlite3.connect(DATABASE)
    for key, value in settings_to_save.items():
        _sqlite_conn.execute(
            'INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)', (key, value)
        )
    _sqlite_conn.commit(); _sqlite_conn.close()

    db_adapter.clear_settings_cache()
    syslog_logger.info(f"SETTINGS_CHANGE key=database db_type={db_type} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/database/test', methods=['POST'])
@admin_required
def test_database_connection():
    data = request.get_json(force=True) or {}
    db_type = data.get('db_type', 'sqlite')

    if db_type == 'sqlite':
        if os.path.exists(DATABASE):
            return jsonify({'success': True, 'message': 'SQLite database found and accessible.'})
        return jsonify({'success': False, 'message': 'SQLite database not found. Run init_db.py first.'})

    if db_type == 'postgres':
        ok, err = db_adapter.test_postgres_connection(
            host=data.get('pg_host', 'localhost'),
            port=data.get('pg_port', 5432),
            dbname=data.get('pg_dbname', 'showadvance'),
            user=data.get('pg_user', ''),
            password=data.get('pg_password', ''),
            schema=data.get('pg_schema', 'showadvance'),
        )
        if ok:
            return jsonify({'success': True, 'message': 'Connected to PostgreSQL successfully.'})
        app.logger.warning(f'PostgreSQL test failed: {err}')
        return jsonify({'success': False, 'message': 'PostgreSQL connection failed. Check host, port, credentials, and schema.'})

    return jsonify({'success': False, 'message': 'Unknown database type.'})


@app.route('/settings/database/migrate', methods=['POST'])
@admin_required
def migrate_database():
    """Migrate data from SQLite to PostgreSQL. Safe to run multiple times."""
    from init_db import migrate_sqlite_to_postgres

    settings = db_adapter.read_db_settings(DATABASE)
    if settings.get('db_type') != 'postgres':
        return jsonify({'success': False, 'error': 'Database type must be PostgreSQL to migrate.'}), 400

    try:
        stats = migrate_sqlite_to_postgres(DATABASE, settings)
    except Exception as e:
        app.logger.error(f'Database migration failed: {e}')
        return jsonify({'success': False, 'error': 'Migration failed. Check server logs.'}), 500

    if 'error' in stats:
        return jsonify({'success': False, 'error': stats['error']}), 500

    total_copied = sum(v.get('copied', 0) for v in stats.values() if isinstance(v, dict))
    total_skipped = sum(v.get('skipped', 0) for v in stats.values() if isinstance(v, dict))

    syslog_logger.info(f"DB_MIGRATE copied={total_copied} skipped={total_skipped} by={session.get('username')}")
    return jsonify({
        'success': True,
        'stats': stats,
        'total_copied': total_copied,
        'total_skipped': total_skipped,
    })


# ─── AI / Ollama Settings ──────────────────────────────────────────────────────

def _is_blocked_host(hostname):
    """Return True if hostname resolves to a cloud metadata or link-local address."""
    import ipaddress
    if not hostname:
        return True
    if hostname in ('169.254.169.254', 'metadata.google.internal'):
        return True
    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_link_local or ip.is_reserved:
            return True
        # Block metadata IP range (169.254.x.x)
        if ip.is_private and str(ip).startswith('169.254.'):
            return True
    except ValueError:
        pass  # DNS name, not an IP literal — not blocked
    return False


def _validate_ollama_url(url):
    """Validate that an Ollama URL is safe (no SSRF to internal/metadata endpoints)."""
    from urllib.parse import urlparse
    import ipaddress
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return False
        hostname = parsed.hostname or ''
        # Allow localhost / 127.x (typical Ollama install)
        if hostname in ('localhost', '127.0.0.1', '::1') or hostname.startswith('127.'):
            return True
        # Block cloud metadata, link-local, and private ranges
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_link_local or ip.is_loopback or ip.is_reserved:
                return False
        except ValueError:
            pass  # hostname is a DNS name, not an IP — allow it
        if _is_blocked_host(hostname):
            return False
        return True
    except Exception:
        return False


@app.route('/settings/ai', methods=['POST'])
@admin_required
def save_ai_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    for key in ('ollama_enabled', 'ollama_url', 'ollama_model', 'ai_max_sessions'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE key=ai by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/ai/test', methods=['POST'])
@admin_required
def test_ai_connection():
    data = request.get_json(force=True) or {}
    url = data.get('ollama_url', 'http://localhost:11434').rstrip('/')
    model = data.get('ollama_model', 'llama3.2')

    # SSRF protection — only allow http(s) to non-internal hosts (except localhost for Ollama)
    if not _validate_ollama_url(url):
        return jsonify({'success': False, 'message': 'Invalid Ollama URL. Only http/https to localhost or non-internal hosts allowed.'})

    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(f'{url}/api/tags', method='GET')
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read())
            models = [m['name'] for m in body.get('models', [])]
            if model in models or any(m.startswith(model.split(':')[0]) for m in models):
                return jsonify({'success': True, 'message': f'Connected. Model "{model}" available.', 'models': models})
            return jsonify({'success': True, 'message': f'Connected, but model "{model}" not found. Available: {", ".join(models[:5])}', 'models': models})
    except urllib.error.URLError as e:
        app.logger.warning(f'Ollama connection failed: {e}')
        return jsonify({'success': False, 'message': 'Cannot reach Ollama. Check URL and ensure Ollama is running.'})
    except Exception as e:
        app.logger.warning(f'Ollama test error: {e}')
        return jsonify({'success': False, 'message': 'Ollama connection test failed.'})


# ─── SMTP / PDF Email Settings ────────────────────────────────────────────────

@app.route('/settings/smtp', methods=['POST'])
@admin_required
def save_smtp_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    for key in ('smtp_host', 'smtp_port', 'smtp_user', 'smtp_pass',
                'smtp_from', 'smtp_tls'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None,
              after={k: v for k, v in data.items() if 'pass' not in k},
              detail='smtp_settings')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE key=smtp by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/smtp/test', methods=['POST'])
@admin_required
def test_smtp_connection():
    import smtplib
    data = request.get_json(force=True) or {}
    host     = data.get('smtp_host', '')
    port     = int(data.get('smtp_port') or 587)
    user     = data.get('smtp_user', '')
    password = data.get('smtp_pass', '')
    use_tls  = data.get('smtp_tls', '1') not in ('0', 'false', 'False', '')
    from_addr = data.get('smtp_from') or user
    to_addr   = data.get('test_to') or user
    if not host:
        return jsonify({'success': False, 'message': 'SMTP host is required.'})
    # Block SSRF — prevent connecting to cloud metadata or link-local addresses
    if _is_blocked_host(host):
        return jsonify({'success': False, 'message': 'Invalid SMTP host.'})
    # Validate email addresses to prevent header injection / open relay abuse
    import re
    _email_re = re.compile(r'^[^@\s\r\n]+@[^@\s\r\n]+\.[^@\s\r\n]+$')
    if from_addr and not _email_re.match(from_addr):
        return jsonify({'success': False, 'message': 'Invalid from address.'})
    if to_addr and not _email_re.match(to_addr):
        return jsonify({'success': False, 'message': 'Invalid test recipient address.'})
    try:
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=10)
            server.ehlo(); server.starttls(); server.ehlo()
        else:
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        if user and password:
            server.login(user, password)
        if to_addr:
            from email.mime.text import MIMEText
            msg = MIMEText('3·2·1→Theater SMTP test — connection successful.')
            msg['Subject'] = '3·2·1→Theater SMTP Test'
            msg['From'] = from_addr
            msg['To']   = to_addr
            server.sendmail(from_addr, [to_addr], msg.as_string())
            server.quit()
            syslog_logger.info(f"SMTP_TEST to={to_addr} by={session.get('username')}")
            return jsonify({'success': True, 'message': f'Test email sent to {to_addr}.'})
        server.quit()
        return jsonify({'success': True, 'message': 'Connected successfully (no test email sent).'})
    except Exception as e:
        app.logger.warning(f'SMTP test failed: {e}')
        return jsonify({'success': False, 'message': 'SMTP connection failed. Check host, port, and credentials.'})


@app.route('/settings/email-provider', methods=['POST'])
@admin_required
def save_email_provider_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    provider = data.get('email_provider', 'smtp')
    if provider not in ('smtp', 'direct'):
        provider = 'smtp'
    db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
               ('email_provider', provider))
    # Save direct send settings
    for key in ('smtp_from', 'direct_ehlo_hostname', 'direct_display_name'):
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None,
              after={'email_provider': provider}, detail='email_provider')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE key=email_provider value={provider} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/email/test', methods=['POST'])
@admin_required
def test_email_provider():
    """Send a test email through the currently configured email provider."""
    data = request.get_json(force=True) or {}
    to_addr = data.get('test_to', '').strip()
    if not to_addr:
        return jsonify({'success': False, 'message': 'Test recipient address is required.'})
    success, message = _send_email(
        subject='ShowAdvance Email Test',
        recipients=[to_addr],
        body_text='This is a test email from ShowAdvance to verify your email configuration.'
    )
    return jsonify({'success': success, 'message': message})


@app.route('/settings/pdf-emails', methods=['POST'])
@admin_required
def save_pdf_email_settings():
    data = request.get_json(force=True) or {}
    db = get_db()
    keys = ('pdf_email_send_hour',
            'advance_email_enabled',   'advance_email_days_before',
            'schedule_email_enabled_1','schedule_email_days_1',
            'schedule_email_enabled_2','schedule_email_days_2')
    for key in keys:
        if key in data:
            db.execute('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)',
                       (key, str(data[key])))
    log_audit(db, 'SETTINGS_CHANGE', 'setting', None, after=data, detail='pdf_email_settings')
    db.commit(); db.close()
    syslog_logger.info(f"SETTINGS_CHANGE key=pdf_emails by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/contacts/<int:cid>/recipient', methods=['POST'])
@admin_required
def toggle_contact_recipient(cid):
    data = request.get_json(force=True) or {}
    val  = 1 if data.get('recipient') else 0
    db   = get_db()
    db.execute('UPDATE contacts SET report_recipient=? WHERE id=?', (val, cid))
    log_audit(db, 'CONTACT_RECIPIENT_TOGGLE', 'contact', cid,
              detail=f"recipient={'yes' if val else 'no'}")
    db.commit(); db.close()
    syslog_logger.info(f"CONTACT_RECIPIENT_TOGGLE id={cid} recipient={'yes' if val else 'no'} by={session.get('username')}")
    return jsonify({'success': True})


# ─── AI Document Extraction ────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/ai-extract', methods=['POST'])
@login_required
def ai_extract(show_id):
    """Extract form field values from an uploaded document using Ollama."""
    ai_sid, slot_error = _claim_ai_session(show_id)
    if slot_error:
        return jsonify({'success': False, 'error': slot_error}), 429
    try:
        return _ai_extract_impl(show_id)
    except Exception as e:
        app.logger.exception("ai_extract unhandled error")
        return jsonify({'success': False, 'error': f'Server error: {e}'}), 500
    finally:
        _release_ai_session(ai_sid)

def _ai_extract_impl(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403

    # Check Ollama is enabled
    ollama_enabled = get_app_setting('ollama_enabled', '0')
    if ollama_enabled != '1':
        return jsonify({'success': False, 'error': 'AI extraction is not enabled. Enable it in Settings → AI.'}), 400

    ollama_url = get_app_setting('ollama_url', 'http://localhost:11434').rstrip('/')
    ollama_model = get_app_setting('ollama_model', 'llama3.2')

    # Get document text
    doc_text = ''
    attachment_id = request.form.get('attachment_id', type=int)
    uploaded_file = request.files.get('document')

    if attachment_id:
        db = get_db()
        row = db.execute(
            'SELECT file_data, mime_type, filename FROM show_attachments WHERE id=? AND show_id=?',
            (attachment_id, show_id)
        ).fetchone()
        db.close()
        if not row:
            return jsonify({'success': False, 'error': 'Attachment not found.'}), 404
        file_bytes = bytes(row['file_data'])
        mime = row['mime_type']
        fname = row['filename']
    elif uploaded_file and uploaded_file.filename:
        file_bytes = uploaded_file.read()
        mime = uploaded_file.content_type or 'application/octet-stream'
        fname = uploaded_file.filename
    else:
        return jsonify({'success': False, 'error': 'No document provided.'}), 400

    # Extract text from document
    try:
        if mime == 'application/pdf' or fname.lower().endswith('.pdf'):
            try:
                import pdfplumber
                from io import BytesIO as _BytesIO
                with pdfplumber.open(_BytesIO(file_bytes)) as pdf:
                    doc_text = '\n'.join(
                        page.extract_text() or '' for page in pdf.pages
                    )
            except ImportError:
                return jsonify({'success': False, 'error': 'pdfplumber not installed. Run: pip install pdfplumber'}), 500
        elif fname.lower().endswith('.docx') or mime in (
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',):
            try:
                import docx as _docx
                from io import BytesIO as _BytesIO
                document = _docx.Document(_BytesIO(file_bytes))
                doc_text = '\n'.join(p.text for p in document.paragraphs if p.text.strip())
            except ImportError:
                return jsonify({'success': False, 'error': 'python-docx not installed. Run: pip install python-docx'}), 500
        elif fname.lower().endswith('.doc'):
            return jsonify({'success': False, 'error': 'Legacy .doc format is not supported. Please save as .docx and re-upload.'}), 400
        elif fname.lower().endswith('.rtf'):
            try:
                from striprtf.striprtf import rtf_to_text as _rtf_to_text
                doc_text = _rtf_to_text(file_bytes.decode('utf-8', errors='replace'))
            except ImportError:
                return jsonify({'success': False, 'error': 'striprtf not installed. Run: pip install striprtf'}), 500
        elif fname.lower().endswith(('.xlsx', '.xls')) or mime in (
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/vnd.ms-excel'):
            try:
                from io import BytesIO as _BytesIO
                if fname.lower().endswith('.xls'):
                    import xlrd as _xlrd
                    wb = _xlrd.open_workbook(file_contents=file_bytes)
                    rows = []
                    for sheet in wb.sheets():
                        for i in range(sheet.nrows):
                            rows.append('\t'.join(str(sheet.cell_value(i, j)) for j in range(sheet.ncols)))
                    doc_text = '\n'.join(rows)
                else:
                    import openpyxl as _openpyxl
                    wb = _openpyxl.load_workbook(_BytesIO(file_bytes), data_only=True)
                    rows = []
                    for sheet in wb.worksheets:
                        for row in sheet.iter_rows(values_only=True):
                            line = '\t'.join(str(c) if c is not None else '' for c in row)
                            if line.strip():
                                rows.append(line)
                    doc_text = '\n'.join(rows)
            except ImportError as ie:
                pkg = 'xlrd' if '.xls' in fname.lower() and not fname.lower().endswith('.xlsx') else 'openpyxl'
                return jsonify({'success': False, 'error': f'{pkg} not installed. Run: pip install {pkg}'}), 500
        else:
            # Plain text (.txt, .csv and others)
            doc_text = file_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        return jsonify({'success': False, 'error': f'Could not extract text: {e}'}), 500

    if not doc_text.strip():
        return jsonify({'success': False, 'error': 'No readable text found in document.'}), 400

    # Load form fields with ai_hint
    db = get_db()
    field_rows = db.execute(
        'SELECT field_key, label, ai_hint FROM form_fields WHERE ai_hint IS NOT NULL AND ai_hint != \'\''
    ).fetchall()
    db.close()

    if not field_rows:
        return jsonify({'success': False, 'error': 'No fields have AI hints configured. Add hints in Settings → Form Fields.'}), 400

    field_map = {r['field_key']: {'label': r['label'], 'hint': r['ai_hint']} for r in field_rows}
    field_schema = {k: v['hint'] for k, v in field_map.items()}

    # Build prompt
    prompt = (
        'You are extracting information from a document to populate a form. '
        'Return ONLY valid JSON — no explanation, no markdown, just the JSON object. '
        'Use null for fields where the information is not found in the document. '
        f'Fields to extract (key: description): {json.dumps(field_schema)}\n\n'
        f'Document text:\n{doc_text[:8000]}'
    )

    # Call Ollama (with SSRF validation)
    if not _validate_ollama_url(ollama_url):
        return jsonify({'success': False, 'error': 'Invalid Ollama URL.'}), 400
    try:
        import urllib.request as _urlreq
        payload = json.dumps({
            'model': ollama_model,
            'messages': [{'role': 'user', 'content': prompt}],
            'format': 'json',
            'stream': True,  # Stream tokens; timeout applies per-chunk, not total
        }).encode()
        req = _urlreq.Request(
            f'{ollama_url}/api/chat',
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        raw_content = ''
        with _urlreq.urlopen(req, timeout=60) as resp:
            for line in resp:
                line = line.strip()
                if not line:
                    continue
                chunk = json.loads(line)
                raw_content += chunk.get('message', {}).get('content', '')
                if chunk.get('done'):
                    break
    except Exception as e:
        app.logger.error(f'Ollama request failed: {e}')
        return jsonify({'success': False, 'error': 'AI extraction request failed. Check Ollama connection.'}), 500

    # Parse JSON response
    try:
        # Strip markdown code fences if model added them
        clean = raw_content.strip()
        if clean.startswith('```'):
            clean = '\n'.join(clean.split('\n')[1:])
            if clean.endswith('```'):
                clean = clean[:-3]
        extracted = json.loads(clean)
    except (json.JSONDecodeError, ValueError):
        app.logger.warning(f'AI returned invalid JSON: {raw_content[:200]}')
        return jsonify({'success': False, 'error': 'AI returned an unparseable response. Try again or use a different model.'}), 500

    # Build suggestions (only non-null values)
    suggestions = {}
    for field_key, value in extracted.items():
        if value is not None and str(value).strip() and field_key in field_map:
            suggestions[field_key] = {
                'value': str(value).strip(),
                'label': field_map[field_key]['label'],
            }

    syslog_logger.info(
        f"AI_EXTRACT show_id={show_id} document={fname} "
        f"fields_found={len(suggestions)} model={ollama_model} "
        f"by={session.get('username')}"
    )
    return jsonify({
        'success': True,
        'suggestions': suggestions,
        'document': fname,
        'model': ollama_model,
        'field_count': len(field_rows),
    })


# ─── Job Positions & Position Categories ─────────────────────────────────────

@app.route('/api/job-positions')
@login_required
def api_job_positions():
    db = get_db()
    rows = db.execute("""
        SELECT jp.id, jp.category_id, pc.name as category_name, jp.name, jp.sort_order
        FROM job_positions jp
        LEFT JOIN position_categories pc ON jp.category_id = pc.id
        ORDER BY pc.sort_order, jp.sort_order, jp.id
    """).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/position-categories')
@login_required
def api_position_categories():
    db = get_db()
    cats = db.execute('SELECT * FROM position_categories ORDER BY sort_order, id').fetchall()
    db.close()
    return jsonify([dict(c) for c in cats])


@app.route('/settings/position-categories/add', methods=['POST'])
@content_admin_required
def add_position_category():
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    db = get_db()
    max_order = db.execute('SELECT MAX(sort_order) FROM position_categories').fetchone()[0] or 0
    cur = db.execute(
        'INSERT INTO position_categories (name, sort_order) VALUES (?, ?)',
        (name, max_order + 10)
    )
    cid = cur.lastrowid
    log_audit(db, 'POSITION_CATEGORY_ADD', 'position_category', cid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"POSITION_CATEGORY_ADD id={cid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True, 'id': cid, 'name': name})


@app.route('/settings/position-categories/<int:cid>/edit', methods=['POST'])
@content_admin_required
def edit_position_category(cid):
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    db = get_db()
    db.execute('UPDATE position_categories SET name=? WHERE id=?', (name, cid))
    log_audit(db, 'POSITION_CATEGORY_EDIT', 'position_category', cid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"POSITION_CATEGORY_EDIT id={cid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/position-categories/<int:cid>/delete', methods=['POST'])
@content_admin_required
def delete_position_category(cid):
    db = get_db()
    # Null out category_id on positions in this category
    row = db.execute('SELECT name FROM position_categories WHERE id=?', (cid,)).fetchone()
    db.execute('UPDATE job_positions SET category_id=NULL WHERE category_id=?', (cid,))
    db.execute('DELETE FROM position_categories WHERE id=?', (cid,))
    log_audit(db, 'POSITION_CATEGORY_DELETE', 'position_category', cid,
              detail=row['name'] if row else str(cid))
    db.commit()
    db.close()
    syslog_logger.info(f"POSITION_CATEGORY_DELETE id={cid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/job-positions/add', methods=['POST'])
@content_admin_required
def add_job_position():
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    category_id = data.get('category_id') or None
    db = get_db()
    max_order = db.execute('SELECT MAX(sort_order) FROM job_positions WHERE category_id IS ?', (category_id,)).fetchone()[0] or 0
    cur = db.execute(
        'INSERT INTO job_positions (category_id, name, sort_order) VALUES (?, ?, ?)',
        (category_id, name, max_order + 10)
    )
    pid = cur.lastrowid
    log_audit(db, 'JOB_POSITION_ADD', 'job_position', pid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"JOB_POSITION_ADD id={pid} name={name!r} category_id={category_id} by={session.get('username')}")
    return jsonify({'success': True, 'id': pid, 'name': name})


@app.route('/settings/job-positions/<int:pid>/edit', methods=['POST'])
@content_admin_required
def edit_job_position(pid):
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    category_id = data.get('category_id') or None
    db = get_db()
    db.execute(
        'UPDATE job_positions SET name=?, category_id=? WHERE id=?',
        (name, category_id, pid)
    )
    log_audit(db, 'JOB_POSITION_EDIT', 'job_position', pid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"JOB_POSITION_EDIT id={pid} name={name!r} category_id={category_id} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/job-positions/<int:pid>/delete', methods=['POST'])
@content_admin_required
def delete_job_position(pid):
    db = get_db()
    row = db.execute('SELECT name FROM job_positions WHERE id=?', (pid,)).fetchone()
    log_audit(db, 'JOB_POSITION_DELETE', 'job_position', pid,
              detail=row['name'] if row else str(pid))
    db.execute('DELETE FROM job_positions WHERE id=?', (pid,))
    db.commit()
    db.close()
    syslog_logger.info(f"JOB_POSITION_DELETE id={pid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/job-positions/reorder', methods=['POST'])
@content_admin_required
def reorder_job_positions():
    data = request.get_json(force=True) or {}
    position_ids = data.get('position_ids', [])
    db = get_db()
    for i, pid in enumerate(position_ids):
        db.execute('UPDATE job_positions SET sort_order=? WHERE id=?', (i * 10, pid))
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── Labor Requests (per show) ────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/labor-requests', methods=['GET'])
@login_required
def get_labor_requests(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    db = get_db()
    rows = db.execute("""
        SELECT lr.*, jp.name as position_name
        FROM labor_requests lr
        LEFT JOIN job_positions jp ON lr.position_id = jp.id
        WHERE lr.show_id = ?
        ORDER BY lr.sort_order, lr.id
    """, (show_id,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/shows/<int:show_id>/labor-requests', methods=['POST'])
@login_required
def add_labor_request(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    data = request.get_json(force=True) or {}
    db = get_db()
    max_order = db.execute(
        'SELECT MAX(sort_order) FROM labor_requests WHERE show_id=?', (show_id,)
    ).fetchone()[0] or 0
    cur = db.execute("""
        INSERT INTO labor_requests (show_id, position_id, in_time, out_time, break_start, break_end, requested_name, sort_order)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (show_id,
          data.get('position_id') or None,
          data.get('in_time', ''),
          data.get('out_time', ''),
          data.get('break_start', ''),
          data.get('break_end', ''),
          data.get('requested_name', ''),
          max_order + 10))
    rid = cur.lastrowid
    log_audit(db, 'LABOR_REQUEST_ADD', 'labor_request', rid, show_id=show_id,
              detail=data.get('requested_name', '') or f'position_id={data.get("position_id")}')
    db.commit()
    db.close()
    syslog_logger.info(f"LABOR_REQUEST_ADD show_id={show_id} id={rid} by={session.get('username')}")
    return jsonify({'success': True, 'id': rid})


@app.route('/shows/<int:show_id>/labor-requests/<int:rid>', methods=['PUT'])
@login_required
def update_labor_request(show_id, rid):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE labor_requests
        SET position_id=?, in_time=?, out_time=?, break_start=?, break_end=?, requested_name=?
        WHERE id=? AND show_id=?
    """, (data.get('position_id') or None,
          data.get('in_time', ''),
          data.get('out_time', ''),
          data.get('break_start', ''),
          data.get('break_end', ''),
          data.get('requested_name', ''),
          rid, show_id))
    log_audit(db, 'LABOR_REQUEST_EDIT', 'labor_request', rid, show_id=show_id)
    db.commit()
    db.close()
    syslog_logger.info(f"LABOR_REQUEST_EDIT show_id={show_id} id={rid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/labor-requests/<int:rid>', methods=['DELETE'])
@login_required
def delete_labor_request(show_id, rid):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    db = get_db()
    db.execute('DELETE FROM labor_requests WHERE id=? AND show_id=?', (rid, show_id))
    log_audit(db, 'LABOR_REQUEST_DELETE', 'labor_request', rid, show_id=show_id)
    db.commit()
    db.close()
    syslog_logger.info(f"LABOR_REQUEST_DELETE show_id={show_id} id={rid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/labor-requests/reorder', methods=['POST'])
@login_required
def reorder_labor_requests(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'success': False, 'error': 'Access denied.'}), 403
    if session.get('is_restricted'):
        return jsonify({'success': False, 'error': 'Read-only access.'}), 403
    data = request.get_json(force=True) or {}
    request_ids = data.get('request_ids', [])
    db = get_db()
    for i, rid in enumerate(request_ids):
        db.execute(
            'UPDATE labor_requests SET sort_order=? WHERE id=? AND show_id=?',
            (i * 10, rid, show_id)
        )
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── Crew Members ────────────────────────────────────────────────────────────

@app.route('/crew')
@login_required
def crew_tracker():
    db = get_db()
    categories = db.execute(
        'SELECT * FROM position_categories ORDER BY sort_order, id'
    ).fetchall()

    # Build categories with their positions
    positions = db.execute(
        'SELECT * FROM job_positions ORDER BY sort_order, id'
    ).fetchall()
    pos_by_cat = {}
    all_positions = []
    for p in positions:
        pos_by_cat.setdefault(p['category_id'], []).append(dict(p))
        all_positions.append(dict(p))

    cats_with_positions = []
    for c in categories:
        c_dict = dict(c)
        c_dict['positions'] = pos_by_cat.get(c['id'], [])
        cats_with_positions.append(c_dict)
    # Uncategorized positions
    uncategorized = pos_by_cat.get(None, [])

    # Crew members
    members = db.execute(
        'SELECT * FROM crew_members ORDER BY sort_order, name'
    ).fetchall()

    # Qualifications — build set of (crew_member_id, position_id)
    quals = db.execute('SELECT crew_member_id, position_id FROM crew_qualifications').fetchall()
    qual_set = {(q['crew_member_id'], q['position_id']) for q in quals}

    # Build member rows with qual flags
    member_rows = []
    for m in members:
        m_dict = dict(m)
        m_dict['qualifications'] = [q[1] for q in qual_set if q[0] == m['id']]
        member_rows.append(m_dict)

    db.close()
    return render_template('crew_tracker.html',
                           categories=cats_with_positions,
                           uncategorized_positions=uncategorized,
                           all_positions=all_positions,
                           members=member_rows,
                           user=get_current_user())


@app.route('/api/crew-members')
@login_required
def api_crew_members():
    db = get_db()
    members = db.execute(
        'SELECT * FROM crew_members ORDER BY sort_order, name'
    ).fetchall()
    quals = db.execute('SELECT crew_member_id, position_id FROM crew_qualifications').fetchall()
    qual_map = {}
    for q in quals:
        qual_map.setdefault(q['crew_member_id'], []).append(q['position_id'])

    result = []
    for m in members:
        m_dict = dict(m)
        m_dict['qualifications'] = qual_map.get(m['id'], [])
        result.append(m_dict)
    db.close()
    return jsonify(result)


@app.route('/settings/crew-members/add', methods=['POST'])
@content_admin_required
def add_crew_member():
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    db = get_db()
    max_order = db.execute('SELECT MAX(sort_order) FROM crew_members').fetchone()[0] or 0
    cur = db.execute(
        'INSERT INTO crew_members (name, sort_order) VALUES (?, ?)',
        (name, max_order + 10)
    )
    mid = cur.lastrowid
    log_audit(db, 'CREW_MEMBER_ADD', 'crew_member', mid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"TECHNICIAN_ADD id={mid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True, 'id': mid, 'name': name})


@app.route('/settings/crew-members/<int:mid>/edit', methods=['POST'])
@content_admin_required
def edit_crew_member(mid):
    data = request.get_json(force=True) or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name is required.'}), 400
    db = get_db()
    db.execute('UPDATE crew_members SET name=? WHERE id=?', (name, mid))
    log_audit(db, 'CREW_MEMBER_EDIT', 'crew_member', mid, detail=name)
    db.commit()
    db.close()
    syslog_logger.info(f"TECHNICIAN_EDIT id={mid} name={name!r} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/crew-members/<int:mid>/delete', methods=['POST'])
@content_admin_required
def delete_crew_member(mid):
    db = get_db()
    db.execute('DELETE FROM crew_members WHERE id=?', (mid,))
    log_audit(db, 'CREW_MEMBER_DELETE', 'crew_member', mid)
    db.commit()
    db.close()
    syslog_logger.info(f"TECHNICIAN_DELETE id={mid} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/crew-members/reorder', methods=['POST'])
@content_admin_required
def reorder_crew_members():
    data = request.get_json(force=True) or {}
    member_ids = data.get('member_ids', [])
    db = get_db()
    for i, mid in enumerate(member_ids):
        db.execute('UPDATE crew_members SET sort_order=? WHERE id=?', (i * 10, mid))
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/api/crew-qualifications/toggle', methods=['POST'])
@content_admin_required
def toggle_crew_qualification():
    data = request.get_json(force=True) or {}
    crew_member_id = data.get('crew_member_id')
    position_id = data.get('position_id')
    if not crew_member_id or not position_id:
        return jsonify({'success': False, 'error': 'crew_member_id and position_id required.'}), 400
    db = get_db()
    existing = db.execute(
        'SELECT 1 FROM crew_qualifications WHERE crew_member_id=? AND position_id=?',
        (crew_member_id, position_id)
    ).fetchone()
    if existing:
        db.execute(
            'DELETE FROM crew_qualifications WHERE crew_member_id=? AND position_id=?',
            (crew_member_id, position_id)
        )
        has = False
    else:
        db.execute(
            'INSERT INTO crew_qualifications (crew_member_id, position_id) VALUES (?, ?)',
            (crew_member_id, position_id)
        )
        has = True
    action = 'QUAL_ADD' if has else 'QUAL_REMOVE'
    log_audit(db, f'CREW_{action}', 'crew_qualification', crew_member_id,
              detail=f'position_id={position_id}')
    db.commit()
    db.close()
    syslog_logger.info(f"TECHNICIAN_{action} crew_member_id={crew_member_id} position_id={position_id} by={session.get('username')}")
    return jsonify({'success': True, 'has': has})


# ─── Asset Manager — Warehouse Locations ──────────────────────────────────────

@app.route('/settings/warehouse-locations', methods=['GET'])
@admin_required
def warehouse_locations_list():
    db = get_db()
    rows = db.execute('SELECT * FROM warehouse_locations ORDER BY sort_order, name').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/warehouse-locations', methods=['POST'])
@admin_required
def warehouse_location_add():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    try:
        max_order = db.execute('SELECT COALESCE(MAX(sort_order),0) FROM warehouse_locations').fetchone()[0]
        db.execute('INSERT INTO warehouse_locations (name, sort_order) VALUES (?,?)', (name, max_order + 1))
        db.commit()
        row = db.execute('SELECT * FROM warehouse_locations WHERE name=?', (name,)).fetchone()
        log_audit(db, 'WAREHOUSE_LOC_ADD', 'warehouse_location', row['id'], detail=name)
        db.commit()
        syslog_logger.info(f"WAREHOUSE_LOC_ADD name={name} by={session.get('username')}")
        result = dict(row)
        db.close()
        return jsonify(result), 201
    except DBIntegrityError:
        db.close()
        return jsonify({'error': 'Location name already exists'}), 409


@app.route('/settings/warehouse-locations/<int:loc_id>', methods=['PUT'])
@admin_required
def warehouse_location_edit(loc_id):
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    try:
        db.execute('UPDATE warehouse_locations SET name=? WHERE id=?', (name, loc_id))
        db.commit()
        log_audit(db, 'WAREHOUSE_LOC_EDIT', 'warehouse_location', loc_id, detail=name)
        db.commit()
        db.close()
        return jsonify({'success': True})
    except DBIntegrityError:
        db.close()
        return jsonify({'error': 'Location name already exists'}), 409


@app.route('/settings/warehouse-locations/<int:loc_id>', methods=['DELETE'])
@admin_required
def warehouse_location_delete(loc_id):
    db = get_db()
    row = db.execute('SELECT name FROM warehouse_locations WHERE id=?', (loc_id,)).fetchone()
    db.execute('DELETE FROM warehouse_locations WHERE id=?', (loc_id,))
    db.commit()
    log_audit(db, 'WAREHOUSE_LOC_DELETE', 'warehouse_location', loc_id,
              detail=row['name'] if row else str(loc_id))
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── Asset Manager — Categories ───────────────────────────────────────────────

@app.route('/settings/asset-categories', methods=['GET'])
@admin_required
def asset_categories_list():
    db = get_db()
    rows = db.execute('SELECT * FROM asset_categories ORDER BY sort_order, name').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/asset-categories', methods=['POST'])
@admin_required
def asset_category_add():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    max_order = db.execute('SELECT COALESCE(MAX(sort_order),0) FROM asset_categories').fetchone()[0]
    db.execute('INSERT INTO asset_categories (name, sort_order) VALUES (?,?)', (name, max_order + 1))
    db.commit()
    row = db.execute('SELECT * FROM asset_categories WHERE name=? ORDER BY id DESC LIMIT 1', (name,)).fetchone()
    log_audit(db, 'ASSET_CATEGORY_ADD', 'asset_category', row['id'], detail=name)
    db.commit()
    syslog_logger.info(f"ASSET_CATEGORY_ADD name={name} by={session.get('username')}")
    result = dict(row)
    db.close()
    return jsonify(result), 201


@app.route('/settings/asset-categories/<int:cat_id>', methods=['PUT'])
@admin_required
def asset_category_edit(cat_id):
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    db.execute('UPDATE asset_categories SET name=? WHERE id=?', (name, cat_id))
    db.commit()
    log_audit(db, 'ASSET_CATEGORY_EDIT', 'asset_category', cat_id, detail=name)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-categories/<int:cat_id>', methods=['DELETE'])
@admin_required
def asset_category_delete(cat_id):
    db = get_db()
    # Block deletion if any types (including retired) exist — preserves history
    type_count = db.execute(
        'SELECT COUNT(*) FROM asset_types WHERE category_id=?', (cat_id,)
    ).fetchone()[0]
    if type_count > 0:
        db.close()
        return jsonify({'error': f'Cannot delete: this category still has {type_count} item type(s). Retire all types first.'}), 400
    row = db.execute('SELECT name FROM asset_categories WHERE id=?', (cat_id,)).fetchone()
    db.execute('DELETE FROM asset_categories WHERE id=?', (cat_id,))
    db.commit()
    log_audit(db, 'ASSET_CATEGORY_DELETE', 'asset_category', cat_id,
              detail=row['name'] if row else str(cat_id))
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── Asset Manager — Types ────────────────────────────────────────────────────

@app.route('/api/asset-types', methods=['GET'])
@login_required
def asset_types_api():
    """Return active (non-retired) asset types for search/browse."""
    db = get_db()
    rows = db.execute("""
        SELECT at.*, ac.name as category_name,
               pt.name as parent_name
        FROM asset_types at
        JOIN asset_categories ac ON ac.id = at.category_id
        LEFT JOIN asset_types pt ON pt.id = at.parent_type_id
        WHERE at.is_retired = 0
        ORDER BY ac.sort_order, ac.name, at.sort_order, at.name
    """).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        d.pop('photo', None)  # Don't send blob over API
        result.append(d)
    return jsonify(result)


@app.route('/settings/asset-types', methods=['GET'])
@admin_required
def asset_types_admin_list():
    db = get_db()
    show_retired = request.args.get('show_retired') == '1'
    where = '' if show_retired else 'WHERE at.is_retired = 0'
    rows = db.execute(f"""
        SELECT at.*, ac.name as category_name,
               pt.name as parent_name,
               (SELECT COUNT(*) FROM asset_items ai WHERE ai.asset_type_id = at.id) as item_count,
               (SELECT COUNT(*) FROM asset_items ai WHERE ai.asset_type_id = at.id AND ai.status = 'retired') as retired_item_count
        FROM asset_types at
        JOIN asset_categories ac ON ac.id = at.category_id
        LEFT JOIN asset_types pt ON pt.id = at.parent_type_id
        {where}
        ORDER BY ac.sort_order, ac.name, at.sort_order, at.name
    """).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        d.pop('photo', None)
        result.append(d)
    return jsonify(result)


@app.route('/settings/asset-types', methods=['POST'])
@admin_required
def asset_type_add():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    category_id = data.get('category_id')
    if not name or not category_id:
        return jsonify({'error': 'Name and category required'}), 400
    db = get_db()
    max_order = db.execute('SELECT COALESCE(MAX(sort_order),0) FROM asset_types WHERE category_id=?',
                           (category_id,)).fetchone()[0]
    db.execute("""
        INSERT INTO asset_types
          (category_id, parent_type_id, name, manufacturer, model,
           storage_location, rental_cost, reserve_count, is_consumable, track_quantity,
           supplier_name, supplier_contact, sort_order)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        category_id,
        data.get('parent_type_id') or None,
        name,
        (data.get('manufacturer') or '').strip(),
        (data.get('model') or '').strip(),
        (data.get('storage_location') or '').strip(),
        float(data.get('rental_cost') or 0),
        int(data.get('reserve_count') or 0),
        1 if data.get('is_consumable') else 0,
        1 if data.get('track_quantity', True) else 0,
        (data.get('supplier_name') or '').strip(),
        (data.get('supplier_contact') or '').strip(),
        max_order + 1,
    ))
    db.commit()
    row = db.execute('SELECT * FROM asset_types ORDER BY id DESC LIMIT 1').fetchone()
    log_audit(db, 'ASSET_TYPE_ADD', 'asset_type', row['id'], detail=name)
    db.commit()
    syslog_logger.info(f"ASSET_TYPE_ADD name={name} category_id={category_id} by={session.get('username')}")
    result = dict(row)
    result.pop('photo', None)
    db.close()
    return jsonify(result), 201


@app.route('/settings/asset-types/<int:type_id>', methods=['PUT'])
@admin_required
def asset_type_edit(type_id):
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    db.execute("""
        UPDATE asset_types SET
          name=?, manufacturer=?, model=?, storage_location=?,
          rental_cost=?, reserve_count=?, is_consumable=?, track_quantity=?,
          supplier_name=?, supplier_contact=?,
          category_id=?, parent_type_id=?
        WHERE id=?
    """, (
        name,
        (data.get('manufacturer') or '').strip(),
        (data.get('model') or '').strip(),
        (data.get('storage_location') or '').strip(),
        float(data.get('rental_cost') or 0),
        int(data.get('reserve_count') or 0),
        1 if data.get('is_consumable') else 0,
        1 if data.get('track_quantity', True) else 0,
        (data.get('supplier_name') or '').strip(),
        (data.get('supplier_contact') or '').strip(),
        data.get('category_id'),
        data.get('parent_type_id') or None,
        type_id,
    ))
    db.commit()
    log_audit(db, 'ASSET_TYPE_EDIT', 'asset_type', type_id, detail=name)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-types/<int:type_id>', methods=['DELETE'])
@admin_required
def asset_type_delete(type_id):
    """Retire an asset type (soft delete) — history is preserved."""
    db = get_db()
    row = db.execute('SELECT name FROM asset_types WHERE id=?', (type_id,)).fetchone()
    db.execute("""
        UPDATE asset_types SET is_retired=1, retired_at=CURRENT_TIMESTAMP WHERE id=?
    """, (type_id,))
    # Retire all active items under this type too
    db.execute("""
        UPDATE asset_items SET status='retired' WHERE asset_type_id=? AND status='available'
    """, (type_id,))
    db.commit()
    log_audit(db, 'ASSET_TYPE_RETIRE', 'asset_type', type_id,
              detail=row['name'] if row else str(type_id))
    db.commit()
    syslog_logger.info(f"ASSET_TYPE_RETIRE type_id={type_id} by={session.get('username')}")
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-types/<int:type_id>/photo', methods=['POST'])
@admin_required
def asset_type_photo_upload(type_id):
    f = request.files.get('photo')
    if not f:
        return jsonify({'error': 'No file'}), 400
    mime = f.mimetype or 'image/jpeg'
    data = f.read()
    db = get_db()
    db.execute('UPDATE asset_types SET photo=?, photo_mime=? WHERE id=?', (data, mime, type_id))
    db.commit()
    log_audit(db, 'ASSET_TYPE_PHOTO', 'asset_type', type_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-types/<int:type_id>/photo', methods=['DELETE'])
@admin_required
def asset_type_photo_delete(type_id):
    db = get_db()
    db.execute("UPDATE asset_types SET photo=NULL, photo_mime='' WHERE id=?", (type_id,))
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/asset-types/<int:type_id>/photo')
@login_required
def asset_type_photo(type_id):
    db = get_db()
    row = db.execute('SELECT photo, photo_mime FROM asset_types WHERE id=?', (type_id,)).fetchone()
    db.close()
    if not row or not row['photo']:
        abort(404)
    resp = make_response(row['photo'])
    resp.headers['Content-Type'] = row['photo_mime'] or 'image/jpeg'
    resp.headers['Cache-Control'] = 'max-age=86400'
    return resp


# ─── Asset Manager — Items ────────────────────────────────────────────────────

@app.route('/settings/asset-types/<int:type_id>/items', methods=['GET'])
@admin_required
def asset_items_list(type_id):
    db = get_db()
    show_retired = request.args.get('show_retired') == '1'
    status_filter = '' if show_retired else "AND ai.status != 'retired'"
    rows = db.execute(f"""
        SELECT ai.*,
               COALESCE(am.status, 'available') as maint_status,
               am.reason as maint_reason,
               am.notes as maint_notes,
               am.id as maint_id
        FROM asset_items ai
        LEFT JOIN asset_maintenance am ON am.asset_item_id = ai.id AND am.status = 'in_progress'
        WHERE ai.asset_type_id = ? {status_filter}
        ORDER BY ai.status, ai.sort_order, ai.id
    """, (type_id,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/asset-types/<int:type_id>/items', methods=['POST'])
@admin_required
def asset_item_add(type_id):
    data = request.get_json() or {}
    count = int(data.get('count') or 1)
    barcode = (data.get('barcode') or '').strip()
    db = get_db()
    max_order = db.execute('SELECT COALESCE(MAX(sort_order),0) FROM asset_items WHERE asset_type_id=?',
                           (type_id,)).fetchone()[0]
    added_ids = []
    for i in range(count):
        bc = barcode if count == 1 else ''
        db.execute('INSERT INTO asset_items (asset_type_id, barcode, status, sort_order) VALUES (?,?,?,?)',
                   (type_id, bc, 'available', max_order + i + 1))
        db.commit()
        row = db.execute('SELECT * FROM asset_items ORDER BY id DESC LIMIT 1').fetchone()
        added_ids.append(row['id'])
    log_audit(db, 'ASSET_ITEM_ADD', 'asset_item', type_id, detail=f'count={count}')
    db.commit()
    syslog_logger.info(f"ASSET_ITEM_ADD type_id={type_id} count={count} by={session.get('username')}")
    db.close()
    return jsonify({'success': True, 'added': len(added_ids)}), 201


@app.route('/settings/asset-items/<int:item_id>', methods=['PUT'])
@admin_required
def asset_item_edit(item_id):
    data = request.get_json() or {}
    db = get_db()
    def _int_or_none(v):
        try: return int(v) if v not in (None, '', 'null') else None
        except (ValueError, TypeError): return None
    def _float_or_none(v):
        try: return float(v) if v not in (None, '', 'null') else None
        except (ValueError, TypeError): return None
    valid_conditions = {'excellent', 'good', 'fair', 'poor', 'retired'}
    condition = data.get('condition', 'good')
    if condition not in valid_conditions:
        condition = 'good'
    db.execute("""
        UPDATE asset_items SET
          barcode=?, condition=?, year_purchased=?, purchase_value=?,
          depreciation_years=?, warranty_expires=?
        WHERE id=?
    """, (
        (data.get('barcode') or '').strip(),
        condition,
        _int_or_none(data.get('year_purchased')),
        _float_or_none(data.get('purchase_value')),
        _int_or_none(data.get('depreciation_years')),
        (data.get('warranty_expires') or '').strip() or None,
        item_id,
    ))
    db.commit()
    log_audit(db, 'ASSET_ITEM_EDIT', 'asset_item', item_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-items/<int:item_id>/logs', methods=['GET'])
@admin_required
def asset_item_logs_list(item_id):
    db = get_db()
    rows = db.execute("""
        SELECT al.*, u.display_name as author_name
        FROM asset_logs al
        LEFT JOIN users u ON u.id = al.user_id
        WHERE al.asset_item_id = ?
        ORDER BY al.log_date DESC, al.created_at DESC
    """, (item_id,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/asset-items/<int:item_id>/logs', methods=['POST'])
@admin_required
def asset_item_log_add(item_id):
    data = request.get_json() or {}
    body = (data.get('body') or '').strip()
    if not body:
        return jsonify({'error': 'Entry body required'}), 400
    log_type = data.get('log_type', 'note')
    if log_type not in ('note', 'damage', 'service', 'usage'):
        log_type = 'note'
    import re as _re
    from datetime import date as _date
    log_date = (data.get('log_date') or '').strip()
    if not log_date:
        log_date = _date.today().isoformat()
    elif not _re.match(r'^\d{4}-\d{2}-\d{2}$', log_date):
        return jsonify({'error': 'Invalid log_date format; use YYYY-MM-DD'}), 400
    db = get_db()
    db.execute("""
        INSERT INTO asset_logs (asset_item_id, user_id, log_date, log_type, body)
        VALUES (?,?,?,?,?)
    """, (item_id, session['user_id'], log_date, log_type, body))
    db.commit()
    row = db.execute("""
        SELECT al.*, u.display_name as author_name
        FROM asset_logs al LEFT JOIN users u ON u.id = al.user_id
        WHERE al.id = last_insert_rowid()
    """).fetchone()
    db.close()
    syslog_logger.info(f"ASSET_LOG_ADD item_id={item_id} log_type={log_type} by={session.get('username')}")
    return jsonify(dict(row)), 201


@app.route('/settings/asset-logs/<int:log_id>', methods=['DELETE'])
@admin_required
def asset_log_delete(log_id):
    db = get_db()
    db.execute('DELETE FROM asset_logs WHERE id=?', (log_id,))
    db.commit()
    db.close()
    syslog_logger.info(f"ASSET_LOG_DELETE log_id={log_id} by={session.get('username')}")
    return jsonify({'success': True})


@app.route('/settings/asset-items/<int:item_id>', methods=['DELETE'])
@admin_required
def asset_item_delete(item_id):
    """Retire an asset item (soft delete) — history is preserved."""
    db = get_db()
    db.execute("UPDATE asset_items SET status='retired' WHERE id=?", (item_id,))
    db.commit()
    log_audit(db, 'ASSET_ITEM_RETIRE', 'asset_item', item_id)
    db.commit()
    syslog_logger.info(f"ASSET_ITEM_RETIRE item_id={item_id} by={session.get('username')}")
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-items/<int:item_id>/maintenance', methods=['POST'])
@admin_required
def asset_item_maintenance_start(item_id):
    data = request.get_json() or {}
    reason = (data.get('reason') or '').strip()
    notes = (data.get('notes') or '').strip()
    db = get_db()
    # Close any open maintenance records first
    db.execute("UPDATE asset_maintenance SET status='resolved', resolved_at=CURRENT_TIMESTAMP WHERE asset_item_id=? AND status='in_progress'",
               (item_id,))
    db.execute("""
        INSERT INTO asset_maintenance (asset_item_id, removed_by, reason, notes, status)
        VALUES (?,?,?,?,'in_progress')
    """, (item_id, session['user_id'], reason, notes))
    db.execute("UPDATE asset_items SET status='maintenance' WHERE id=?", (item_id,))
    db.commit()
    log_audit(db, 'ASSET_MAINT_START', 'asset_item', item_id, detail=reason)
    db.commit()
    syslog_logger.info(f"ASSET_MAINT_START item_id={item_id} reason={reason} by={session.get('username')}")
    db.close()
    return jsonify({'success': True})


@app.route('/settings/asset-items/<int:item_id>/maintenance/resolve', methods=['POST'])
@admin_required
def asset_item_maintenance_resolve(item_id):
    data = request.get_json() or {}
    notes = (data.get('notes') or '').strip()
    db = get_db()
    db.execute("""
        UPDATE asset_maintenance
        SET status='resolved', resolved_at=CURRENT_TIMESTAMP, notes=COALESCE(NULLIF(?,''), notes)
        WHERE asset_item_id=? AND status='in_progress'
    """, (notes, item_id))
    db.execute("UPDATE asset_items SET status='available' WHERE id=?", (item_id,))
    db.commit()
    log_audit(db, 'ASSET_MAINT_RESOLVE', 'asset_item', item_id)
    db.commit()
    syslog_logger.info(f"ASSET_MAINT_RESOLVE item_id={item_id} by={session.get('username')}")
    db.close()
    return jsonify({'success': True})


# ─── Asset Manager — Availability ─────────────────────────────────────────────

def _get_asset_availability(db, asset_type_id, start_date=None, end_date=None):
    """
    Return dict with:
      total_items   — count of all items for this type
      reserve_count — held back as spares
      in_maintenance — items currently in maintenance
      available     — items available for the date range (may be negative)
      shows         — list of shows requesting this asset with quantities
    """
    type_row = db.execute('SELECT * FROM asset_types WHERE id=?', (asset_type_id,)).fetchone()
    if not type_row:
        return None

    total_items = db.execute(
        "SELECT COUNT(*) FROM asset_items WHERE asset_type_id=? AND status != 'retired'",
        (asset_type_id,)
    ).fetchone()[0]

    in_maintenance = db.execute(
        "SELECT COUNT(*) FROM asset_items WHERE asset_type_id=? AND status='maintenance'",
        (asset_type_id,)
    ).fetchone()[0]

    reserve_count = type_row['reserve_count'] or 0

    # For consumables with unlimited stock
    if type_row['is_consumable'] and not type_row['track_quantity']:
        return {
            'total_items': None,
            'reserve_count': reserve_count,
            'in_maintenance': 0,
            'available': None,
            'shows': [],
            'unlimited': True,
        }

    # Shows requesting this asset (optionally filtered by date range overlap)
    params = [asset_type_id]
    date_filter = ''
    if start_date and end_date:
        date_filter = ' AND (sa.rental_end >= ? AND sa.rental_start <= ?)'
        params.extend([start_date, end_date])

    shows = db.execute(f"""
        SELECT sa.id, sa.show_id, sa.quantity, sa.rental_start, sa.rental_end,
               sa.is_hidden, s.name as show_name
        FROM show_assets sa
        JOIN shows s ON s.id = sa.show_id
        WHERE sa.asset_type_id = ?{date_filter}
        ORDER BY sa.rental_start
    """, params).fetchall()

    total_reserved = sum(r['quantity'] for r in shows)
    available = total_items - in_maintenance - reserve_count - total_reserved

    return {
        'total_items': total_items,
        'reserve_count': reserve_count,
        'in_maintenance': in_maintenance,
        'total_reserved': total_reserved,
        'available': available,
        'shows': [dict(r) for r in shows],
        'unlimited': False,
    }


@app.route('/api/asset-types/<int:type_id>/availability')
@login_required
def asset_type_availability(type_id):
    start = request.args.get('start')
    end = request.args.get('end')
    db = get_db()
    result = _get_asset_availability(db, type_id, start, end)
    db.close()
    if result is None:
        abort(404)
    return jsonify(result)


@app.route('/api/assets/availability')
@login_required
def assets_availability_bulk():
    """Return availability summary for all asset types, plus by-show data."""
    date_from = request.args.get('from')
    date_to   = request.args.get('to')
    db = get_db()
    type_ids = [r['id'] for r in db.execute('SELECT id FROM asset_types WHERE is_retired=0').fetchall()]
    by_type = {}
    for tid in type_ids:
        info = _get_asset_availability(db, tid, date_from, date_to)
        if info:
            by_type[tid] = {
                'total':       info.get('total_items'),
                'maintenance': info.get('in_maintenance', 0),
                'reserved':    info.get('reserve_count', 0),
                'available':   info.get('available'),
            }

    # By-show summary (for 'by_show' layout) — respect access control
    accessible_ids = get_accessible_shows(session['user_id'])  # None = all, [] = none, list = specific
    params = []
    where  = []
    if accessible_ids is not None and len(accessible_ids) > 0:
        placeholders = ','.join('?' * len(accessible_ids))
        where.append(f's.id IN ({placeholders})')
        params.extend(accessible_ids)
    elif accessible_ids is not None and len(accessible_ids) == 0:
        db.close()
        return jsonify({'by_type': by_type, 'by_show': []})
    if date_from: where.append("COALESCE(s.show_date,'9999') >= ?"); params.append(date_from)
    if date_to:   where.append("COALESCE(s.show_date,'0000') <= ?"); params.append(date_to)
    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    shows_raw = db.execute(f"""
        SELECT s.id, s.name, s.show_date FROM shows s {where_sql}
        ORDER BY s.show_date
    """, params).fetchall()
    by_show = []
    for sr in shows_raw:
        assets = db.execute("""
            SELECT sa.quantity, sa.locked_price, sa.rental_start, sa.rental_end,
                   at.name as type_name, at.manufacturer,
                   ac.name as category_name
            FROM show_assets sa
            JOIN asset_types at ON at.id = sa.asset_type_id
            JOIN asset_categories ac ON ac.id = at.category_id
            WHERE sa.show_id = ? AND sa.is_hidden = 0
            ORDER BY ac.name, at.name
        """, (sr['id'],)).fetchall()
        if assets:
            by_show.append({
                'id':       sr['id'],
                'name':     sr['name'],
                'show_date':sr['show_date'],
                'assets':   [dict(a) for a in assets],
            })
    db.close()
    return jsonify({'by_type': by_type, 'by_show': by_show})


# ─── Asset Manager — Show Assets (per-show tab) ───────────────────────────────

@app.route('/shows/<int:show_id>/assets', methods=['GET'])
@login_required
def show_assets_list(show_id):
    if not can_access_show(session['user_id'], show_id):
        return jsonify({'error': 'Access denied'}), 403
    db = get_db()
    user_is_restricted = session.get('is_restricted', False)
    user_is_admin = session.get('user_role') == 'admin'

    rows = db.execute("""
        SELECT sa.*, at.name as type_name, at.is_consumable, at.manufacturer, at.model,
               at.rental_cost as current_price,
               ac.name as category_name
        FROM show_assets sa
        JOIN asset_types at ON at.id = sa.asset_type_id
        JOIN asset_categories ac ON ac.id = at.category_id
        WHERE sa.show_id = ?
          AND (? = 1 OR sa.is_hidden = 0)
        ORDER BY ac.name, at.name, sa.created_at
    """, (show_id, 1 if user_is_admin else 0)).fetchall()

    # External rentals
    ext_rows = db.execute("""
        SELECT * FROM show_external_rentals WHERE show_id=? ORDER BY sort_order, id
    """, (show_id,)).fetchall()

    db.close()
    return jsonify({
        'assets': [dict(r) for r in rows],
        'external_rentals': [{k: v for k, v in dict(r).items() if k != 'pdf_data'} for r in ext_rows],
    })


@app.route('/shows/<int:show_id>/assets', methods=['POST'])
@content_admin_required
def show_asset_add(show_id):
    data = request.get_json() or {}
    asset_type_id = data.get('asset_type_id')
    quantity = int(data.get('quantity') or 1)
    if not asset_type_id or quantity < 1:
        return jsonify({'error': 'asset_type_id and quantity required'}), 400

    db = get_db()

    # Get show dates for rental period defaults
    show = db.execute('SELECT * FROM shows WHERE id=?', (show_id,)).fetchone()
    perfs = db.execute(
        'SELECT perf_date FROM show_performances WHERE show_id=? ORDER BY perf_date', (show_id,)
    ).fetchall()
    rental_start = data.get('rental_start') or (perfs[0]['perf_date'] if perfs else show['show_date'])
    rental_end = data.get('rental_end') or (perfs[-1]['perf_date'] if perfs else show['show_date'])

    # Lock current price
    type_row = db.execute('SELECT rental_cost FROM asset_types WHERE id=?', (asset_type_id,)).fetchone()
    locked_price = float(data.get('locked_price') if data.get('locked_price') is not None
                         else (type_row['rental_cost'] if type_row else 0))

    is_hidden = 1 if data.get('is_hidden') else 0

    db.execute("""
        INSERT INTO show_assets
          (show_id, asset_type_id, quantity, rental_start, rental_end, locked_price, is_hidden, notes, added_by)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (show_id, asset_type_id, quantity, rental_start, rental_end, locked_price,
          is_hidden, (data.get('notes') or '').strip(), session['user_id']))
    db.commit()
    row = db.execute('SELECT * FROM show_assets ORDER BY id DESC LIMIT 1').fetchone()
    log_audit(db, 'ASSET_ADDED_TO_SHOW', 'show_asset', row['id'], show_id=show_id,
              detail=f'type_id={asset_type_id} qty={quantity}')
    db.commit()
    syslog_logger.info(f"ASSET_ADDED_TO_SHOW show_id={show_id} type_id={asset_type_id} qty={quantity} by={session.get('username')}")
    result = dict(row)
    # Check for over-allocation and notify admins
    try:
        _avail = _get_asset_availability(db, asset_type_id, rental_start, rental_end)
        if _avail and not _avail.get('unlimited') and _avail.get('available') is not None and _avail['available'] < 0:
            _trow = db.execute('SELECT name FROM asset_types WHERE id=?', (asset_type_id,)).fetchone()
            _type_name = _trow['name'] if _trow else f'Type #{asset_type_id}'
            _show_name = show['name'] if show else f'Show #{show_id}'
            _admin_emails = [r['email'] for r in db.execute(
                "SELECT email FROM users WHERE role='admin' AND email != '' AND email IS NOT NULL"
            ).fetchall()]
            for _ae in _admin_emails:
                _send_simple_email(
                    _ae,
                    f'3\u00b72\u00b71\u2192THEATER: Asset Over-Allocated \u2014 {_type_name}',
                    f'Asset "{_type_name}" is now over-allocated for show "{_show_name}".\n\n'
                    f'Current availability: {_avail["available"]} (negative = over-allocated)\n'
                    f'Total units: {_avail.get("total_items","?")}, In maintenance: {_avail.get("in_maintenance",0)}, '
                    f'Reserved spares: {_avail.get("reserve_count",0)}\n\n'
                    f'Review the show\'s assets tab for details.',
                )
    except Exception:
        pass
    db.close()
    return jsonify(result), 201


@app.route('/shows/<int:show_id>/assets/<int:sa_id>', methods=['PUT'])
@content_admin_required
def show_asset_edit(show_id, sa_id):
    data = request.get_json() or {}
    db = get_db()
    db.execute("""
        UPDATE show_assets SET quantity=?, rental_start=?, rental_end=?,
               is_hidden=?, notes=?
        WHERE id=? AND show_id=?
    """, (
        int(data.get('quantity') or 1),
        data.get('rental_start') or None,
        data.get('rental_end') or None,
        1 if data.get('is_hidden') else 0,
        (data.get('notes') or '').strip(),
        sa_id, show_id,
    ))
    db.commit()
    log_audit(db, 'ASSET_SHOW_EDIT', 'show_asset', sa_id, show_id=show_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/assets/<int:sa_id>', methods=['DELETE'])
@content_admin_required
def show_asset_remove(show_id, sa_id):
    db = get_db()
    row = db.execute('SELECT * FROM show_assets WHERE id=? AND show_id=?', (sa_id, show_id)).fetchone()
    if not row:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    db.execute('DELETE FROM show_assets WHERE id=?', (sa_id,))
    db.commit()
    log_audit(db, 'ASSET_REMOVED_FROM_SHOW', 'show_asset', sa_id, show_id=show_id,
              detail=f'type_id={row["asset_type_id"]}')
    db.commit()
    syslog_logger.info(f"ASSET_REMOVED_FROM_SHOW show_id={show_id} sa_id={sa_id} by={session.get('username')}")
    db.close()
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/assets/<int:sa_id>/toggle-hidden', methods=['POST'])
@content_admin_required
def show_asset_toggle_hidden(show_id, sa_id):
    db = get_db()
    row = db.execute('SELECT is_hidden FROM show_assets WHERE id=? AND show_id=?', (sa_id, show_id)).fetchone()
    if not row:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    new_val = 0 if row['is_hidden'] else 1
    db.execute('UPDATE show_assets SET is_hidden=? WHERE id=?', (new_val, sa_id))
    db.commit()
    log_audit(db, 'ASSET_HIDE_TOGGLE', 'show_asset', sa_id, show_id=show_id,
              detail=f'hidden={new_val}')
    db.commit()
    db.close()
    return jsonify({'success': True, 'is_hidden': new_val})


# ─── Asset Manager — External Rentals ─────────────────────────────────────────

@app.route('/shows/<int:show_id>/external-rentals', methods=['POST'])
@content_admin_required
def external_rental_add(show_id):
    db = get_db()
    description = (request.form.get('description') or '').strip()
    cost = float(request.form.get('cost') or 0)
    if not description:
        return jsonify({'error': 'Description required'}), 400
    pdf_data = None
    pdf_filename = ''
    f = request.files.get('pdf')
    if f:
        pdf_data = f.read()
        pdf_filename = secure_filename(f.filename)
    max_order = db.execute('SELECT COALESCE(MAX(sort_order),0) FROM show_external_rentals WHERE show_id=?',
                           (show_id,)).fetchone()[0]
    db.execute("""
        INSERT INTO show_external_rentals (show_id, description, cost, pdf_data, pdf_filename, sort_order)
        VALUES (?,?,?,?,?,?)
    """, (show_id, description, cost, pdf_data, pdf_filename, max_order + 1))
    db.commit()
    row = db.execute('SELECT * FROM show_external_rentals ORDER BY id DESC LIMIT 1').fetchone()
    log_audit(db, 'EXTERNAL_RENTAL_ADD', 'show_external_rental', row['id'], show_id=show_id,
              detail=description)
    db.commit()
    result = {k: v for k, v in dict(row).items() if k != 'pdf_data'}
    db.close()
    return jsonify(result), 201


@app.route('/shows/<int:show_id>/external-rentals/<int:er_id>', methods=['DELETE'])
@content_admin_required
def external_rental_delete(show_id, er_id):
    db = get_db()
    db.execute('DELETE FROM show_external_rentals WHERE id=? AND show_id=?', (er_id, show_id))
    db.commit()
    log_audit(db, 'EXTERNAL_RENTAL_DELETE', 'show_external_rental', er_id, show_id=show_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/shows/<int:show_id>/external-rentals/<int:er_id>/pdf')
@login_required
def external_rental_pdf(show_id, er_id):
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    row = db.execute('SELECT * FROM show_external_rentals WHERE id=? AND show_id=?',
                     (er_id, show_id)).fetchone()
    db.close()
    if not row or not row['pdf_data']:
        abort(404)
    resp = make_response(row['pdf_data'])
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = _safe_content_disposition(row['pdf_filename'] or 'rental.pdf')
    return resp


# ─── Asset Manager — Asset Page (admin view) ──────────────────────────────────

@app.route('/assets')
@admin_required
def assets_admin():
    db = get_db()
    categories = db.execute('SELECT * FROM asset_categories ORDER BY sort_order, name').fetchall()
    locations = db.execute('SELECT * FROM warehouse_locations ORDER BY sort_order, name').fetchall()
    db.close()
    return render_template('assets.html',
                           categories=[dict(c) for c in categories],
                           locations=[dict(l) for l in locations],
                           user=get_current_user())


@app.route('/assets/retired')
@admin_required
def assets_retired():
    db = get_db()
    # Retired types with their items and log counts
    types = db.execute("""
        SELECT at.*, ac.name as category_name,
               (SELECT COUNT(*) FROM asset_items ai WHERE ai.asset_type_id = at.id) as total_items,
               (SELECT COUNT(*) FROM asset_items ai WHERE ai.asset_type_id = at.id AND ai.status='retired') as retired_items
        FROM asset_types at
        JOIN asset_categories ac ON ac.id = at.category_id
        WHERE at.is_retired = 1
        ORDER BY at.retired_at DESC
    """).fetchall()
    # Also standalone retired items (type is still active, but item was individually retired)
    standalone = db.execute("""
        SELECT ai.*, at.name as type_name, at.manufacturer, at.model,
               ac.name as category_name,
               (SELECT COUNT(*) FROM asset_logs al WHERE al.asset_item_id = ai.id) as log_count
        FROM asset_items ai
        JOIN asset_types at ON at.id = ai.asset_type_id
        JOIN asset_categories ac ON ac.id = at.category_id
        WHERE ai.status = 'retired' AND at.is_retired = 0
        ORDER BY ai.created_at DESC
    """).fetchall()
    db.close()
    return render_template('asset_retired.html',
                           retired_types=[dict(t) for t in types],
                           standalone_items=[dict(s) for s in standalone],
                           user=get_current_user())


# ─── Asset Invoice PDF ───────────────────────────────────────────────────────

@app.route('/shows/<int:show_id>/assets/invoice.pdf')
@login_required
def show_asset_invoice(show_id):
    """Generate a PDF invoice for all show assets and external rentals."""
    if not can_access_show(session['user_id'], show_id):
        abort(403)
    db = get_db()
    show = db.execute('SELECT * FROM shows WHERE id=?', (show_id,)).fetchone()
    if not show:
        db.close()
        abort(404)

    assets = db.execute("""
        SELECT sa.quantity, sa.locked_price, sa.rental_start, sa.rental_end, sa.notes,
               at.name as type_name, at.manufacturer, at.model,
               ac.name as category_name
        FROM show_assets sa
        JOIN asset_types at ON at.id = sa.asset_type_id
        JOIN asset_categories ac ON ac.id = at.category_id
        WHERE sa.show_id = ? AND sa.is_hidden = 0
        ORDER BY ac.sort_order, at.name
    """, (show_id,)).fetchall()

    external_rentals = db.execute("""
        SELECT description, cost, pdf_filename
        FROM show_external_rentals
        WHERE show_id = ?
        ORDER BY sort_order
    """, (show_id,)).fetchall()

    perf_company_row = db.execute(
        "SELECT field_value FROM advance_data WHERE show_id=? AND field_key='performance_company'",
        (show_id,)
    ).fetchone()
    performance_company = perf_company_row['field_value'] if perf_company_row else ''

    assets_list = [dict(a) for a in assets]
    ext_list    = [dict(e) for e in external_rentals]
    assets_subtotal  = sum((a['locked_price'] or 0) * a['quantity'] for a in assets_list)
    external_subtotal = sum(e['cost'] or 0 for e in ext_list)
    grand_total = assets_subtotal + external_subtotal
    db.close()

    html_str = render_template(
        'pdf/asset_invoice_pdf.html',
        show=dict(show),
        assets=assets_list,
        external_rentals=ext_list,
        assets_subtotal=assets_subtotal,
        external_subtotal=external_subtotal,
        grand_total=grand_total,
        performance_company=performance_company,
        generated_date=date.today().isoformat(),
    )

    try:
        from weasyprint import HTML as WP_HTML
        pdf_bytes = WP_HTML(string=html_str, base_url=request.host_url).write_pdf()
    except Exception as e:
        app.logger.error(f'WeasyPrint invoice error: {e}')
        return f'PDF generation failed: {e}', 500

    safe_name = secure_filename(show['name'] or f'show_{show_id}')
    resp = make_response(pdf_bytes)
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = _safe_content_disposition(f'{safe_name}_invoice.pdf')
    return resp


# ─── In-App Updates ───────────────────────────────────────────────────────────

_update_state = {'running': False, 'log': [], 'phase': 'idle', 'error': None}
_update_lock  = threading.Lock()

def _update_log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    entry = f'[{ts}] {msg}'
    _update_state['log'].append(entry)
    app.logger.info(f'[updater] {msg}')

def _detect_service_name():
    """Auto-detect the systemd service this process is running under."""
    pid = os.getpid()
    try:
        r = subprocess.run(['systemctl', 'status', str(pid)],
                           capture_output=True, text=True, timeout=5)
        for line in r.stdout.split('\n'):
            m = re.search(r'(\S+\.service)', line)
            if m:
                return m.group(1)
    except Exception:
        pass
    # Try common names
    for name in ['showadvance', 'showadvance.service', '321theater', '321theater.service',
                 'gunicorn', 'gunicorn.service']:
        try:
            r = subprocess.run(['systemctl', 'is-active', name],
                               capture_output=True, text=True, timeout=2)
            if r.stdout.strip() == 'active':
                return name
        except Exception:
            pass
    return None

def _run_update(service_name):
    """Background thread: git pull + archive + restart + rollback on failure."""
    import glob as _glob
    update_archive = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'backups', f'pre_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    archived_files = []

    try:
        _update_state['phase'] = 'checking'
        _update_log('Fetching latest commits from remote…')
        r = subprocess.run(['git', 'fetch', 'origin'], capture_output=True, text=True, timeout=30,
                           cwd=os.path.dirname(os.path.abspath(__file__)))
        if r.returncode != 0:
            raise RuntimeError(f'git fetch failed: {r.stderr.strip()}')
        _update_log('Fetch complete.')

        # Get list of files that will change
        r = subprocess.run(['git', 'diff', '--name-only', 'HEAD', 'origin/HEAD'],
                           capture_output=True, text=True, timeout=10,
                           cwd=os.path.dirname(os.path.abspath(__file__)))
        changed = [f.strip() for f in r.stdout.split('\n') if f.strip()]
        if not changed:
            _update_log('Already up to date — no changes to apply.')
            _update_state['phase'] = 'done'
            return

        _update_log(f'Files to update: {", ".join(changed)}')

        # Archive changed files
        _update_state['phase'] = 'archiving'
        _update_log(f'Archiving {len(changed)} files to {update_archive}…')
        os.makedirs(update_archive, exist_ok=True)
        base = os.path.dirname(os.path.abspath(__file__))
        for rel in changed:
            src = os.path.join(base, rel)
            if os.path.exists(src):
                dst = os.path.join(update_archive, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                archived_files.append((src, dst))
        _update_log(f'Archived {len(archived_files)} files.')

        # Pull
        _update_state['phase'] = 'pulling'
        _update_log('Running git pull…')
        r = subprocess.run(['git', 'pull', '--ff-only'],
                           capture_output=True, text=True, timeout=60,
                           cwd=os.path.dirname(os.path.abspath(__file__)))
        if r.returncode != 0:
            raise RuntimeError(f'git pull failed: {r.stderr.strip() or r.stdout.strip()}')
        _update_log(r.stdout.strip() or 'Pull successful.')

        # Run DB migration
        _update_log('Running database migrations…')
        r = subprocess.run(['python', 'init_db.py', '--migrate'],
                           capture_output=True, text=True, timeout=60,
                           cwd=os.path.dirname(os.path.abspath(__file__)))
        _update_log(r.stdout.strip() or 'Migrations complete.')

        # Restart service
        if service_name:
            _update_state['phase'] = 'restarting'
            _update_log(f'Restarting service {service_name}…')
            r = subprocess.run(['systemctl', 'restart', service_name],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                raise RuntimeError(f'Service restart failed: {r.stderr.strip()}')
            # Wait and check it came back
            import time as _time
            _time.sleep(3)
            r2 = subprocess.run(['systemctl', 'is-active', service_name],
                                capture_output=True, text=True, timeout=5)
            if r2.stdout.strip() != 'active':
                raise RuntimeError(f'Service not active after restart: {r2.stdout.strip()}')
            _update_log(f'Service {service_name} is active.')
        else:
            _update_log('No systemd service detected — please restart the app manually.')

        _update_state['phase'] = 'done'
        _update_log('Update complete!')
        syslog_logger.info('APP_UPDATE applied successfully')

    except Exception as exc:
        _update_state['error'] = str(exc)
        _update_log(f'ERROR: {exc}')
        _update_log('Attempting rollback…')
        _update_state['phase'] = 'rolling_back'
        try:
            for src, bak in archived_files:
                shutil.copy2(bak, src)
                _update_log(f'  Restored {os.path.basename(src)}')
            _update_log('Rollback complete.')
            if service_name:
                _update_log(f'Restarting {service_name} after rollback…')
                subprocess.run(['systemctl', 'restart', service_name], timeout=30)
                _update_log('Restart issued.')
        except Exception as rb_exc:
            _update_log(f'Rollback failed: {rb_exc}')
        _update_state['phase'] = 'failed'
        syslog_logger.error(f'APP_UPDATE failed: {exc}')


@app.route('/settings/update/check')
@admin_required
def update_check():
    """Check whether remote has updates without applying them."""
    try:
        r = subprocess.run(['git', 'fetch', 'origin'], capture_output=True, text=True, timeout=20,
                           cwd=os.path.dirname(os.path.abspath(__file__)))
        r2 = subprocess.run(['git', 'log', 'HEAD..origin/HEAD', '--oneline'],
                            capture_output=True, text=True, timeout=10,
                            cwd=os.path.dirname(os.path.abspath(__file__)))
        commits = [l.strip() for l in r2.stdout.split('\n') if l.strip()]
        r3 = subprocess.run(['git', 'diff', '--name-only', 'HEAD', 'origin/HEAD'],
                            capture_output=True, text=True, timeout=10,
                            cwd=os.path.dirname(os.path.abspath(__file__)))
        files = [f.strip() for f in r3.stdout.split('\n') if f.strip()]
        return jsonify({'available': bool(commits), 'commits': commits, 'files': files})
    except Exception as e:
        return jsonify({'available': False, 'error': str(e)})


@app.route('/settings/update/apply', methods=['POST'])
@admin_required
def update_apply():
    """Start the update process in a background thread."""
    with _update_lock:
        if _update_state['running']:
            return jsonify({'error': 'Update already in progress'}), 409
        _update_state.update({'running': True, 'log': [], 'phase': 'starting', 'error': None})
    data = request.get_json(force=True) or {}
    service_name = data.get('service_name') or _detect_service_name()
    log_audit(get_db(), 'APP_UPDATE_START', 'system', None,
              detail=f'service={service_name} by={session.get("username")}')
    try:
        get_db().commit()
        get_db().close()
    except Exception:
        pass
    t = threading.Thread(target=_run_update, args=(service_name,), daemon=True)
    t.start()
    syslog_logger.info(f'APP_UPDATE_START service={service_name} by={session.get("username")}')
    return jsonify({'success': True, 'service': service_name})


@app.route('/api/update/status')
@admin_required
def update_status_api():
    return jsonify({
        'phase':   _update_state['phase'],
        'running': _update_state['running'],
        'log':     _update_state['log'],
        'error':   _update_state['error'],
    })


@app.route('/settings/update/detect-service')
@admin_required
def detect_service():
    return jsonify({'service': _detect_service_name()})


# ─── User Registration & Recovery ─────────────────────────────────────────────

def _send_simple_email(to_addr, subject, body_text, body_html=None):
    """Send a plain email using the configured provider (reuses app email infra)."""
    try:
        _send_email(to_addr, subject, body_text, body_html)
        return True
    except Exception as e:
        app.logger.error(f'Email send failed to {to_addr}: {e}')
        return False

def _send_email(to_addr, subject, plain, html=None):
    """Dispatch via SMTP or direct MX depending on settings."""
    provider = get_app_setting('email_provider', 'smtp')
    from_addr = get_app_setting('smtp_from', 'noreply@localhost')
    if provider == 'smtp':
        _send_email_smtp(from_addr, [to_addr], subject, plain, html)
    else:
        _send_email_direct(from_addr, [to_addr], subject, plain, html)


def _register_route():
    if session.get('user_id'):
        return redirect(url_for('dashboard'))
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        confirm  = request.form.get('confirm_password', '')
        score    = int(request.form.get('captcha_score', 0))

        if score < 1:
            error = 'Please complete the mini-game challenge first.'
        elif not username or not email or not password:
            error = 'All fields are required.'
        elif password != confirm:
            error = 'Passwords do not match.'
        elif len(password) < 8:
            error = 'Password must be at least 8 characters.'
        elif not re.match(r'^[a-z0-9_.-]{3,32}$', username):
            error = 'Username: 3-32 characters, letters/numbers/._- only.'
        else:
            db = get_db()
            existing = db.execute('SELECT id FROM users WHERE username=?', (username,)).fetchone()
            if existing:
                error = 'That username or email is not available.'
            else:
                existing_pending = db.execute(
                    'SELECT id FROM user_pending_registration WHERE username=?', (username,)).fetchone()
                if existing_pending:
                    error = 'That username or email is not available.'
                else:
                    token = secrets.token_urlsafe(32)
                    expires = datetime.utcnow() + timedelta(hours=24)
                    pw_hash = generate_password_hash(password)
                    try:
                        db.execute("""
                            INSERT INTO user_pending_registration
                              (username, display_name, email, password_hash, confirm_token, token_expires)
                            VALUES (?,?,?,?,?,?)
                        """, (username, display_name, email, pw_hash, token, expires.isoformat()))
                        db.commit()
                        confirm_url = url_for('confirm_email', token=token, _external=True)
                        _send_simple_email(
                            email,
                            '3·2·1→THEATER: Confirm Your Email',
                            f'Click the link to confirm your email address:\n{confirm_url}\n\nThis link expires in 24 hours.',
                            f'<p>Click the link below to confirm your email address:</p>'
                            f'<p><a href="{confirm_url}">{confirm_url}</a></p>'
                            f'<p>This link expires in 24 hours.</p>'
                        )
                        syslog_logger.info(f'REGISTER_PENDING username={username} email={email}')
                        # Notify admins of new pending registration
                        try:
                            _adb = get_db()
                            _admins = _adb.execute(
                                "SELECT email FROM users WHERE role='admin' AND email != '' AND email IS NOT NULL"
                            ).fetchall()
                            _adb.close()
                            _settings_url = url_for('settings', _external=True) + '#registrations'
                            for _adm in _admins:
                                _send_simple_email(
                                    _adm['email'],
                                    '3\u00b72\u00b71\u2192THEATER: New Registration Pending',
                                    f'A new account registration is awaiting your approval.\n\n'
                                    f'Username: {username}\nDisplay name: {display_name or "(none)"}\nEmail: {email}\n\n'
                                    f'Review and approve at:\n{_settings_url}',
                                )
                        except Exception:
                            pass
                        db.close()
                        return render_template('register.html',
                                               success='Registration submitted! Check your email to confirm, then wait for admin approval.',
                                               user=None)
                    except Exception as exc:
                        app.logger.error(f'Registration error: {exc}')
                        error = 'Registration failed due to a server error. Please try again.'
                    finally:
                        try:
                            db.close()
                        except Exception:
                            pass
    return render_template('register.html', error=error, user=None)


if _limiter_available and limiter:
    @app.route('/register', methods=['GET', 'POST'])
    @limiter.limit("10 per minute", methods=["POST"])
    def register():
        return _register_route()
else:
    @app.route('/register', methods=['GET', 'POST'])
    def register():
        return _register_route()


@app.route('/confirm-email/<token>')
def confirm_email(token):
    db = get_db()
    reg = db.execute(
        'SELECT * FROM user_pending_registration WHERE confirm_token=?', (token,)
    ).fetchone()
    if not reg:
        db.close()
        return render_template('register.html', error='Invalid or expired confirmation link.', user=None)
    if datetime.fromisoformat(reg['token_expires']) < datetime.utcnow():
        db.execute('DELETE FROM user_pending_registration WHERE id=?', (reg['id'],))
        db.commit()
        db.close()
        return render_template('register.html', error='Confirmation link expired. Please register again.', user=None)
    db.execute('UPDATE user_pending_registration SET email_confirmed=1 WHERE id=?', (reg['id'],))
    db.commit()
    syslog_logger.info(f'EMAIL_CONFIRMED username={reg["username"]}')
    db.close()
    return render_template('register.html',
                           success='Email confirmed! Your account is now awaiting admin approval.',
                           user=None)


@app.route('/settings/pending-registrations')
@admin_required
def pending_registrations():
    db = get_db()
    rows = db.execute("""
        SELECT * FROM user_pending_registration
        WHERE email_confirmed=1 AND admin_approved=0
        ORDER BY created_at
    """).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/pending-registrations/<int:reg_id>/approve', methods=['POST'])
@admin_required
def approve_registration(reg_id):
    data = request.get_json(force=True) or {}
    role = data.get('role', 'user')
    db = get_db()
    reg = db.execute('SELECT * FROM user_pending_registration WHERE id=?', (reg_id,)).fetchone()
    if not reg:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    try:
        db.execute("""
            INSERT INTO users (username, display_name, email, password_hash, role, email_confirmed)
            VALUES (?,?,?,?,?,1)
        """, (reg['username'], reg['display_name'] or reg['username'],
              reg['email'], reg['password_hash'], role))
        db.commit()
        uid = db.execute('SELECT id FROM users WHERE username=?', (reg['username'],)).fetchone()['id']
        db.execute('DELETE FROM user_pending_registration WHERE id=?', (reg_id,))
        db.commit()
        log_audit(db, 'USER_APPROVED', 'user', uid,
                  detail=f'username={reg["username"]} role={role} approved_by={session.get("username")}')
        db.commit()
        _send_simple_email(
            reg['email'],
            '3·2·1→THEATER: Account Approved',
            f'Your account "{reg["username"]}" has been approved. You can now log in.',
        )
        syslog_logger.info(f'USER_APPROVED username={reg["username"]} role={role} by={session.get("username")}')
        db.close()
        return jsonify({'success': True})
    except DBIntegrityError:
        db.close()
        return jsonify({'error': 'Username already exists'}), 409


@app.route('/settings/pending-registrations/<int:reg_id>/deny', methods=['POST'])
@admin_required
def deny_registration(reg_id):
    db = get_db()
    reg = db.execute('SELECT * FROM user_pending_registration WHERE id=?', (reg_id,)).fetchone()
    if reg:
        db.execute('DELETE FROM user_pending_registration WHERE id=?', (reg_id,))
        db.commit()
        _send_simple_email(
            reg['email'],
            '3·2·1→THEATER: Registration Declined',
            f'Your registration request for "{reg["username"]}" was not approved.',
        )
        syslog_logger.info(f'USER_DENIED username={reg["username"]} by={session.get("username")}')
    db.close()
    return jsonify({'success': True})


def _forgot_password_route():
    if session.get('user_id'):
        return redirect(url_for('dashboard'))
    sent = False
    error = None
    if request.method == 'POST':
        identifier = request.form.get('identifier', '').strip()
        score = int(request.form.get('captcha_score', 0))
        if score < 1:
            error = 'Please complete the mini-game challenge first.'
        elif not identifier:
            error = 'Enter your username or email.'
        else:
            db = get_db()
            user = db.execute(
                'SELECT * FROM users WHERE username=? OR email=?', (identifier, identifier)
            ).fetchone()
            # Always show success even if user not found (security best practice)
            if user and user.get('email'):
                token = secrets.token_urlsafe(48)
                expires = datetime.utcnow() + timedelta(hours=2)
                # Invalidate old tokens
                db.execute('UPDATE password_reset_tokens SET used=1 WHERE user_id=? AND used=0',
                           (user['id'],))
                db.execute("""
                    INSERT INTO password_reset_tokens (user_id, token, expires_at)
                    VALUES (?,?,?)
                """, (user['id'], token, expires.isoformat()))
                db.commit()
                reset_url = url_for('reset_password', token=token, _external=True)
                _send_simple_email(
                    user['email'],
                    '3·2·1→THEATER: Password Reset',
                    f'Click the link below to reset your password (expires in 2 hours):\n{reset_url}\n\n'
                    f'If you did not request this, ignore this email.',
                    f'<p><a href="{reset_url}">{reset_url}</a></p>'
                    f'<p>This link expires in 2 hours. If you did not request this, ignore this email.</p>'
                )
                syslog_logger.info(f'PASSWORD_RESET_REQUEST user={user["username"]}')
            db.close()
            sent = True
    return render_template('forgot_password.html', sent=sent, error=error, user=None)


if _limiter_available and limiter:
    @app.route('/forgot-password', methods=['GET', 'POST'])
    @limiter.limit("5 per minute", methods=["POST"])
    def forgot_password():
        return _forgot_password_route()
else:
    @app.route('/forgot-password', methods=['GET', 'POST'])
    def forgot_password():
        return _forgot_password_route()


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    if session.get('user_id'):
        return redirect(url_for('dashboard'))
    db = get_db()
    rec = db.execute(
        'SELECT * FROM password_reset_tokens WHERE token=? AND used=0', (token,)
    ).fetchone()
    if not rec or datetime.fromisoformat(rec['expires_at']) < datetime.utcnow():
        db.close()
        return render_template('forgot_password.html',
                               error='This reset link has expired or already been used.', user=None)
    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm  = request.form.get('confirm_password', '')
        if len(password) < 8:
            error = 'Password must be at least 8 characters.'
        elif password != confirm:
            error = 'Passwords do not match.'
        else:
            pw_hash = generate_password_hash(password)
            db.execute('UPDATE users SET password_hash=? WHERE id=?', (pw_hash, rec['user_id']))
            db.execute('UPDATE password_reset_tokens SET used=1 WHERE token=?', (token,))
            db.commit()
            user_row = db.execute('SELECT username FROM users WHERE id=?', (rec['user_id'],)).fetchone()
            log_audit(db, 'PASSWORD_RESET_COMPLETE', 'user', rec['user_id'])
            db.commit()
            syslog_logger.info(f'PASSWORD_RESET_COMPLETE user={user_row["username"] if user_row else rec["user_id"]}')
            db.close()
            flash('Password reset successfully. You can now log in.', 'success')
            return redirect(url_for('login'))
    db.close()
    return render_template('forgot_password.html', token=token, reset_mode=True, error=error, user=None)


# ─── Site-Wide Messaging ───────────────────────────────────────────────────────

def get_active_messages(user_id=None, msg_type=None):
    """Return active, non-dismissed, non-expired messages."""
    db = get_db()
    now = datetime.utcnow().isoformat()
    rows = db.execute("""
        SELECT m.*,
               CASE WHEN d.user_id IS NOT NULL THEN 1 ELSE 0 END as dismissed
        FROM site_messages m
        LEFT JOIN site_message_dismissals d ON d.message_id = m.id AND d.user_id = ?
        WHERE m.is_active = 1
          AND (m.expires_at IS NULL OR m.expires_at > ?)
          AND (m.scheduled_for IS NULL OR m.scheduled_for <= ?)
          AND (? IS NULL OR m.msg_type = ?)
        ORDER BY m.created_at DESC
    """, (user_id or 0, now, now, msg_type, msg_type)).fetchall()
    db.close()
    return [dict(r) for r in rows if not r['dismissed'] or r['dismissible_by'] == 'admin']


@app.route('/api/messages')
@login_required
def get_messages_api():
    msg_type = request.args.get('type')
    msgs = get_active_messages(session['user_id'], msg_type)
    # Filter out already dismissed for users
    result = [m for m in msgs if not m['dismissed']]
    return jsonify(result)


@app.route('/api/messages/<int:msg_id>/dismiss', methods=['POST'])
@login_required
def dismiss_message(msg_id):
    db = get_db()
    msg = db.execute('SELECT * FROM site_messages WHERE id=?', (msg_id,)).fetchone()
    if not msg:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    if msg['dismissible_by'] == 'admin' and session.get('user_role') != 'admin':
        db.close()
        return jsonify({'error': 'Only admins can dismiss this message'}), 403
    try:
        db.execute('INSERT OR IGNORE INTO site_message_dismissals (message_id, user_id) VALUES (?,?)',
                   (msg_id, session['user_id']))
        db.commit()
    except Exception:
        pass
    db.close()
    return jsonify({'success': True})


@app.route('/settings/messages', methods=['GET'])
@admin_required
def messages_list():
    db = get_db()
    rows = db.execute("""
        SELECT m.*, u.display_name as author
        FROM site_messages m
        LEFT JOIN users u ON u.id = m.created_by
        ORDER BY m.created_at DESC
    """).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/settings/messages', methods=['POST'])
@admin_required
def message_create():
    data = request.get_json(force=True) or {}
    title = (data.get('title') or '').strip()
    body_html = _sanitize_html((data.get('body_html') or '').strip())
    if not title:
        return jsonify({'error': 'Title required'}), 400
    db = get_db()
    db.execute("""
        INSERT INTO site_messages
          (title, body_html, msg_type, dismissible_by, expires_at, scheduled_for,
           is_active, show_on_login, created_by)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        title, body_html,
        data.get('msg_type', 'motd'),
        data.get('dismissible_by', 'user'),
        data.get('expires_at') or None,
        data.get('scheduled_for') or None,
        1 if data.get('is_active', True) else 0,
        1 if data.get('show_on_login') else 0,
        session['user_id'],
    ))
    db.commit()
    row = db.execute('SELECT * FROM site_messages ORDER BY id DESC LIMIT 1').fetchone()
    log_audit(db, 'MESSAGE_CREATE', 'site_message', row['id'], detail=title)
    db.commit()
    syslog_logger.info(f'MESSAGE_CREATE title="{title}" type={data.get("msg_type","motd")} by={session.get("username")}')
    result = dict(row)
    db.close()
    return jsonify(result), 201


@app.route('/settings/messages/<int:msg_id>', methods=['PUT'])
@admin_required
def message_edit(msg_id):
    data = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""
        UPDATE site_messages SET
          title=?, body_html=?, msg_type=?, dismissible_by=?,
          expires_at=?, scheduled_for=?, is_active=?, show_on_login=?
        WHERE id=?
    """, (
        (data.get('title') or '').strip(),
        _sanitize_html((data.get('body_html') or '').strip()),
        data.get('msg_type', 'motd'),
        data.get('dismissible_by', 'user'),
        data.get('expires_at') or None,
        data.get('scheduled_for') or None,
        1 if data.get('is_active', True) else 0,
        1 if data.get('show_on_login') else 0,
        msg_id,
    ))
    db.commit()
    log_audit(db, 'MESSAGE_EDIT', 'site_message', msg_id, detail=data.get('title', ''))
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/messages/<int:msg_id>', methods=['DELETE'])
@admin_required
def message_delete(msg_id):
    db = get_db()
    db.execute('DELETE FROM site_messages WHERE id=?', (msg_id,))
    db.commit()
    log_audit(db, 'MESSAGE_DELETE', 'site_message', msg_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/settings/messages/<int:msg_id>/dismiss-all', methods=['POST'])
@admin_required
def message_dismiss_all(msg_id):
    """Admin globally deactivates (removes) a message for everyone."""
    db = get_db()
    db.execute('UPDATE site_messages SET is_active=0 WHERE id=?', (msg_id,))
    db.commit()
    log_audit(db, 'MESSAGE_DISMISS_ALL', 'site_message', msg_id)
    db.commit()
    db.close()
    return jsonify({'success': True})


# ─── AI Session Management ─────────────────────────────────────────────────────

def _get_ai_slot_limit():
    return int(get_app_setting('ai_max_sessions', '2'))

def _count_active_ai_sessions():
    """Count running AI sessions, pruning stale ones (>5 min) first."""
    db = get_db()
    cutoff = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
    db.execute("""
        UPDATE ai_sessions SET status='timeout', ended_at=CURRENT_TIMESTAMP
        WHERE status='running' AND started_at < ?
    """, (cutoff,))
    db.commit()
    count = db.execute("SELECT COUNT(*) FROM ai_sessions WHERE status='running'").fetchone()[0]
    db.close()
    return count

def _claim_ai_session(show_id):
    """Reserve a slot. Returns (session_id, None) or (None, error)."""
    db = get_db()
    limit = _get_ai_slot_limit()
    count = _count_active_ai_sessions()
    if count >= limit:
        db.close()
        return None, f'All {limit} AI processing slots are busy. Please try again in a moment.'
    db.execute("""
        INSERT INTO ai_sessions (user_id, show_id, status)
        VALUES (?,?,'running')
    """, (session.get('user_id'), show_id))
    db.commit()
    sid = db.execute('SELECT id FROM ai_sessions ORDER BY id DESC LIMIT 1').fetchone()['id']
    db.close()
    return sid, None

def _release_ai_session(session_id):
    if not session_id:
        return
    db = get_db()
    db.execute("""
        UPDATE ai_sessions SET status='done', ended_at=CURRENT_TIMESTAMP WHERE id=?
    """, (session_id,))
    db.commit()
    db.close()


@app.route('/api/ai/slots')
@login_required
def ai_slots_status():
    """Return current AI slot availability for dynamic UI."""
    limit = _get_ai_slot_limit()
    count = _count_active_ai_sessions()
    return jsonify({
        'limit': limit,
        'active': count,
        'available': max(0, limit - count),
        'busy': count >= limit,
    })


# ─── Asset Availability Dashboard ─────────────────────────────────────────────

@app.route('/dashboards')
@login_required
def dashboards_list():
    db = get_db()
    rows = db.execute("""
        SELECT d.*, u.display_name as owner_name
        FROM asset_dashboards d
        JOIN users u ON u.id = d.user_id
        WHERE d.user_id = ? OR d.is_public = 1
        ORDER BY d.user_id = ? DESC, d.name
    """, (session['user_id'], session['user_id'])).fetchall()
    db.close()
    return render_template('dashboards.html',
                           dashboards=[dict(r) for r in rows],
                           user=get_current_user())


@app.route('/dashboards/new', methods=['POST'])
@login_required
def dashboard_create():
    data = request.get_json(force=True) or {}
    name = (data.get('name') or 'My Dashboard').strip()
    slug = secrets.token_urlsafe(12) if data.get('is_public') else None
    db = get_db()
    db.execute("""
        INSERT INTO asset_dashboards (user_id, name, is_public, public_slug, layout, config_json)
        VALUES (?,?,?,?,?,?)
    """, (session['user_id'], name,
          1 if data.get('is_public') else 0,
          slug,
          data.get('layout', 'combined'),
          json.dumps(data.get('config', {}))))
    db.commit()
    row = db.execute('SELECT * FROM asset_dashboards ORDER BY id DESC LIMIT 1').fetchone()
    db.close()
    return jsonify(dict(row)), 201


@app.route('/dashboards/<int:dash_id>')
@login_required
def dashboard_view(dash_id):
    db = get_db()
    d = db.execute('SELECT * FROM asset_dashboards WHERE id=?', (dash_id,)).fetchone()
    if not d:
        db.close()
        abort(404)
    if d['user_id'] != session['user_id'] and not d['is_public']:
        if session.get('user_role') != 'admin':
            db.close()
            abort(403)
    cats = db.execute('SELECT * FROM asset_categories ORDER BY sort_order, name').fetchall()
    types = db.execute("""
        SELECT at.*, ac.name as category_name
        FROM asset_types at
        JOIN asset_categories ac ON ac.id = at.category_id
        ORDER BY ac.sort_order, at.sort_order, at.name
    """).fetchall()
    db.close()
    config = json.loads(d['config_json'] or '{}')
    return render_template('dashboard_view.html',
                           dash=dict(d),
                           categories=[dict(c) for c in cats],
                           asset_types=[{k: v for k, v in dict(t).items() if k != 'photo'} for t in types],
                           config=config,
                           user=get_current_user())


@app.route('/dashboards/<int:dash_id>', methods=['PUT'])
@login_required
def dashboard_edit(dash_id):
    db = get_db()
    d = db.execute('SELECT * FROM asset_dashboards WHERE id=?', (dash_id,)).fetchone()
    if not d or d['user_id'] != session['user_id']:
        db.close()
        abort(403)
    data = request.get_json(force=True) or {}
    slug = d['public_slug']
    if data.get('is_public') and not slug:
        slug = secrets.token_urlsafe(12)
    db.execute("""
        UPDATE asset_dashboards SET name=?, is_public=?, public_slug=?,
               layout=?, config_json=?, updated_at=CURRENT_TIMESTAMP
        WHERE id=?
    """, (
        (data.get('name') or 'My Dashboard').strip(),
        1 if data.get('is_public') else 0,
        slug if data.get('is_public') else None,
        data.get('layout', 'combined'),
        json.dumps(data.get('config', {})),
        dash_id,
    ))
    db.commit()
    db.close()
    return jsonify({'success': True, 'slug': slug})


@app.route('/dashboards/<int:dash_id>', methods=['DELETE'])
@login_required
def dashboard_delete(dash_id):
    db = get_db()
    d = db.execute('SELECT user_id FROM asset_dashboards WHERE id=?', (dash_id,)).fetchone()
    if not d or (d['user_id'] != session['user_id'] and session.get('user_role') != 'admin'):
        db.close()
        abort(403)
    db.execute('DELETE FROM asset_dashboards WHERE id=?', (dash_id,))
    db.commit()
    db.close()
    return jsonify({'success': True})


@app.route('/d/<slug>')
def public_dashboard(slug):
    """Public dashboard — no login required."""
    db = get_db()
    d = db.execute(
        'SELECT * FROM asset_dashboards WHERE public_slug=? AND is_public=1', (slug,)
    ).fetchone()
    if not d:
        db.close()
        abort(404)
    cats = db.execute('SELECT * FROM asset_categories ORDER BY sort_order, name').fetchall()
    types = db.execute("""
        SELECT at.*, ac.name as category_name
        FROM asset_types at
        JOIN asset_categories ac ON ac.id = at.category_id
        ORDER BY ac.sort_order, at.sort_order, at.name
    """).fetchall()
    db.close()
    config = json.loads(d['config_json'] or '{}')
    return render_template('dashboard_view.html',
                           dash=dict(d),
                           categories=[dict(c) for c in cats],
                           asset_types=[{k: v for k, v in dict(t).items() if k != 'photo'} for t in types],
                           config=config,
                           public=True,
                           user=None)


# ─── Asset Reports ─────────────────────────────────────────────────────────────

@app.route('/reports/assets')
@admin_required
def asset_reports():
    db = get_db()
    companies = db.execute("""
        SELECT DISTINCT ad.field_value as company
        FROM advance_data ad
        WHERE ad.field_key = 'performance_company' AND ad.field_value != ''
        ORDER BY ad.field_value
    """).fetchall()
    db.close()
    return render_template('asset_reports.html',
                           companies=[r['company'] for r in companies],
                           user=get_current_user())


@app.route('/api/reports/assets')
@admin_required
def asset_reports_data():
    company = request.args.get('company', '')
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to', '')
    db = get_db()

    params = []
    where = []

    if company:
        where.append("""
            s.id IN (
                SELECT show_id FROM advance_data
                WHERE field_key='performance_company' AND field_value=?
            )
        """)
        params.append(company)

    if date_from:
        where.append("COALESCE(s.show_date, '9999') >= ?")
        params.append(date_from)
    if date_to:
        where.append("COALESCE(s.show_date, '0000') <= ?")
        params.append(date_to)

    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''

    rows = db.execute(f"""
        SELECT sa.id, sa.quantity, sa.locked_price, sa.rental_start, sa.rental_end,
               at.name as type_name, at.manufacturer, at.model,
               ac.name as category_name,
               s.id as show_id, s.name as show_name, s.show_date,
               (sa.quantity * sa.locked_price) as line_total,
               (SELECT field_value FROM advance_data
                WHERE show_id=s.id AND field_key='performance_company') as performance_company
        FROM show_assets sa
        JOIN asset_types at ON at.id = sa.asset_type_id
        JOIN asset_categories ac ON ac.id = at.category_id
        JOIN shows s ON s.id = sa.show_id
        {where_sql}
        ORDER BY s.show_date DESC, ac.name, at.name
    """, params).fetchall()

    total_revenue = sum(r['line_total'] or 0 for r in rows)
    db.close()
    return jsonify({
        'rows': [dict(r) for r in rows],
        'total_revenue': total_revenue,
        'count': len(rows),
    })


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


@app.errorhandler(500)
def internal_error(e):
    app.logger.exception("500 Internal Server Error")
    return render_template('error.html', code=500,
                           message="An unexpected server error occurred.",
                           user=get_current_user()), 500


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
    app.run(debug=os.environ.get('FLASK_DEBUG', '0') == '1', port=run_port)
