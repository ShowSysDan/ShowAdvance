# ShowAdvance (3·2·1→THEATER) — Production Management System

ShowAdvance is a web-based production advance and day-of-show management tool built for Dr. Phillips Center for the Performing Arts (DPC). It provides a central place to fill out advance forms, build production schedules, record post-show notes, manage labor requests, send schedule emails, and share documents with crew and clients.

---

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [First Login](#first-login)
4. [User Guide](#user-guide)
   - [Dashboard](#dashboard)
   - [Advance Sheet](#advance-sheet)
   - [Production Schedule](#production-schedule)
   - [Post-Show Notes](#post-show-notes)
   - [Labor Requests](#labor-requests)
   - [Comments](#comments)
   - [Export & Files](#export--files)
   - [Email](#email)
   - [Public Show Page](#public-show-page)
5. [Admin & Settings Guide](#admin--settings-guide)
   - [Contacts](#contacts)
   - [Users & Roles](#users--roles)
   - [Groups & Show Access](#groups--show-access)
   - [Form Field Customisation](#form-field-customisation)
   - [Venues & Radio Channels](#venues--radio-channels)
   - [WiFi Defaults](#wifi-defaults)
   - [Organisation Logo](#organisation-logo)
   - [Upload Size Limit](#upload-size-limit)
   - [Email Settings](#email-settings)
   - [AI Extraction (Ollama)](#ai-extraction-ollama)
   - [Syslog Settings](#syslog-settings)
   - [Database Backups](#database-backups)
   - [File Manager](#file-manager)
   - [God Mode](#god-mode)
6. [Security](#security)
7. [Troubleshooting](#troubleshooting)

---

## System Requirements

| Component | Minimum |
|-----------|---------|
| Python | 3.9+ (3.11 recommended) |
| OS | Linux (systemd) |
| RAM | 512 MB |
| Disk | 1 GB (for database and backups) |
| Network | LAN access for crew devices |

Python packages installed automatically: Flask, Werkzeug, gunicorn, WeasyPrint (PDF generation), APScheduler (backups), flask-limiter (login rate limiting), qrcode[pil] + Pillow (WiFi QR codes), dnspython (direct MX email delivery), pdfplumber + python-docx + openpyxl + xlrd + striprtf (document import/AI extraction), psycopg2-binary (optional PostgreSQL support).

### WeasyPrint system dependencies (Ubuntu/Debian)

```bash
sudo apt install libpango-1.0-0 libpangoft2-1.0-0 libffi-dev libcairo2
```

---

## Installation

```bash
# Full install with systemd service (recommended):
sudo ./install.sh

# Without root (manual start only):
./install.sh
```

The installer: creates a Python venv, installs dependencies, initialises/migrates the SQLite database, creates backup directories, writes a systemd service unit, generates a SECRET_KEY, and starts the service.

After installation the app is available at `http://<server-ip>:<port>` (default port **5400**).

### Updating

Re-run `./install.sh` (or `sudo ./install.sh`). It detects the existing database and runs migrations automatically — no data is lost.

---

## First Login

Default credentials: **admin / admin123**

**Change the admin password immediately** via Settings → My Account → Change Password.

---

## User Guide

### Dashboard

Lists all active and archived shows. Click a show to open it. **New Show** creates a new show.

### Advance Sheet

Contains all pre-show information: show details, contacts, arrival & parking, security, hospitality, audio, video, backline, stage, wardrobe, special elements, and labor needs.

**Saving:** Changes are auto-saved as you type. The **Save** button forces an immediate save.

**Conditional fields:** Fields appear/hide based on related field values (e.g. "Rentalworks Order #" appears when "Rental Works?" = Yes).

**Contacts:** Contact dropdowns populate from the Contacts list in Settings.

**Venues / Radio Channels:** If configured in Settings, these show as dropdowns instead of free-text fields.

**Version History:** Click **History** to view, preview, and restore previous snapshots.

**Real-time collaboration:** Multiple users can work simultaneously. Changes sync every 5 seconds and each user's active field is highlighted.

### Production Schedule

**Venue & Tech Info** — WiFi network/code, parking/security info. Radio Channel and Mix Position are read-only from the Advance Sheet.

**Timeline** — Time rows with Start, End, Description, Notes. Times are auto-normalised to 24-hour format on blur (e.g. "4pm" → "16:00", "1600" → "16:00").

**Show Contacts** — All DPC contacts (PM, Hospitality, Programming, Event Manager, Education, Guest Services, Runner) are read-only, pulled from the Advance Sheet. Security Email is editable.

### Post-Show Notes

Record production manager (read-only from advance), crew call time, show notes, house notes, equipment issues, and miscellaneous notes. A collapsible schedule timeline is shown for reference.

Click **Export PDF** to generate a Post-Show Notes PDF.

### Labor Requests

Track labor needs per show. Add requests with department, position, quantity, date/time, and notes. Drag rows to reorder. Restricted (read-only) users can view but not modify labor requests.

### Comments

Show-specific comment thread with `@mention` autocomplete. Visible to all authorised users. Admins can view comment edit history.

### Export & Files

| Action | Description |
|--------|-------------|
| Export Advance → vN | Generates Advance Sheet PDF |
| Export Schedule → vN | Generates Production Schedule PDF with timeline, contacts, WiFi QR code, and logo |
| Export PDF (postnotes tab) | Generates Post-Show Notes PDF |
| ↓ (history) | Re-downloads a previously generated PDF |

PDFs are stored in the database — use the **↓** button in Export History to re-download without generating a new version.

**Attachments:** Drag-and-drop or click **+ Attach File**. Upload progress bar shown. Files stored in database.

**Read Receipts:** Tracks who opened the advance at which version.

### Email

Send production schedule PDFs to contacts directly from the app. Supports two delivery methods:

- **SMTP relay** — send via a configured mail server (Gmail, Outlook, etc.)
- **Direct MX delivery** — send directly to the recipient's mail server (no relay needed; requires DNS/MX access)

Configure email settings in Settings → Email. A test button verifies connectivity before sending.

### Public Show Page

`/public` — no login required. Lists all active shows with download links for the latest advance and schedule PDFs. Share with clients, tour managers, and crew who don't have an account.

---

## Admin & Settings Guide

### Contacts

Add, edit, delete DPC contacts. Fields: name, title, department, phone, email. Contacts appear in dropdowns on advance and schedule forms.

### Users & Roles

| Role | Access |
|------|--------|
| `admin` | Full access: all shows, settings, user management |
| `user` | Access controlled by group membership |

Add users via Settings → Users. Admins can reset passwords.

### Groups & Show Access

| Group Type | Behaviour |
|------------|-----------|
| `all_access` | Can see and edit all shows |
| `restricted` | Can only view/export assigned shows |

1. Create group: Settings → Groups → **+ New Group**
2. Add members
3. For restricted groups: assign shows via **Assign show...**

### Form Field Customisation

Settings → Form Fields (admin or content_admin).

- Drag rows to reorder fields
- Edit field label, type, conditional logic, width
- Add fields and sections
- Changes are immediate across all shows

Field types: `text`, `textarea`, `date`, `time`, `number`, `yes_no`, `select`, `checkbox`, `contact_dropdown`

Conditional: `field_key=Value` (e.g. `runner_needed=Yes`)

### Venues & Radio Channels

Settings → Syslog → **Venue & Channel Lists**

One item per line. These populate the Venue and Radio Channel dropdowns on the advance form.

### WiFi Defaults

Settings → Syslog → **WiFi Defaults**

Set default WiFi SSID and password. Appears on Schedule PDFs as text and QR code.

### Organisation Logo

Settings → Syslog → **Organisation Logo**

Upload PNG/JPG/SVG logo (max 2 MB). Shown in PDF headers.

### Upload Size Limit

Settings → Syslog → **Upload Size Limit**

Maximum file attachment size (default 20 MB, max 500 MB).

### Email Settings

Settings → Email. Configure outbound email for sending schedule PDFs to contacts.

| Setting | Description |
|---------|-------------|
| Provider | `smtp` (relay) or `direct` (MX delivery) |
| SMTP Host / Port | Mail server address and port (default 587) |
| SMTP User / Pass | Authentication credentials |
| From Address | Sender address |
| Use TLS | Enable STARTTLS (recommended) |
| EHLO Hostname | Custom EHLO hostname for direct delivery |
| Display Name | Friendly name shown in the From field |

### AI Extraction (Ollama)

Settings → AI. Connect to a local [Ollama](https://ollama.com) instance for AI-powered data extraction from uploaded documents (PDF, DOCX, XLSX, RTF, TXT). Configure the Ollama server URL and enable/disable the feature. The AI can pre-populate advance form fields from uploaded rider documents.

### Syslog Settings

Settings → Syslog. Send audit events to a remote syslog server via UDP.

Events: LOGIN/LOGOUT · SHOW_CREATE/ARCHIVE/DELETE/RESTORE · FORM_SAVE · PDF_EXPORT · USER_CREATE/DELETE/PASSWORD_CHANGE · GROUP_ASSIGN/REMOVE · BACKUP_CREATED · SETTINGS_CHANGE

### Database Backups

Settings → Backups. Automatic hourly (keeps 24) and daily at midnight (keeps 30) SQLite backups in `backups/`. Click **Run Backup Now** for immediate backup.

**Restore:**
```bash
cp backups/daily/advance_YYYYMMDD_0000.db advance.db
sudo systemctl restart showadvance
```

### File Manager

Settings → Files (admin only). View and delete all file attachments across all shows.

### God Mode

Settings → God Mode (admin only).

- **Active Sessions** — users on a show page in the last 5 minutes (user, show, tab, last seen)
- **User Last Login** — last login timestamp per user

---

## Security

Passwords are hashed using Werkzeug's `generate_password_hash` (scrypt with Werkzeug 3.x+, pbkdf2:sha256 with older versions). Passwords are **never** stored in plaintext.

The installer generates a cryptographically random `SECRET_KEY` and stores it in `.env` (chmod 600). This key signs Flask session cookies.

Login rate limiting (15 attempts/minute per IP) is enforced via `flask-limiter`.

An audit log records all significant actions (logins, show changes, exports, user management) with timestamps and IP addresses. View via Settings → Audit Log (admin only).

For a detailed security assessment, see [SECURITY_AUDIT.md](SECURITY_AUDIT.md).

---

## Troubleshooting

**Settings page tabs don't work** — Clear browser cache and reload.

**Backup fails with PermissionError:**
```bash
sudo chown -R <service_user>:<service_user> /path/to/ShowAdvance/backups
# Or re-run: sudo ./install.sh
```

**PDF generation fails:** Install WeasyPrint system dependencies:
```bash
sudo apt install libpango-1.0-0 libpangoft2-1.0-0 libcairo2 libffi-dev
```

**Port change doesn't take effect:** Restart the service:
```bash
sudo systemctl restart showadvance
```

**Service logs:**
```bash
journalctl -u showadvance -f
journalctl -u showadvance -n 100
```

**Database migration errors:**
```bash
venv/bin/python init_db.py --migrate
```

**Login rate limiting:** After 15 failed login attempts per minute from an IP, further attempts return HTTP 429. Wait 60 seconds or restart the app.
