# ShowAdvance — Production Management System

ShowAdvance is a web-based production advance and day-of-show management tool built for Dr. Phillips Center for the Performing Arts (DPC). It provides a central place to fill out advance forms, build production schedules, record post-show notes, and share documents with crew and clients.

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
   - [Comments](#comments)
   - [Export & Files](#export--files)
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
   - [Syslog Settings](#syslog-settings)
   - [Database Backups](#database-backups)
   - [File Manager](#file-manager)
   - [God Mode](#god-mode)
6. [Troubleshooting](#troubleshooting)

---

## System Requirements

| Component | Minimum |
|-----------|---------|
| Python | 3.9+ (3.11 recommended) |
| OS | Linux (systemd) |
| RAM | 512 MB |
| Disk | 1 GB (for database and backups) |
| Network | LAN access for crew devices |

Python packages installed automatically: Flask, Werkzeug, gunicorn, WeasyPrint (PDF generation), APScheduler (backups), flask-limiter (login rate limiting), qrcode[pil] + Pillow (WiFi QR codes).

### WeasyPrint system dependencies (Ubuntu/Debian)

```bash
sudo apt install libpango-1.0-0 libpangoft2-1.0-0 libffi-dev libcairo2
```

This will:
1. Create a Python virtual environment
2. Install all dependencies from `requirements.txt`
3. Initialize the database (`advance.db`)
4. Configure and start a systemd service
5. Generate a secure `SECRET_KEY` automatically

Open the app at `http://<your-server-ip>:5400`

**Default login:** `admin` / `admin123`
⚠️ Change the admin password immediately after first login — Settings → My Account.

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

Default credentials: **admin / admin**

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

### Comments

Show-specific comment thread with `@mention` autocomplete. Visible to all authorised users.

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
