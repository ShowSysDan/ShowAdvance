# ShowAdvance (3·2·1→THEATER) — Production Management System

ShowAdvance is a web-based production advance and day-of-show management tool built for Dr. Phillips Center for the Performing Arts (DPC). It provides a central place to fill out advance forms, build production schedules, record post-show notes, manage labor requests, track inventory and rentals, send schedule emails, and share documents with crew and clients.

---

## Version Numbering

**Current version: `2.1.0`**

This project uses **semantic versioning**: `MAJOR.MINOR.PATCH`

| Segment | When to increment |
|---------|------------------|
| **MAJOR** | Breaking schema changes, major architectural overhaul, or changes that require a full DB re-init |
| **MINOR** | New feature sets added (e.g. asset manager, user system enhancements, messaging system) |
| **PATCH** | Bug fixes, security patches, small UI tweaks, wording changes |

### Rules for AI coding sessions

> **IMPORTANT for future AI sessions:** Before committing any change, determine which version segment to increment and update `APP_VERSION` in `app.py`. The format is `'MAJOR.MINOR.PATCH'` as a string constant near the top of the file. Do not skip this step. The version displays in the sidebar footer of every page.
>
> - New feature → increment MINOR (reset PATCH to 0)
> - Bug fix only → increment PATCH
> - Schema changes requiring migration → evaluate MAJOR vs MINOR based on impact
> - Always commit the version bump in the same commit as the feature/fix

Version history:
- `1.x` — Initial release through security hardening and red team audit
- `2.0.0` — Asset Manager (inventory tracking, rental pricing, show reservations, external rentals), Performance Company field, version numbering system
- `2.1.0` — User registration with CAPTCHA, password recovery via email, pending registration approval workflow, in-app git update system with rollback, site-wide messaging (MOTD/maintenance/alerts with dismissal), AI session concurrency management, asset availability dashboards (public/private), asset usage reports by company/date range, Dashboards and Asset Reports in sidebar nav

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
   - [Assets Tab](#assets-tab)
   - [Asset Availability Dashboards](#asset-availability-dashboards)
   - [Comments](#comments)
   - [Export & Files](#export--files)
   - [Email](#email)
   - [Public Show Page](#public-show-page)
5. [Admin & Settings Guide](#admin--settings-guide)
   - [Asset Manager](#asset-manager)
   - [Asset Reports](#asset-reports)
   - [Contacts](#contacts)
   - [Users & Roles](#users--roles)
   - [Registration Approval](#registration-approval)
   - [Groups & Show Access](#groups--show-access)
   - [Form Field Customisation](#form-field-customisation)
   - [Site-Wide Messages](#site-wide-messages)
   - [In-App Updates](#in-app-updates)
   - [Venues & Radio Channels](#venues--radio-channels)
   - [WiFi Defaults](#wifi-defaults)
   - [Organisation Logo](#organisation-logo)
   - [Upload Size Limit](#upload-size-limit)
   - [Email Settings](#email-settings)
   - [AI Extraction (Ollama)](#ai-extraction-ollama)
   - [AI Session Concurrency](#ai-session-concurrency)
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

### Assets Tab

The **Assets** tab on every show allows content admins to:
- **Search** the asset inventory and add items to the show
- Set **quantity**, **rental period** (defaults to show production dates), and **unit price** (locked at time of reservation — subsequent database price changes do not affect existing reservations)
- Add **external rental line items** with optional PDF attachment (vendor quote, contract, etc.)
- View the combined **total cost** for internal + external rentals
- **Hide** specific items from production managers (admin only) — useful when e.g. an admin needs to confirm a lens before adding it

Availability is checked in real time when adding items. Items that are over-allocated or in maintenance show their status clearly.

### Asset Availability Dashboards

Access via **Dashboards** in the sidebar. Create personal or public availability views showing real-time asset status across your date range.

- **Layouts:** Combined (all assets), By Category, or By Show
- **Public dashboards** get a shareable URL (`/d/<slug>`) accessible without login — useful for tour managers and external clients
- Each dashboard refreshes live from the `/api/assets/availability` endpoint

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

### Asset Manager

Access via **Asset Manager** in the sidebar (admin only). The asset manager uses a three-level hierarchy:

```
Category (e.g. Video)
  └── Item Type (e.g. Laser Projector — Christie Crimson+3DLP)
        └── Individual Unit (ID:42, barcode: X1234)
```

**Categories** group related equipment. **Item Types** define a make/model with:
- Photo, storage location, rental cost per show, reserve count (units held back as spares)
- Consumable flag + optional quantity tracking

**Individual Units** are each tracked with a database ID (always unique, even without a barcode). Barcodes are optional.

**Maintenance:** Remove a unit from service with a reason and notes. Return it to service when resolved. Both actions are captured in the Audit Log and Syslog.

**Warehouse Locations:** Manage a central list of storage location names (click **Warehouse Locations** button). These appear as a dropdown when editing item types.

**Availability:** When a unit is added to a show, the system checks real-time availability for the rental period, accounting for maintenance units, reserved spares, and other shows requesting the same item type. Negative availability is displayed — it does not prevent allocation, but makes the over-allocation visible.

**Rental pricing:** Each item type has a base rental cost. When added to a show the price is **locked** immediately — if the database price is updated later, existing show reservations keep the original price. New reservations use the current price.

### Asset Reports

Access via **Asset Reports** in the sidebar (admin only). Filter asset usage by performance company and date range. Export results as CSV.

- Summary cards show total revenue, line item count, show count, and categories used
- The **Performance Company** field on each show's advance sheet drives company-level filtering

### Contacts

Add, edit, delete DPC contacts. Fields: name, title, department, phone, email. Contacts appear in dropdowns on advance and schedule forms.

### Users & Roles

| Role | Access |
|------|--------|
| `admin` | Full access: all shows, settings, user management |
| `user` | Access controlled by group membership |

Add users via Settings → Users. Admins can reset passwords.

### Registration Approval

New users can self-register at `/register`. The flow:
1. User fills out registration form and completes the Dino CAPTCHA (score ≥ 1 to pass)
2. A confirmation email is sent — user must click the link to verify their address
3. Admin sees pending requests in Settings → Registrations (with badge count)
4. Admin selects a role and clicks **Approve** (or **Deny**)
5. User receives an approval email and can log in

**Forgot password:** Available at `/forgot-password`. Sends a 2-hour reset link via email. Also requires CAPTCHA.

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

### AI Session Concurrency

Settings → AI → **Max Concurrent AI Sessions**. Limits how many AI extraction jobs can run simultaneously across all Gunicorn workers (stored in DB, shared across processes). Default: 2. The AI extract button is dynamically disabled in the UI when all slots are busy.

### Site-Wide Messages

Settings → Messages. Create banners visible to all logged-in users.

| Field | Description |
|-------|-------------|
| Type | `MOTD` (message of the day), `Maintenance` (scheduled downtime notice), `Alert` (urgent) |
| Dismissible by | `user` (anyone can dismiss) or `admin` (only admins, persists for regular users) |
| Expires at | Automatically hides after this datetime |
| Show on login | Display prominently on the login page |

Messages are fetched via `/api/messages` on every page load and appear as dismissible flash banners at the top of the main content area. Admins can deactivate a message for **all** users at once with the **✕ All** button.

### In-App Updates

Settings → Updates. Pull the latest release from git and auto-restart the service.

1. Click **Auto-Detect** to identify the systemd service name (or enter it manually)
2. Click **Check for Updates** to see pending commits and changed files
3. Click **Apply Update** to:
   - Archive all changed files to `backups/pre_update_<timestamp>/` (rollback point)
   - Run `git pull`
   - Run `python init_db.py --migrate` (applies any schema changes)
   - Restart the systemd service
   - If any step fails, the archived files are restored and the service restarted

The update progress log is displayed live in the browser. If the service restarts, the page automatically detects when Flask comes back up.

### Syslog Settings

Settings → Syslog. Send audit events to a remote syslog server via UDP.

Events: LOGIN/LOGOUT · SHOW_CREATE/ARCHIVE/DELETE/RESTORE · FORM_SAVE · PDF_EXPORT · USER_CREATE/DELETE/PASSWORD_CHANGE · GROUP_ASSIGN/REMOVE · BACKUP_CREATED · SETTINGS_CHANGE · REGISTER_PENDING · EMAIL_CONFIRMED · USER_APPROVED · USER_DENIED · PASSWORD_RESET_REQUEST · PASSWORD_RESET_COMPLETE · APP_UPDATE_START · MESSAGE_CREATE · MESSAGE_EDIT · MESSAGE_DELETE

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
