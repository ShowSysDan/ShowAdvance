# 3·2·1→THEATER — Production Management System

3·2·1→THEATER (321Theater) is a web-based production advance and day-of-show management tool built for Dr. Phillips Center for the Performing Arts (DPC). It provides a central place to fill out advance forms, build production schedules, record post-show notes, manage labor requests, track inventory and rentals, send schedule emails, and share documents with crew and clients.

---

## Version Numbering

**Current version: `2.7.0`**

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
- `2.2.0` — Asset invoice PDF export, MOTD cards on dashboard home page, admin email notifications (new registration + asset over-allocation), password strength meter on register/reset, scheduled_for field in site messages, message Scheduled/Expired status display, read-only badge in users table, email + is_readonly in Add User form
- `2.2.1` — Security hardening: HTML sanitizer on message body_html (prevents stored XSS), access control on /api/assets/availability (respects show permissions for restricted users), unified registration error messages (prevents username enumeration), rate limiting on /register (10/min) and /forgot-password (5/min), exception details no longer exposed to users
- `2.3.0` — Asset Manager enhancements: condition rating (excellent/good/fair/poor/retired) per unit, supplier/vendor name and contact per item type, warranty expiry date, year purchased, purchase value, straight-line depreciation with live remaining-capital calculator, per-unit maintenance log (note/damage/service/usage entries with date, author, and body)
- `2.4.0` — Admin "View As" role switcher: admins can preview the site as Content Admin, User, or Read-only without logging out; amber preview banner shown while in preview mode; one-click return to admin
- `2.4.1` — Soft-retire instead of hard delete: asset types and individual units can only be retired (never deleted); full history preserved permanently; dedicated Retired Assets archive page (/assets/retired); show/hide retired toggle in Asset Manager; category delete blocked while types exist
- `2.4.2` — Asset Manager sort and search: sort type tree by name, unit count, or rental cost (asc/desc); filter units in items modal by barcode with leading-zero tolerance (normBarcode)
- `2.5.0` — Global site-wide search: persistent search box in sidebar (/ or Ctrl+K to focus) searches shows (access-controlled), contacts, asset types, and barcodes; grouped results panel with keyboard navigation (↑↓ Enter Escape); `<mark>` highlight on matching text; leading-zero barcode tolerance client- and server-side
- `2.5.1` — Security patch: XSS fix in Retired Assets JS template literals (esc() helper); rate limiting on /api/search (60/min); max query length guard; log_date ISO format validation; syslog coverage for ADMIN_VIEW_AS, ADMIN_VIEW_AS_RESET, ASSET_LOG_ADD, ASSET_LOG_DELETE
- `2.6.0` — RentalWorks bulk import script (`import_assets.py`): one-time migration from RentalWorks exports into Asset Manager with full 3-tier hierarchy, container/kit linking, daily+weekly rates, depreciation dates, and replacement costs. Kit/container feature: items can be flagged as containers and linked to their contents. Load-in/load-out dates on shows for smart asset rental pricing (weekly rate applies when load period ≥ 7 days; daily × days otherwise). Sidebar redesign: gradient background, scaled-up nav items, pill-style active state.
- `2.7.0` — PostgreSQL dual-schema support: user/auth tables live in a `shared` schema (reusable across apps) while theater-specific tables live in an `app` schema (default `theater321`). Database credentials stored in gitignored `db_config.ini`. CLI commands for schema init and SQLite→PostgreSQL data migration. Settings UI simplified to read-only database status. Fixed schema creation bug that prevented PostgreSQL init.

---

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [First Login](#first-login)
4. [User Guide](#user-guide)
   - [Dashboard](#dashboard)
   - [Global Search](#global-search)
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
   - [Importing from RentalWorks](#importing-from-rentalworks)
   - [Asset Financial Tracking](#asset-financial-tracking)
   - [Asset Maintenance Log](#asset-maintenance-log)
   - [Retired Assets](#retired-assets)
   - [Asset Reports](#asset-reports)
   - [Contacts](#contacts)
   - [Users & Roles](#users--roles)
   - [View As (Role Preview)](#view-as-role-preview)
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
6. [Database Configuration](#database-configuration)
   - [SQLite (Default)](#sqlite-default)
   - [PostgreSQL (Dual-Schema)](#postgresql-dual-schema)
   - [Migrating from SQLite to PostgreSQL](#migrating-from-sqlite-to-postgresql)
7. [Security](#security)
8. [Troubleshooting](#troubleshooting)

---

## System Requirements

| Component | Minimum |
|-----------|---------|
| Python | 3.9+ (3.11 recommended) |
| OS | Linux (systemd) |
| RAM | 512 MB |
| Disk | 1 GB (for database and backups) |
| Network | LAN access for crew devices |
| Database | SQLite (built-in) or PostgreSQL 13+ (optional) |

Python packages installed automatically: Flask, Werkzeug, gunicorn, WeasyPrint (PDF generation), APScheduler (backups), flask-limiter (login rate limiting), qrcode[pil] + Pillow (WiFi QR codes), dnspython (direct MX email delivery), pdfplumber + python-docx + openpyxl + xlrd + striprtf (document import/AI extraction), psycopg2-binary (optional PostgreSQL support).

### WeasyPrint system dependencies (Ubuntu/Debian)

```bash
sudo apt install libpango-1.0-0 libpangoft2-1.0-0 libffi-dev libcairo2
```

---

## Installation

```bash
# Clone to a sensible location, then install:
git clone https://github.com/ShowSysDan/ShowAdvance 321theater
cd 321theater

# Full install with systemd service (recommended):
sudo ./install.sh

# Without root (manual start only):
./install.sh
```

The installer: creates a Python venv, installs dependencies, initialises/migrates the SQLite database, creates backup directories, writes a systemd service unit (`321theater`), generates a SECRET_KEY, and starts the service. For PostgreSQL setup, see [Database Configuration](#database-configuration).

After installation the app is available at `http://<server-ip>:<port>` (default port **5400**).

**Useful service commands:**
```bash
systemctl status 321theater
journalctl -u 321theater -f
sudo systemctl restart 321theater
```

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

### Global Search

A persistent search box lives in the left sidebar (below the logo). Press **/** or **Ctrl+K** from anywhere to focus it.

- Searches **shows** (by name, venue, company, date — respects your show access permissions), **contacts** (name, department, email, title), **asset types** (name, manufacturer, model — admin only), and **asset barcodes** (admin only, with leading-zero tolerance)
- Results appear in a grouped panel with match highlighting
- Keyboard navigation: **↑ / ↓** to move, **Enter** to open, **Escape** to close
- Minimum 2 characters to trigger, maximum 255 characters

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
- Supplier/vendor name and contact

**Individual Units** are each tracked with a database ID (always unique, even without a barcode). Barcodes are optional.

**Search & Sort:** Use the search bar above the type tree to filter by name/manufacturer/model. Sort by name, unit count, or rental cost (ascending/descending). Within the units modal, filter units by barcode with leading-zero tolerance.

**Maintenance:** Remove a unit from service with a reason and notes. Return it to service when resolved. Both actions are captured in the Audit Log and Syslog.

**Retiring:** Asset types and individual units are **never deleted** — only retired. Retiring a type also retires all its available units. Use the **Show Retired** checkbox to view retired entries inline. The **Retired Archive** link opens the full retired-assets history page.

**Warehouse Locations:** Manage a central list of storage location names (click **Warehouse Locations** button). These appear as a dropdown when editing item types.

**Availability:** When a unit is added to a show, the system checks real-time availability for the rental period, accounting for maintenance units, reserved spares, and other shows requesting the same item type. Negative availability is displayed — it does not prevent allocation, but makes the over-allocation visible.

**Rental pricing:** Each item type has a base rental cost. When added to a show the price is **locked** immediately — if the database price is updated later, existing show reservations keep the original price. New reservations use the current price.

### Importing from RentalWorks

If your organisation previously used **RentalWorks** (rental management software by Wynne Systems / HelixIntel), you can bulk-import your entire inventory into the Asset Manager using the included migration script `import_assets.py`.

#### What you need

Two Excel exports from RentalWorks (exported via its reporting module):

| Export | File naming pattern | Contents |
|--------|--------------------|-|
| Rental Inventory | `RentalInventory_<date>.xlsx` | Item types: name, category, manufacturer, part number, daily/weekly rates, active/inactive |
| Items | `Item_<date>.xlsx` or `Items_<date>.xlsx` | Individual physical units: barcode, serial number, status, purchase date, replacement cost, depreciation date |

#### What gets imported

| Source | Destination | Notes |
|--------|------------|-------|
| `InventoryType` | `asset_categories.name` | Top-level groupings (e.g. Audio, Video, Lighting) |
| `Category` | `asset_types` (parent tier) | Mid-level categories within each type |
| `Description` | `asset_types` (leaf tier) | Specific make/model names |
| `Manufacturer` | `asset_types.manufacturer` | |
| `ManufacturerPartNumber` / `SubCategory` | `asset_types.model` | Part number preferred; SubCategory used as fallback |
| `DailyRate` | `asset_types.rental_cost` | Per-day rental price |
| `WeeklyRate` | `asset_types.weekly_rate` | Per-week rental price (enables smart rate calc on shows) |
| `Inactive` | `asset_types.is_retired` | Retired types are hidden from active inventory |
| `BarCode` / `SerialNumber` | `asset_items.barcode` | Uses barcode if tracked by barcode; serial number otherwise |
| `InventoryStatus` | `asset_items.status` | IN / IN CONTAINER / STAGED → `available`; IN REPAIR → `maintenance` |
| `PurchaseDate` | `asset_items.year_purchased` | Year extracted from date |
| `DepreciationStartDate` | `asset_items.depreciation_start_date` | |
| `ReplacementCost` | `asset_items.replacement_cost` | |
| Container assignments | `asset_items.container_item_id` | Physical cases/racks linked to their contents via `ContainerBarCode` |

#### Running the import

```bash
# From the ShowAdvance directory:
python3 import_assets.py \
  --inventory /path/to/RentalInventory_2026-03-30.xlsx \
  --items     /path/to/Items_2026-03-30.xlsx

# Options:
#   --inventory PATH   Path to RentalInventory export (required)
#   --items PATH       Path to Items export (required)
#   --db PATH          Path to database file (default: advance.db)
#   --force            Skip duplicate-data guard (use if re-running)
#   --dry-run          Print what would be imported without writing anything
```

Expected output (numbers will vary):
```
[1/4] Categories:   9 created
[2/4] Parent types: 46 created
[3/4] Leaf types:   334 created
[4/4] Items:        1503 created  (0 warnings)
      Containers:   99 assigned
Done. Import complete.
```

#### Notes

- The script creates a backup of your database (`advance.db.bak`) before writing anything.
- Run `python3 import_assets.py --dry-run` first to preview the import without modifying the database.
- If the Asset Manager already has data, the script will abort unless you pass `--force`.
- Re-running with `--force` will skip rows that would create duplicate category or type names — existing records are left unchanged.
- The three-tier hierarchy (`InventoryType → Category → Description`) maps cleanly to the existing Asset Manager structure using `parent_type_id` — no schema changes needed for the organisational hierarchy itself.

---

### Asset Financial Tracking

Each individual unit can store financial metadata:

| Field | Description |
|-------|-------------|
| Condition | excellent / good / fair / poor / retired |
| Year Purchased | Calendar year of acquisition |
| Purchase Value | Original cost in dollars |
| Depreciation (years) | Straight-line depreciation timeframe |
| Warranty Expires | Date warranty coverage ends |

When **Purchase Value** and **Depreciation Years** are both set, the unit detail panel shows a live **remaining capital value** with a color-coded bar (green → amber → red as the asset approaches full depreciation). The calculation is straight-line: `remaining = max(0, value − (value ÷ years) × age)`.

### Asset Maintenance Log

Each individual unit has a built-in log for recording its history. Access it from the **Log** tab in the unit detail pane.

| Log Type | Use for |
|----------|---------|
| `note` | General observations |
| `damage` | Damage noticed during use or inspection |
| `service` | Repairs, cleaning, calibration |
| `usage` | Notable usage events |

Each entry records a date, the author (logged-in user), and a free-text body. Admins can delete entries. Entries are preserved permanently even after a unit is retired.

### Retired Assets

Access via **Retired Archive** link in Asset Manager, or **Retired Assets** in the sidebar.

Retired assets are split into two sections:

1. **Retired Item Types** — the entire type was retired. Expand each row to view all units that belonged to that type.
2. **Individually Retired Units** — the parent type is still active, but this specific unit was retired. The table includes condition, purchase value, warranty, and a link to view the unit's full log history inline.

All records are **read-only** and preserved permanently.

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

### View As (Role Preview)

Admins can preview the site from another role's perspective without logging out. The **VIEW SITE AS** control appears at the bottom of the sidebar (admin only).

| Preview Mode | Simulates |
|---|---|
| **C.Admin** | Content Admin (can edit form fields, manage messages) |
| **User** | Standard user (show access controlled by groups) |
| **R/O** | Read-only user (view-only, no edits) |

An amber banner appears at the top of every page while in preview mode. Click **Exit Preview** (or **RETURN TO ADMIN** in the sidebar) to restore full admin access. The real session is preserved — no actual role change occurs in the database.

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

Events: LOGIN/LOGOUT · SHOW_CREATE/ARCHIVE/DELETE/RESTORE · FORM_SAVE · PDF_EXPORT · USER_CREATE/DELETE/PASSWORD_CHANGE · GROUP_ASSIGN/REMOVE · BACKUP_CREATED · SETTINGS_CHANGE · REGISTER_PENDING · EMAIL_CONFIRMED · USER_APPROVED · USER_DENIED · PASSWORD_RESET_REQUEST · PASSWORD_RESET_COMPLETE · APP_UPDATE_START · MESSAGE_CREATE · MESSAGE_EDIT · MESSAGE_DELETE · ASSET_TYPE_RETIRE · ASSET_ITEM_RETIRE · ASSET_LOG_ADD · ASSET_LOG_DELETE · ADMIN_VIEW_AS · ADMIN_VIEW_AS_RESET

### Database Backups

Settings → Backups. Automatic hourly (keeps 24) and daily at midnight (keeps 30) SQLite backups in `backups/`. Click **Run Backup Now** for immediate backup.

**SQLite Restore:**
```bash
cp backups/daily/advance_YYYYMMDD_0000.db advance.db
sudo systemctl restart 321theater
```

**PostgreSQL Backups:** When using PostgreSQL, use standard `pg_dump` for database backups. The in-app backup system backs up the SQLite bootstrap file (`advance.db`) only.
```bash
pg_dump -h localhost -U showadvance 321theater > backup_$(date +%Y%m%d).sql
```

### File Manager

Settings → Files (admin only). View and delete all file attachments across all shows.

### God Mode

Settings → God Mode (admin only).

- **Active Sessions** — users on a show page in the last 5 minutes (user, show, tab, last seen)
- **User Last Login** — last login timestamp per user

---

## Database Configuration

321Theater supports two database backends: **SQLite** (default, zero-config) and **PostgreSQL** (recommended for production and multi-app environments).

### SQLite (Default)

Out of the box, all data lives in a single file: `advance.db`. No configuration needed. The installer handles initialization and migrations automatically.

SQLite is ideal for single-server installs and development.

### PostgreSQL (Dual-Schema)

PostgreSQL mode uses **two schemas** within one database:

| Schema | Default Name | Contents | Purpose |
|--------|-------------|----------|---------|
| **Shared** | `shared` | `users`, `user_groups`, `user_group_members`, `app_settings`, `password_reset_tokens`, `user_pending_registration`, `site_messages`, `site_message_dismissals` | User/auth data — designed to be shared across multiple apps |
| **App** | `theater321` | Shows, schedules, contacts, forms, assets, labor, exports, comments, active_sessions, audit_log, and all other theater-specific tables | App-specific data |

This separation means another app can connect to the same PostgreSQL database and share the user/auth system without touching theater data.

#### Setup

1. **Create a PostgreSQL database and user** on your server:
   ```sql
   CREATE DATABASE "321theater";
   CREATE USER showadvance WITH PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE "321theater" TO showadvance;
   -- Grant schema creation permission:
   ALTER DATABASE "321theater" OWNER TO showadvance;
   ```

2. **Create `db_config.ini`** in the app directory (copy from the example):
   ```bash
   cp db_config.ini.example db_config.ini
   nano db_config.ini
   ```

   ```ini
   [postgresql]
   host           = localhost
   port           = 5432
   dbname         = 321theater
   user           = showadvance
   password       = your_secure_password
   app_schema     = theater321
   shared_schema  = shared
   ```

   This file is **gitignored** — credentials are never committed.

3. **Initialize the PostgreSQL schemas and tables:**
   ```bash
   python3 init_db.py --init-postgres
   ```
   This creates both schemas and all tables. Safe to run multiple times (uses `IF NOT EXISTS`).

4. **Set the app to use PostgreSQL:**
   ```bash
   # In the SQLite database, set db_type to 'postgres':
   sqlite3 advance.db "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('db_type', 'postgres');"
   ```

5. **Restart the app:**
   ```bash
   sudo systemctl restart 321theater
   ```

#### Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `host` | `localhost` | PostgreSQL server hostname |
| `port` | `5432` | PostgreSQL server port |
| `dbname` | `321theater` | Database name |
| `user` | — | Database user |
| `password` | — | Database password |
| `app_schema` | `theater321` | Schema for theater-specific tables |
| `shared_schema` | `shared` | Schema for user/auth tables (shared across apps) |

Legacy note: the old `schema` key is still accepted as a fallback for `app_schema`.

#### How it Works at Runtime

When the app connects to PostgreSQL, it sets `search_path` to `"app_schema", "shared_schema"`. This means all SQL queries work with unqualified table names — no code changes needed. Foreign key references (e.g., `shows.created_by → users.id`) resolve correctly across schemas.

SQLite remains the "bootstrap" database — it always stores the `db_type` setting so the app knows which backend to use on startup.

### Migrating from SQLite to PostgreSQL

Two options: **CLI** (recommended) or **Web UI**.

#### CLI Migration

```bash
# 1. Ensure db_config.ini is configured (see above)

# 2. Initialize PostgreSQL schemas and tables:
python3 init_db.py --init-postgres

# 3. Copy all data from SQLite to PostgreSQL:
python3 init_db.py --migrate-to-postgres

# 4. Set the app to use PostgreSQL:
sqlite3 advance.db "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('db_type', 'postgres');"

# 5. Restart:
sudo systemctl restart 321theater
```

The migration is **idempotent** — duplicate rows are skipped via `ON CONFLICT DO NOTHING`. You can safely re-run it if interrupted. Tables are copied in foreign-key dependency order, and serial sequences are synced after copy so new inserts get correct IDs.

Each table is routed to the correct schema: shared tables go to the `shared` schema, app tables go to the `theater321` schema.

#### Web UI Migration

If the app is already set to `db_type=postgres`, go to **Settings → Database** and click **Migrate Now**. This runs the same migration as the CLI command. Progress and per-table stats are shown in the browser.

#### CLI Reference

| Command | Description |
|---------|-------------|
| `python3 init_db.py` | Fresh SQLite init (skips if DB exists) |
| `python3 init_db.py --force` | Destroy and reinitialize SQLite |
| `python3 init_db.py --migrate` | Run schema migrations on existing SQLite DB |
| `python3 init_db.py --init-postgres` | Create PostgreSQL schemas + tables from `db_config.ini` |
| `python3 init_db.py --migrate-to-postgres` | Copy all SQLite data → PostgreSQL |

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
sudo systemctl restart 321theater
```

**Service logs:**
```bash
journalctl -u 321theater -f
journalctl -u 321theater -n 100
```

**SQLite migration errors:**
```bash
venv/bin/python init_db.py --migrate
```

**PostgreSQL "no schema has been selected to create in":** Ensure your `db_config.ini` has valid `app_schema` and `shared_schema` values. The database user must have permission to create schemas. Re-run:
```bash
python3 init_db.py --init-postgres
```

**PostgreSQL connection refused:** Check that PostgreSQL is running, the host/port/credentials in `db_config.ini` are correct, and `pg_hba.conf` allows connections from the app server.

**Falling back to SQLite:** If the app logs `PostgreSQL connection failed — falling back to SQLite`, check `db_config.ini` credentials and PostgreSQL server status. The app silently falls back to SQLite when PostgreSQL is unreachable.

**Login rate limiting:** After 15 failed login attempts per minute from an IP, further attempts return HTTP 429. Wait 60 seconds or restart the app.

---

## Transition Notes (ShowAdvance → 321Theater)

The git repository and codebase were previously named **ShowAdvance**. The rename to **321Theater** is in progress. For the current transition period:

- The **service name** on new installs is `321theater` (old installs still use `showadvance` — both are auto-detected)
- The **SQLite database file** remains `advance.db` as the bootstrap database (stores `db_type` setting even when using PostgreSQL)
- The **PostgreSQL database** is named `321theater` with schemas `theater321` (app data) and `shared` (user/auth data)
- The **syslog identifier** (`showadvance`) will update to `321theater` on the new server install — update any syslog filters at that time
- The **folder** should be cloned as `321theater/` on new servers (`git clone <url> 321theater`)
- Internal table names are generic (`shows`, `asset_types`, etc.) and require no renaming
- The `shared` schema is designed for future multi-app use — other apps can share the same user/auth system by connecting to the same database and setting their `search_path` to include the `shared` schema
