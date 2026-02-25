# ShowAdvance

Production advance sheet and scheduling management for live events and performing arts venues.

---

## What It Does

ShowAdvance replaces Excel-based production advance workflows with a web app that multiple staff members can use simultaneously. Every show gets a structured advance form, a production schedule, and versioned PDF exports — all in one place.

---

## Install

### Requirements
- Python 3.9+
- Linux or macOS (Windows via WSL)

### Quick Install (recommended)

Run the install script as root for full systemd service setup (auto-starts on boot):

```bash
sudo ./install.sh
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

### Manual Install (non-root / development)

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize the database
python init_db.py

# Start the app
python app.py
```

Open `http://localhost:5400`

---

### Dependencies (`requirements.txt`)

| Package | Purpose |
|---|---|
| `flask >= 3.0` | Web framework |
| `werkzeug >= 3.0` | Password hashing, utilities |
| `weasyprint >= 60` | PDF generation (HTML → PDF) |
| `APScheduler >= 3.10` | Automatic database backups |
| `gunicorn >= 21` | Production WSGI server |

---

## Usage

### Dashboard
The dashboard lists all upcoming shows sorted by date. Shows whose date has passed are automatically moved to the **Archived** tab. Archived shows can be restored or permanently deleted (admin only).

### Creating a Show
Click **New Show** and enter the show name, venue, and date/time. The show is immediately visible to all logged-in users.

### Advance Sheet
Each show has a fully editable advance form organized into collapsible sections (e.g. Show Information, Arrival & Parking, Hospitality, Security, Front of House, General Information). Sections and fields are fully customizable in Settings.

- **Auto-saves** 1.5 seconds after you stop typing, or press `Ctrl+S` / `Cmd+S`
- A toast notification confirms every save
- While a field is being edited by another user, their name appears next to the field in real time

### Production Schedule
A timeline table attached to each show with:
- Add / remove / reorder rows (time, description, notes)
- Venue & tech info (WiFi credentials, radio channel, mix position)
- Contact assignments for all departments and artist/tour contacts

### Export PDFs
- **Advance Sheet PDF** — two-column layout; empty fields are omitted automatically
- **Production Schedule PDF** — landscape format with full timeline and all contacts
- Every export **auto-increments the version number** (v1 → v2 → v3 …)
- Full export log shows who exported, when, and which version

### Form History & Versioning
Every save creates a versioned snapshot. Open the **History** panel on any advance form to browse previous versions, see what changed, and restore any prior version.

### Post-Show Notes
A dedicated notes area on each show for wrap-up notes, issues, and follow-ups.

---

## Multi-User / Real-Time Collaboration

Multiple staff members can work on the same show simultaneously:

- **Live presence indicators** — each field shows who is currently editing it
- **No overwrite conflicts** — changes sync automatically across all open sessions
- **Concurrent user capacity** — 4 Gunicorn workers handle ~80 requests/second; 30–50+ simultaneous users is typical without any queuing

---

## Settings

| Section | What You Can Do |
|---|---|
| **Contacts** | Add, edit, delete contacts; filter by department; contacts populate dropdowns on all forms |
| **Form Sections** | Add, rename, reorder, or delete advance form sections |
| **Form Fields** | Add fields with custom types (text, textarea, checkbox, dropdown, etc.), help text, placeholder, and layout width |
| **Users** *(admin)* | Add users, reset passwords, delete accounts |
| **My Account** | Change your own password |
| **Server** *(admin)* | Change the app port (triggers automatic service restart) |
| **Syslog** *(admin)* | Forward app logs to a remote syslog server |

---

## Backups

The app automatically backs up `advance.db`:
- **Hourly** — keeps the last 24 hourly backups
- **Daily** — keeps the last 30 daily backups

Backups are stored in `backups/hourly/` and `backups/daily/`. The backup schedule runs in the background using APScheduler and requires no configuration.

To restore: stop the app, replace `advance.db` with the desired backup file, restart.

---

## Production Deployment

The install script handles everything, but for a manual production setup:

```bash
# Set a strong secret key (or put it in a .env file)
export SECRET_KEY="replace-with-a-long-random-secret"

# Run with Gunicorn (4 workers recommended)
venv/bin/gunicorn --workers 4 --bind 0.0.0.0:5400 --chdir /path/to/ShowAdvance app:app
```

To change the port, use **Settings → Server** in the UI — no config file edits needed. The service restarts automatically and picks up the new port.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python / Flask |
| Database | SQLite (`advance.db`) |
| PDF generation | WeasyPrint (HTML → PDF) |
| Frontend | Vanilla JS + CSS (no npm, no build step) |
| Process manager | systemd + Gunicorn |
| Background tasks | APScheduler |

---

## Project Structure

```
ShowAdvance/
├── app.py              # Flask application and all routes
├── init_db.py          # Database initialization and migrations
├── install.sh          # Full install / upgrade script (systemd)
├── start.sh            # Service launcher (reads port from DB)
├── requirements.txt    # Python dependencies
├── advance.db          # SQLite database (created on first run)
├── backups/            # Automatic hourly and daily DB backups
├── static/
│   ├── css/style.css
│   └── js/app.js       # Auto-save, real-time sync, UI logic
└── templates/
    ├── dashboard.html
    ├── show.html
    ├── settings.html
    ├── login.html
    └── pdf/
        ├── advance_pdf.html
        └── schedule_pdf.html
```

---

## User Roles

| Permission | Regular User | Admin |
|---|---|---|
| View all shows | ✓ | ✓ |
| Edit advance / schedule | ✓ | ✓ |
| Export PDFs | ✓ | ✓ |
| Create shows | ✓ | ✓ |
| Delete / archive shows | — | ✓ |
| Manage users | — | ✓ |
| Change server settings | — | ✓ |

---

## Troubleshooting

**Service won't start**
```bash
journalctl -u showadvance -n 50
```

**Port already in use**
Change the port via Settings → Server in the UI, or edit `advance.db` directly:
```bash
sqlite3 advance.db "UPDATE app_settings SET value='5401' WHERE key='app_port';"
```

**Reset admin password**
```bash
python3 -c "
from werkzeug.security import generate_password_hash
import sqlite3
db = sqlite3.connect('advance.db')
db.execute(\"UPDATE users SET password=? WHERE username='admin'\", (generate_password_hash('newpassword'),))
db.commit()
"
```

**WeasyPrint PDF errors on Linux**
Install system fonts and Cairo:
```bash
sudo apt install libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info
```
