# DPC Advance App

Production advance sheet and scheduling management for Dr. Phillips Center.

## Setup

### 1. Install Dependencies
```bash
pip install flask werkzeug weasyprint
```

### 2. Initialize Database
```bash
python init_db.py
```
This creates `advance.db` and seeds all 54 contacts from the original Excel file.

**Default login:** `admin` / `admin123`  
⚠️ Change the admin password immediately after first login (Settings → My Account).

### 3. Run the App
```bash
python app.py
```
Open http://localhost:5001

---

## Features

### Shows
- **Active shows** listed on the dashboard, sorted by upcoming date
- Shows with past dates are **automatically archived**
- Archived shows can be restored or permanently deleted (admin only)

### Advance Sheet
All fields from the original Excel advance form, organized into collapsible sections:
- Show Information (contacts auto-populated from Settings)
- Arrival & Parking
- Security
- Hospitality
- Front of House
- General Information

**Auto-saves** after 1.5 seconds of inactivity, or press `Ctrl+S` / `Cmd+S`.

### Production Schedule
- Editable timeline table (add/remove rows)
- Venue & tech info (WiFi, radio channel, mix position)
- Contact assignments for all DPC departments + artist/tour contacts

### Export PDFs
- **Advance Sheet PDF** — matches the Excel two-column layout; empty fields are omitted
- **Production Schedule PDF** — landscape format with full timeline and contacts
- Every export **auto-increments the version number** (v1, v2, v3...)
- Full export history log with date and user

### Settings
- **Contacts Directory** — searchable, filterable by department. All contacts feed into dropdown selects on advance and schedule forms.
- **User Management** (admin only) — add users, reset passwords, delete accounts
- **My Account** — change your own password

### Users
- Multiple users, all shows shared/accessible to everyone
- Admin role has extra permissions (delete shows, manage users)

---

## Tech Stack
- **Backend:** Flask + SQLite (single file: `advance.db`)
- **PDF Generation:** WeasyPrint (HTML→PDF, so PDFs match web layout exactly)
- **Frontend:** Vanilla JS + CSS (no npm, no build step)

## Production Deployment
For a real server, set a strong secret key:
```bash
export SECRET_KEY="your-very-long-random-secret"
python app.py
```
Or use gunicorn:
```bash
pip install gunicorn
gunicorn -w 2 -b 0.0.0.0:5001 app:app
```
