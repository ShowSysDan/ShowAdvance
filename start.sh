#!/usr/bin/env bash
# ShowAdvance service launcher.
# Reads the configured port from the database so that changing the port
# in Settings takes effect on the next service restart — no need to
# edit the systemd unit or re-run install.sh.

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${APP_DIR}/venv"
DB="${APP_DIR}/advance.db"

# Read port from app_settings, fall back to 5400
PORT=$(python3 -c "
import sqlite3, os
try:
    c = sqlite3.connect('${DB}')
    r = c.execute(\"SELECT value FROM app_settings WHERE key='app_port'\").fetchone()
    c.close()
    print(r[0] if r else '5400')
except Exception:
    print('5400')
" 2>/dev/null || echo "5400")

echo "[showadvance] Starting on port ${PORT}"

exec "${VENV}/bin/gunicorn" \
    --workers 2 \
    --bind "0.0.0.0:${PORT}" \
    --timeout 120 \
    --access-logfile - \
    --chdir "${APP_DIR}" \
    app:app
