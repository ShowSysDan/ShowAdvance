#!/usr/bin/env bash
# 321Theater — install / update script
# Supports fresh installs and in-place upgrades.
# Run as root for full systemd service setup; run as a regular user for manual-start mode.

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_NAME="321theater"
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"
ENV_FILE="${APP_DIR}/.env"

# Read port from database (falls back to 5400 if DB not yet created or setting absent)
_read_port() {
    python3 -c "
import sqlite3, os
db = os.path.join('${APP_DIR}', 'advance.db')
if os.path.exists(db):
    try:
        c = sqlite3.connect(db)
        r = c.execute(\"SELECT value FROM app_settings WHERE key='app_port'\").fetchone()
        c.close()
        print(r[0] if r else '5400')
    except Exception:
        print('5400')
else:
    print('5400')
" 2>/dev/null || echo "5400"
}
PORT=5400  # will be updated after DB is ready

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
step()  { echo -e "\n${CYAN}==> $*${NC}"; }

echo ""
echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║    3·2·1→THEATER — Production Manager    ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
echo ""

# ── Python check ──────────────────────────────────────────────────────────────
step "Checking Python 3..."
if ! command -v python3 &>/dev/null; then
    error "python3 not found. Install Python 3.9+ and re-run this script."
fi
PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$(echo "$PY_VER" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 9 ]; }; then
    error "Python 3.9+ is required (found $PY_VER)."
fi
info "Found Python $PY_VER at $(command -v python3)"

# ── Virtual environment ───────────────────────────────────────────────────────
step "Setting up virtual environment..."
VENV="${APP_DIR}/venv"
if [ ! -d "$VENV" ]; then
    info "Creating virtualenv at ${VENV}..."
    python3 -m venv "$VENV"
else
    info "Virtualenv already exists at ${VENV}"
fi

PIP="${VENV}/bin/pip"
PYTHON="${VENV}/bin/python"

info "Upgrading pip..."
"$PIP" install --upgrade pip --quiet

step "Installing Python dependencies..."
"$PIP" install -r "${APP_DIR}/requirements.txt" --quiet
info "Dependencies installed."

# ── Tailwind CSS + DaisyUI build ─────────────────────────────────────────────
step "Building front-end CSS (Tailwind + DaisyUI)..."

TOOLS_DIR="${APP_DIR}/tools"
TAILWIND_BIN="${TOOLS_DIR}/tailwindcss"
TAILWIND_VERSION="v3.4.17"
DAISYUI_VERSION="3"
STATIC_CSS="${APP_DIR}/static/css"
FONTS_DIR="${APP_DIR}/static/fonts"

mkdir -p "$TOOLS_DIR" "$FONTS_DIR"

# ── Tailwind CLI standalone binary ──
if [ ! -f "$TAILWIND_BIN" ]; then
    info "Downloading Tailwind CSS CLI ${TAILWIND_VERSION} (standalone, no Node.js required)..."
    TW_URL="https://github.com/tailwindlabs/tailwindcss/releases/download/${TAILWIND_VERSION}/tailwindcss-linux-x64"
    if curl -sL --max-time 120 --fail "$TW_URL" -o "$TAILWIND_BIN"; then
        chmod +x "$TAILWIND_BIN"
        info "Tailwind CLI ready."
    else
        warn "Could not download Tailwind CLI — will use committed tailwind.css as-is."
        TAILWIND_BIN=""
    fi
else
    info "Tailwind CLI already present at tools/tailwindcss."
fi

# ── DaisyUI pre-built CSS ──
DAISYUI_CSS="${TOOLS_DIR}/daisyui.css"
info "Downloading DaisyUI v${DAISYUI_VERSION} component CSS..."
DAISYUI_URL="https://cdn.jsdelivr.net/npm/daisyui@${DAISYUI_VERSION}/dist/styled.min.css"
if curl -sL --max-time 60 --fail "$DAISYUI_URL" -o "$DAISYUI_CSS"; then
    info "DaisyUI CSS downloaded."
else
    warn "Could not download DaisyUI CSS — will use committed tailwind.css as-is."
    DAISYUI_CSS=""
fi

# ── Web fonts (offline serving) ──
step "Downloading web fonts for offline serving..."

_dl_font() {
    local dest="$1" url="$2"
    if [ -f "$dest" ]; then
        info "Font already present: $(basename "$dest")"
    elif curl -sL --max-time 30 --fail "$url" -o "$dest" 2>/dev/null; then
        info "Downloaded: $(basename "$dest")"
    else
        warn "Font download failed: $(basename "$dest") — browser will use system fallback"
    fi
}

# Poppins (via @fontsource on jsDelivr — latin subset)
_dl_font "${FONTS_DIR}/Poppins-400.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/poppins@5/files/poppins-latin-400-normal.woff2"
_dl_font "${FONTS_DIR}/Poppins-500.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/poppins@5/files/poppins-latin-500-normal.woff2"
_dl_font "${FONTS_DIR}/Poppins-600.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/poppins@5/files/poppins-latin-600-normal.woff2"
_dl_font "${FONTS_DIR}/Poppins-700.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/poppins@5/files/poppins-latin-700-normal.woff2"

# JetBrains Mono (via @fontsource on jsDelivr — latin subset)
_dl_font "${FONTS_DIR}/JetBrainsMono-400.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/jetbrains-mono@5/files/jetbrains-mono-latin-400-normal.woff2"
_dl_font "${FONTS_DIR}/JetBrainsMono-500.woff2" \
    "https://cdn.jsdelivr.net/npm/@fontsource/jetbrains-mono@5/files/jetbrains-mono-latin-500-normal.woff2"

# ── Run CSS build ──
if [ -n "$TAILWIND_BIN" ] && [ -n "$DAISYUI_CSS" ]; then
    step "Compiling tailwind.css..."
    TW_UTILS="${TOOLS_DIR}/tw-utilities.css"
    if "${TAILWIND_BIN}" \
           -i "${STATIC_CSS}/input.css" \
           -o "$TW_UTILS" \
           --config "${APP_DIR}/tailwind.config.js" \
           --minify 2>/dev/null; then
        # Combine: DaisyUI components → DPC theme overrides → Tailwind utilities
        cat "$DAISYUI_CSS" \
            "${STATIC_CSS}/theme-dpc.css" \
            "$TW_UTILS" \
            > "${STATIC_CSS}/tailwind.css"
        rm -f "$TW_UTILS"
        info "tailwind.css rebuilt successfully."
    else
        warn "Tailwind build failed — committed tailwind.css will be used."
    fi
elif [ -f "${STATIC_CSS}/tailwind.css" ]; then
    info "CSS build tools unavailable — using committed tailwind.css."
else
    warn "No tailwind.css found and build tools unavailable. UI may not render correctly."
fi

# ── Backup directories ────────────────────────────────────────────────────────
step "Creating backup directories..."
mkdir -p "${APP_DIR}/backups/hourly"
mkdir -p "${APP_DIR}/backups/daily"
chmod -R 755 "${APP_DIR}/backups"
info "Backup dirs ready: ${APP_DIR}/backups/{hourly,daily}"

# ── Database ──────────────────────────────────────────────────────────────────
step "Initializing database..."
DB="${APP_DIR}/advance.db"
if [ -f "$DB" ]; then
    info "Existing database found — running migration (data preserved)..."
    "$PYTHON" "${APP_DIR}/init_db.py" --migrate
    info "Migration complete."
else
    info "No database found — creating fresh installation..."
    "$PYTHON" "${APP_DIR}/init_db.py"
    info "Database initialized with default admin account."
fi

# Read the configured port from the database now that it exists
PORT=$(_read_port)
info "App port: ${PORT}"

# ── Systemd service setup (root only) ─────────────────────────────────────────
if [ "$(id -u)" -eq 0 ]; then
    step "Configuring systemd service (running as root)..."

    # Determine service user (prefer the SUDO_USER who invoked sudo)
    if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
        RUN_USER="$SUDO_USER"
    else
        RUN_USER="$(logname 2>/dev/null || echo root)"
    fi
    info "Service will run as user: ${RUN_USER}"

    # Ensure all app files are owned by the service user
    chown -R "${RUN_USER}:${RUN_USER}" "${APP_DIR}/backups" 2>/dev/null || true
    chown "${RUN_USER}:${RUN_USER}" "${APP_DIR}/advance.db" 2>/dev/null || true
    chown "${RUN_USER}:${RUN_USER}" "${APP_DIR}" 2>/dev/null || true

    # Generate SECRET_KEY if not already present
    if [ ! -f "$ENV_FILE" ] || ! grep -q '^SECRET_KEY=' "$ENV_FILE" 2>/dev/null; then
        SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
        echo "SECRET_KEY=${SECRET_KEY}" >> "$ENV_FILE"
        chmod 600 "$ENV_FILE"
        info "Generated new SECRET_KEY → ${ENV_FILE}"
    else
        info "SECRET_KEY already present in ${ENV_FILE}"
    fi

    # Ensure .env is readable by service user
    chown "${RUN_USER}" "$ENV_FILE" 2>/dev/null || true

    # Make start.sh executable
    chmod +x "${APP_DIR}/start.sh"

    # Write the systemd unit — ExecStart uses start.sh so the port is
    # read from the database on every service restart (no need to edit
    # this file when the port changes via the Settings UI).
    cat > "$SERVICE_FILE" << EOF
[Unit]
Description=321Theater — Production Management
Documentation=https://github.com/ShowSysDan/ShowAdvance
After=network.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/start.sh
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${APP_NAME}

[Install]
WantedBy=multi-user.target
EOF

    info "Systemd unit written to ${SERVICE_FILE}"

    # Allow the service user to restart 321theater without a password.
    # This enables the "Change Port" UI button to trigger a live restart.
    SUDOERS_FILE="/etc/sudoers.d/${APP_NAME}"
    echo "${RUN_USER} ALL=(ALL) NOPASSWD: /bin/systemctl restart ${APP_NAME}, /usr/bin/systemctl restart ${APP_NAME}" \
        > "$SUDOERS_FILE"
    chmod 440 "$SUDOERS_FILE"
    info "Sudoers entry written to ${SUDOERS_FILE} (allows service restart from UI)"

    systemctl daemon-reload
    systemctl enable "$APP_NAME" --quiet
    systemctl restart "$APP_NAME"

    # Brief wait to confirm startup
    sleep 2
    if systemctl is-active --quiet "$APP_NAME"; then
        info "Service is running."
    else
        warn "Service may have failed to start. Check: journalctl -u ${APP_NAME} -n 50"
    fi

    # Determine LAN IP for the success message
    LAN_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")

    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║          Installation Complete!          ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  Open 321Theater at: ${CYAN}http://${LAN_IP}:${PORT}${NC}"
    echo ""
    echo -e "  ${YELLOW}Default login:${NC}  admin / admin   ← change this immediately!"
    echo ""
    echo "  Useful commands:"
    echo "    systemctl status ${APP_NAME}"
    echo "    journalctl -u ${APP_NAME} -f"
    echo "    systemctl restart ${APP_NAME}"
    echo ""

else
    # ── Non-root: manual-start instructions ──────────────────────────────────
    step "Non-root install complete — systemd service not configured."
    echo ""
    echo "  To start 321Theater manually:"
    echo ""
    echo "    ${VENV}/bin/python ${APP_DIR}/app.py"
    echo ""
    echo "  Or, to run with gunicorn (production-style):"
    echo ""
    echo "    ${VENV}/bin/gunicorn --workers 2 --bind 0.0.0.0:${PORT} --chdir ${APP_DIR} app:app"
    echo ""
    echo "  Then open: http://localhost:${PORT}"
    echo ""
    echo -e "  ${YELLOW}Default login:${NC}  admin / admin   ← change this immediately!"
    echo ""
    warn "To install the systemd service, re-run as root:  sudo ./install.sh"
    echo ""
fi
