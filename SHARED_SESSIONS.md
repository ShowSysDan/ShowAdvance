# Shared DB-Backed Sessions

321Theater now stores Flask sessions in PostgreSQL instead of in signed
cookies. The session cookie holds only a random ID; the actual session data
lives in the `app_sessions` table inside the **shared** schema. Two apps that
point at the same PostgreSQL database and the same `shared` schema will share
login state automatically — sign in once, you're authenticated in both.

This document is the porting guide for the **other** Flask app that uses the
same login system. Follow these steps and the cookie issued by either app
will be honored by both.

---

## 1. Database

A new table lives in the shared schema:

```sql
CREATE TABLE IF NOT EXISTS app_sessions (
    sid         TEXT PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
    data        TEXT NOT NULL DEFAULT '{}',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at  TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_app_sessions_expires ON app_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_app_sessions_user    ON app_sessions(user_id);
```

321Theater creates this on startup via `migrate_db_postgres()`. As long as
both apps target the same PG database and have `shared` in their search path,
the second app will see the table without doing anything. If the other app
runs first or doesn't run migrations, just execute the `CREATE TABLE` above
against the shared schema once.

## 2. Cookie compatibility

Both apps must agree on:

| Setting                     | Value                                |
|-----------------------------|--------------------------------------|
| `SESSION_COOKIE_NAME`       | `session` (Flask default)            |
| `SESSION_COOKIE_DOMAIN`     | Same on both apps (or both unset)    |
| `SESSION_COOKIE_PATH`       | `/` (Flask default)                  |
| `SESSION_COOKIE_SAMESITE`   | `Lax`                                |
| `SESSION_COOKIE_HTTPONLY`   | `True`                               |

The cookie value is a random 256-bit token (no signing involved), so the
apps do **not** need to share `SECRET_KEY` for sessions to work — but you
should still set a stable `SECRET_KEY` per app for `flash()` messages and
other Flask internals.

## 3. Drop in the session interface

Copy the following into the other app, somewhere after `app = Flask(...)`:

```python
import os, re, json, secrets as _secrets_mod
from datetime import datetime, timedelta
from flask.sessions import (
    SessionInterface as _FlaskSessionInterface,
    SessionMixin as _FlaskSessionMixin,
)
from werkzeug.datastructures import CallbackDict as _CallbackDict

_SID_RE = re.compile(r'^[A-Za-z0-9_-]{20,128}$')


class _DBSession(_CallbackDict, _FlaskSessionMixin):
    def __init__(self, initial=None, sid=None, new=False):
        def _on_update(_self):
            _self.modified = True
        _CallbackDict.__init__(self, initial, _on_update)
        self.sid = sid
        self.new = new
        self.modified = False


class _DBSessionInterface(_FlaskSessionInterface):
    """Server-side session backend that stores session data in the
    `app_sessions` table inside the shared PG schema."""

    def _new_sid(self):
        return _secrets_mod.token_urlsafe(32)

    def _load(self, sid):
        try:
            db = get_db()                  # <-- replace with your DB accessor
        except Exception:
            return {}, False
        try:
            row = db.execute(
                "SELECT data, expires_at FROM app_sessions WHERE sid = ?", (sid,)
            ).fetchone()
        except Exception:
            try: db.close()
            except Exception: pass
            return {}, False
        if not row:
            try: db.close()
            except Exception: pass
            return {}, False
        expires = row['expires_at']
        if isinstance(expires, str):
            try:
                expires_dt = datetime.fromisoformat(expires.split('.')[0].replace('Z', ''))
            except Exception:
                expires_dt = datetime.utcnow() - timedelta(seconds=1)
        else:
            expires_dt = expires
        if expires_dt < datetime.utcnow():
            try:
                db.execute("DELETE FROM app_sessions WHERE sid = ?", (sid,))
                db.commit()
            except Exception:
                pass
            try: db.close()
            except Exception: pass
            return {}, False
        try:
            data = json.loads(row['data']) if row['data'] else {}
            if not isinstance(data, dict):
                data = {}
        except (json.JSONDecodeError, TypeError, ValueError):
            data = {}
        try: db.close()
        except Exception: pass
        return data, True

    def open_session(self, app, request):
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        sid = request.cookies.get(cookie_name)
        if not sid or not _SID_RE.match(sid):
            return _DBSession(sid=self._new_sid(), new=True)
        data, ok = self._load(sid)
        if not ok:
            return _DBSession(sid=sid, new=True)
        return _DBSession(data, sid=sid, new=False)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')

        if not session:
            if session.modified:
                try:
                    db = get_db()
                    try:
                        db.execute("DELETE FROM app_sessions WHERE sid = ?", (session.sid,))
                        db.commit()
                    finally:
                        db.close()
                except Exception:
                    pass
                response.delete_cookie(cookie_name, domain=domain, path=path)
            return

        if not session.modified and not session.new:
            return

        lifetime = app.permanent_session_lifetime
        expires_dt = datetime.utcnow() + lifetime
        try:
            data_json = json.dumps(dict(session), default=str)
        except (TypeError, ValueError):
            data_json = '{}'
        user_id = session.get('user_id')
        try:
            db = get_db()
            try:
                # PostgreSQL form — adjust the placeholder style to match
                # your db driver (psycopg2 uses %s).
                db.execute(
                    "INSERT INTO app_sessions (sid, user_id, data, last_seen, expires_at) "
                    "VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?) "
                    "ON CONFLICT (sid) DO UPDATE SET "
                    "  user_id   = EXCLUDED.user_id, "
                    "  data      = EXCLUDED.data, "
                    "  last_seen = CURRENT_TIMESTAMP, "
                    "  expires_at= EXCLUDED.expires_at",
                    (session.sid, user_id, data_json, expires_dt)
                )
                db.commit()
            finally:
                db.close()
        except Exception as e:
            app.logger.warning(f'DB session save failed: {e}')
            return

        response.set_cookie(
            cookie_name,
            session.sid,
            expires=expires_dt,
            httponly=self.get_cookie_httponly(app),
            domain=domain,
            path=path,
            secure=self.get_cookie_secure(app),
            samesite=self.get_cookie_samesite(app),
        )


app.session_interface = _DBSessionInterface()
```

Notes:
- The `?` placeholder works in 321Theater because its `db_adapter` rewrites
  `?` → `%s` when talking to PostgreSQL. If the other app uses raw
  `psycopg2`, change the placeholders to `%s` directly.
- `get_db()` is whatever your app uses to open a DB connection. The
  connection's cursor row factory must return dict-like rows so
  `row['data']` and `row['expires_at']` work — or change the accessors to
  positional (`row[0]`, `row[1]`).

## 4. Things to double-check

1. **Same `users` table**: both apps must look users up in the shared
   `users` table. If the other app maintains a separate user table, sessions
   would still share IDs but the IDs would point at different people — fix
   the other app to read from `shared.users`.
2. **Search path**: when you connect to PG, make sure the search path
   includes the `shared` schema, e.g.
   `SET search_path TO "<app_schema>", "shared"`.
3. **Reverse proxy / domain**: cookies are scoped by domain. If the apps
   live on different hostnames (e.g. `theater.local` vs. `crew.local`),
   browsers will not share the cookie. Put them behind the same hostname
   (different paths or subdomain-with-shared-Domain).
4. **Clock skew**: `expires_at` is compared against `datetime.utcnow()` on
   whichever app is reading the row. If the two app servers' clocks drift,
   one may see a session as expired while the other doesn't. Keep them in
   sync with NTP.
5. **Logging out**: `session.clear()` in either app removes the row from
   the DB and deletes the cookie, which logs the user out of both apps at
   once. That's usually what you want — confirm it matches your UX
   expectation.

## 5. Rolling back

321Theater respects `DISABLE_DB_SESSIONS=1` in the environment — set it and
restart and Flask's default signed-cookie sessions come back. The
`app_sessions` rows remain in the DB harmlessly until they expire.
