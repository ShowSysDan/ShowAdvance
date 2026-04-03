"""
Database adapter for ShowAdvance.
Provides a unified interface for SQLite and PostgreSQL connections.
SQL written for SQLite (using ? placeholders) is automatically adapted for PostgreSQL.

PostgreSQL credentials are read from db_config.ini (gitignored) in the same
directory as the SQLite bootstrap file. db_type is still stored in the SQLite
app_settings table and toggled via the Settings UI.
"""
import configparser
import re
import sqlite3
import os
import time


class _Row(dict):
    """Dict-based row that also supports integer index access (row[0]).
    Supports .get(), named key access, and positional index access so both
    new-style (row['col']) and legacy (row[0]) patterns work on Python 3.12+.
    """
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)

    def get(self, key, default=None):
        if isinstance(key, int):
            vals = list(self.values())
            return vals[key] if key < len(vals) else default
        return super().get(key, default)


def _row_factory(cursor, row):
    return _Row(zip((col[0] for col in cursor.description), row))

# ─── Settings Cache ────────────────────────────────────────────────────────────
# read_db_settings() is called on every get_db() invocation. Cache results for
# 30 s so we're not opening a second SQLite connection on every request.
_settings_cache: dict = {}
_settings_ts: float = 0.0
_CACHE_TTL = 30  # seconds


def clear_settings_cache():
    """Invalidate the settings cache immediately (call after saving DB settings)."""
    global _settings_cache, _settings_ts
    _settings_cache = {}
    _settings_ts = 0.0

# Re-export sqlite3.IntegrityError so callers can use DBIntegrityError
# and still be caught by existing `except sqlite3.IntegrityError:` clauses.
class DBIntegrityError(sqlite3.IntegrityError):
    """Unified integrity error raised for both SQLite and PostgreSQL violations."""
    pass


# Regex patterns for SQL adaptation
_INSERT_OR_IGNORE_RE = re.compile(r'\bINSERT\s+OR\s+IGNORE\s+INTO\b', re.IGNORECASE)
_INSERT_OR_REPLACE_RE = re.compile(
    r'\bINSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)',
    re.IGNORECASE
)
_INSERT_RE = re.compile(r'^\s*INSERT\b', re.IGNORECASE)

# Conflict columns for each table (used for INSERT OR REPLACE → ON CONFLICT ... DO UPDATE SET)
_CONFLICT_COLS = {
    'app_settings':      ['key'],
    'advance_data':      ['show_id', 'field_key'],
    'schedule_meta':     ['show_id', 'field_key'],
    'post_show_notes':   ['show_id', 'field_key'],
    'show_group_access': ['show_id', 'group_id'],
    'user_group_members':['user_id', 'group_id'],
    'advance_reads':     ['show_id', 'user_id'],
    'active_sessions':   ['user_id', 'show_id'],
    'form_fields':       ['field_key'],
    'form_sections':     ['section_key'],
    'schedule_meta_fields': ['field_key'],
}


class AdaptedCursor:
    """Wraps a database cursor to provide a consistent interface."""

    def __init__(self, cursor, db_type):
        self._cur = cursor
        self.db_type = db_type
        self.lastrowid = None

    def __iter__(self):
        return iter(self._cur)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self):
        return self._cur.rowcount

    def __getitem__(self, key):
        return self._cur[key]


class DBConnection:
    """
    Normalized database connection for SQLite and PostgreSQL.
    Accepts SQL written for SQLite (? placeholders, INSERT OR IGNORE/REPLACE)
    and automatically adapts it for PostgreSQL when needed.
    """

    def __init__(self, conn, db_type, schema=None):
        self._conn = conn
        self.db_type = db_type
        self._schema = schema

    def _adapt_sql(self, sql):
        """
        Convert SQLite SQL to PostgreSQL-compatible SQL.
        Returns (adapted_sql, needs_lastval) where needs_lastval signals
        that a plain INSERT was made and lastval() should be called.
        """
        if self.db_type != 'postgres':
            return sql, False

        result = sql.replace('?', '%s')

        # INSERT OR IGNORE → INSERT INTO ... ON CONFLICT DO NOTHING
        if _INSERT_OR_IGNORE_RE.search(result):
            result = _INSERT_OR_IGNORE_RE.sub('INSERT INTO', result)
            result = result.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
            return result, False

        # INSERT OR REPLACE → INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
        m = _INSERT_OR_REPLACE_RE.search(result)
        if m:
            table = m.group(1).lower()
            all_cols = [c.strip() for c in m.group(2).split(',')]
            conflict_cols = _CONFLICT_COLS.get(table, [all_cols[0]])
            update_cols = [c for c in all_cols if c not in conflict_cols]

            result = _INSERT_OR_REPLACE_RE.sub(
                f'INSERT INTO {m.group(1)} ({m.group(2)})', result
            )

            conflict_str = ', '.join(conflict_cols)
            if update_cols:
                update_str = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
                suffix = f' ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}'
            else:
                suffix = f' ON CONFLICT ({conflict_str}) DO NOTHING'
            result = result.rstrip().rstrip(';') + suffix
            return result, False

        # Plain INSERT — needs lastval() after for lastrowid
        if _INSERT_RE.match(result):
            return result, True

        return result, False

    def execute(self, sql, params=()):
        adapted_sql, needs_lastval = self._adapt_sql(sql)

        if self.db_type == 'postgres':
            import psycopg2
            import psycopg2.extras
            import psycopg2.errors

            cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                cur.execute(adapted_sql, params)
                adapted = AdaptedCursor(cur, 'postgres')
                if needs_lastval:
                    try:
                        cur.execute("SELECT lastval()")
                        row = cur.fetchone()
                        adapted.lastrowid = row[0] if row else None
                    except Exception:
                        adapted.lastrowid = None
                return adapted
            except psycopg2.errors.UniqueViolation as e:
                raise DBIntegrityError(str(e)) from e
        else:
            try:
                cur = self._conn.execute(adapted_sql, params)
                adapted = AdaptedCursor(cur, 'sqlite')
                adapted.lastrowid = cur.lastrowid
                return adapted
            except sqlite3.IntegrityError:
                raise

    def executemany(self, sql, params_list):
        adapted_sql, _ = self._adapt_sql(sql)
        if self.db_type == 'postgres':
            import psycopg2.extras
            cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.executemany(adapted_sql, params_list)
            return AdaptedCursor(cur, 'postgres')
        else:
            cur = self._conn.executemany(adapted_sql, params_list)
            return AdaptedCursor(cur, 'sqlite')

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _read_pg_config(database_path):
    """
    Read PostgreSQL credentials from db_config.ini, located in the same
    directory as the SQLite bootstrap file. Returns a dict with pg_* keys,
    or an empty dict if the file doesn't exist or can't be parsed.

    Two schemas are supported:
      pg_app_schema    – theater-specific data (shows, schedules, etc.)
      pg_shared_schema – user/auth data shared across apps
    Legacy 'schema' key maps to pg_app_schema for backward compatibility.
    """
    config_path = os.path.join(os.path.dirname(os.path.abspath(database_path)), 'db_config.ini')
    if not os.path.exists(config_path):
        return {}
    try:
        cp = configparser.ConfigParser()
        cp.read(config_path, encoding='utf-8')
        sec = cp['postgresql'] if 'postgresql' in cp else {}
        # Support new dual-schema keys with fallback to legacy 'schema' key
        legacy_schema = sec.get('schema', '')
        return {
            'pg_host':          sec.get('host',     'localhost'),
            'pg_port':          sec.get('port',     '5432'),
            'pg_dbname':        sec.get('dbname',   '321theater'),
            'pg_user':          sec.get('user',     ''),
            'pg_password':      sec.get('password', ''),
            'pg_app_schema':    sec.get('app_schema', '') or legacy_schema or 'theater321',
            'pg_shared_schema': sec.get('shared_schema', '') or 'shared',
        }
    except Exception:
        return {}


def read_db_settings(database_path):
    """
    Read database connection settings.
    - db_type is read from the SQLite bootstrap app_settings table.
    - PostgreSQL credentials are read from db_config.ini (gitignored),
      located in the same directory as the SQLite file.
    Results are cached for _CACHE_TTL seconds.
    """
    global _settings_cache, _settings_ts
    if _settings_cache and (time.time() - _settings_ts) < _CACHE_TTL:
        return _settings_cache
    result = {}
    if os.path.exists(database_path):
        try:
            conn = sqlite3.connect(database_path)
            conn.row_factory = _row_factory
            rows = conn.execute(
                "SELECT key, value FROM app_settings WHERE key = 'db_type'"
            ).fetchall()
            conn.close()
            result = {r['key']: r['value'] for r in rows}
        except Exception:
            pass
    # Merge PG credentials from flat config file
    result.update(_read_pg_config(database_path))
    _settings_cache = result
    _settings_ts = time.time()
    return result


_SAFE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z0-9_]+$')


def _validate_identifier(name, label='identifier'):
    """Validate that a SQL identifier (schema/table name) is safe."""
    if not name or not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f'Invalid {label}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*')
    return name


def test_postgres_connection(host, port, dbname, user, password,
                             schema=None, app_schema=None, shared_schema=None):
    """Test a PostgreSQL connection. Returns (True, None) or (False, error_message).
    Accepts either legacy 'schema' or the new dual-schema keys."""
    app_sch = app_schema or schema or 'theater321'
    shared_sch = shared_schema or 'shared'
    try:
        _validate_identifier(app_sch, 'app_schema')
        _validate_identifier(shared_sch, 'shared_schema')
    except ValueError as e:
        return False, str(e)
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=int(port or 5432),
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=5,
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{app_sch}"')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{shared_sch}"')
        conn.autocommit = False
        cur.execute(f'SET search_path TO "{app_sch}", "{shared_sch}"')
        cur.execute("SELECT 1")
        conn.rollback()
        conn.close()
        return True, None
    except ImportError:
        return False, "psycopg2 is not installed. Run: pip install psycopg2-binary"
    except Exception as e:
        return False, str(e)


def connect(database_path, settings=None):
    """
    Create a normalized DBConnection based on configured settings.

    SQLite settings (db_type credentials) are always read from the SQLite
    bootstrap file. Other DB types use those credentials to connect.
    """
    if settings is None:
        settings = read_db_settings(database_path)

    db_type = settings.get('db_type', 'sqlite')

    if db_type == 'postgres':
        try:
            import psycopg2
            app_schema = settings.get('pg_app_schema', '') or settings.get('pg_schema', '') or 'theater321'
            shared_schema = settings.get('pg_shared_schema', '') or 'shared'
            _validate_identifier(app_schema, 'app_schema')
            _validate_identifier(shared_schema, 'shared_schema')
            conn = psycopg2.connect(
                host=settings.get('pg_host', 'localhost'),
                port=int(settings.get('pg_port', 5432) or 5432),
                dbname=settings.get('pg_dbname', '321theater'),
                user=settings.get('pg_user', ''),
                password=settings.get('pg_password', ''),
                connect_timeout=10,
            )
            # Create schemas with autocommit so they're visible immediately
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{app_schema}"')
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{shared_schema}"')
            cur.close()
            # Now switch to transactional mode and set the search path
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute(f'SET search_path TO "{app_schema}", "{shared_schema}"')
            cur.close()
            conn.commit()
            return DBConnection(conn, 'postgres', schema=app_schema)
        except ImportError:
            import logging
            logging.getLogger('showadvance').warning(
                'PostgreSQL configured but psycopg2 not installed — falling back to SQLite'
            )
        except Exception as e:
            import logging
            logging.getLogger('showadvance').warning(
                f'PostgreSQL connection failed — falling back to SQLite: {e}'
            )

    # SQLite (default)
    conn = sqlite3.connect(database_path)
    # Use _Row so both row['col'] and row[0] integer-index access work
    # (sqlite3.Row dropped undocumented dict-like behaviour in Python 3.13)
    conn.row_factory = _row_factory
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return DBConnection(conn, 'sqlite')
