"""
Run this once to create the database and seed it with contacts from the Excel.
Usage: python init_db.py
"""
import sqlite3
import os
from werkzeug.security import generate_password_hash

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'advance.db')

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    role TEXT DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    show_date DATE,
    show_time TEXT DEFAULT '',
    venue TEXT DEFAULT 'Judson''s Live',
    status TEXT DEFAULT 'active',
    advance_version INTEGER DEFAULT 0,
    schedule_version INTEGER DEFAULT 0,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS advance_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    field_key TEXT NOT NULL,
    field_value TEXT DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(show_id, field_key)
);

CREATE TABLE IF NOT EXISTS schedule_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    sort_order INTEGER DEFAULT 0,
    start_time TEXT DEFAULT '',
    end_time TEXT DEFAULT '',
    description TEXT DEFAULT '',
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS schedule_meta (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    field_key TEXT NOT NULL,
    field_value TEXT DEFAULT '',
    UNIQUE(show_id, field_key)
);

CREATE TABLE IF NOT EXISTS post_show_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    field_key TEXT NOT NULL,
    field_value TEXT DEFAULT '',
    UNIQUE(show_id, field_key)
);

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    title TEXT DEFAULT '',
    department TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    email TEXT DEFAULT '',
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS export_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER REFERENCES shows(id) ON DELETE SET NULL,
    export_type TEXT,
    version INTEGER,
    exported_by INTEGER REFERENCES users(id),
    exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filename TEXT DEFAULT ''
);
"""

SEED_CONTACTS = [
    # Production
    ('Allie Shidel',          'Production Manager',         'Production',     '239-898-4419', ''),
    ('Alyssa Marinello',      'Production Manager',         'Production',     '860-707-0224', ''),
    ('Ashley Kreischer',      'Production Manager',         'Production',     '832-527-1507', ''),
    ('Cheyenne Young',        'Production Manager',         'Production',     '407-953-9686', ''),
    ('Jeff Sturgis',          'Production Manager',         'Production',     '801-971-2240', ''),
    ('John Gallagher',        'Production Manager',         'Production',     '732-770-1406', ''),
    ('Noah Mencia',           'Production Manager',         'Production',     '407-269-0286', ''),
    ('Troy Mitchell',         'Production Manager',         'Production',     '716-622-7675', ''),
    ('Don Teer',              'Director, Production',       'Production',     '407-376-4149', ''),
    ('Dw Phineas Perkins',    'Director, Production',       'Production',     '407-421-4331', ''),
    ('Kevin Griffin',         'Technical Director',         'Production',     '407-921-4584', ''),
    ('Rich Neu',              'Assoc Technical Director',   'Production',     '407-803-5153', ''),
    # Programming
    ('Andrew Birgensmith',    'Sr. Director, Programming',          'Programming',    '816-935-9120', ''),
    ('Chris Belt',            "Manager, Judson's Programming",      'Programming',    '689-248-6768', ''),
    ('Foster Cronin',         'VP, Programming',                    'Programming',    '267-438-4371', ''),
    ('Geraldine Diaz',        'Programming Coordinator',            'Programming',    '352-942-4672', ''),
    ('Jovanna Hernandez',     'Director, Regional Programming',     'Programming',    '407-430-8939', ''),
    ('Mariah Roberts',        'Manager, Commercial Booking',        'Programming',    '352-634-0157', ''),
    ('Melissa Hopkins',       'Programming Coordinator',            'Programming',    '407-839-0119', ''),
    ('Toni Chandler',         'Manager, Regional Arts Programming', 'Programming',    '386-216-4493', ''),
    ('Zachary Hines',         'Manager, Commercial Booking',        'Programming',    '609-413-1869', ''),
    # Event Manager
    ('Grace Smith',           'Sales Manager, Events',      'Event Manager',  '850-728-8183', ''),
    ('Jenna Rogers',          'Director, Events',           'Event Manager',  '407-383-6008', ''),
    ('Kelsie Taylor',         'Sr. Sales Manager, Events',  'Event Manager',  '407-739-5909', ''),
    ('Robyn Pigozzi',         'Sr. Manager, Events',        'Event Manager',  '407-619-2609', ''),
    ('Sarah-Lynn Sharpton',   'Event Manager',              'Event Manager',  '334-740-9042', ''),
    ('Trevor Starr',          'Event Manager',              'Event Manager',  '321-289-6063', ''),
    # Education Team
    ('Brooke Saad',           'Manager, Education',         'Education Team', '407-409-1035', ''),
    ('Gabrielle Lawlor',      'Supervisor, SoA Education',  'Education Team', '',             ''),
    ('Khristy Chamberlain',   'Manager, Education',         'Education Team', '954-439-6620', ''),
    ('Ryan Simpson',          'Director, Education',        'Education Team', '847-951-9439', ''),
    ('Sara York',             'Sr. Manager, Education',     'Education Team', '334-618-8886', ''),
    ('Tati Bello',            'SOA Manager, Education',     'Education Team', '321-263-8004', ''),
    # Hospitality
    ('Dana Desposito',        'Supervisor, Craft Services', 'Hospitality',    '772-349-1347', ''),
    ('Jackie Einfeldt',       'Manager, Concession',        'Hospitality',    '321-304-0482', ''),
    ('Jenna Wickens',         'Manager, Backstage Catering','Hospitality',    '772-521-5521', ''),
    ('F&B Management',        'Management',                 'Hospitality',    '',             'foodbeveragemanagement@drphillipscenter.org'),
    # Guest Services
    ('Aaron Sandford-Wetherell','Sr. Manager, Guest Services','Guest Services','407-489-8620',''),
    ('Diana Mattoni',         'Manager, Front of House',    'Guest Services', '',             ''),
    ('Meghan Godber',         'Manager, Guest Services',    'Guest Services', '407-353-1593', ''),
    ('Charlie Robuck',        'Manager, Front of House',    'Guest Services', '910-520-3668', ''),
    ('Zakiya Smith-Dore',     'Director, Guest Services',   'Guest Services', '407-373-1949', ''),
    # Security
    ('Security Dept',         'Security',                   'Security',       '',             'security@drphillipscenter.org'),
    # Runners
    ('Anik Pariseleti',       'Runner', 'Runners', '', ''),
    ('David Becker',          'Runner', 'Runners', '', ''),
    ('Josh Cassady',          'Runner', 'Runners', '', ''),
    ('Kathy Wiebe',           'Runner', 'Runners', '', ''),
    ('Keith (KJ) Sales',      'Runner', 'Runners', '', ''),
    ('Kenzie Smith',          'Runner', 'Runners', '', ''),
    ("Kyle O'Toole",          'Runner', 'Runners', '', ''),
    ('Luke St. Jean',         'Runner (no alcohol)', 'Runners', '', ''),
    ('Matt McGregor',         'Runner (no people)',  'Runners', '', ''),
    ('Rick Luciano',          'Runner', 'Runners', '', ''),
    ('Sofia Rivera',          'Runner', 'Runners', '', ''),
]

def init_db(force=False):
    if os.path.exists(DATABASE) and not force:
        print(f"Database already exists at {DATABASE}")
        print("Use --force flag to reinitialize (WARNING: destroys all data)")
        return

    conn = sqlite3.connect(DATABASE)
    conn.executescript(SCHEMA)

    # Admin user
    conn.execute("""
        INSERT OR REPLACE INTO users (username, password_hash, display_name, role)
        VALUES (?, ?, ?, ?)
    """, ('admin', generate_password_hash('admin123'), 'Administrator', 'admin'))

    # Seed contacts
    for row in SEED_CONTACTS:
        conn.execute("""
            INSERT INTO contacts (name, title, department, phone, email)
            VALUES (?, ?, ?, ?, ?)
        """, row)

    conn.commit()
    conn.close()

    print("✓ Database created:", DATABASE)
    print("✓ Admin account:   username=admin  password=admin123")
    print("✓ Contacts seeded:", len(SEED_CONTACTS), "contacts imported")
    print()
    print("⚠  Change the admin password after first login via Settings → Users")


if __name__ == '__main__':
    import sys
    force = '--force' in sys.argv
    init_db(force=force)
