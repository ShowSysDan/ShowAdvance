"""
Database initialization and migration for ShowAdvance.

Usage:
  python init_db.py           — fresh init (skips if DB exists)
  python init_db.py --force   — destroy and reinitialize
  python init_db.py --migrate — run migrations on existing DB (safe for production)
"""
import sqlite3
import os
import json
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
    theme TEXT DEFAULT 'dark',
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

CREATE TABLE IF NOT EXISTS form_sections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    section_key TEXT UNIQUE NOT NULL,
    label TEXT NOT NULL,
    sort_order INTEGER DEFAULT 0,
    collapsible INTEGER DEFAULT 1,
    icon TEXT DEFAULT '◈'
);

CREATE TABLE IF NOT EXISTS form_fields (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    section_id INTEGER NOT NULL REFERENCES form_sections(id) ON DELETE CASCADE,
    field_key TEXT UNIQUE NOT NULL,
    label TEXT NOT NULL,
    field_type TEXT NOT NULL DEFAULT 'text',
    sort_order INTEGER DEFAULT 0,
    options_json TEXT DEFAULT NULL,
    contact_dept TEXT DEFAULT NULL,
    conditional_show_when TEXT DEFAULT NULL,
    help_text TEXT DEFAULT NULL,
    placeholder TEXT DEFAULT '',
    width_hint TEXT DEFAULT 'full',
    is_notes_field INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS form_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    form_type TEXT NOT NULL,
    saved_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    snapshot_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    group_type TEXT NOT NULL DEFAULT 'all_access',
    description TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS user_group_members (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE IF NOT EXISTS show_group_access (
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
    PRIMARY KEY (show_id, group_id)
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS active_sessions (
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    show_id       INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    tab           TEXT NOT NULL DEFAULT 'advance',
    focused_field TEXT,           -- field_key the user currently has focused
    last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, show_id)
);

CREATE TABLE IF NOT EXISTS show_comments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id     INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body        TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS show_attachments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id      INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    uploaded_by  INTEGER REFERENCES users(id) ON DELETE SET NULL,
    filename     TEXT NOT NULL,
    mime_type    TEXT DEFAULT 'application/octet-stream',
    file_data    BLOB NOT NULL,
    file_size    INTEGER DEFAULT 0,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS advance_reads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id      INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    version_read INTEGER DEFAULT 0,
    read_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(show_id, user_id)
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

# (section_key, label, sort_order, collapsible, icon)
FORM_SECTIONS_SEED = [
    ('show_info',        'SHOW INFORMATION',         1,  0, '◈'),
    ('arrival_parking',  'ARRIVAL & PARKING',        2,  1, '◈'),
    ('security',         'SECURITY',                 3,  1, '◈'),
    ('hospitality',      'HOSPITALITY',              4,  1, '◈'),
    ('front_of_house',   'FRONT OF HOUSE',           5,  1, '◈'),
    ('audio_section',    'AUDIO',                    6,  1, '◈'),
    ('video_section',    'VIDEO / PROJECTION',       7,  1, '◈'),
    ('backline_section', 'BACKLINE',                 8,  1, '◈'),
    ('stage_props',      'STAGE & PROPS',            9,  1, '◈'),
    ('wardrobe',         'WARDROBE',                 10, 1, '◈'),
    ('special_elements', 'SPECIAL / OTHER ELEMENTS', 11, 1, '◈'),
    ('labor_needs',      'LABOR NEEDS',              12, 1, '◈'),
    ('general_info',     'GENERAL INFORMATION',      13, 1, '◈'),
]

# (section_key, field_key, label, field_type, sort_order,
#  options_json, contact_dept, conditional_show_when,
#  help_text, placeholder, width_hint, is_notes_field)
FORM_FIELDS_SEED = [
    # ── Show Information ──────────────────────────────────────────────────────
    ('show_info', 'show_name',           'SHOW NAME',                  'text',             10,  None, None, None, None, '',                       'full',  0),
    ('show_info', 'show_date',           'SHOW DATE',                  'date',             20,  None, None, None, None, '',                       'half',  0),
    ('show_info', 'show_time',           'SHOW TIME(S)',               'text',             30,  None, None, None, None, 'e.g. 7pm and 9pm',       'half',  0),
    ('show_info', 'venue',               'VENUE',                      'text',             40,  None, None, None, None, '',                       'full',  0),
    ('show_info', 'production_manager',  'PRODUCTION MANAGER',         'contact_dropdown', 50,  None, 'Production',    None, None, '',            'full',  0),
    ('show_info', 'tour_manager',        'TOUR MANAGER',               'text',             60,  None, None, None, None, 'Name · email / phone',   'full',  0),
    ('show_info', 'promoter',            'PROMOTER',                   'text',             70,  None, None, None, None, '',                       'full',  0),
    ('show_info', 'additional_contacts', 'ADDITIONAL CONTACTS',        'textarea',         80,  None, None, None, None, 'Name, role, phone/email...', 'full', 0),
    ('show_info', 'programming',         'PROGRAMMING',                'contact_dropdown', 90,  None, 'Programming',   None, None, '',            'full',  0),
    ('show_info', 'events',              'EVENTS',                     'contact_dropdown', 100, None, 'Event Manager', None, None, '',            'full',  0),
    ('show_info', 'hospitality_contact', 'HOSPITALITY',                'contact_dropdown', 110, None, 'Hospitality',   None, None, '',            'full',  0),
    ('show_info', 'guest_services',      'GUEST SERVICES',             'contact_dropdown', 120, None, 'Guest Services',None, None, '',            'full',  0),
    ('show_info', 'radio_channel',       'RADIO CHANNEL',              'text',             130, None, None, None, None, "e.g. 16/Judson's",       'half',  0),
    ('show_info', 'rental_works',        'RENTAL WORKS?',              'yes_no',           150, None, None, None, None, '',                       'half',  0),
    ('show_info', 'rentalworks_order_num','RENTALWORKS ORDER #',       'text',             155, None, None, 'rental_works=Yes', None, 'Order number', 'half', 0),
    ('show_info', 'mix_position',        'MIX POSITION',               'text',             158, None, None, None, None, 'e.g. FOH',               'half',  0),
    ('show_info', 'show_length',         'SHOW LENGTH',                'text',             159, None, None, None, None, 'e.g. 1hr 45min',         'half',  0),
    ('show_info', 'budget_what',         'BUDGET / ESTIMATE — WHAT',   'text',             160, None, None, None, None, 'What',                   'half',  0),
    ('show_info', 'budget_amount',       'BUDGET / ESTIMATE — AMOUNT', 'text',             170, None, None, None, None, 'Amount',                 'half',  0),

    # ── Arrival & Parking ─────────────────────────────────────────────────────
    ('arrival_parking', 'access_time',                 'ACCESS TIME TO BUILDING',                  'text',     10,  None, None, None, None, 'e.g. 3:30pm',      'half', 0),
    ('arrival_parking', 'loading_dock',                'LOADING DOCK — WHICH BAY(S)?',             'select',   20,
        json.dumps(['-', 'N/A', 'Bay 1', 'Bay 2', 'Bay 3', 'Bay 4', 'Bay 5', 'Bay 1+2', 'Bay 1+2+3', 'Other — See Notes']),
        None, None, None, '', 'half', 0),
    ('arrival_parking', 'vehicle_type',                'VEHICLE TYPE',                             'select',   30,
        json.dumps(['-', 'DPC Van (15-passenger)', 'DPC Truck', 'Rental Vehicle', 'Other']),
        None, None, None, '', 'half', 0),
    ('arrival_parking', 'vehicle_notes',               'VEHICLE NOTES',                            'text',     35,  None, None, 'vehicle_type=Other', None, 'Describe vehicle...', 'half', 0),
    ('arrival_parking', 'runner_needed',               'RUNNER NEEDED?',                           'yes_no',   80,  None, None, None, None, '', 'third', 0),
    ('arrival_parking', 'rental_car_needed',           'RENTAL CAR NEEDED?',                       'yes_no',   90,  None, None, None, None, '', 'third', 0),
    ('arrival_parking', 'rental_drop_offs',            'RENTAL DROP-OFFS?',                        'yes_no',   100, None, None, None, None, '', 'third', 0),
    ('arrival_parking', 'runner_contact',              'RUNNER',                                   'contact_dropdown', 105, None, 'Runners', 'runner_needed=Yes', None, '', 'half', 0),
    ('arrival_parking', 'runner_time',                 'RUNNER PICKUP TIME',                       'text',     110, None, None, 'runner_needed=Yes', None, 'e.g. 2:00pm', 'half', 0),
    ('arrival_parking', 'runner_vehicle',              'RUNNER VEHICLE',                           'select',   120,
        json.dumps(['-', 'DPC Van', 'DPC Truck', 'Rental Vehicle', 'Other']),
        None, 'runner_needed=Yes', None, '', 'half', 0),
    ('arrival_parking', 'parking_validations',         'PARKING VALIDATIONS NEEDED?',              'yes_no',   130, None, None, None, None, '', 'half', 0),
    ('arrival_parking', 'parking_validations_count',   'HOW MANY?',                                'number',   140, None, None, 'parking_validations=Yes', None, '', 'half', 0),
    ('arrival_parking', 'special_accommodations',      'SPECIAL ACCOMMODATIONS NEEDED?',           'yes_no',   150, None, None, None, None, '', 'half', 0),
    ('arrival_parking', 'special_accommodations_details','DETAILS',                                'textarea', 160, None, None, 'special_accommodations=Yes', None, '', 'full', 0),
    ('arrival_parking', 'additional_space',            'ADDITIONAL HOLDING / REHEARSAL SPACE NEEDED?', 'yes_no', 170, None, None, None, None, '', 'full', 0),
    ('arrival_parking', 'additional_space_details',    'DETAILS',                                  'textarea', 180, None, None, 'additional_space=Yes', None, '', 'full', 0),
    ('arrival_parking', 'arrival_notes',               'ARRIVAL & PARKING NOTES',                  'textarea', 190, None, None, None, None, 'Additional arrival and parking notes...', 'full', 1),

    # ── Security ──────────────────────────────────────────────────────────────
    ('security', 'backstage_headcount',    'HOW MANY PEOPLE BACKSTAGE (Cast/Crew/Staff)?', 'number',   10, None, None, None, None, '0',          'half', 0),
    ('security', 'credentials_badges',    'CREDENTIALS / BADGES?',                         'select',   20,
        json.dumps(['-', 'Yes - Tour Provided', 'No - Use DPC Lanyards']),
        None, None, None, '', 'half', 0),
    ('security', 'extra_security',        'EXTRA SECURITY NEEDS?',                         'yes_no',   30, None, None, None, None, '',           'half', 0),
    ('security', 'extra_security_details','DETAILS',                                        'textarea', 40, None, None, 'extra_security=Yes', None, '', 'full', 0),
    ('security', 'security_meeting',      'SECURITY MEETING NEEDED?',                      'yes_no',   50, None, None, None, None, '',           'half', 0),
    ('security', 'security_meeting_time', 'SECURITY MEETING TIME',                         'text',     60, None, None, 'security_meeting=Yes', None, 'e.g. 5:30pm', 'half', 0),
    ('security', 'security_notes',        'SECURITY NOTES',                                'textarea', 70, None, None, None, None, 'Security notes...', 'full', 1),

    # ── Hospitality ───────────────────────────────────────────────────────────
    ('hospitality', 'food_beverage',         'SPECIFIC FOOD & BEVERAGE NEEDS?', 'yes_no',   10, None, None, None, None, '', 'half', 0),
    ('hospitality', 'food_beverage_details', 'DETAILS',                          'textarea', 20, None, None, 'food_beverage=Yes', None, '', 'full', 0),
    ('hospitality', 'allergies',             'ALLERGIES TO BE AWARE OF?',        'yes_no',   30, None, None, None, None, '', 'half', 0),
    ('hospitality', 'allergies_details',     'DETAILS',                          'textarea', 40, None, None, 'allergies=Yes', None, '', 'full', 0),
    ('hospitality', 'hospitality_notes',     'HOSPITALITY NOTES',                'textarea', 50, None, None, None, None, 'Hospitality notes...', 'full', 1),

    # ── Front of House ────────────────────────────────────────────────────────
    ('front_of_house', 'foh_contact',            'FOH CONTACT',              'text',     10, None, None, None, None, 'Name / contact info', 'half', 0),
    ('front_of_house', 'foh_activations',        'SPECIAL FOH ACTIVATIONS?', 'yes_no',   20, None, None, None, None, '',                    'half', 0),
    ('front_of_house', 'foh_activations_details','DETAILS',                  'textarea', 30, None, None, 'foh_activations=Yes', None, '', 'full', 0),
    ('front_of_house', 'foh_notes',              'FRONT-OF-HOUSE NOTES',     'textarea', 40, None, None, None, None, 'FOH notes...', 'full', 1),

    # ── Audio ─────────────────────────────────────────────────────────────────
    ('audio_section', 'audio_foh_engineer',   'FOH ENGINEER?',         'yes_no',   10, None, None, None, None, '',               'half', 0),
    ('audio_section', 'audio_microphones',    'MICROPHONES',           'select',   20,
        json.dumps(['-', 'Venue Provided', 'Tour Provided', 'N/A']),
        None, None, None, '', 'half', 0),
    ('audio_section', 'audio_mic_count',      'MIC COUNT',             'number',   30, None, None, None, None, '0',              'third', 0),
    ('audio_section', 'audio_mic_types',      'MIC TYPES',             'text',     40, None, None, None, None, 'e.g. SM58, DI',  'full',  0),
    ('audio_section', 'audio_monitors',       'MONITORS',              'select',   50,
        json.dumps(['-', 'Venue Provided', 'Tour Provided', 'In-Ears', 'N/A']),
        None, None, None, '', 'half', 0),
    ('audio_section', 'audio_inears',         'IN-EARS?',              'yes_no',   60, None, None, None, None, '',               'half', 0),
    ('audio_section', 'audio_playback',       'PLAYBACK?',             'yes_no',   70, None, None, None, None, '',               'half', 0),
    ('audio_section', 'audio_recording',      'RECORDING?',            'yes_no',   80, None, None, None, None, '',               'half', 0),
    ('audio_section', 'audio_notes',          'AUDIO NOTES',           'textarea', 90, None, None, None, None, 'Audio notes...', 'full', 1),

    # ── Video / Projection ───────────────────────────────────────────────────
    ('video_section', 'video_projector_needed', 'PROJECTOR / VIDEO NEEDED?', 'yes_no',   10, None, None, None, None, '', 'half', 0),
    ('video_section', 'video_notes',            'VIDEO NOTES',               'textarea', 20, None, None, None, None, 'Video/projection notes...', 'full', 1),

    # ── Backline ─────────────────────────────────────────────────────────────
    ('backline_section', 'backline_piano',         'PIANO?',                    'yes_no',   10, None, None, None, None, '', 'half', 0),
    ('backline_section', 'backline_piano_notes',   'PIANO DETAILS',             'text',     20, None, None, 'backline_piano=Yes', None, 'Type, tuning...', 'half', 0),
    ('backline_section', 'backline_tuning',        'PIANO TUNING NEEDED?',      'yes_no',   30, None, None, 'backline_piano=Yes', None, '', 'half', 0),
    ('backline_section', 'backline_tuning_time',   'TUNING TIME',               'text',     40, None, None, 'backline_tuning=Yes', None, 'e.g. 4:00pm', 'half', 0),
    ('backline_section', 'backline_own_gear',      'TOUR BRINGS OWN GEAR?',     'yes_no',   50, None, None, None, None, '', 'half', 0),
    ('backline_section', 'backline_own_gear_list', 'GEAR LIST',                 'textarea', 60, None, None, 'backline_own_gear=Yes', None, 'List tour gear...', 'full', 0),
    ('backline_section', 'backline_rental_needed', 'RENTAL GEAR NEEDED?',       'yes_no',   70, None, None, None, None, '', 'half', 0),
    ('backline_section', 'backline_notes',         'BACKLINE NOTES',            'textarea', 80, None, None, None, None, 'Backline notes...', 'full', 1),

    # ── Stage & Props ─────────────────────────────────────────────────────────
    ('stage_props', 'stage_plot',              'STAGE PLOT?',               'yes_no',   10, None, None, None, None, '', 'half', 0),
    ('stage_props', 'music_stands',            'MUSIC STANDS?',             'yes_no',   20, None, None, None, None, '', 'half', 0),
    ('stage_props', 'musician_chairs',         'MUSICIAN CHAIRS?',          'yes_no',   30, None, None, None, None, '', 'half', 0),
    ('stage_props', 'other_equipment',         'OTHER EQUIPMENT?',          'yes_no',   40, None, None, None, None, '', 'half', 0),
    ('stage_props', 'other_equipment_list',    'EQUIPMENT LIST',            'textarea', 50, None, None, 'other_equipment=Yes', None, 'List required equipment...', 'full', 0),
    ('stage_props', 'stage_notes',             'STAGE & PROPS NOTES',       'textarea', 60, None, None, None, None, 'Stage notes...', 'full', 1),

    # ── Wardrobe ─────────────────────────────────────────────────────────────
    ('wardrobe', 'wardrobe_dressing_room', 'DRESSING ROOM NEEDED?',     'yes_no',   10, None, None, None, None, '', 'third', 0),
    ('wardrobe', 'wardrobe_equipment',     'WARDROBE EQUIPMENT?',        'yes_no',   20, None, None, None, None, '', 'third', 0),
    ('wardrobe', 'wardrobe_towels',        'TOWELS NEEDED?',             'yes_no',   30, None, None, None, None, '', 'third', 0),
    ('wardrobe', 'wardrobe_notes',         'WARDROBE NOTES',             'textarea', 40, None, None, None, None, 'Wardrobe notes...', 'full', 1),

    # ── Special / Other Elements ──────────────────────────────────────────────
    ('special_elements', 'special_elements_desc', 'SPECIAL ELEMENTS',      'textarea', 10, None, None, None, None, 'Describe any special requirements, effects, or elements...', 'full', 0),
    ('special_elements', 'haze_fog_needed',        'HAZE / FOG NEEDED?',    'yes_no',   20, None, None, None, None, '', 'half', 0),
    ('special_elements', 'special_notes',          'SPECIAL NOTES',         'textarea', 30, None, None, None, None, 'Additional special notes...', 'full', 1),

    # ── Labor Needs ───────────────────────────────────────────────────────────
    ('labor_needs', 'labor_load_in',          'LOAD-IN LABOR?',            'yes_no',   10, None, None, None, None, '', 'third', 0),
    ('labor_needs', 'labor_show_call',        'SHOW CALL LABOR?',          'yes_no',   20, None, None, None, None, '', 'third', 0),
    ('labor_needs', 'labor_load_out',         'LOAD-OUT LABOR?',           'yes_no',   30, None, None, None, None, '', 'third', 0),
    ('labor_needs', 'labor_estimate_needed',  'ESTIMATE NEEDED?',          'yes_no',   40, None, None, None, None, '', 'half', 0),
    ('labor_needs', 'labor_notes',            'LABOR NOTES',               'textarea', 50, None, None, None, None, 'Labor requirements...', 'full', 1),

    # ── General Information ───────────────────────────────────────────────────
    ('general_info', 'load_in_needed',  'LOAD-IN TIME NEEDED?', 'yes_no',   10, None, None, None, None, '', 'full', 0),
    ('general_info', 'load_in_details', 'DETAILS',              'textarea', 20, None, None, 'load_in_needed=Yes', None, '', 'full', 0),
    ('general_info', 'general_notes',   'GENERAL NOTES',        'textarea', 30, None, None, None, None, 'General notes...', 'full', 1),
]

APP_SETTINGS_SEED = [
    # Server
    ('app_port',              '5400'),
    # Syslog
    ('syslog_enabled',        '0'),
    ('syslog_host',           '127.0.0.1'),
    ('syslog_port',           '514'),
    ('syslog_facility',       'LOG_LOCAL0'),
    # Venue / Channel lists (JSON arrays)
    ('venue_list',            json.dumps(["Judson's Live", "Walt Disney Theater", "Alexis & Jim Pugh Theater", "Dr. Phillips CenterStage"])),
    ('radio_channel_list',    json.dumps(["16/Judson's", "17/Walt Disney", "18/Alexis", "19/CenterStage"])),
    # WiFi defaults
    ('wifi_network',          ''),
    ('wifi_password',         ''),
    # Upload limit
    ('upload_max_mb',         '20'),
    # Logo (base64 encoded image data, empty = no logo)
    ('logo_data',             ''),
]


def _seed_form_data(conn):
    """Seed form_sections and form_fields if tables are empty."""
    count = conn.execute('SELECT COUNT(*) FROM form_sections').fetchone()[0]
    if count > 0:
        return

    section_id_map = {}
    for (section_key, label, sort_order, collapsible, icon) in FORM_SECTIONS_SEED:
        cur = conn.execute(
            """INSERT OR IGNORE INTO form_sections
               (section_key, label, sort_order, collapsible, icon)
               VALUES (?, ?, ?, ?, ?)""",
            (section_key, label, sort_order, collapsible, icon)
        )
        if cur.lastrowid:
            section_id_map[section_key] = cur.lastrowid
        else:
            row = conn.execute(
                'SELECT id FROM form_sections WHERE section_key=?', (section_key,)
            ).fetchone()
            section_id_map[section_key] = row[0]

    for row in FORM_FIELDS_SEED:
        (section_key, field_key, label, field_type, sort_order,
         options_json, contact_dept, conditional_show_when,
         help_text, placeholder, width_hint, is_notes_field) = row
        sid = section_id_map[section_key]
        conn.execute(
            """INSERT OR IGNORE INTO form_fields
               (section_id, field_key, label, field_type, sort_order,
                options_json, contact_dept, conditional_show_when,
                help_text, placeholder, width_hint, is_notes_field)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (sid, field_key, label, field_type, sort_order,
             options_json, contact_dept, conditional_show_when,
             help_text, placeholder, width_hint, is_notes_field)
        )

    print(f"  Seeded {len(FORM_SECTIONS_SEED)} sections and {len(FORM_FIELDS_SEED)} fields")


def _seed_app_settings(conn):
    """Seed app_settings with defaults if empty."""
    for (key, value) in APP_SETTINGS_SEED:
        conn.execute(
            'INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)',
            (key, value)
        )


def _migrate_form_data(conn):
    """Add any missing form sections and fields from the current seed data."""
    # Build a map of existing section keys
    existing_sections = {r[0]: r[1] for r in conn.execute(
        'SELECT section_key, id FROM form_sections'
    ).fetchall()}

    # Add missing sections
    for (section_key, label, sort_order, collapsible, icon) in FORM_SECTIONS_SEED:
        if section_key not in existing_sections:
            cur = conn.execute(
                """INSERT OR IGNORE INTO form_sections
                   (section_key, label, sort_order, collapsible, icon)
                   VALUES (?, ?, ?, ?, ?)""",
                (section_key, label, sort_order, collapsible, icon)
            )
            if cur.lastrowid:
                existing_sections[section_key] = cur.lastrowid

    # Rebuild section map after inserts
    existing_sections = {r[0]: r[1] for r in conn.execute(
        'SELECT section_key, id FROM form_sections'
    ).fetchall()}

    # Build set of existing field keys
    existing_fields = {r[0] for r in conn.execute(
        'SELECT field_key FROM form_fields'
    ).fetchall()}

    # Add missing fields
    for row in FORM_FIELDS_SEED:
        (section_key, field_key, label, field_type, sort_order,
         options_json, contact_dept, conditional_show_when,
         help_text, placeholder, width_hint, is_notes_field) = row
        if field_key in existing_fields:
            continue
        sid = existing_sections.get(section_key)
        if not sid:
            continue
        conn.execute(
            """INSERT OR IGNORE INTO form_fields
               (section_id, field_key, label, field_type, sort_order,
                options_json, contact_dept, conditional_show_when,
                help_text, placeholder, width_hint, is_notes_field)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (sid, field_key, label, field_type, sort_order,
             options_json, contact_dept, conditional_show_when,
             help_text, placeholder, width_hint, is_notes_field)
        )


def migrate_db():
    """Run safe migrations on an existing database."""
    conn = sqlite3.connect(DATABASE)
    conn.execute('PRAGMA foreign_keys = ON')

    # Create all new tables (IF NOT EXISTS = safe to rerun)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS form_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_key TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0,
            collapsible INTEGER DEFAULT 1,
            icon TEXT DEFAULT '◈'
        );

        CREATE TABLE IF NOT EXISTS form_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER NOT NULL REFERENCES form_sections(id) ON DELETE CASCADE,
            field_key TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL,
            field_type TEXT NOT NULL DEFAULT 'text',
            sort_order INTEGER DEFAULT 0,
            options_json TEXT DEFAULT NULL,
            contact_dept TEXT DEFAULT NULL,
            conditional_show_when TEXT DEFAULT NULL,
            help_text TEXT DEFAULT NULL,
            placeholder TEXT DEFAULT '',
            width_hint TEXT DEFAULT 'full',
            is_notes_field INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS form_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            form_type TEXT NOT NULL,
            saved_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
            saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            snapshot_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS user_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            group_type TEXT NOT NULL DEFAULT 'all_access',
            description TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS user_group_members (
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            group_id INTEGER NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
            PRIMARY KEY (user_id, group_id)
        );

        CREATE TABLE IF NOT EXISTS show_group_access (
            show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            group_id INTEGER NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
            PRIMARY KEY (show_id, group_id)
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT DEFAULT ''
        );

        -- Tracks who is currently viewing/editing a show (for presence indicators)
        -- Rows older than 60 s are considered stale and pruned automatically.
        CREATE TABLE IF NOT EXISTS active_sessions (
            user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            show_id       INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            tab           TEXT NOT NULL DEFAULT 'advance',
            focused_field TEXT,
            last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, show_id)
        );
    """)

    # New feature tables (safe to rerun)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS show_comments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id    INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            body       TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS show_attachments (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id     INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            uploaded_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
            filename    TEXT NOT NULL,
            mime_type   TEXT DEFAULT 'application/octet-stream',
            file_data   BLOB NOT NULL,
            file_size   INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS advance_reads (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id      INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
            user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            version_read INTEGER DEFAULT 0,
            read_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(show_id, user_id)
        );
    """)

    # ALTER TABLE for new columns (SQLite errors if column already exists)
    for alter_sql in [
        'ALTER TABLE shows ADD COLUMN last_saved_by INTEGER REFERENCES users(id)',
        'ALTER TABLE shows ADD COLUMN last_saved_at TIMESTAMP',
        "ALTER TABLE users ADD COLUMN theme TEXT DEFAULT 'dark'",
        'ALTER TABLE users ADD COLUMN last_login TIMESTAMP',
        'ALTER TABLE export_log ADD COLUMN pdf_data BLOB',
        "ALTER TABLE export_log ADD COLUMN filename TEXT DEFAULT ''",
    ]:
        try:
            conn.execute(alter_sql)
        except Exception:
            pass  # Column already exists

    # Seed form data and settings if empty
    _seed_form_data(conn)
    _seed_app_settings(conn)

    # Add missing form sections (safe to run even if some already exist)
    _migrate_form_data(conn)

    conn.commit()
    conn.close()
    print("✓ Migration complete")


def init_db(force=False):
    if os.path.exists(DATABASE) and not force:
        print(f"Database already exists at {DATABASE}")
        print("Use --force flag to reinitialize (WARNING: destroys all data)")
        print("Use --migrate flag to safely apply migrations to the existing DB")
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

    # Seed form data and settings
    _seed_form_data(conn)
    _seed_app_settings(conn)

    conn.commit()
    conn.close()

    print("✓ Database created:", DATABASE)
    print("✓ Admin account:   username=admin  password=admin123")
    print("✓ Contacts seeded:", len(SEED_CONTACTS), "contacts imported")
    print("✓ Form sections and fields seeded")
    print()
    print("⚠  Change the admin password after first login via Settings → Users")


if __name__ == '__main__':
    import sys
    if '--migrate' in sys.argv:
        if not os.path.exists(DATABASE):
            print("No database found. Run without --migrate to create a new one.")
            sys.exit(1)
        print(f"Running migrations on: {DATABASE}")
        migrate_db()
    else:
        force = '--force' in sys.argv
        init_db(force=force)
