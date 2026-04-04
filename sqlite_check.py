"""Quick check: what's in the SQLite source database."""
import sqlite3, os

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'advance.db')
conn = sqlite3.connect(DATABASE)

print(f"SQLite: {DATABASE}\n")

tables = [
    'users', 'shows', 'contacts', 'asset_categories', 'asset_types',
    'asset_items', 'advance_data', 'form_fields', 'form_sections',
    'schedule_rows', 'user_groups', 'user_group_members', 'app_settings',
    'job_positions', 'position_categories', 'labor_requests',
    'crew_members', 'crew_qualifications', 'show_comments',
    'export_log', 'audit_log', 'schedule_templates',
]

for t in sorted(tables):
    try:
        count = conn.execute(f'SELECT count(*) FROM {t}').fetchone()[0]
        if count > 0:
            print(f"  {t:40s} {count:>6d} rows")
    except Exception as e:
        print(f"  {t:40s} (not found)")

conn.close()
