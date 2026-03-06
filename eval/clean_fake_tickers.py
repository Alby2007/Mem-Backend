"""Remove fake/test ticker atoms from the local KB so no_data queries work correctly."""
import sqlite3, os

FAKE_SUBJECTS_EXACT = [
    'notreal99', 'fakeco', 'madeupticker', 'xyz', 'randomticker123',
    'xyz corp', 'xyzco', 'fakecorp', 'testco', 'badticker',
]
FAKE_SUBJECTS_LIKE = [
    '%notreal%', '%madeup%', '%fakeco%', '%randomticker%',
    '%fakecorp%', '%testco%', '%badticker%',
]

db = 'trading_knowledge.db'
if not os.path.exists(db):
    print('DB not found:', db)
    exit(1)

conn = sqlite3.connect(db)
c = conn.cursor()

total_deleted = 0
for subj in FAKE_SUBJECTS_EXACT:
    c.execute("SELECT COUNT(*) FROM facts WHERE LOWER(subject) = ?", (subj,))
    count = c.fetchone()[0]
    if count:
        c.execute("DELETE FROM facts WHERE LOWER(subject) = ?", (subj,))
        print(f'Deleted {count} atoms for subject (exact): {subj}')
        total_deleted += count

for pattern in FAKE_SUBJECTS_LIKE:
    c.execute("SELECT COUNT(*) FROM facts WHERE LOWER(subject) LIKE ?", (pattern,))
    count = c.fetchone()[0]
    if count:
        c.execute("DELETE FROM facts WHERE LOWER(subject) LIKE ?", (pattern,))
        print(f'Deleted {count} atoms for subject LIKE: {pattern}')
        total_deleted += count

# Also delete atoms where fake names appear in the object column
FAKE_OBJECT_LIKE = [
    '%notreal99%', '%madeupticker%', '%fakeco%', '%randomticker123%',
]
for pattern in FAKE_OBJECT_LIKE:
    c.execute("SELECT COUNT(*) FROM facts WHERE LOWER(object) LIKE ?", (pattern,))
    count = c.fetchone()[0]
    if count:
        c.execute("DELETE FROM facts WHERE LOWER(object) LIKE ?", (pattern,))
        print(f'Deleted {count} atoms for object LIKE: {pattern}')
        total_deleted += count

# Delete atoms whose subject is a section header / garbage string (starts with # or --)
c.execute("SELECT COUNT(*) FROM facts WHERE subject LIKE '#%' OR subject LIKE '--%' OR subject LIKE '$%'")
count = c.fetchone()[0]
if count:
    print(f'[skip] {count} atoms with header-like subjects (not deleting \u2014 may be legitimate)')

conn.commit()
conn.close()
print(f'\nTotal deleted: {total_deleted}')
