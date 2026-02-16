#!/usr/bin/env python3
"""Show all submissions and their validation status."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

print("All submissions:\n")

subs = conn.execute("""
    SELECT submission_id, original_filename, lab_vendor, validation_status
    FROM lab_submissions
    ORDER BY submission_id
""").fetchall()

for s in subs:
    status_marker = "✓" if s[3] == 'validated' else "○"
    print(f"{status_marker} Sub {s[0]}: {s[1][:45]}")
    
    # Get counts
    counts = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN validation_status='validated' THEN 1 ELSE 0 END) as validated,
            SUM(CASE WHEN validation_status='pending' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN validation_status='skipped' THEN 1 ELSE 0 END) as skipped
        FROM lab_results
        WHERE submission_id = ?
    """, (s[0],)).fetchone()
    
    print(f"    Total: {counts[0]}, Validated: {counts[1]}, Pending: {counts[2]}, Skipped: {counts[3]}")

print("\n" + "="*80)
print("Next steps:")

# Find unvalidated submissions
unvalidated = conn.execute("""
    SELECT submission_id, original_filename
    FROM lab_submissions
    WHERE validation_status != 'validated'
    ORDER BY submission_id
""").fetchall()

if unvalidated:
    print("\nValidate these submissions:")
    for u in unvalidated:
        print(f"  python scripts/21_validate_interactive.py --submission-id {u[0]} --auto-accept-confident")
else:
    print("\nAll submissions validated! ✓")

conn.close()
