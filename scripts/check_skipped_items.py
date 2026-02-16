#!/usr/bin/env python3
"""Check what items are being skipped during validation."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

print("Items left as PENDING in VALIDATED submissions:\n")
print("(These are items the user chose to skip)\n")

skipped = conn.execute("""
    SELECT lr.chemical_raw, COUNT(*) as cnt 
    FROM lab_results lr
    JOIN lab_submissions ls ON lr.submission_id = ls.submission_id
    WHERE ls.validation_status = 'validated' 
    AND lr.validation_status = 'pending'
    GROUP BY lr.chemical_raw 
    ORDER BY cnt DESC 
    LIMIT 50
""").fetchall()

if skipped:
    for r in skipped:
        print(f"  {r[1]:2d}x: {r[0]}")
else:
    print("  (No skipped items found)")

print("\n" + "="*80)
print("All validated submissions:\n")

subs = conn.execute("""
    SELECT submission_id, original_filename,
           COUNT(*) as total_items,
           SUM(CASE WHEN validation_status='validated' THEN 1 ELSE 0 END) as validated,
           SUM(CASE WHEN validation_status='pending' THEN 1 ELSE 0 END) as skipped
    FROM lab_results lr
    JOIN lab_submissions ls ON lr.submission_id = ls.submission_id
    WHERE ls.validation_status = 'validated'
    GROUP BY lr.submission_id
    ORDER BY lr.submission_id
""").fetchall()

for s in subs:
    print(f"  Sub {s[0]}: {s[1]}")
    print(f"    Total: {s[2]}, Validated: {s[3]}, Skipped: {s[4]}")

conn.close()
