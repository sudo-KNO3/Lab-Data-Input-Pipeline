#!/usr/bin/env python3
"""Show skipped chemicals from a submission."""

import sqlite3
import sys

submission_id = int(sys.argv[1]) if len(sys.argv) > 1 else 12

conn = sqlite3.connect('data/lab_results.db')

# Get file info
file_info = conn.execute(
    "SELECT original_filename, lab_vendor FROM lab_submissions WHERE submission_id = ?",
    (submission_id,)
).fetchone()

if not file_info:
    print(f"Submission {submission_id} not found")
    sys.exit(1)

filename, vendor = file_info

print("=" * 80)
print(f"SKIPPED ITEMS FROM SUBMISSION {submission_id}")
print("=" * 80)
print(f"File: {filename}")
print(f"Vendor: {vendor}")
print()

# Get skipped items
results = conn.execute("""
    SELECT result_id, chemical_raw, validation_notes
    FROM lab_results
    WHERE submission_id = ?
    AND validation_status = 'skipped'
    ORDER BY result_id
""", (submission_id,)).fetchall()

if not results:
    print("No skipped items found.")
else:
    print(f"Total skipped: {len(results)}\n")
    for i, (result_id, chem_raw, notes) in enumerate(results, 1):
        print(f"  {i:2d}. [{result_id:4d}] {chem_raw}")
        if notes:
            print(f"      Note: {notes}")

conn.close()
