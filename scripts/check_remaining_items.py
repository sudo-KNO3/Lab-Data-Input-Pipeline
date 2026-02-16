#!/usr/bin/env python3
"""Check remaining items to validate."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

print("Submissions with 95-99% confidence items still pending:\n")

results = conn.execute("""
    SELECT submission_id, COUNT(*) as cnt 
    FROM lab_results 
    WHERE match_confidence >= 0.95 
    AND match_confidence < 1.0 
    AND validation_status = 'pending'
    GROUP BY submission_id 
    ORDER BY submission_id
""").fetchall()

total = 0
for r in results:
    print(f"  Sub {r[0]}: {r[1]} items")
    total += r[1]

if total > 0:
    print(f"\nTotal remaining: {total}")
    print("\nNext steps:")
    for r in results:
        print(f"  python scripts/21_validate_interactive.py --submission-id {r[0]}")
else:
    print("\nAll items validated! ✓")

conn.close()
