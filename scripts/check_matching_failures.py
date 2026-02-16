#!/usr/bin/env python3
"""Check why common parameters aren't matching."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

print("Actual text from lab files that users are skipping:\n")

# Get the actual raw text and matching results
results = conn.execute("""
    SELECT lr.chemical_raw, lr.chemical_normalized, lr.analyte_id, lr.match_confidence
    FROM lab_results lr
    JOIN lab_submissions ls ON lr.submission_id = ls.submission_id
    WHERE ls.validation_status = 'validated' 
    AND lr.validation_status = 'pending'
    ORDER BY lr.chemical_raw
    LIMIT 50
""").fetchall()

for r in results:
    conf_str = f"{r[3]:.1%}" if r[3] else "No match"
    print(f"{r[0]:45s} | {r[2] if r[2] else 'None':20s} | {conf_str}")

conn.close()
