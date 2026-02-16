#!/usr/bin/env python3
"""Check validation status counts."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='pending'").fetchone()[0]
skipped = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='skipped'").fetchone()[0]
validated = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='validated'").fetchone()[0]

print("Validation status counts:")
print(f"  Pending:   {pending}")
print(f"  Validated: {validated}")
print(f"  Skipped:   {skipped}")
print(f"  Total:     {pending + validated + skipped}")

conn.close()
