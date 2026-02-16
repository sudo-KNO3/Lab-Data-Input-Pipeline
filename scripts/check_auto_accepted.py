#!/usr/bin/env python3
"""Check auto-accepted items that need validation."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

print("Auto-accepted items analysis:\n")

# Count high-confidence pending items
high_conf_pending = conn.execute("""
    SELECT COUNT(*) 
    FROM lab_results 
    WHERE match_confidence >= 0.95 
    AND validation_status = 'pending'
""").fetchone()[0]

print(f"High-confidence (>=95%) items still pending: {high_conf_pending}")

# Sample of these items
if high_conf_pending > 0:
    print(f"\nSample of auto-accepted items that weren't validated:")
    samples = conn.execute("""
        SELECT chemical_raw, analyte_id, match_confidence
        FROM lab_results
        WHERE match_confidence >= 0.95
        AND validation_status = 'pending'
        LIMIT 20
    """).fetchall()
    
    for s in samples:
        print(f"  {s[0]:40s} -> {s[1]:20s} ({s[2]:.1%})")

print("\n" + "="*80)
print("Options:")
print("1. Mark all high-confidence items as validated (trust the matching)")
print("2. Review them manually to verify accuracy")

conn.close()
