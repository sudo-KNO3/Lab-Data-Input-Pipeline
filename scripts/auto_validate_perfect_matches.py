#!/usr/bin/env python3
"""Auto-validate 100% confidence matches, prepare 95-99% for review."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

# Mark 100% confidence as validated
result = conn.execute("""
    UPDATE lab_results 
    SET validation_status = 'validated',
        correct_analyte_id = analyte_id,
        human_override = 0,
        validation_notes = 'Auto-validated (100% confidence)'
    WHERE match_confidence = 1.0 
    AND validation_status = 'pending'
""")
perfect_matches = result.rowcount

conn.commit()

# Count remaining for review (95-99%)
needs_review = conn.execute("""
    SELECT COUNT(*) 
    FROM lab_results 
    WHERE match_confidence >= 0.95 
    AND match_confidence < 1.0
    AND validation_status = 'pending'
""").fetchone()[0]

print(f"Auto-validated {perfect_matches} perfect matches (100% confidence)")
print(f"\nRemaining for review: {needs_review} items (95-99% confidence)")

if needs_review > 0:
    print("\nSample of items needing review:")
    samples = conn.execute("""
        SELECT chemical_raw, analyte_id, match_confidence, submission_id
        FROM lab_results
        WHERE match_confidence >= 0.95
        AND match_confidence < 1.0
        AND validation_status = 'pending'
        ORDER BY match_confidence DESC
        LIMIT 20
    """).fetchall()
    
    for s in samples:
        print(f"  Sub {s[3]}: {s[0]:40s} -> {s[1]:20s} ({s[2]:.1%})")

conn.close()
