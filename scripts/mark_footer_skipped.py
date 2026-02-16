#!/usr/bin/env python3
"""Mark footer/disclaimer text as skipped so it won't appear in validation."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')

# Mark footer/disclaimer text as skipped
footer_patterns = [
    '%prior written consent%',
    '%analytical results%',
    '%R.L. =%',
    '%Reproduction of this%',
    '%prohibited without%'
]

updated = 0
for pattern in footer_patterns:
    result = conn.execute("""
        UPDATE lab_results 
        SET validation_status = 'skipped',
            validation_notes = 'Footer/disclaimer text - auto-skipped'
        WHERE validation_status = 'pending' 
        AND chemical_raw LIKE ?
    """, (pattern,))
    updated += result.rowcount

conn.commit()
conn.close()

print(f"Updated {updated} footer/disclaimer items to 'skipped'")
print("\nThese items will no longer appear during validation.")
