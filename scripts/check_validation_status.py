#!/usr/bin/env python3
"""Quick script to check validation status."""

import sqlite3
import sys

def main():
    submission_id = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    
    conn = sqlite3.connect('data/lab_results.db')
    
    # Get submission info
    sub = conn.execute('''
        SELECT submission_id, original_filename, lab_vendor, 
               extraction_accuracy, validation_status
        FROM lab_submissions 
        WHERE submission_id = ?
    ''', (submission_id,)).fetchone()
    
    if not sub:
        print(f"Submission {submission_id} not found")
        return
    
    print(f"\n{'='*80}")
    print(f"SUBMISSION {sub[0]} STATUS")
    print(f"{'='*80}")
    print(f"File: {sub[1]}")
    print(f"Vendor: {sub[2]}")
    print(f"Accuracy: {sub[3]:.1f}%" if sub[3] else "Accuracy: Not calculated")
    print(f"Status: {sub[4]}")
    
    # Get chemical counts
    results = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN correct_analyte_id IS NOT NULL THEN 1 ELSE 0 END) as validated,
            SUM(CASE WHEN correct_analyte_id IS NOT NULL 
                     AND correct_analyte_id != analyte_id THEN 1 ELSE 0 END) as corrected
        FROM lab_results
        WHERE submission_id = ?
    ''', (submission_id,)).fetchone()
    
    print(f"\nChemicals:")
    print(f"  Total extracted: {results[0]}")
    print(f"  Validated: {results[1]}")
    print(f"  Corrected: {results[2]}")
    print(f"  Skipped: {results[0] - results[1]}")
    
    # Get new synonyms learned
    conn2 = sqlite3.connect('data/reg153_matcher.db')
    new_synonyms = conn2.execute('''
        SELECT COUNT(*) FROM synonyms WHERE harvest_source = 'user_validated'
    ''').fetchone()[0]
    conn2.close()
    
    print(f"\nLearning:")
    print(f"  New synonyms learned: {new_synonyms}")
    
    conn.close()

if __name__ == '__main__':
    main()
