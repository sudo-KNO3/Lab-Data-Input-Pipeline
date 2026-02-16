#!/usr/bin/env python3
"""Generate final validation summary report."""

import sqlite3

conn = sqlite3.connect('data/lab_results.db')
conn2 = sqlite3.connect('data/reg153_matcher.db')

print("="*80)
print("VALIDATION COMPLETE - SUMMARY REPORT")
print("="*80)

# Overall stats
total = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
validated = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='validated'").fetchone()[0]
skipped = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='skipped'").fetchone()[0]
corrected = conn.execute("SELECT COUNT(*) FROM lab_results WHERE correct_analyte_id IS NOT NULL AND correct_analyte_id != analyte_id").fetchone()[0]

print(f"\nOverall Statistics:")
print(f"  Total items extracted:        {total}")
print(f"  Successfully validated:       {validated} ({validated/total*100:.1f}%)")
print(f"  Skipped (headers/junk):       {skipped} ({skipped/total*100:.1f}%)")
print(f"  Manually corrected:           {corrected}")

# Synonyms learned
user_syns = conn2.execute("SELECT COUNT(*) FROM synonyms WHERE harvest_source='user_validated'").fetchone()[0]
print(f"\nLearning:")
print(f"  User-validated synonyms added: {user_syns}")

# Per-file breakdown
print(f"\nPer-File Results:")
subs = conn.execute("""
    SELECT 
        ls.submission_id,
        ls.original_filename,
        COUNT(lr.result_id) as total,
        SUM(CASE WHEN lr.validation_status='validated' THEN 1 ELSE 0 END) as validated,
        SUM(CASE WHEN lr.validation_status='skipped' THEN 1 ELSE 0 END) as skipped,
        SUM(CASE WHEN lr.correct_analyte_id IS NOT NULL AND lr.correct_analyte_id != lr.analyte_id THEN 1 ELSE 0 END) as corrected
    FROM lab_submissions ls
    JOIN lab_results lr ON ls.submission_id = lr.submission_id
    WHERE ls.validation_status = 'validated'
    GROUP BY ls.submission_id
    ORDER BY ls.submission_id
""").fetchall()

for s in subs:
    accuracy = (s[3] / s[2] * 100) if s[2] > 0 else 0
    print(f"  {s[0]}. {s[1][:50]}")
    print(f"     Total: {s[2]}, Validated: {s[3]} ({accuracy:.1f}%), Skipped: {s[4]}, Corrected: {s[5]}")

print("\n" + "="*80)
print("NEXT STEPS:")
print("="*80)
print("\n1. Check synonym learning:")
print("   python scripts/check_user_validated_synonyms.py")
print("\n2. Process more files to continue learning:")
print("   Get-ChildItem \"Excel Lab examples\" -Filter *Caduceon*.xlsx | Select-Object -Skip 8 -First 5 | ForEach-Object {")
print("       python scripts/20_ingest_lab_file.py --input $_.FullName --vendor Caduceon")
print("   }")
print("\n3. Generate accuracy report:")
print("   python scripts/generate_accuracy_report.py")

conn.close()
conn2.close()
