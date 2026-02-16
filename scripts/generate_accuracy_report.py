#!/usr/bin/env python3
"""Generate comprehensive accuracy report."""

import sqlite3
from datetime import datetime

conn = sqlite3.connect('data/lab_results.db')
conn2 = sqlite3.connect('data/reg153_matcher.db')

print("=" * 100)
print("CHEMICAL MATCHER - ACCURACY REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)

# Overall statistics
print("\n" + "=" * 100)
print("OVERALL STATISTICS")
print("=" * 100)

total_items = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
validated = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='validated'").fetchone()[0]
skipped = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='skipped'").fetchone()[0]
pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status='pending'").fetchone()[0]

print(f"\nTotal items extracted:        {total_items:>6}")
print(f"Successfully validated:       {validated:>6}  ({validated/total_items*100:>5.1f}%)")
print(f"Skipped (headers/junk):       {skipped:>6}  ({skipped/total_items*100:>5.1f}%)")
print(f"Pending validation:           {pending:>6}  ({pending/total_items*100:>5.1f}%)")

# Confidence breakdown for validated items
print("\n" + "=" * 100)
print("CONFIDENCE DISTRIBUTION (Validated Items)")
print("=" * 100)

conf_dist = conn.execute("""
    SELECT 
        CASE 
            WHEN match_confidence = 1.0 THEN '100%'
            WHEN match_confidence >= 0.99 THEN '99%'
            WHEN match_confidence >= 0.98 THEN '98%'
            WHEN match_confidence >= 0.97 THEN '97%'
            WHEN match_confidence >= 0.95 THEN '95-96%'
            WHEN match_confidence >= 0.90 THEN '90-94%'
            ELSE 'Below 90%'
        END as conf_range,
        COUNT(*) as count
    FROM lab_results
    WHERE validation_status = 'validated'
    GROUP BY conf_range
    ORDER BY MIN(match_confidence) DESC
""").fetchall()

for c in conf_dist:
    pct = c[1] / validated * 100 if validated > 0 else 0
    bar = '█' * int(pct / 2)
    print(f"  {c[0]:12s}: {c[1]:>4} items  {pct:>5.1f}%  {bar}")

# Per-submission accuracy
print("\n" + "=" * 100)
print("PER-FILE ACCURACY TRENDS")
print("=" * 100)

submissions = conn.execute("""
    SELECT 
        ls.submission_id,
        ls.original_filename,
        ls.created_at,
        COUNT(lr.result_id) as total_items,
        SUM(CASE WHEN lr.validation_status='validated' THEN 1 ELSE 0 END) as validated_items,
        SUM(CASE WHEN lr.validation_status='skipped' THEN 1 ELSE 0 END) as skipped_items,
        SUM(CASE WHEN lr.match_confidence >= 0.95 THEN 1 ELSE 0 END) as high_conf_items,
        SUM(CASE WHEN lr.correct_analyte_id IS NOT NULL AND lr.correct_analyte_id != lr.analyte_id THEN 1 ELSE 0 END) as corrected_items,
        AVG(CASE WHEN lr.validation_status='validated' THEN lr.match_confidence END) as avg_confidence
    FROM lab_submissions ls
    LEFT JOIN lab_results lr ON ls.submission_id = lr.submission_id
    GROUP BY ls.submission_id
    ORDER BY ls.submission_id
""").fetchall()

print(f"\n{'ID':<4} {'File':<50} {'Total':>6} {'Valid':>6} {'Skip':>5} {'Acc%':>6} {'AvgConf':>8} {'Corrections':>12}")
print("-" * 100)

for s in submissions:
    sub_id, filename, created, total, validated, skipped, high_conf, corrected, avg_conf = s
    accuracy = (validated / total * 100) if total > 0 else 0
    avg_conf_pct = (avg_conf * 100) if avg_conf else 0
    
    print(f"{sub_id:<4} {filename[:48]:<50} {total:>6} {validated:>6} {skipped:>5} {accuracy:>5.1f}% {avg_conf_pct:>7.1f}% {corrected:>12}")

# Learning progress
print("\n" + "=" * 100)
print("LEARNING PROGRESS")
print("=" * 100)

print("\nAccuracy improvement over time:")
first_3 = submissions[:3]
last_3 = submissions[-3:]

if len(first_3) >= 3:
    avg_early = sum((s[4] / s[3] * 100) if s[3] > 0 else 0 for s in first_3) / 3
    print(f"  First 3 files average:  {avg_early:.1f}%")

if len(last_3) >= 3:
    avg_late = sum((s[4] / s[3] * 100) if s[3] > 0 else 0 for s in last_3) / 3
    print(f"  Last 3 files average:   {avg_late:.1f}%")
    
    if len(first_3) >= 3:
        improvement = avg_late - avg_early
        print(f"  Improvement:            +{improvement:.1f}%")

# Database stats
print("\n" + "=" * 100)
print("KNOWLEDGE BASE STATISTICS")
print("=" * 100)

total_synonyms = conn2.execute("SELECT COUNT(*) FROM synonyms").fetchone()[0]
total_analytes = conn2.execute("SELECT COUNT(*) FROM analytes").fetchone()[0]

print(f"\nTotal analytes in database:     {total_analytes:>6}")
print(f"Total synonyms in database:     {total_synonyms:>6,}")

syn_sources = conn2.execute("""
    SELECT harvest_source, COUNT(*) as count
    FROM synonyms
    GROUP BY harvest_source
    ORDER BY count DESC
""").fetchall()

print("\nSynonyms by source:")
for src in syn_sources:
    pct = src[1] / total_synonyms * 100 if total_synonyms > 0 else 0
    print(f"  {src[0]:30s}: {src[1]:>8,}  ({pct:>5.1f}%)")

# Most frequently validated chemicals
print("\n" + "=" * 100)
print("TOP 20 MOST FREQUENT CHEMICALS")
print("=" * 100)

top_chems = conn.execute("""
    SELECT 
        lr.analyte_id,
        COUNT(*) as frequency
    FROM lab_results lr
    WHERE lr.validation_status = 'validated'
    AND lr.analyte_id IS NOT NULL
    GROUP BY lr.analyte_id
    ORDER BY frequency DESC
    LIMIT 20
""").fetchall()

for chem in top_chems:
    # Get preferred name from matcher db
    analyte = conn2.execute(
        "SELECT preferred_name FROM analytes WHERE analyte_id = ?", 
        (chem[0],)
    ).fetchone()
    name = analyte[0] if analyte else "Unknown"
    print(f"  {chem[0]:20s}  {name:40s}  {chem[1]:>3}x")

# Summary and recommendations
print("\n" + "=" * 100)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 100)

total_subs = len(submissions)
validated_subs = sum(1 for s in submissions if (s[3] > 0 and s[4] / s[3] >= 0.95))
avg_accuracy = sum((s[4] / s[3] * 100) if s[3] > 0 else 0 for s in submissions) / total_subs if total_subs > 0 else 0

print(f"\n✓ Processed {total_subs} files")
print(f"✓ Average accuracy: {avg_accuracy:.1f}%")
print(f"✓ {validated_subs}/{total_subs} files with ≥95% accuracy")

if avg_accuracy >= 98:
    print("\n🎉 EXCELLENT: System is performing at target accuracy!")
    print("   → Continue processing remaining files")
    print("   → System is ready for production use")
elif avg_accuracy >= 95:
    print("\n✓ GOOD: System is performing well")
    print("   → Process 10-20 more files to improve further")
    print("   → Focus on correcting low-confidence matches")
elif avg_accuracy >= 90:
    print("\n⚠ MODERATE: System needs more training")
    print("   → Validate more files to teach the system")
    print("   → Add missing analytes/synonyms")
else:
    print("\n⚠ LOW: System needs significant training")
    print("   → Check for missing analytes in database")
    print("   → Review matching algorithm parameters")

print("\n" + "=" * 100)

conn.close()
conn2.close()
