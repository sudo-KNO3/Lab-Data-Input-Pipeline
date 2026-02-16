#!/usr/bin/env python3
"""Cross-file validation report for Caduceon lab files."""

import sqlite3
from collections import defaultdict

conn = sqlite3.connect('data/lab_results.db')
conn2 = sqlite3.connect('data/reg153_matcher.db')

print("=" * 100)
print("CADUCEON LAB REPORTS - CROSS-FILE VALIDATION")
print("=" * 100)

# Get all Caduceon submissions
submissions = conn.execute("""
    SELECT submission_id, original_filename
    FROM lab_submissions
    WHERE lab_vendor = 'Caduceon'
    ORDER BY submission_id
""").fetchall()

print(f"\nProcessed {len(submissions)} Caduceon files:")
for s in submissions:
    print(f"  {s[0]}. {s[1]}")

# Get all validated chemicals across all files
print("\n" + "=" * 100)
print("CHEMICAL CONSISTENCY CHECK")
print("=" * 100)

# Find chemicals that appear in multiple files
chemical_appearances = defaultdict(list)

for sub_id, filename in submissions:
    chemicals = conn.execute("""
        SELECT chemical_raw, analyte_id, match_confidence
        FROM lab_results
        WHERE submission_id = ?
        AND validation_status = 'validated'
        ORDER BY chemical_raw
    """, (sub_id,)).fetchall()
    
    for chem_raw, analyte_id, conf in chemicals:
        chemical_appearances[chem_raw].append({
            'submission': sub_id,
            'filename': filename,
            'analyte_id': analyte_id,
            'confidence': conf
        })

# Find chemicals with inconsistent matching
print("\nChecking for matching inconsistencies...")
inconsistent = []
for chem_raw, appearances in chemical_appearances.items():
    if len(appearances) > 1:
        # Check if all matched to same analyte_id
        analyte_ids = set(a['analyte_id'] for a in appearances)
        if len(analyte_ids) > 1:
            inconsistent.append((chem_raw, appearances))

if inconsistent:
    print(f"\n⚠ Found {len(inconsistent)} chemicals with inconsistent matching:\n")
    for chem_raw, appearances in inconsistent[:10]:  # Show first 10
        print(f"  '{chem_raw}' matched to:")
        for app in appearances:
            analyte_id = app.get('analyte_id', 'Unknown')
            if analyte_id is None:
                analyte_id = 'None'
            analyte_name = conn2.execute(
                "SELECT preferred_name FROM analytes WHERE analyte_id = ?",
                (analyte_id,)
            ).fetchone()
            name = analyte_name[0] if analyte_name else "Unknown"
            conf_val = app.get('confidence')
            conf_str = f"{conf_val:.0%}" if conf_val is not None else "N/A"
            sub_id = app.get('submission', '?')
            print(f"    - {str(analyte_id):20s} ({str(name):30s}) in file {sub_id} ({conf_str})")
        print()
else:
    print("\n✓ All chemicals matched consistently across files!")

# Chemicals appearing in all files
print("\n" + "=" * 100)
print("MOST COMMON CHEMICALS (appearing in multiple files)")
print("=" * 100)

common_chems = [(chem, apps) for chem, apps in chemical_appearances.items() if len(apps) >= 5]
common_chems.sort(key=lambda x: len(x[1]), reverse=True)

print(f"\nChemicals appearing in 5+ files:\n")
for chem_raw, appearances in common_chems[:20]:
    analyte_id = appearances[0]['analyte_id']
    analyte_name = conn2.execute(
        "SELECT preferred_name FROM analytes WHERE analyte_id = ?",
        (analyte_id,)
    ).fetchone()
    name = analyte_name[0] if analyte_name else "Unknown"
    confs = [a['confidence'] for a in appearances if a['confidence'] is not None]
    avg_conf = sum(confs) / len(confs) if confs else 0.0
    print(f"  {chem_raw:40s} -> {name:30s} ({len(appearances)}/{len(submissions)} files, {avg_conf:.0%} avg)")

# Unique chemicals per file
print("\n" + "=" * 100)
print("UNIQUE CHEMICALS PER FILE")
print("=" * 100)

print("\nChemicals appearing in only one file:\n")

unique_per_file = defaultdict(list)
for chem_raw, appearances in chemical_appearances.items():
    if len(appearances) == 1:
        unique_per_file[appearances[0]['submission']].append((chem_raw, appearances[0]))

for sub_id, filename in submissions:
    unique = unique_per_file.get(sub_id, [])
    print(f"  File {sub_id}: {len(unique)} unique chemical(s)")
    for chem_raw, app in unique[:5]:  # Show first 5
        analyte_name = conn2.execute(
            "SELECT preferred_name FROM analytes WHERE analyte_id = ?",
            (app['analyte_id'],)
        ).fetchone()
        name = analyte_name[0] if analyte_name else "Unknown"
        print(f"    - {chem_raw:40s} -> {name}")

# Validation quality metrics
print("\n" + "=" * 100)
print("VALIDATION QUALITY METRICS")
print("=" * 100)

print("\nPer-file validation completeness:\n")

for sub_id, filename in submissions:
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN validation_status='validated' THEN 1 ELSE 0 END) as validated,
            SUM(CASE WHEN match_confidence >= 0.95 THEN 1 ELSE 0 END) as high_conf,
            AVG(CASE WHEN validation_status='validated' THEN match_confidence END) as avg_conf
        FROM lab_results
        WHERE submission_id = ?
    """, (sub_id,)).fetchone()
    
    total, validated, high_conf, avg_conf = stats
    val_pct = (validated / total * 100) if total > 0 else 0
    high_pct = (high_conf / total * 100) if total > 0 else 0
    
    print(f"  {sub_id}. {filename[:50]:<52}")
    print(f"     Validated: {validated}/{total} ({val_pct:.1f}%), High-conf: {high_conf} ({high_pct:.1f}%), Avg: {avg_conf:.1%}")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)

total_chems = len(chemical_appearances)
consistent_chems = total_chems - len(inconsistent)
consistency_rate = (consistent_chems / total_chems * 100) if total_chems > 0 else 0

print(f"\n✓ Total unique chemical names: {total_chems}")
print(f"✓ Consistently matched: {consistent_chems} ({consistency_rate:.1f}%)")
if inconsistent:
    print(f"⚠ Inconsistent matches: {len(inconsistent)} ({100-consistency_rate:.1f}%)")
print(f"✓ Chemicals in 5+ files: {len(common_chems)}")

if consistency_rate >= 99:
    print("\n🎉 EXCELLENT: Nearly perfect consistency across all files!")
elif consistency_rate >= 95:
    print("\n✓ GOOD: High consistency across files")
else:
    print("\n⚠ REVIEW: Some inconsistencies found - review recommended")

print("\n" + "=" * 100)

conn.close()
conn2.close()
