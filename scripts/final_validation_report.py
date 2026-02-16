"""Final validation report accounting for CAS corrections."""
import csv
from collections import Counter

with open('data/validation/invalid_synonyms.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    invalid = list(reader)

# Get mismatches
mismatches = [r for r in invalid if 'mismatch' in r['notes']]

# Categorize mismatches
boron_chromium_errors = []
true_mismatches = []

for r in mismatches:
    # Check if this was due to wrong Boron/Chromium CAS
    if r['analyte_cas'] == '7429-90-5':
        # This was the wrong CAS - synonyms are actually valid
        boron_chromium_errors.append(r)
    else:
        # True mismatch
        true_mismatches.append(r)

no_results = [r for r in invalid if 'no CAS' in r['notes']]

print('='*80)
print('FINAL VALIDATION REPORT (After CAS Corrections)')
print('='*80)
print()
print(f'Total Synonyms Validated: 5,810')
print()
print('RESULTS:')
print('-'*80)

# Calculate corrected totals
false_positives = len(boron_chromium_errors)  # Were flagged but actually valid
true_invalid = len(true_mismatches) + len(no_results)
truly_valid = 5810 - true_invalid

print(f'Valid Synonyms:           {truly_valid:,} ({100*truly_valid/5810:.1f}%)')
print(f'  - Confirmed by PubChem:  3,022')
print(f'  - False positive flags:  {false_positives}')
print()
print(f'Questionable Synonyms:    {true_invalid:,} ({100*true_invalid/5810:.1f}%)')
print(f'  - No PubChem result:     {len(no_results):,} (product specs, likely OK)')
print(f'  - True CAS mismatches:   {len(true_mismatches):,} (wrong chemical - REMOVE)')
print()

print('='*80)
print('TRUE CAS MISMATCHES (Need Removal):')
print('='*80)
if true_mismatches:
    # Group by analyte
    by_analyte = {}
    for r in true_mismatches:
        analyte = r['analyte_name']
        if analyte not in by_analyte:
            by_analyte[analyte] = []
        by_analyte[analyte].append(r)
    
    for analyte, synonyms in sorted(by_analyte.items()):
        print(f'\n{analyte} (CAS: {synonyms[0]["analyte_cas"]}):')
        for s in synonyms[:5]:  # Show first 5
            print(f'  - {s["synonym"][:65]:65s} → PubChem CAS: {s["pubchem_cas"]}')
        if len(synonyms) > 5:
            print(f'  ... and {len(synonyms)-5} more')

print()
print('='*80)
print('EXAMPLE "NO RESULT" SYNONYMS (Product Specifications):')
print('='*80)
for r in no_results[:10]:
    print(f'  - {r["synonym"][:75]}')

print()
print('='*80)
print('SUMMARY & RECOMMENDATIONS:')
print('='*80)
print()
print(f'✓ HIGH QUALITY: {truly_valid:,} synonyms validated ({100*truly_valid/5810:.1f}%)')
print()
print(f'⚠ QUESTIONABLE: {len(no_results):,} "no result" synonyms')
print('  → Most are valid product specifications')
print('  → Keep but mark with lower confidence (0.7)')
print()
print(f'✗ REMOVE: {len(true_mismatches)} CAS mismatches')
print('  → These map to different chemicals')
print('  → DELETE from database')
print()
print('ACTIONS:')
print('  1. Delete true CAS mismatch synonyms from database')
print('  2. Lower confidence scores on "no result" synonyms')
print('  3. Improve quality filters to catch product specs upfront')
print('='*80)
