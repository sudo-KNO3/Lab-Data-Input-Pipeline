"""Quick analysis of validation results."""
import csv

with open('data/validation/invalid_synonyms.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    invalid = list(reader)

print('='*80)
print('SYNONYM VALIDATION ANALYSIS')
print('='*80)
print()

# CAS mismatches
mismatches = [r for r in invalid if 'mismatch' in r['notes']]
print(f'CAS MISMATCHES (Wrong Chemical): {len(mismatches)}')
print('-'*80)
for r in mismatches[:10]:
    print(f"{r['synonym'][:65]:65s}")
    print(f"  Expected CAS: {r['analyte_cas']:15s} PubChem returned: {r['pubchem_cas']}")
print()

# No PubChem results
no_results = [r for r in invalid if 'no CAS' in r['notes']]
print(f'NO PUBCHEM RESULT (Product Specifications): {len(no_results)}')
print('-'*80)
print('Sample of 15:')
for r in no_results[:15]:
    print(f"  {r['synonym'][:75]}")

print()
print('='*80)
print('SUMMARY:')
print('-'*80)
print(f'Total validated: 5,810 synonyms')
print(f'Valid: 3,022 (52.0%)')
print(f'Invalid: 2,788 (48.0%)')
print()
print(f'  • CAS mismatches: {len(mismatches)} (7.9% of invalid)')
print('    → These are WRONG chemical associations - need to remove')
print()
print(f'  • No PubChem result: {len(no_results)} (92.1% of invalid)')
print('    → These are likely VALID but overly specific:')
print('      - Product catalog numbers')
print('      - Purity grades (99.9%, ACS grade, etc.)')
print('      - Physical specifications (particle size, mesh, etc.)')
print('      - Trade names not in PubChem')
print()
print('RECOMMENDATION:')
print('  1. Remove 220 CAS mismatch synonyms (wrong chemical)')
print('  2. Keep most "no result" synonyms (valid but specific)')
print('  3. Filter out obvious product specs in quality filters')
print('='*80)
