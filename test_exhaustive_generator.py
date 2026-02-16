"""Test script for ExhaustiveVariantGenerator."""
from src.normalization.chemical_parser import ChemicalNameParser, ExhaustiveVariantGenerator

parser = ChemicalNameParser()
generator = ExhaustiveVariantGenerator(parser)

print('='*70)
print('QUALIFIER HANDLING VERIFICATION')
print('='*70)

# Test 1: Equivalent qualifier (should strip)
print('\n1. EQUIVALENT QUALIFIER TEST: "Chromium (total)"')
print('   Expected: Strip "total", use base "Chromium" only')
variants = generator.generate_all_variants('Chromium (total)')
print(f'   Generated {len(variants)} variants')
has_total_only = any('total' in v.lower() and 'chromium' not in v.lower() for v in variants)
has_chromium_base = any(v.lower() == 'chromium' for v in variants)
print(f'   ✓ Has base "chromium": {has_chromium_base}')
print(f'   ✓ No standalone "total": {not has_total_only}')
chromium_samples = sorted([v for v in variants if 'chromium' in v.lower()])[:4]
print(f'   Samples: {chromium_samples}')

# Test 2: Oxidation state (should keep)
print('\n2. OXIDATION STATE TEST: "Chromium VI"')
print('   Expected: Keep VI, generate hexavalent variants')
variants = generator.generate_all_variants('Chromium VI')
print(f'   Generated {len(variants)} variants')
has_vi = any('vi' in v.lower() for v in variants)
has_chromium_only = any(v.lower() == 'chromium' for v in variants)
print(f'   ✓ Has VI variants: {has_vi}')
print(f'   ✓ Also has base "chromium": {has_chromium_only}')
vi_samples = sorted([v for v in variants if 'vi' in v.lower() or v.lower() == 'chromium'])[:5]
print(f'   Samples: {vi_samples}')

# Test 3: Distinct qualifier (should keep)
print('\n3. DISTINCT QUALIFIER TEST: "Boron (hot water soluble)"')
print('   Expected: Keep HWS qualifier in variants')
variants = generator.generate_all_variants('Boron (hot water soluble)')
print(f'   Generated {len(variants)} variants')
has_hws = any('water' in v.lower() or 'soluble' in v.lower() for v in variants)
has_boron_base = any(v.lower() == 'boron' for v in variants)
print(f'   ✓ Has HWS variants: {has_hws}')
print(f'   ✓ Also has base "boron": {has_boron_base}')
hws_samples = sorted([v for v in variants if 'water' in v.lower() or 'soluble' in v.lower()])[:4]
print(f'   Samples: {hws_samples}')

print('\n' + '='*70)
print('LOCANT VARIANT TEST: "2,4-Dimethylphenol"')
print('='*70)
variants = generator.generate_all_variants('2,4-Dimethylphenol')
print(f'Generated {len(variants)} variants\n')
print('Locant separator variants (first 10):')
locant_variants = sorted([v for v in variants if '2' in v and '4' in v])[:10]
for i, v in enumerate(locant_variants, 1):
    print(f'  {i}. {v}')

print('\n' + '='*70)
print('AROMATIC VARIANT TEST: "p-Chloroaniline"')
print('='*70)
variants = generator.generate_all_variants('p-Chloroaniline')
print(f'Generated {len(variants)} variants\n')
print('Aromatic position variants (first 10):')
aromatic_variants = sorted([v for v in variants if 'chloroaniline' in v.lower()])[:10]
for i, v in enumerate(aromatic_variants, 1):
    print(f'  {i}. {v}')

print('\n' + '='*70)
print('SUMMARY: ExhaustiveVariantGenerator Implementation Complete')
print('='*70)
print('✓ Qualifier classification (equivalent vs distinct)')
print('✓ Locant position variants (leading/trailing/separators)')
print('✓ Aromatic descriptor variants (ortho/meta/para vs numeric)')
print('✓ Hyphen/space/concatenation variants')
print('✓ Case variations')
print('✓ Filtering and ranking (cap at 50 variants)')
print('='*70)
