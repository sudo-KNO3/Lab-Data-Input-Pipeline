"""
Example usage of synthetic lab EDDs for testing chemical matching.

Demonstrates:
1. Loading EDDs from different lab formats
2. Extracting chemical names
3. Using ontario_variants_known.csv for validation
4. Testing normalization pipeline
"""

import pandas as pd
from pathlib import Path
from collections import Counter

# Base path
base_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher")
edd_dir = base_path / "data" / "raw" / "lab_edds"
variants_file = base_path / "data" / "training" / "ontario_variants_known.csv"


def load_all_edds():
    """Load chemical names from all three lab EDD formats."""
    
    print("="*70)
    print("LOADING SYNTHETIC LAB EDDs")
    print("="*70)
    
    # ALS format
    print("\n1. Loading ALS Environmental EDD...")
    df_als = pd.read_excel(edd_dir / "ALS_example_report.xlsx", sheet_name='Results')
    als_chemicals = df_als['Parameter'].tolist()
    print(f"   Found {len(als_chemicals)} results with {len(set(als_chemicals))} unique chemical names")
    print(f"   Column used: 'Parameter'")
    
    # SGS format
    print("\n2. Loading SGS Canada EDD...")
    df_sgs = pd.read_excel(edd_dir / "SGS_example_report.xlsx", sheet_name='Analytical Results')
    sgs_chemicals = df_sgs['Analyte'].tolist()  # Note: SGS uses 'Analyte' not 'Parameter'
    print(f"   Found {len(sgs_chemicals)} results with {len(set(sgs_chemicals))} unique chemical names")
    print(f"   Column used: 'Analyte' (SGS-specific)")
    
    # Bureau Veritas format
    print("\n3. Loading Bureau Veritas EDD...")
    df_bv = pd.read_excel(edd_dir / "BureauVeritas_example_report.xlsx", 
                          sheet_name='Data Results', header=1)  # Nested headers!
    bv_chemicals = df_bv['Analyte_Name'].tolist()
    print(f"   Found {len(bv_chemicals)} results with {len(set(bv_chemicals))} unique chemical names")
    print(f"   Column used: 'Analyte_Name'")
    print(f"   Note: header=1 used due to nested headers")
    
    return {
        'ALS': als_chemicals,
        'SGS': sgs_chemicals,
        'BureauVeritas': bv_chemicals
    }


def analyze_naming_patterns(all_chemicals):
    """Analyze naming patterns across all labs."""
    
    print("\n" + "="*70)
    print("NAMING PATTERN ANALYSIS")
    print("="*70)
    
    # Combine all chemicals
    all_names = []
    for lab, chemicals in all_chemicals.items():
        all_names.extend(chemicals)
    
    name_counts = Counter(all_names)
    
    print(f"\nTotal observations: {len(all_names)}")
    print(f"Unique chemical names: {len(name_counts)}")
    
    print(f"\nTop 15 most frequent chemical names:")
    for name, count in name_counts.most_common(15):
        print(f"  {name}: {count}")
    
    # Identify patterns
    print(f"\nNaming patterns detected:")
    
    # Abbreviations
    abbreviations = [name for name in name_counts if len(name) <= 6 and name.isupper()]
    print(f"  Abbreviations (short, all-caps): {len(abbreviations)}")
    print(f"    Examples: {abbreviations[:5]}")
    
    # Trailing spaces
    trailing_spaces = [name for name in name_counts if name.endswith(' ')]
    print(f"  Trailing spaces: {len(trailing_spaces)}")
    if trailing_spaces:
        print(f"    Examples: {[repr(name) for name in trailing_spaces[:3]]}")
    
    # Contains "Total"
    with_total = [name for name in name_counts if 'Total' in name]
    print(f"  Contains 'Total' qualifier: {len(with_total)}")
    print(f"    Examples: {with_total[:3]}")
    
    # Contains commas
    with_commas = [name for name in name_counts if ',' in name]
    print(f"  Contains commas: {len(with_commas)}")
    print(f"    Examples: {with_commas[:3]}")
    
    # PAH abbreviations
    pah_abbrev = [name for name in name_counts if 'B(' in name or 'BaP' in name]
    print(f"  PAH abbreviations: {len(pah_abbrev)}")
    print(f"    Examples: {pah_abbrev}")


def load_known_variants():
    """Load known Ontario lab variants for validation."""
    
    print("\n" + "="*70)
    print("KNOWN VARIANTS (GROUND TRUTH)")
    print("="*70)
    
    df_variants = pd.read_csv(variants_file)
    
    print(f"\nLoaded {len(df_variants)} known variant mappings")
    print(f"Covering {df_variants['canonical_analyte_id'].nunique()} canonical chemicals")
    
    print(f"\nVariant types:")
    for vtype, count in df_variants['variant_type'].value_counts().items():
        print(f"  {vtype}: {count}")
    
    print(f"\nTop 10 most frequent variants (by lab observation frequency):")
    top_variants = df_variants.nlargest(10, 'frequency')
    for _, row in top_variants.iterrows():
        print(f"  '{row['observed_text']}' → {row['canonical_analyte_id']}")
        print(f"    Type: {row['variant_type']}, Frequency: {row['frequency']}, Lab: {row['lab_vendor']}")
    
    return df_variants


def test_variant_matching(all_chemicals, df_variants):
    """Test how many observed chemicals match known variants."""
    
    print("\n" + "="*70)
    print("VARIANT MATCHING TEST")
    print("="*70)
    
    # Get all unique observed names
    all_observed = set()
    for chemicals in all_chemicals.values():
        all_observed.update(chemicals)
    
    # Get known variant names
    known_variants = set(df_variants['observed_text'].tolist())
    
    print(f"\nUnique chemicals in synthetic EDDs: {len(all_observed)}")
    print(f"Known variants in ground truth: {len(known_variants)}")
    
    # Find matches
    matched = all_observed.intersection(known_variants)
    unmatched = all_observed - known_variants
    
    print(f"\nMatched (in ground truth): {len(matched)}")
    print(f"Unmatched (not in ground truth): {len(unmatched)}")
    
    if matched:
        print(f"\nExamples of matched variants:")
        for name in list(matched)[:10]:
            canonical = df_variants[df_variants['observed_text'] == name]['canonical_analyte_id'].values[0]
            print(f"  '{name}' → {canonical}")
    
    if unmatched:
        print(f"\nUnmatched chemicals (would need to add to ground truth):")
        for name in list(unmatched)[:10]:
            print(f"  {name}")
    
    match_rate = len(matched) / len(all_observed) * 100
    print(f"\nMatch rate: {match_rate:.1f}%")


def demonstrate_normalization_needs():
    """Show why normalization is needed."""
    
    print("\n" + "="*70)
    print("NORMALIZATION NEEDS DEMONSTRATION")
    print("="*70)
    
    df_variants = pd.read_csv(variants_file)
    
    # Group by canonical ID to show multiple variants
    print("\nExamples where multiple observed names map to same chemical:")
    
    for canonical in ['Benzene', 'Trichloroethylene', 'Benzo(a)pyrene', 'Lead']:
        variants = df_variants[df_variants['canonical_analyte_id'] == canonical]
        if len(variants) > 0:
            print(f"\n{canonical} has {len(variants)} variants:")
            for _, row in variants.iterrows():
                print(f"  '{row['observed_text']}' ({row['variant_type']}, lab: {row['lab_vendor']})")
    
    print("\n" + "="*70)
    print("This demonstrates why normalization and matching are essential!")
    print("="*70)


def main():
    """Run all demonstrations."""
    
    # Load EDDs
    all_chemicals = load_all_edds()
    
    # Analyze patterns
    analyze_naming_patterns(all_chemicals)
    
    # Load known variants
    df_variants = load_known_variants()
    
    # Test matching
    test_variant_matching(all_chemicals, df_variants)
    
    # Show normalization needs
    demonstrate_normalization_needs()
    
    print("\n" + "="*70)
    print("DEMONSTRATION COMPLETE")
    print("="*70)
    print("\nNext steps:")
    print("  1. Use these EDDs to test your normalization pipeline")
    print("  2. Test fuzzy matching algorithms against known variants")
    print("  3. Measure matching accuracy using ground truth data")
    print("  4. Expand ground truth with additional variants as needed")


if __name__ == "__main__":
    main()
