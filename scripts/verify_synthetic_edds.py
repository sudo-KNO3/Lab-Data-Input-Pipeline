"""
Verification script for synthetic lab EDD files.

Validates structure, parses content, and generates summary statistics
for the synthetic EDDs created by generate_synthetic_edds.py
"""

import pandas as pd
from pathlib import Path
import openpyxl
from collections import Counter, defaultdict


def verify_als_edd(file_path):
    """Verify ALS Environmental EDD structure and content."""
    
    print(f"\n{'='*70}")
    print(f"VERIFYING: {file_path.name}")
    print(f"{'='*70}")
    
    try:
        # Check all sheets exist
        excel_file = pd.ExcelFile(file_path)
        expected_sheets = ['Cover', 'Sample Summary', 'Results', 'QAQC']
        
        print(f"\nExpected sheets: {expected_sheets}")
        print(f"Found sheets: {excel_file.sheet_names}")
        
        missing = set(expected_sheets) - set(excel_file.sheet_names)
        if missing:
            print(f"⚠ WARNING: Missing sheets: {missing}")
        else:
            print("✓ All expected sheets present")
        
        # Verify Cover sheet
        df_cover = pd.read_excel(file_path, sheet_name='Cover')
        print(f"\nCover Sheet:")
        print(f"  Laboratory: {df_cover[df_cover['Item'] == 'Laboratory']['Value'].values[0]}")
        print(f"  Sample Count: {df_cover[df_cover['Item'] == 'Number of Samples']['Value'].values[0]}")
        
        # Verify Results sheet
        df_results = pd.read_excel(file_path, sheet_name='Results')
        
        expected_columns = [
            'Lab_Sample_ID', 'Client_Sample_ID', 'Collection_Date', 'Matrix',
            'Analysis_Method', 'Parameter', 'CAS_Number', 'Result_Value',
            'Units', 'MDL', 'RL', 'Qualifier_Flags'
        ]
        
        print(f"\nResults Sheet:")
        print(f"  Expected columns: {len(expected_columns)}")
        print(f"  Found columns: {len(df_results.columns)}")
        print(f"  Column names: {list(df_results.columns)}")
        
        missing_cols = set(expected_columns) - set(df_results.columns)
        if missing_cols:
            print(f"  ⚠ WARNING: Missing columns: {missing_cols}")
        else:
            print("  ✓ All expected columns present")
        
        # Statistics
        print(f"\nStatistics:")
        print(f"  Total rows: {len(df_results)}")
        print(f"  Unique samples: {df_results['Lab_Sample_ID'].nunique()}")
        print(f"  Unique parameters: {df_results['Parameter'].nunique()}")
        print(f"  Matrices: {df_results['Matrix'].value_counts().to_dict()}")
        
        # Parameter variants
        print(f"\nParameter Variants (Top 15):")
        param_counts = df_results['Parameter'].value_counts()
        for param, count in param_counts.head(15).items():
            print(f"  {param}: {count}")
        
        # Qualifier flags
        print(f"\nQualifier Flags:")
        qualifier_counts = df_results['Qualifier_Flags'].value_counts()
        for qual, count in qualifier_counts.items():
            qual_display = qual if qual and str(qual) != 'nan' else '(none)'
            print(f"  {qual_display}: {count}")
        
        # Non-detects
        non_detects = df_results['Result_Value'].astype(str).str.contains('<', na=False).sum()
        print(f"\nNon-detects: {non_detects} ({non_detects/len(df_results)*100:.1f}%)")
        
        # CAS numbers
        missing_cas = df_results['CAS_Number'].isna().sum()
        print(f"Missing CAS numbers: {missing_cas} ({missing_cas/len(df_results)*100:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def verify_sgs_edd(file_path):
    """Verify SGS Canada EDD structure and content."""
    
    print(f"\n{'='*70}")
    print(f"VERIFYING: {file_path.name}")
    print(f"{'='*70}")
    
    try:
        excel_file = pd.ExcelFile(file_path)
        expected_sheets = ['Report Information', 'Sample Information', 'Analytical Results', 'Quality Control']
        
        print(f"\nExpected sheets: {expected_sheets}")
        print(f"Found sheets: {excel_file.sheet_names}")
        
        missing = set(expected_sheets) - set(excel_file.sheet_names)
        if missing:
            print(f"⚠ WARNING: Missing sheets: {missing}")
        else:
            print("✓ All expected sheets present")
        
        # Verify Report Information
        df_info = pd.read_excel(file_path, sheet_name='Report Information')
        print(f"\nReport Information:")
        print(f"  Laboratory: {df_info[df_info['Field'] == 'Laboratory']['Information'].values[0]}")
        
        # Verify Analytical Results
        df_results = pd.read_excel(file_path, sheet_name='Analytical Results')
        
        # Note: SGS uses "Analyte" not "Parameter"
        expected_columns = [
            'SGS_Sample_ID', 'Field_Sample_ID', 'Sample_Date', 'Matrix',
            'Method', 'Analyte', 'CAS_RN', 'Result', 'Unit', 'RL', 'Flag'
        ]
        
        print(f"\nAnalytical Results Sheet:")
        print(f"  Expected columns: {len(expected_columns)}")
        print(f"  Found columns: {len(df_results.columns)}")
        print(f"  Column names: {list(df_results.columns)}")
        
        missing_cols = set(expected_columns) - set(df_results.columns)
        if missing_cols:
            print(f"  ⚠ WARNING: Missing columns: {missing_cols}")
        else:
            print("  ✓ All expected columns present")
        
        # Statistics
        print(f"\nStatistics:")
        print(f"  Total rows: {len(df_results)}")
        print(f"  Unique samples: {df_results['SGS_Sample_ID'].nunique()}")
        print(f"  Unique analytes: {df_results['Analyte'].nunique()}")
        print(f"  Matrices: {df_results['Matrix'].value_counts().to_dict()}")
        
        # Analyte variants (note: SGS uses "Total" qualifiers)
        print(f"\nAnalyte Variants (Top 15):")
        analyte_counts = df_results['Analyte'].value_counts()
        for analyte, count in analyte_counts.head(15).items():
            print(f"  {analyte}: {count}")
        
        # Check for "Total" qualifiers
        total_qualifiers = df_results['Analyte'].astype(str).str.contains('Total', na=False).sum()
        print(f"\nAnalytes with 'Total' qualifier: {total_qualifiers}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def verify_bureauveritas_edd(file_path):
    """Verify Bureau Veritas EDD structure and content."""
    
    print(f"\n{'='*70}")
    print(f"VERIFYING: {file_path.name}")
    print(f"{'='*70}")
    
    try:
        excel_file = pd.ExcelFile(file_path)
        expected_sheets = ['Report Cover', 'Sample Registry', 'Data Results']
        
        print(f"\nExpected sheets: {expected_sheets}")
        print(f"Found sheets: {excel_file.sheet_names}")
        
        missing = set(expected_sheets) - set(excel_file.sheet_names)
        if missing:
            print(f"⚠ WARNING: Missing sheets: {missing}")
        else:
            print("✓ All expected sheets present")
        
        # Verify Report Cover
        df_cover = pd.read_excel(file_path, sheet_name='Report Cover')
        print(f"\nReport Cover:")
        print(f"  Laboratory: {df_cover.iloc[0, 1]}")
        
        # Verify Data Results
        # Note: Bureau Veritas may have nested headers - skip first row if needed
        df_results = pd.read_excel(file_path, sheet_name='Data Results', header=1)
        
        expected_columns = [
            'BV_Lab_ID', 'LIMS_ID', 'Client_ID', 'Date_Collected', 'Matrix',
            'Test_Method', 'Analyte_Name', 'CAS_Registry', 'Measured_Value',
            'Result_Units', 'Detection_Limit', 'Reporting_Limit', 'Data_Qualifier', 'ND_Flag'
        ]
        
        print(f"\nData Results Sheet:")
        print(f"  Expected columns: {len(expected_columns)}")
        print(f"  Found columns: {len(df_results.columns)}")
        print(f"  Column names: {list(df_results.columns)}")
        
        # Statistics
        print(f"\nStatistics:")
        print(f"  Total rows: {len(df_results)}")
        print(f"  Unique samples: {df_results['BV_Lab_ID'].nunique()}")
        print(f"  Unique analytes: {df_results['Analyte_Name'].nunique()}")
        print(f"  Matrices: {df_results['Matrix'].value_counts().to_dict()}")
        
        # Analyte variants
        print(f"\nAnalyte Variants (Top 15):")
        analyte_counts = df_results['Analyte_Name'].value_counts()
        for analyte, count in analyte_counts.head(15).items():
            print(f"  {analyte}: {count}")
        
        # ND flags
        nd_counts = df_results['ND_Flag'].value_counts()
        print(f"\nND Flags: {nd_counts.to_dict()}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def compare_parameter_variants():
    """Compare parameter naming variants across all three lab vendors."""
    
    print(f"\n{'='*70}")
    print("CROSS-LAB PARAMETER VARIANT COMPARISON")
    print(f"{'='*70}")
    
    base_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher")
    edd_dir = base_path / "data" / "raw" / "lab_edds"
    
    als_params = set()
    sgs_params = set()
    bv_params = set()
    
    # Collect parameters from each lab
    try:
        df_als = pd.read_excel(edd_dir / "ALS_example_report.xlsx", sheet_name='Results')
        als_params = set(df_als['Parameter'].unique())
    except Exception as e:
        print(f"Could not read ALS parameters: {e}")
    
    try:
        df_sgs = pd.read_excel(edd_dir / "SGS_example_report.xlsx", sheet_name='Analytical Results')
        sgs_params = set(df_sgs['Analyte'].unique())
    except Exception as e:
        print(f"Could not read SGS parameters: {e}")
    
    try:
        df_bv = pd.read_excel(edd_dir / "BureauVeritas_example_report.xlsx", 
                              sheet_name='Data Results', header=1)
        bv_params = set(df_bv['Analyte_Name'].unique())
    except Exception as e:
        print(f"Could not read BV parameters: {e}")
    
    # Find common base chemicals (ignoring variants)
    print(f"\nUnique parameters by lab:")
    print(f"  ALS: {len(als_params)}")
    print(f"  SGS: {len(sgs_params)}")
    print(f"  Bureau Veritas: {len(bv_params)}")
    
    # Print some examples of variants for same chemical
    print(f"\nExample variants for common chemicals:")
    
    common_bases = ['Benzene', 'Toluene', 'Chromium', 'Lead']
    for base in common_bases:
        print(f"\n  {base}:")
        
        als_variants = [p for p in als_params if base.lower() in p.lower()]
        if als_variants:
            print(f"    ALS: {als_variants}")
        
        sgs_variants = [p for p in sgs_params if base.lower() in p.lower()]
        if sgs_variants:
            print(f"    SGS: {sgs_variants}")
        
        bv_variants = [p for p in bv_params if base.lower() in p.lower()]
        if bv_variants:
            print(f"    BV: {bv_variants}")


def verify_ontario_variants():
    """Verify the Ontario variants known CSV."""
    
    print(f"\n{'='*70}")
    print("VERIFYING: ontario_variants_known.csv")
    print(f"{'='*70}")
    
    base_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher")
    variants_file = base_path / "data" / "training" / "ontario_variants_known.csv"
    
    try:
        df = pd.read_csv(variants_file)
        
        print(f"\nFile structure:")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Total rows: {len(df)}")
        
        print(f"\nStatistics:")
        print(f"  Unique canonical analytes: {df['canonical_analyte_id'].nunique()}")
        print(f"  Unique observed variants: {df['observed_text'].nunique()}")
        print(f"  Lab vendors: {df['lab_vendor'].unique()}")
        
        print(f"\nVariant types:")
        variant_type_counts = df['variant_type'].value_counts()
        for vtype, count in variant_type_counts.items():
            print(f"  {vtype}: {count}")
        
        print(f"\nTop 10 most frequent variants:")
        top_variants = df.nlargest(10, 'frequency')[['observed_text', 'canonical_analyte_id', 'frequency', 'variant_type']]
        for _, row in top_variants.iterrows():
            print(f"  '{row['observed_text']}' → {row['canonical_analyte_id']} ({row['variant_type']}, n={row['frequency']})")
        
        print(f"\nExamples by variant type:")
        for vtype in ['abbreviation', 'typo', 'spacing', 'truncation']:
            examples = df[df['variant_type'] == vtype].head(3)
            if len(examples) > 0:
                print(f"\n  {vtype}:")
                for _, row in examples.iterrows():
                    print(f"    '{row['observed_text']}' → {row['canonical_analyte_id']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification checks."""
    
    base_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher")
    edd_dir = base_path / "data" / "raw" / "lab_edds"
    
    print("="*70)
    print("SYNTHETIC LAB EDD VERIFICATION")
    print("="*70)
    
    results = {}
    
    # Verify each EDD file
    als_file = edd_dir / "ALS_example_report.xlsx"
    if als_file.exists():
        results['ALS'] = verify_als_edd(als_file)
    else:
        print(f"\n⚠ WARNING: {als_file} does not exist")
        results['ALS'] = False
    
    sgs_file = edd_dir / "SGS_example_report.xlsx"
    if sgs_file.exists():
        results['SGS'] = verify_sgs_edd(sgs_file)
    else:
        print(f"\n⚠ WARNING: {sgs_file} does not exist")
        results['SGS'] = False
    
    bv_file = edd_dir / "BureauVeritas_example_report.xlsx"
    if bv_file.exists():
        results['BV'] = verify_bureauveritas_edd(bv_file)
    else:
        print(f"\n⚠ WARNING: {bv_file} does not exist")
        results['BV'] = False
    
    # Cross-lab comparison
    if any(results.values()):
        compare_parameter_variants()
    
    # Verify Ontario variants CSV
    results['Variants'] = verify_ontario_variants()
    
    # Summary
    print(f"\n{'='*70}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*70}")
    
    for name, passed in results.items():
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
    
    if all(results.values()):
        print(f"\n✓ All verifications passed!")
        print(f"\nNext steps:")
        print(f"  1. Use these EDDs to test your normalization pipeline")
        print(f"  2. Test chemical name matching against ontario_variants_known.csv")
        print(f"  3. Experiment with different parsing strategies for each lab format")
    else:
        print(f"\n⚠ Some verifications failed - review output above")


if __name__ == "__main__":
    main()
