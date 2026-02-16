# Synthetic Lab EDDs - Documentation

This directory contains realistic synthetic Electronic Data Deliverable (EDD) files from Ontario environmental laboratories, created for testing the chemical matcher system.

## Overview

Three complete lab EDD files have been generated representing different Ontario lab vendors:

1. **ALS_example_report.xlsx** - ALS Environmental format
2. **SGS_example_report.xlsx** - SGS Canada format  
3. **BureauVeritas_example_report.xlsx** - Bureau Veritas format

Plus supporting files:
- **ontario_variants_known.csv** - Ground truth mapping of lab naming variants
- **generate_synthetic_edds.py** - Script to regenerate/modify EDDs
- **verify_synthetic_edds.py** - Validation and QA script

## Generated Data Statistics

### ALS Environmental (ALS_example_report.xlsx)
- **Format**: Classic ALS multi-sheet workbook
- **Sheets**: Cover, Sample Summary, Results, QAQC
- **Samples**: 25 (Soil: 132 rows, Groundwater: 168 rows)
- **Total Rows**: 300 analytical results
- **Unique Parameters**: 34 different chemical name variants
- **Notable Features**:
  - Typos: "Benezene", "Toluenne"
  - Trailing spaces: "Benzene ", "Naphthalene "
  - Truncations: "1,4 Diox" for 1,4-Dioxane
  - Abbreviations: "TCE", "B(a)P", "As", "Pb", "Cr"
  - Missing CAS numbers: ~24% of rows
  - Non-detects: 40% with "<RL" notation
  - Qualifier flags: U, J, B

### SGS Canada (SGS_example_report.xlsx)
- **Format**: SGS multi-sheet format
- **Sheets**: Report Information, Sample Information, Analytical Results, Quality Control
- **Samples**: 20 (Soil: 100 rows, Groundwater: 100 rows)
- **Total Rows**: 200 analytical results
- **Unique Analytes**: 24 different chemical name variants
- **Notable Features**:
  - Column name: "Analyte" (not "Parameter")
  - "Total" qualifiers: "Chromium, Total", "Lead (Total)"
  - All-caps variants: "BENZENE", "TOLUENE"
  - Inverted order: "Dioxane, 1,4-"
  - Uses "RL" not "MDL"
  - Total qualifiers: 52 instances

### Bureau Veritas (BureauVeritas_example_report.xlsx)
- **Format**: Bureau Veritas with nested headers
- **Sheets**: Report Cover, Sample Registry, Data Results
- **Samples**: 22 (Soil: 99 rows, Groundwater: 143 rows)
- **Total Rows**: 242 analytical results
- **Unique Analytes**: 12 different chemical name variants
- **Notable Features**:
  - LIMS IDs included
  - Nested/grouped column headers
  - Combined naming: "Trichloroethylene (TCE)"
  - Total qualifiers: "Chromium, Total"
  - Non-detect flag column (Y/N)
  - Visual formatting (colored headers)

## Chemical Naming Variants Included

The EDDs intentionally include Ontario lab-specific naming patterns:

### Common Abbreviations
- **B(a)P** → Benzo(a)pyrene (very common, n=1850)
- **TCE** → Trichloroethylene (n=1200)
- **PCE** → Tetrachloroethene (n=1450)
- **MEK** → Methyl Ethyl Ketone (n=1100)
- **DCM** → Methylene Chloride (n=380)
- **1,1,1-TCA** → 1,1,1-Trichloroethane (n=920)

### Petroleum Hydrocarbons
- **PHC F2** → Petroleum Hydrocarbons F2 (n=1500)
- **PHC F3** → Petroleum Hydrocarbons F3 (n=1450)
- **PHC F2 (C10-C16)** → Detailed with carbon range
- **F2 Petroleum Hydrocarbons** → Inverted order (SGS)

### Metal Qualifiers
- **Chromium, Total** (SGS-specific, n=1650)
- **Lead, Total** (n=1720)
- **Arsenic, Total** (n=1580)
- **Chromium (Total)** - parenthetical variant

### Typos and Spacing Issues
- **Benezene** → Benzene (missing 'n')
- **Toluenne** → Toluene (double 'n')
- **"Benzene "** → Trailing space
- **"1, 1, 1-TCA"** → Spaces after commas

### Truncations
- **"1,4 Diox"** → 1,4-Dioxane (common lab truncation)

### Case Variations
- **BENZENE** → Benzene (SGS all-caps)
- **TOLUENE** → Toluene

## ontario_variants_known.csv

Ground truth mapping for testing chemical matching algorithms.

**Structure**:
```
observed_text, canonical_analyte_id, lab_vendor, frequency, variant_type, notes
```

**Statistics**:
- **Total entries**: 64
- **Unique canonical chemicals**: 26
- **Unique observed variants**: 63
- **Lab vendors**: ALS, SGS, BureauVeritas

**Variant Types**:
- abbreviation (17)
- standard (20)
- qualifier (6)
- spacing (6)
- typo (2)
- truncation (1)
- case (3)
- inverted (3)
- detailed (2)
- combined (1)
- bracket_variant (1)
- plural (1)

**Use Cases**:
1. Training data for name matching algorithms
2. Test cases for normalization pipeline
3. Evaluation metrics for fuzzy matching
4. Understanding lab-specific patterns

## File Generation

### To Regenerate EDDs:
```bash
python scripts/generate_synthetic_edds.py
```

This will create fresh EDDs with:
- Random but realistic sample IDs
- Collection dates in past 6 months
- Lognormal distribution for concentration values
- Random detect/non-detect status
- Realistic qualifier flags

### To Verify EDDs:
```bash
python scripts/verify_synthetic_edds.py
```

Validates:
- Sheet structure
- Column names
- Data types
- Naming variant coverage
- Cross-lab comparison

## Usage in Testing

### 1. Normalization Pipeline Testing
```python
# Test text normalization
from src.normalization.text_normalizer import TextNormalizer

normalizer = TextNormalizer()
result = normalizer.normalize("B(a)P")  # Should handle abbreviation
result = normalizer.normalize("Benzene ")  # Should trim spaces
result = normalizer.normalize("1, 1, 1-TCA")  # Should fix spacing
```

### 2. Chemical Matching Testing
```python
# Test against known variants
import pandas as pd
variants_df = pd.read_csv('data/training/ontario_variants_known.csv')

# For each observed text, verify correct canonical match
for _, row in variants_df.iterrows():
    observed = row['observed_text']
    expected = row['canonical_analyte_id']
    # Test your matching algorithm
```

### 3. EDD Parsing Testing
```python
# Test parsing different lab formats
import pandas as pd

# ALS format
df_als = pd.read_excel('data/raw/lab_edds/ALS_example_report.xlsx', 
                       sheet_name='Results')
# Verify 'Parameter' column exists
# Test normalization on parameter names

# SGS format  
df_sgs = pd.read_excel('data/raw/lab_edds/SGS_example_report.xlsx',
                       sheet_name='Analytical Results')
# Verify 'Analyte' column (different from ALS)
# Handle "Total" qualifiers

# Bureau Veritas format
df_bv = pd.read_excel('data/raw/lab_edds/BureauVeritas_example_report.xlsx',
                      sheet_name='Data Results', header=1)  # Note: nested headers!
# Verify 'Analyte_Name' column
# Handle LIMS IDs
```

### 4. Pattern Analysis
```python
# Analyze naming patterns across labs
from collections import Counter

# Extract all parameter names
all_params = []
all_params.extend(df_als['Parameter'].tolist())
all_params.extend(df_sgs['Analyte'].tolist())
all_params.extend(df_bv['Analyte_Name'].tolist())

# Identify patterns
param_counts = Counter(all_params)
print(f"Most common variants: {param_counts.most_common(20)}")
```

## Realistic Features

### Environmental Chemistry Patterns
- **Lognormal distributions**: Concentration values follow typical environmental patterns
- **Detection limits**: Realistic MDL/RL ranges for soil vs. groundwater
- **Non-detect rates**: ~40% typical for contaminated sites
- **Qualifier flags**: J (estimated), B (blank), U (non-detect)

### Lab Vendor Specifics
- **ALS**: Most parameter variants, typos, truncations
- **SGS**: "Total" qualifiers, all-caps, inverted order
- **Bureau Veritas**: Combined names, LIMS integration

### Ontario-Specific
- **PHC fractions**: F2, F3 (CCME Canada standard)
- **PAH abbreviations**: B(a)P, B(k)F, B(b)F
- **Metals as Total**: Ontario regulation O.Reg 153 focus

## Next Steps

1. **Expand coverage**: Add more chemicals (pesticides, PCBs)
2. **More variants**: Additional typos, misspellings, abbreviations
3. **QC samples**: Blanks, duplicates, matrix spikes
4. **Batch effects**: Simulate detection limit changes over time
5. **Real vs synthetic**: Compare against actual lab data

## Important Notes

- **DO NOT** use for production/reporting - synthetic data only
- Sample IDs, dates, and values are randomly generated
- Intentionally includes errors (typos, spacing) for testing
- Patterns based on real Ontario lab reports but anonymized
- Frequency counts in variants CSV are simulated, not actual

## References

- Ontario Regulation 153/04 (Records of Site Condition)
- CCME Petroleum Hydrocarbon Fractions (F1-F4)
- EPA Test Methods (8260, 8270, 6010)
- ALS Environmental EDD format specifications
- SGS Canada reporting standards
