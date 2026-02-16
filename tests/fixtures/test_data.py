"""
Test data fixtures for chemical matcher testing.

Provides sample data including:
- Analyte names with variations
- Lab EDD data samples
- Match decisions
- Expected match results
"""

from typing import List, Dict, Any


# ============================================================================
# SAMPLE ANALYTE NAMES WITH VARIATIONS
# ============================================================================

SAMPLE_ANALYTE_VARIANTS = {
    'benzene': [
        'Benzene',
        'benzene',
        'BENZENE',
        'Benzene (CAS 71-43-2)',
        '71-43-2',
        'benzol',
        'Benzen',  # typo
        'Benzeen',  # typo
    ],
    'toluene': [
        'Toluene',
        'toluene',
        'Toluenne',  # typo
        'methylbenzene',
        'methyl benzene',
        '108-88-3',
        'Toluene (CAS 108-88-3)',
    ],
    'xylenes': [
        'Xylenes (total)',
        'Total Xylenes',
        'Xylenes, total',
        'Xylene (total)',
        'xylenes',
        'XYLENES',
        'Mixed Xylenes',
    ],
    'phc_f2': [
        'PHC F2',
        'F2',
        'Fraction 2',
        'PHC F2 (C10-C16)',
        'F2 (C10-C16)',
        'C10-C16',
        'PHC Fraction 2',
        'Petroleum Hydrocarbons F2',
    ],
    'chromium_vi': [
        'Chromium VI',
        'Chromium (VI)',
        'Hexavalent Chromium',
        'Cr(VI)',
        'Chromium 6',
        'Cr6+',
        'CrVI',
    ],
}


# ============================================================================
# LAB EDD SAMPLE DATA
# ============================================================================

SAMPLE_LAB_EDD = [
    {
        'sample_id': 'MW-01-20250201-0.5',
        'analyte': 'Benzene',
        'result': 0.5,
        'units': 'mg/kg',
        'lab': 'ALS Canada',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'MW-01-20250201-0.5',
        'analyte': 'Toluenne',  # typo
        'result': 1.2,
        'units': 'mg/kg',
        'lab': 'ALS Canada',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'MW-01-20250201-0.5',
        'analyte': 'Ethyl Benzene',
        'result': 0.3,
        'units': 'mg/kg',
        'lab': 'ALS Canada',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'MW-01-20250201-0.5',
        'analyte': 'Total Xylenes',
        'result': 2.1,
        'units': 'mg/kg',
        'lab': 'ALS Canada',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-01-20250201-1.0',
        'analyte': 'PHC F2 (C10-C16)',
        'result': 150,
        'units': 'mg/kg',
        'lab': 'Exova',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-01-20250201-1.0',
        'analyte': 'F3',
        'result': 450,
        'units': 'mg/kg',
        'lab': 'Exova',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-02-20250201-0.5',
        'analyte': 'Arsenic, total',
        'result': 12.5,
        'units': 'mg/kg',
        'lab': 'SGS',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-02-20250201-0.5',
        'analyte': 'Lead',
        'result': 45.2,
        'units': 'mg/kg',
        'lab': 'SGS',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-02-20250201-0.5',
        'analyte': 'Chromium (VI)',
        'result': 2.8,
        'units': 'mg/kg',
        'lab': 'SGS',
        'date_sampled': '2025-02-01',
    },
    {
        'sample_id': 'SS-03-20250202-1.0',
        'analyte': 'Napthalene',  # typo (common)
        'result': 0.8,
        'units': 'mg/kg',
        'lab': 'Bureau Veritas',
        'date_sampled': '2025-02-02',
    },
]


# ============================================================================
# EXPECTED MATCH RESULTS
# ============================================================================

EXPECTED_EXACT_MATCHES = {
    'Benzene': {
        'analyte_id': 'REG153_VOCS_001',
        'preferred_name': 'Benzene',
        'confidence': 1.0,
        'method': 'exact',
    },
    'benzene': {
        'analyte_id': 'REG153_VOCS_001',
        'preferred_name': 'Benzene',
        'confidence': 1.0,
        'method': 'exact',
    },
    '71-43-2': {
        'analyte_id': 'REG153_VOCS_001',
        'preferred_name': 'Benzene',
        'confidence': 1.0,
        'method': 'cas_extracted',
    },
    'PHC F2': {
        'analyte_id': 'REG153_PHCS_002',
        'preferred_name': 'PHC F2 (C10-C16)',
        'confidence': 1.0,
        'method': 'exact',
    },
    'F2 (C10-C16)': {
        'analyte_id': 'REG153_PHCS_002',
        'preferred_name': 'PHC F2 (C10-C16)',
        'confidence': 1.0,
        'method': 'exact',
    },
}


EXPECTED_FUZZY_MATCHES = {
    'Benzen': {  # typo
        'analyte_id': 'REG153_VOCS_001',
        'preferred_name': 'Benzene',
        'confidence_min': 0.90,
        'method': 'fuzzy',
    },
    'Toluenne': {  # typo
        'analyte_id': 'REG153_VOCS_002',
        'preferred_name': 'Toluene',
        'confidence_min': 0.85,
        'method': 'fuzzy',
    },
    'Napthalene': {  # common typo
        'analyte_id': 'REG153_PAHS_001',
        'preferred_name': 'Naphthalene',
        'confidence_min': 0.90,
        'method': 'fuzzy',
    },
}


EXPECTED_UNKNOWNS = [
    'Mystery Chemical X',
    'gibberish123',
    'UNKNOWN_COMPOUND',
    '!!!invalid!!!',
    'ZZZ-999-9',  # invalid CAS
]


# ============================================================================
# TEST MATCH DECISIONS
# ============================================================================

SAMPLE_MATCH_DECISIONS = [
    {
        'lab_variant': 'Benzene',
        'analyte_id': 'REG153_VOCS_001',
        'confidence': 1.0,
        'method': 'exact',
        'validation_status': 'AUTO_ACCEPT',
    },
    {
        'lab_variant': 'Toluenne',
        'analyte_id': 'REG153_VOCS_002',
        'confidence': 0.87,
        'method': 'fuzzy',
        'validation_status': 'REVIEW',
    },
    {
        'lab_variant': 'Total Xylenes',
        'analyte_id': 'REG153_VOCS_004',
        'confidence': 1.0,
        'method': 'exact',
        'validation_status': 'AUTO_ACCEPT',
    },
    {
        'lab_variant': 'PHC F2 (C10-C16)',
        'analyte_id': 'REG153_PHCS_002',
        'confidence': 1.0,
        'method': 'exact',
        'validation_status': 'AUTO_ACCEPT',
    },
]


# ============================================================================
# NORMALIZATION TEST CASES
# ============================================================================

NORMALIZATION_TEST_CASES = [
    # Unicode normalization
    ('Benzène', 'benzene'),  # accented character
    ('toluène', 'toluene'),
    ('Naphtalène', 'naphtalene'),
    
    # Whitespace and punctuation
    ('  Benzene  ', 'benzene'),
    ('Benzene\t\n', 'benzene'),
    ('1,2-Dichloroethane', '12 dichloroethane'),
    ('Benzene (total)', 'benzene total'),
    
    # Case folding
    ('BENZENE', 'benzene'),
    ('BeNzEnE', 'benzene'),
    ('tOlUeNe', 'toluene'),
    
    # Chemical abbreviations
    ('tert-Butanol', 'tertiary butanol'),
    ('sec-Butanol', 'secondary butanol'),
    ('ortho-Xylene', 'ortho xylene'),
    ('para-Xylene', 'para xylene'),
    
    # Numeric prefixes
    ('1,2-Dichloroethane', '12 dichloroethane'),
    ('1,1,1-Trichloroethane', '111 trichloroethane'),
]


# ============================================================================
# CAS EXTRACTION TEST CASES
# ============================================================================

CAS_EXTRACTION_TEST_CASES = [
    ('71-43-2', '71-43-2'),
    ('Benzene (CAS 71-43-2)', '71-43-2'),
    ('CAS: 71-43-2', '71-43-2'),
    ('108-88-3', '108-88-3'),
    ('Toluene 108-88-3', '108-88-3'),
    ('100-41-4 Ethylbenzene', '100-41-4'),
    ('No CAS here', None),
    ('Invalid 999-99-999', None),
    ('71432', None),  # missing hyphens
]


# ============================================================================
# PHC FRACTION TEST CASES
# ============================================================================

PHC_FRACTION_TEST_CASES = [
    ('PHC F1', 'F1'),
    ('F1', 'F1'),
    ('Fraction 1', 'F1'),
    ('C6-C10', 'F1'),
    ('PHC F2 (C10-C16)', 'F2'),
    ('F2 (C10-C16)', 'F2'),
    ('C10-C16', 'F2'),
    ('PHC F3', 'F3'),
    ('F3 (C16-C34)', 'F3'),
    ('PHC F4', 'F4'),
    ('>C34', 'F4'),
    ('Greater than C34', 'F4'),
    ('Not a PHC fraction', None),
]


# ============================================================================
# QUALIFIER TEST CASES
# ============================================================================

QUALIFIER_TEST_CASES = [
    {
        'input': 'Arsenic, total',
        'expected_base': 'Arsenic',
        'expected_qualifier': 'total',
        'should_preserve': False,  # if database doesn't differentiate
    },
    {
        'input': 'Chromium (VI)',
        'expected_base': 'Chromium',
        'expected_qualifier': 'hexavalent',
        'should_preserve': True,  # always preserve valence
    },
    {
        'input': 'Hexavalent chromium',
        'expected_base': 'chromium',
        'expected_qualifier': 'hexavalent',
        'should_preserve': True,
    },
    {
        'input': 'Lead, dissolved',
        'expected_base': 'Lead',
        'expected_qualifier': 'dissolved',
        'should_preserve': False,  # if database doesn't differentiate
    },
]


# ============================================================================
# PERFORMANCE TEST DATA
# ============================================================================

# Generate large batch for performance testing
PERFORMANCE_BATCH_1000 = []
analytes_cycle = [
    'Benzene', 'Toluene', 'Ethylbenzene', 'Xylenes (total)',
    'PHC F1', 'PHC F2', 'PHC F3', 'PHC F4',
    'Arsenic', 'Lead', 'Chromium', 'Naphthalene',
]

for i in range(1000):
    analyte = analytes_cycle[i % len(analytes_cycle)]
    PERFORMANCE_BATCH_1000.append({
        'sample_id': f'PERF-{i:04d}',
        'analyte': analyte,
        'result': (i * 0.1) % 100,
        'units': 'mg/kg',
    })


# ============================================================================
# DATABASE CRUD TEST DATA
# ============================================================================

CRUD_TEST_ANALYTE = {
    'analyte_id': 'TEST_001',
    'preferred_name': 'Test Chemical',
    'analyte_type': 'single_substance',
    'cas_number': '123-45-6',
    'group_code': 'TEST',
    'chemical_group': 'Test Group',
    'table_number': 1,
}

CRUD_TEST_SYNONYM = {
    'analyte_id': 'TEST_001',
    'synonym_raw': 'Test Chem',
    'synonym_norm': 'test chem',
    'synonym_type': 'common',
    'confidence_score': 0.95,
    'source': 'test',
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_sample_by_analyte(analyte_name: str) -> List[Dict[str, Any]]:
    """Get all lab EDD samples for a specific analyte."""
    return [
        sample for sample in SAMPLE_LAB_EDD
        if sample['analyte'].lower() == analyte_name.lower()
    ]


def get_variants_by_base(base_name: str) -> List[str]:
    """Get all variants for a base analyte name."""
    return SAMPLE_ANALYTE_VARIANTS.get(base_name.lower(), [])


def generate_typo_variants(text: str, num_variants: int = 5) -> List[str]:
    """Generate simple typo variants of a text string."""
    import random
    
    variants = []
    for _ in range(num_variants):
        chars = list(text)
        if len(chars) > 3:
            # Random character deletion
            idx = random.randint(1, len(chars) - 2)
            chars.pop(idx)
            variants.append(''.join(chars))
    
    return variants
