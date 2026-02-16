"""
Pytest configuration and shared fixtures for Reg 153 Chemical Matcher tests.

Provides:
- In-memory test database with canonical analytes
- Sample synonyms and lab variants
- Normalizers and matchers
- Performance tracking utilities
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import Base, Analyte, Synonym, AnalyteType, SynonymType
from src.database import crud_new as crud
from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.normalization.qualifier_handler import QualifierHandler
from src.normalization.petroleum_handler import PetroleumHandler
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.resolution_engine import ResolutionEngine


# ============================================================================
# NORMALIZATION ASSERTION HELPER
# ============================================================================

def assert_normalized_equal(actual: str, expected: str, *, msg: str = ""):
    """
    Assert two strings are equal after collapsing syllable-tokenized spaces.
    
    The TextNormalizer produces syllable-tokenized output (e.g. "be nz ene").
    This helper compares strings after removing intra-token spaces, so tests
    don't encode implementation-specific tokenization boundaries.
    
    If tokenization rules change, update this ONE function — not 50 tests.
    
    Args:
        actual: Output from TextNormalizer.normalize()
        expected: Expected normalized form (collapsed, e.g. "benzene")
        msg: Optional failure message context
    """
    collapsed_actual = actual.replace(" ", "")
    collapsed_expected = expected.replace(" ", "")
    assert collapsed_actual == collapsed_expected, (
        f"Normalized mismatch{' (' + msg + ')' if msg else ''}: "
        f"got '{actual}' (collapsed: '{collapsed_actual}'), "
        f"expected '{expected}' (collapsed: '{collapsed_expected}')"
    )


def assert_normalized_contains(actual: str, expected_substring: str, *, msg: str = ""):
    """
    Assert that collapsed normalized output contains a substring.
    
    Args:
        actual: Output from TextNormalizer.normalize()
        expected_substring: Substring expected in collapsed form
        msg: Optional failure message context  
    """
    collapsed = actual.replace(" ", "")
    assert expected_substring in collapsed, (
        f"Normalized substring not found{' (' + msg + ')' if msg else ''}: "
        f"'{expected_substring}' not in '{actual}' (collapsed: '{collapsed}')"
    )


# ============================================================================
# DATABASE FIXTURES
# ============================================================================

@pytest.fixture(scope="function")
def test_db_engine():
    """Create a fresh in-memory SQLite database engine for each test."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def test_db_session(test_db_engine) -> Generator[Session, None, None]:
    """
    Provide a transactional database session for testing.
    
    Each test gets a clean session that rolls back after completion.
    """
    SessionLocal = sessionmaker(bind=test_db_engine)
    session = SessionLocal()
    
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture(scope="function")
def preloaded_analytes(test_db_session: Session) -> Session:
    """
    Database session preloaded with canonical Reg 153 analytes.
    
    Includes representative samples from:
    - VOCs (Benzene, Toluene, Xylenes)
    - PHCs (F1, F2, F3, F4)
    - Metals (Chromium, Lead, Arsenic)
    - PAHs (Naphthalene, Benzo(a)pyrene)
    """
    analytes_data = [
        # VOCs
        {
            'analyte_id': 'REG153_VOCS_001',
            'preferred_name': 'Benzene',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '71-43-2',
            'group_code': 'BTEX',
            'chemical_group': 'VOCs',
            'table_number': 1,
        },
        {
            'analyte_id': 'REG153_VOCS_002',
            'preferred_name': 'Toluene',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '108-88-3',
            'group_code': 'BTEX',
            'chemical_group': 'VOCs',
            'table_number': 1,
        },
        {
            'analyte_id': 'REG153_VOCS_003',
            'preferred_name': 'Ethylbenzene',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '100-41-4',
            'group_code': 'BTEX',
            'chemical_group': 'VOCs',
            'table_number': 1,
        },
        {
            'analyte_id': 'REG153_VOCS_004',
            'preferred_name': 'Xylenes (total)',
            'analyte_type': AnalyteType.FRACTION_OR_GROUP,
            'cas_number': '1330-20-7',
            'group_code': 'BTEX',
            'chemical_group': 'VOCs',
            'table_number': 1,
        },
        # PHCs
        {
            'analyte_id': 'REG153_PHCS_001',
            'preferred_name': 'PHC F1 (C6-C10)',
            'analyte_type': AnalyteType.FRACTION_OR_GROUP,
            'group_code': 'PHC_F1',
            'chemical_group': 'PHCs',
            'table_number': 4,
        },
        {
            'analyte_id': 'REG153_PHCS_002',
            'preferred_name': 'PHC F2 (C10-C16)',
            'analyte_type': AnalyteType.FRACTION_OR_GROUP,
            'group_code': 'PHC_F2',
            'chemical_group': 'PHCs',
            'table_number': 4,
        },
        {
            'analyte_id': 'REG153_PHCS_003',
            'preferred_name': 'PHC F3 (C16-C34)',
            'analyte_type': AnalyteType.FRACTION_OR_GROUP,
            'group_code': 'PHC_F3',
            'chemical_group': 'PHCs',
            'table_number': 4,
        },
        {
            'analyte_id': 'REG153_PHCS_004',
            'preferred_name': 'PHC F4 (>C34)',
            'analyte_type': AnalyteType.FRACTION_OR_GROUP,
            'group_code': 'PHC_F4',
            'chemical_group': 'PHCs',
            'table_number': 4,
        },
        # Metals
        {
            'analyte_id': 'REG153_METALS_001',
            'preferred_name': 'Arsenic',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '7440-38-2',
            'group_code': 'METAL',
            'chemical_group': 'Metals',
            'table_number': 3,
        },
        {
            'analyte_id': 'REG153_METALS_002',
            'preferred_name': 'Lead',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '7439-92-1',
            'group_code': 'METAL',
            'chemical_group': 'Metals',
            'table_number': 3,
        },
        {
            'analyte_id': 'REG153_METALS_003',
            'preferred_name': 'Chromium',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '7440-47-3',
            'group_code': 'METAL',
            'chemical_group': 'Metals',
            'table_number': 3,
        },
        {
            'analyte_id': 'REG153_METALS_004',
            'preferred_name': 'Chromium VI',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '18540-29-9',
            'group_code': 'METAL',
            'chemical_group': 'Metals',
            'table_number': 3,
        },
        # PAHs
        {
            'analyte_id': 'REG153_PAHS_001',
            'preferred_name': 'Naphthalene',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '91-20-3',
            'group_code': 'PAH',
            'chemical_group': 'PAHs',
            'table_number': 2,
        },
        {
            'analyte_id': 'REG153_PAHS_002',
            'preferred_name': 'Benzo(a)pyrene',
            'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            'cas_number': '50-32-8',
            'group_code': 'PAH',
            'chemical_group': 'PAHs',
            'table_number': 2,
        },
    ]
    
    # Insert analytes
    for data in analytes_data:
        crud.insert_analyte(test_db_session, **data)
    
    test_db_session.commit()
    return test_db_session


@pytest.fixture(scope="function")
def sample_synonyms(preloaded_analytes: Session) -> Session:
    """
    Add sample synonyms to the preloaded analytes database.
    
    Includes exact names, common variants, and lab spellings.
    synonym_norm values are computed via TextNormalizer to match
    the exact normalization used by ExactMatcher/FuzzyMatcher.
    """
    normalizer = TextNormalizer()
    
    # (analyte_id, synonym_raw, synonym_type, confidence)
    # synonym_norm is computed dynamically via normalizer.normalize()
    synonyms_raw = [
        # Benzene synonyms
        ('REG153_VOCS_001', 'Benzene', SynonymType.IUPAC, 1.0),
        ('REG153_VOCS_001', 'benzol', SynonymType.COMMON, 0.95),
        ('REG153_VOCS_001', '71-43-2', SynonymType.COMMON, 1.0),
        
        # Toluene synonyms
        ('REG153_VOCS_002', 'Toluene', SynonymType.IUPAC, 1.0),
        ('REG153_VOCS_002', 'methylbenzene', SynonymType.COMMON, 0.95),
        ('REG153_VOCS_002', 'toluol', SynonymType.COMMON, 0.90),
        
        # Ethylbenzene synonyms
        ('REG153_VOCS_003', 'Ethylbenzene', SynonymType.IUPAC, 1.0),
        ('REG153_VOCS_003', 'ethyl benzene', SynonymType.LAB_VARIANT, 0.98),
        
        # Xylenes synonyms
        ('REG153_VOCS_004', 'Xylenes (total)', SynonymType.IUPAC, 1.0),
        ('REG153_VOCS_004', 'Total Xylenes', SynonymType.LAB_VARIANT, 1.0),
        ('REG153_VOCS_004', 'Xylenes, total', SynonymType.LAB_VARIANT, 1.0),
        
        # PHC synonyms
        ('REG153_PHCS_001', 'PHC F1', SynonymType.FRACTION_NOTATION, 1.0),
        ('REG153_PHCS_001', 'F1', SynonymType.ABBREVIATION, 1.0),
        ('REG153_PHCS_001', 'Fraction 1', SynonymType.COMMON, 1.0),
        ('REG153_PHCS_001', 'C6-C10', SynonymType.FRACTION_NOTATION, 1.0),
        
        ('REG153_PHCS_002', 'PHC F2', SynonymType.FRACTION_NOTATION, 1.0),
        ('REG153_PHCS_002', 'F2', SynonymType.ABBREVIATION, 1.0),
        ('REG153_PHCS_002', 'F2 (C10-C16)', SynonymType.FRACTION_NOTATION, 1.0),
        
        # Metals synonyms
        ('REG153_METALS_001', 'Arsenic', SynonymType.IUPAC, 1.0),
        ('REG153_METALS_001', 'As', SynonymType.ABBREVIATION, 1.0),
        ('REG153_METALS_001', 'Arsenic, total', SynonymType.LAB_VARIANT, 1.0),
        
        ('REG153_METALS_002', 'Lead', SynonymType.IUPAC, 1.0),
        ('REG153_METALS_002', 'Pb', SynonymType.ABBREVIATION, 1.0),
        
        ('REG153_METALS_003', 'Chromium', SynonymType.IUPAC, 1.0),
        ('REG153_METALS_003', 'Cr', SynonymType.ABBREVIATION, 1.0),
        ('REG153_METALS_003', 'Chrome', SynonymType.COMMON, 0.85),
        
        ('REG153_METALS_004', 'Chromium VI', SynonymType.IUPAC, 1.0),
        ('REG153_METALS_004', 'Chromium (VI)', SynonymType.LAB_VARIANT, 1.0),
        ('REG153_METALS_004', 'Hexavalent Chromium', SynonymType.COMMON, 1.0),
        ('REG153_METALS_004', 'Cr(VI)', SynonymType.ABBREVIATION, 1.0),
        
        # PAHs synonyms
        ('REG153_PAHS_001', 'Naphthalene', SynonymType.IUPAC, 1.0),
        ('REG153_PAHS_001', 'Napthalene', SynonymType.LAB_VARIANT, 0.95),
        
        ('REG153_PAHS_002', 'Benzo(a)pyrene', SynonymType.IUPAC, 1.0),
        ('REG153_PAHS_002', 'BaP', SynonymType.ABBREVIATION, 0.95),
        ('REG153_PAHS_002', 'Benzo[a]pyrene', SynonymType.LAB_VARIANT, 1.0),
    ]
    
    # Insert synonyms with computed synonym_norm
    for analyte_id, syn_raw, syn_type, confidence in synonyms_raw:
        syn_norm = normalizer.normalize(syn_raw)
        crud.insert_synonym(
            preloaded_analytes,
            analyte_id=analyte_id,
            synonym_raw=syn_raw,
            synonym_norm=syn_norm,
            synonym_type=syn_type,
            confidence=confidence,
            harvest_source='test_fixture',
        )
    
    preloaded_analytes.commit()
    return preloaded_analytes


# ============================================================================
# NORMALIZATION FIXTURES
# ============================================================================

@pytest.fixture(scope="function")
def text_normalizer() -> TextNormalizer:
    """Fresh text normalizer instance."""
    return TextNormalizer()


@pytest.fixture(scope="function")
def cas_extractor() -> CASExtractor:
    """Fresh CAS extractor instance."""
    return CASExtractor()


@pytest.fixture(scope="function")
def qualifier_handler() -> QualifierHandler:
    """Fresh qualifier handler instance."""
    return QualifierHandler()


@pytest.fixture(scope="function")
def petroleum_handler() -> PetroleumHandler:
    """Fresh petroleum handler instance."""
    return PetroleumHandler()


# ============================================================================
# MATCHING FIXTURES
# ============================================================================

@pytest.fixture(scope="function")
def exact_matcher(text_normalizer, cas_extractor) -> ExactMatcher:
    """Fresh exact matcher instance."""
    return ExactMatcher(normalizer=text_normalizer, cas_extractor=cas_extractor)


@pytest.fixture(scope="function")
def fuzzy_matcher(text_normalizer) -> FuzzyMatcher:
    """Fresh fuzzy matcher instance."""
    return FuzzyMatcher(normalizer=text_normalizer)


@pytest.fixture(scope="function")
def resolution_engine(sample_synonyms, text_normalizer, cas_extractor, exact_matcher, fuzzy_matcher) -> ResolutionEngine:
    """Fresh resolution engine with preloaded test database."""
    return ResolutionEngine(
        db_session=sample_synonyms,
        normalizer=text_normalizer,
        cas_extractor=cas_extractor,
        exact_matcher=exact_matcher,
        fuzzy_matcher=fuzzy_matcher,
    )


# ============================================================================
# TEMPORARY DIRECTORY FIXTURES
# ============================================================================

@pytest.fixture(scope="function")
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test file operations."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


# ============================================================================
# PERFORMANCE TRACKING FIXTURES
# ============================================================================

@pytest.fixture(scope="function")
def performance_tracker():
    """Simple performance tracking for benchmarks."""
    import time
    
    class PerformanceTracker:
        def __init__(self):
            self.measurements = []
        
        def measure(self, func, *args, **kwargs):
            """Measure execution time of a function."""
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.measurements.append(elapsed_ms)
            return result, elapsed_ms
        
        def avg_time(self):
            """Calculate average execution time."""
            return sum(self.measurements) / len(self.measurements) if self.measurements else 0
        
        def max_time(self):
            """Get maximum execution time."""
            return max(self.measurements) if self.measurements else 0
    
    return PerformanceTracker()
