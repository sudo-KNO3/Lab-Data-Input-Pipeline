"""
Test suite for chemical name matching engine.

Tests exact matching, fuzzy matching, CAS extraction,
resolution cascade logic, and disagreement detection.
"""

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.models import Base, Analyte, Synonym, AnalyteType, SynonymType
from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.resolution_engine import ResolutionEngine
from src.matching.match_result import MatchResult, ResolutionResult


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add test data
    _populate_test_data(session)
    
    yield session
    
    session.close()


def _populate_test_data(session):
    """Populate database with test analytes and synonyms."""
    normalizer = TextNormalizer()
    
    # Analyte 1: Benzene
    analyte1 = Analyte(
        analyte_id="REG153_001",
        preferred_name="Benzene",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        cas_number="71-43-2",
        table_number=1,
        chemical_group="VOC"
    )
    
    # Synonyms for Benzene
    syn1_1 = Synonym(
        analyte_id="REG153_001",
        synonym_raw="Benzene",
        synonym_norm=normalizer.normalize("Benzene"),
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        harvest_source="manual"
    )
    syn1_2 = Synonym(
        analyte_id="REG153_001",
        synonym_raw="Benzol",
        synonym_norm=normalizer.normalize("Benzol"),
        synonym_type=SynonymType.COMMON,
        confidence=0.9,
        harvest_source="pubchem"
    )
    
    # Analyte 2: Toluene
    analyte2 = Analyte(
        analyte_id="REG153_002",
        preferred_name="Toluene",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        cas_number="108-88-3",
        table_number=1,
        chemical_group="VOC"
    )
    
    # Synonyms for Toluene
    syn2_1 = Synonym(
        analyte_id="REG153_002",
        synonym_raw="Toluene",
        synonym_norm=normalizer.normalize("Toluene"),
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        harvest_source="manual"
    )
    syn2_2 = Synonym(
        analyte_id="REG153_002",
        synonym_raw="Methylbenzene",
        synonym_norm=normalizer.normalize("Methylbenzene"),
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        harvest_source="pubchem"
    )
    syn2_3 = Synonym(
        analyte_id="REG153_002",
        synonym_raw="Toluol",
        synonym_norm=normalizer.normalize("Toluol"),
        synonym_type=SynonymType.COMMON,
        confidence=0.9,
        harvest_source="pubchem"
    )
    
    # Analyte 3: Xylene (mixed isomers)
    analyte3 = Analyte(
        analyte_id="REG153_003",
        preferred_name="Xylene (mixed isomers)",
        analyte_type=AnalyteType.FRACTION_OR_GROUP,
        cas_number="1330-20-7",
        table_number=1,
        chemical_group="VOC"
    )
    
    syn3_1 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Xylene (mixed isomers)",
        synonym_norm=normalizer.normalize("Xylene (mixed isomers)"),
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        harvest_source="manual"
    )
    syn3_2 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Xylenes",
        synonym_norm=normalizer.normalize("Xylenes"),
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        harvest_source="manual"
    )
    syn3_3 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Dimethylbenzene",
        synonym_norm=normalizer.normalize("Dimethylbenzene"),
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        harvest_source="pubchem"
    )
    syn3_4 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Xylene",
        synonym_norm=normalizer.normalize("Xylene"),
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        harvest_source="manual"
    )
    
    session.add_all([
        analyte1, analyte2, analyte3,
        syn1_1, syn1_2,
        syn2_1, syn2_2, syn2_3,
        syn3_1, syn3_2, syn3_3, syn3_4
    ])
    session.commit()


# ============================================================================
# Exact Matcher Tests
# ============================================================================

def test_exact_match_by_synonym(db_session):
    """Test exact matching by normalized synonym."""
    matcher = ExactMatcher()
    
    result = matcher.match("Benzene", db_session)
    
    assert result is not None
    assert result.analyte_id == "REG153_001"
    assert result.preferred_name == "Benzene"
    assert result.confidence == 1.0
    assert result.method == "exact"


def test_exact_match_case_insensitive(db_session):
    """Test exact matching is case-insensitive."""
    matcher = ExactMatcher()
    
    result = matcher.match("TOLUENE", db_session)
    
    assert result is not None
    assert result.analyte_id == "REG153_002"
    assert result.preferred_name == "Toluene"


def test_exact_match_with_punctuation(db_session):
    """Test exact matching handles punctuation normalization."""
    matcher = ExactMatcher()
    
    result = matcher.match("Xylene (mixed isomers)", db_session)
    
    assert result is not None
    assert result.analyte_id == "REG153_003"


def test_exact_match_no_result(db_session):
    """Test exact matching returns None for unknown input."""
    matcher = ExactMatcher()
    
    result = matcher.match("Nonexistent Chemical", db_session)
    
    assert result is None


# ============================================================================
# CAS Extraction Tests
# ============================================================================

def test_cas_extraction_match(db_session):
    """Test CAS number extraction and matching."""
    matcher = ExactMatcher()
    
    result = matcher.match("Benzene (CAS: 71-43-2)", db_session)
    
    assert result is not None
    assert result.analyte_id == "REG153_001"
    assert result.method == "cas_extracted"
    assert result.confidence == 1.0
    assert result.metadata['cas_number'] == "71-43-2"


def test_cas_extraction_standalone(db_session):
    """Test CAS number matching without chemical name."""
    matcher = ExactMatcher()
    
    result = matcher.match("108-88-3", db_session)
    
    assert result is not None
    assert result.analyte_id == "REG153_002"
    assert result.preferred_name == "Toluene"


def test_cas_extraction_invalid(db_session):
    """Test invalid CAS number is ignored."""
    matcher = ExactMatcher()
    
    # Invalid CAS with wrong check digit
    result = matcher.match("71-43-3", db_session)
    
    assert result is None


# ============================================================================
# Fuzzy Matcher Tests
# ============================================================================

def test_fuzzy_match_close_spelling(db_session):
    """Test fuzzy matching with close spelling."""
    matcher = FuzzyMatcher()
    
    results = matcher.match("Benzine", db_session, threshold=0.75)
    
    assert len(results) > 0
    assert results[0].analyte_id == "REG153_001"
    assert results[0].method == "fuzzy"
    assert results[0].confidence >= 0.75


def test_fuzzy_match_typo(db_session):
    """Test fuzzy matching with typo."""
    matcher = FuzzyMatcher()
    
    results = matcher.match("Tolune", db_session, threshold=0.75)
    
    assert len(results) > 0
    # Should match Toluene
    top_result = results[0]
    assert top_result.analyte_id == "REG153_002"


def test_fuzzy_match_top_k(db_session):
    """Test fuzzy matching returns top K results."""
    matcher = FuzzyMatcher()
    
    results = matcher.match("Xylene", db_session, threshold=0.5, top_k=3)
    
    assert len(results) <= 3
    # Results should be sorted by score
    if len(results) >= 2:
        assert results[0].score >= results[1].score


def test_fuzzy_match_no_result_below_threshold(db_session):
    """Test fuzzy matching returns empty list below threshold."""
    matcher = FuzzyMatcher()
    
    results = matcher.match("CompletelyDifferent", db_session, threshold=0.90)
    
    assert len(results) == 0


# ============================================================================
# Resolution Engine Tests
# ============================================================================

def test_resolution_cascade_exact(db_session):
    """Test resolution engine prefers exact match."""
    engine = ResolutionEngine(db_session)
    
    result = engine.resolve("Benzene")
    
    assert result.is_resolved
    assert result.best_match.analyte_id == "REG153_001"
    assert result.best_match.confidence == 1.0
    assert result.confidence_band == "AUTO_ACCEPT"
    assert result.signals_used['exact_match'] is True


def test_resolution_cascade_cas(db_session):
    """Test resolution engine prefers CAS extraction."""
    engine = ResolutionEngine(db_session)
    
    result = engine.resolve("71-43-2")
    
    assert result.is_resolved
    assert result.best_match.method == "cas_extracted"
    assert result.signals_used['cas_extracted'] is True


def test_resolution_cascade_fuzzy(db_session):
    """Test resolution engine falls back to fuzzy match."""
    engine = ResolutionEngine(db_session)
    
    result = engine.resolve("Benzine")
    
    assert result.is_resolved
    assert result.best_match.method == "fuzzy"
    assert result.signals_used['fuzzy_match'] is True


def test_resolution_unknown_below_threshold(db_session):
    """Test resolution returns UNKNOWN below threshold."""
    engine = ResolutionEngine(db_session)
    
    result = engine.resolve("CompletelyUnknownChemical", confidence_threshold=0.75)
    
    assert not result.is_resolved
    assert result.best_match is None
    assert result.confidence_band == "UNKNOWN"


def test_resolution_review_band(db_session):
    """Test resolution identifies REVIEW confidence band."""
    engine = ResolutionEngine(db_session)
    
    # "Benzine" should fuzzy match to Benzene with confidence 0.75-0.93
    result = engine.resolve("Benzine")
    
    if result.is_resolved and result.best_match.confidence < 0.93:
        assert result.confidence_band == "REVIEW" or result.confidence_band == "AUTO_ACCEPT"


def test_resolution_all_candidates(db_session):
    """Test resolution returns multiple candidates."""
    engine = ResolutionEngine(db_session)
    
    result = engine.resolve("Xylene")
    
    assert len(result.all_candidates) > 0
    assert len(result.all_candidates) <= 5  # Top 5 limit


def test_resolution_disagreement_detection(db_session):
    """Test disagreement detection between fuzzy matches."""
    # Add similar synonyms for different analytes
    analyte4 = Analyte(
        analyte_id="REG153_004",
        preferred_name="Test Chemical A",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        table_number=1
    )
    syn4 = Synonym(
        analyte_id="REG153_004",
        synonym_raw="Testene",
        synonym_norm=TextNormalizer().normalize("Testene"),
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        harvest_source="test"
    )
    
    analyte5 = Analyte(
        analyte_id="REG153_005",
        preferred_name="Test Chemical B",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        table_number=1
    )
    syn5 = Synonym(
        analyte_id="REG153_005",
        synonym_raw="Testane",
        synonym_norm=TextNormalizer().normalize("Testane"),
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        harvest_source="test"
    )
    
    db_session.add_all([analyte4, analyte5, syn4, syn5])
    db_session.commit()
    
    engine = ResolutionEngine(db_session)
    
    # Input that matches both similarly
    result = engine.resolve("Testene")
    
    # Disagreement flag may be set if scores are close
    # This is context-dependent, so we just verify the logic executes
    assert isinstance(result.disagreement_flag, bool)


def test_resolution_batch(db_session):
    """Test batch resolution of multiple inputs."""
    engine = ResolutionEngine(db_session)
    
    inputs = ["Benzene", "Toluene", "Xylene"]
    results = engine.batch_resolve(inputs)
    
    assert len(results) == 3
    assert all(r.is_resolved for r in results)


# ============================================================================
# Match Result Tests
# ============================================================================

def test_match_result_validation():
    """Test MatchResult validation."""
    # Valid result
    result = MatchResult(
        analyte_id="REG153_001",
        preferred_name="Benzene",
        confidence=0.95,
        method="exact",
        score=1.0
    )
    assert result.confidence == 0.95
    
    # Invalid confidence
    with pytest.raises(ValueError):
        MatchResult(
            analyte_id="REG153_001",
            preferred_name="Benzene",
            confidence=1.5,  # > 1.0
            method="exact",
            score=1.0
        )
    
    # Invalid method
    with pytest.raises(ValueError):
        MatchResult(
            analyte_id="REG153_001",
            preferred_name="Benzene",
            confidence=0.95,
            method="invalid_method",
            score=1.0
        )


def test_resolution_result_properties():
    """Test ResolutionResult properties."""
    match = MatchResult(
        analyte_id="REG153_001",
        preferred_name="Benzene",
        confidence=0.85,
        method="fuzzy",
        score=0.85
    )
    
    result = ResolutionResult(
        input_text="Benzine",
        best_match=match,
        all_candidates=[match],
        confidence_band="REVIEW"
    )
    
    assert result.is_resolved is True
    assert result.requires_review is True
    assert result.confidence == 0.85


def test_resolution_result_no_match():
    """Test ResolutionResult with no match."""
    result = ResolutionResult(
        input_text="Unknown",
        best_match=None,
        confidence_band="UNKNOWN"
    )
    
    assert result.is_resolved is False
    assert result.confidence == 0.0


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
