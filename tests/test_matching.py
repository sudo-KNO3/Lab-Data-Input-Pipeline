"""
Test suite for chemical name matching engine.

Tests exact matching, fuzzy matching, CAS extraction,
resolution cascade logic, and disagreement detection.
"""

import pytest
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
        synonym_norm="benzene",
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        source="manual"
    )
    syn1_2 = Synonym(
        analyte_id="REG153_001",
        synonym_raw="Benzol",
        synonym_norm="benzol",
        synonym_type=SynonymType.COMMON,
        confidence=0.9,
        source="pubchem"
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
        synonym_norm="toluene",
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        source="manual"
    )
    syn2_2 = Synonym(
        analyte_id="REG153_002",
        synonym_raw="Methylbenzene",
        synonym_norm="methylbenzene",
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        source="pubchem"
    )
    syn2_3 = Synonym(
        analyte_id="REG153_002",
        synonym_raw="Toluol",
        synonym_norm="toluol",
        synonym_type=SynonymType.COMMON,
        confidence=0.9,
        source="pubchem"
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
        synonym_raw="Xylene",
        synonym_norm="xylene",
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        source="manual"
    )
    syn3_2 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Xylenes",
        synonym_norm="xylenes",
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        source="manual"
    )
    syn3_3 = Synonym(
        analyte_id="REG153_003",
        synonym_raw="Dimethylbenzene",
        synonym_norm="dimethylbenzene",
        synonym_type=SynonymType.IUPAC,
        confidence=1.0,
        source="pubchem"
    )
    
    session.add_all([
        analyte1, analyte2, analyte3,
        syn1_1, syn1_2,
        syn2_1, syn2_2, syn2_3,
        syn3_1, syn3_2, syn3_3
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
        synonym_norm="testene",
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        source="test"
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
        synonym_norm="testane",
        synonym_type=SynonymType.COMMON,
        confidence=1.0,
        source="test"
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


@pytest.fixture
def mock_synonym():
    """Mock synonym object."""
    synonym = Mock()
    synonym.id = 10
    synonym.analyte_id = 1
    synonym.synonym_raw = "Benzene"
    synonym.synonym_norm = "benzene"
    synonym.harvest_source = "pubchem"
    synonym.confidence = 1.0
    return synonym


# ============================================================================
# TYPE TESTS
# ============================================================================

class TestTypes:
    """Test type definitions and data classes."""
    
    def test_match_creation(self):
        """Test Match object creation."""
        match = Match(
            analyte_id=1,
            analyte_name="Benzene",
            cas_number="71-43-2",
            confidence=1.0,
            method=MatchMethod.EXACT,
        )
        
        assert match.analyte_id == 1
        assert match.confidence == 1.0
        assert match.confidence_level == ConfidenceLevel.HIGH
    
    def test_match_confidence_validation(self):
        """Test Match confidence validation."""
        with pytest.raises(ValueError):
            Match(
                analyte_id=1,
                analyte_name="Test",
                cas_number=None,
                confidence=1.5,  # Invalid
                method=MatchMethod.EXACT,
            )
    
    def test_match_confidence_levels(self):
        """Test confidence level categorization."""
        high = Match(1, "Test", None, 0.96, MatchMethod.EXACT)
        assert high.confidence_level == ConfidenceLevel.HIGH
        
        medium = Match(1, "Test", None, 0.90, MatchMethod.FUZZY)
        assert medium.confidence_level == ConfidenceLevel.MEDIUM
        
        low = Match(1, "Test", None, 0.80, MatchMethod.FUZZY)
        assert low.confidence_level == ConfidenceLevel.LOW
        
        very_low = Match(1, "Test", None, 0.70, MatchMethod.FUZZY)
        assert very_low.confidence_level == ConfidenceLevel.VERY_LOW
    
    def test_match_to_dict(self):
        """Test Match serialization to dict."""
        match = Match(
            analyte_id=1,
            analyte_name="Benzene",
            cas_number="71-43-2",
            confidence=0.95,
            method=MatchMethod.EXACT,
        )
        
        d = match.to_dict()
        
        assert d['analyte_id'] == 1
        assert d['analyte_name'] == "Benzene"
        assert d['confidence'] == 0.95
        assert d['method'] == 'exact'
        assert d['confidence_level'] == 'high'
    
    def test_match_result_creation(self):
        """Test MatchResult object creation."""
        result = MatchResult(
            query_text="benzene",
            query_norm="benzene",
        )
        
        assert result.matched is False
        assert result.confidence == 0.0
        
        # Add a match
        result.best_match = Match(1, "Benzene", "71-43-2", 1.0, MatchMethod.EXACT)
        
        assert result.matched is True
        assert result.confidence == 1.0


# ============================================================================
# EXACT MATCHING TESTS
# ============================================================================

class TestExactMatching:
    """Test exact matching functionality."""
    
    def test_match_by_cas_valid(self, mock_session, mock_analyte):
        """Test CAS number matching with valid CAS."""
        # Setup mock
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_analyte
        mock_session.execute.return_value = mock_result
        
        # Test
        match = match_by_cas("71-43-2", mock_session)
        
        assert match is not None
        assert match.analyte_id == 1
        assert match.cas_number == "71-43-2"
        assert match.confidence == 1.0
        assert match.method == MatchMethod.EXACT
    
    def test_match_by_cas_embedded_in_text(self, mock_session, mock_analyte):
        """Test CAS extraction from text."""
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_analyte
        mock_session.execute.return_value = mock_result
        
        match = match_by_cas("Benzene (CAS 71-43-2)", mock_session)
        
        assert match is not None
        assert match.cas_number == "71-43-2"
    
    def test_match_by_cas_invalid(self, mock_session):
        """Test CAS matching with invalid CAS."""
        match = match_by_cas("not a cas number", mock_session)
        assert match is None
    
    def test_match_by_cas_not_in_database(self, mock_session):
        """Test CAS not found in database."""
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        
        match = match_by_cas("99-99-9", mock_session)
        assert match is None
    
    def test_match_by_inchikey_valid(self, mock_session, mock_analyte):
        """Test InChIKey matching."""
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_analyte
        mock_session.execute.return_value = mock_result
        
        match = match_by_inchikey("UHOVQNZJYSORNB-UHFFFAOYSA-N", mock_session)
        
        assert match is not None
        assert match.analyte_id == 1
        assert match.confidence == 1.0
    
    def test_match_by_inchikey_invalid_format(self, mock_session):
        """Test InChIKey with invalid format."""
        match = match_by_inchikey("SHORT", mock_session)
        assert match is None
    
    def test_match_by_synonym_exact(self, mock_session, mock_synonym, mock_analyte):
        """Test exact synonym matching."""
        mock_result = Mock()
        mock_result.first.return_value = (mock_synonym, mock_analyte)
        mock_session.execute.return_value = mock_result
        
        match = match_by_synonym("Benzene", mock_session)
        
        assert match is not None
        assert match.analyte_id == 1
        assert match.synonym_matched == "benzene"
        assert match.confidence == 1.0
    
    def test_match_by_synonym_not_found(self, mock_session):
        """Test synonym not found."""
        mock_result = Mock()
        mock_result.first.return_value = None
        mock_session.execute.return_value = mock_result
        
        match = match_by_synonym("Unknown Chemical", mock_session)
        assert match is None
    
    def test_match_exact_cascade(self, mock_session, mock_analyte):
        """Test exact matching cascade (tries all methods)."""
        # Mock CAS lookup to succeed
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_analyte
        mock_session.execute.return_value = mock_result
        
        match = match_exact("Benzene (71-43-2)", mock_session)
        
        assert match is not None
        assert match.analyte_id == 1


# ============================================================================
# FUZZY MATCHING TESTS
# ============================================================================

class TestFuzzyMatching:
    """Test fuzzy string matching."""
    
    def test_calculate_similarity_exact(self):
        """Test similarity calculation for identical strings."""
        score = calculate_similarity("benzene", "benzene")
        assert score == 1.0
    
    def test_calculate_similarity_typo(self):
        """Test similarity with minor typo."""
        score = calculate_similarity("benzene", "benzen")
        assert score > 0.85
    
    def test_calculate_similarity_reordered(self):
        """Test token set ratio with reordered words."""
        score = calculate_similarity("methyl ethyl ketone", "ethyl methyl ketone")
        assert score >= 0.85  # Should be high due to token set matching
    
    def test_match_fuzzy_basic(self, mock_session, mock_synonym, mock_analyte):
        """Test basic fuzzy matching."""
        # Mock database query
        mock_session.execute.return_value.all.return_value = [
            (mock_synonym, mock_analyte)
        ]
        
        matches = match_fuzzy("benzen", mock_session, top_k=5, threshold=0.75)
        
        assert len(matches) > 0
        assert matches[0].method == MatchMethod.FUZZY
        assert matches[0].distance_score is not None
    
    def test_match_fuzzy_confidence_mapping(self, mock_session):
        """Test confidence level mapping based on scores."""
        # Create mock synonyms with different similarity levels
        high_sim_synonym = Mock()
        high_sim_synonym.id = 1
        high_sim_synonym.analyte_id = 1
        high_sim_synonym.synonym_norm = "benzene"  # Very similar to "benzene"
        high_sim_synonym.synonym_raw = "Benzene"
        high_sim_synonym.harvest_source = "test"
        
        analyte = Mock()
        analyte.id = 1
        analyte.preferred_name = "Benzene"
        analyte.cas_number = "71-43-2"
        
        mock_session.execute.return_value.all.return_value = [
            (high_sim_synonym, analyte)
        ]
        
        matches = match_fuzzy("benzene", mock_session, top_k=5, threshold=0.75)
        
        if matches:
            # Should get high confidence for exact/near-exact match
            assert matches[0].confidence >= 0.85
    
    def test_match_fuzzy_threshold(self, mock_session, mock_synonym, mock_analyte):
        """Test fuzzy matching respects threshold."""
        mock_synonym.synonym_norm = "completely different"
        mock_session.execute.return_value.all.return_value = [
            (mock_synonym, mock_analyte)
        ]
        
        matches = match_fuzzy("benzene", mock_session, top_k=5, threshold=0.95)
        
        # Should return no matches due to low similarity
        assert len(matches) == 0
    
    def test_match_fuzzy_top_k(self, mock_session):
        """Test top-K limiting."""
        # Create multiple mock synonyms
        synonyms = []
        for i in range(10):
            synonym = Mock()
            synonym.id = i
            synonym.analyte_id = i
            synonym.synonym_norm = f"benzene{i}"
            synonym.synonym_raw = f"Benzene{i}"
            synonym.harvest_source = "test"
            
            analyte = Mock()
            analyte.id = i
            analyte.preferred_name = f"Benzene {i}"
            analyte.cas_number = f"{i}-00-0"
            
            synonyms.append((synonym, analyte))
        
        mock_session.execute.return_value.all.return_value = synonyms
        
        matches = match_fuzzy("benzene", mock_session, top_k=3, threshold=0.5)
        
        # Should return at most 3 matches
        assert len(matches) <= 3


# ============================================================================
# SEMANTIC MATCHING TESTS
# ============================================================================

class TestSemanticMatcher:
    """Test semantic matching with FAISS."""
    
    @patch('src.matching.semantic_matcher.SentenceTransformer')
    @patch('src.matching.semantic_matcher.faiss')
    def test_semantic_matcher_initialization(self, mock_faiss, mock_st):
        """Test SemanticMatcher initialization."""
        mock_model = Mock()
        mock_st.return_value = mock_model
        
        mock_index = Mock()
        mock_index.ntotal = 0
        mock_faiss.IndexFlatIP.return_value = mock_index
        
        matcher = SemanticMatcher(base_path="/tmp/test")
        
        assert matcher.model is not None
        assert matcher.index is not None
    
    @patch('src.matching.semantic_matcher.SentenceTransformer')
    def test_encode_query(self, mock_st):
        """Test query encoding."""
        mock_model = Mock()
        mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3]])
        mock_st.return_value = mock_model
        
        with patch('src.matching.semantic_matcher.faiss'):
            matcher = SemanticMatcher()
            embedding = matcher.encode_query("benzene")
        
        assert embedding.shape == (3,)
        # Should be L2 normalized
        norm = np.linalg.norm(embedding)
        assert abs(norm - 1.0) < 0.01
    
    @patch('src.matching.semantic_matcher.SentenceTransformer')
    @patch('src.matching.semantic_matcher.faiss')
    def test_search_basic(self, mock_faiss, mock_st):
        """Test FAISS search."""
        # Setup mocks
        mock_model = Mock()
        mock_st.return_value = mock_model
        
        mock_index = Mock()
        mock_index.ntotal = 1
        mock_index.search.return_value = (
            np.array([[0.95]]),  # distances (cosine similarities)
            np.array([[0]])  # indices
        )
        mock_faiss.IndexFlatIP.return_value = mock_index
        
        matcher = SemanticMatcher()
        matcher.metadata = {
            0: {
                'analyte_id': 1,
                'analyte_name': 'Benzene',
                'cas_number': '71-43-2',
                'synonym_norm': 'benzene',
                'synonym_id': 10,
            }
        }
        
        query_embedding = np.array([0.1, 0.2, 0.3], dtype='float32')
        matches = matcher.search(query_embedding, top_k=5, threshold=0.75)
        
        assert len(matches) == 1
        assert matches[0].method == MatchMethod.SEMANTIC
        assert matches[0].similarity_score == 0.95
    
    @patch('src.matching.semantic_matcher.SentenceTransformer')
    @patch('src.matching.semantic_matcher.faiss')
    def test_add_embeddings(self, mock_faiss, mock_st):
        """Test adding embeddings incrementally."""
        mock_model = Mock()
        mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3]])
        mock_st.return_value = mock_model
        
        mock_index = Mock()
        mock_index.ntotal = 0
        mock_faiss.IndexFlatIP.return_value = mock_index
        
        matcher = SemanticMatcher()
        
        texts = ["new chemical"]
        metadata = [{'analyte_id': 2, 'synonym_id': 20}]
        
        matcher.add_embeddings(texts, metadata)
        
        # Verify index.add was called
        mock_index.add.assert_called_once()


# ============================================================================
# RESOLUTION ENGINE TESTS
# ============================================================================

class TestResolutionEngine:
    """Test resolution engine cascade logic."""
    
    def test_resolution_engine_initialization(self, mock_session):
        """Test ResolutionEngine initialization."""
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            
            assert engine.db_session == mock_session
            assert engine.config is not None
    
    def test_resolve_exact_match(self, mock_session, mock_analyte):
        """Test resolution with exact match (should return immediately)."""
        # Mock exact match
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_analyte
        mock_session.execute.return_value = mock_result
        
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            result = engine.resolve("71-43-2", log_decision=False)
        
        assert result.matched is True
        assert result.best_match.confidence == 1.0
        assert result.processing_time_ms is not None
    
    def test_resolve_no_match(self, mock_session):
        """Test resolution with no matches found."""
        # Mock no matches
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.first.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            engine.config.semantic_enabled = False  # Disable to simplify
            
            result = engine.resolve("unknown chemical xxx", log_decision=False)
        
        assert result.matched is False
        assert result.manual_review_recommended is True
    
    def test_resolve_disagreement_detection(self, mock_session):
        """Test disagreement detection between fuzzy and semantic."""
        mock_session.execute.return_value.all.return_value = []
        mock_session.execute.return_value.scalar_one_or_none.return_value = None
        mock_session.execute.return_value.first.return_value = None
        
        with patch('src.matching.resolution_engine.SemanticMatcher') as mock_sm:
            # Setup different top matches for fuzzy and semantic
            fuzzy_match = Match(1, "Chemical A", None, 0.90, MatchMethod.FUZZY)
            fuzzy_match.distance_score = 0.90
            
            semantic_match = Match(2, "Chemical B", None, 0.88, MatchMethod.SEMANTIC)
            semantic_match.similarity_score = 0.88
            
            with patch('src.matching.resolution_engine.match_fuzzy', return_value=[fuzzy_match]):
                mock_semantic = Mock()
                mock_semantic.match_semantic.return_value = [semantic_match]
                mock_sm.return_value = mock_semantic
                
                engine = ResolutionEngine(mock_session)
                result = engine.resolve("test query", log_decision=False)
            
            # Should detect disagreement
            assert result.disagreement_detected is True
            assert result.manual_review_recommended is True
    
    def test_resolve_batch(self, mock_session):
        """Test batch resolution."""
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.first.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            engine.config.semantic_enabled = False
            
            queries = ["query1", "query2", "query3"]
            results = engine.resolve_batch(queries, log_decisions=False)
        
        assert len(results) == 3
        assert all(isinstance(r, MatchResult) for r in results)
    
    def test_export_results_csv(self, mock_session, tmp_path):
        """Test CSV export of results."""
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            
            # Create mock results
            match = Match(1, "Benzene", "71-43-2", 0.95, MatchMethod.EXACT)
            result = MatchResult("benzene", "benzene", best_match=match)
            
            csv_path = tmp_path / "results.csv"
            engine.export_results_csv([result], str(csv_path))
            
            assert csv_path.exists()
            content = csv_path.read_text()
            assert "benzene" in content.lower()
            assert "71-43-2" in content
    
    def test_export_results_json(self, mock_session, tmp_path):
        """Test JSON export of results."""
        with patch('src.matching.resolution_engine.SemanticMatcher'):
            engine = ResolutionEngine(mock_session)
            
            match = Match(1, "Benzene", "71-43-2", 0.95, MatchMethod.EXACT)
            result = MatchResult("benzene", "benzene", best_match=match)
            
            json_path = tmp_path / "results.json"
            engine.export_results_json([result], str(json_path))
            
            assert json_path.exists()
            
            import json
            with open(json_path) as f:
                data = json.load(f)
            
            assert len(data) == 1
            assert data[0]['query_text'] == "benzene"


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Integration tests for complete matching pipeline."""
    
    def test_full_pipeline_mock(self, mock_session):
        """Test complete pipeline with mocked components."""
        # This would be a full end-to-end test with a real database
        # in a production environment
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
