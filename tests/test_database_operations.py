"""
Tests for database CRUD operations.

Tests:
- Analyte insertion and retrieval
- Synonym insertion and querying
- Match decision logging
- Validated decision retrieval
- Bulk operations
"""

import pytest
from datetime import datetime, date
from sqlalchemy.exc import IntegrityError

from src.database import crud_new as crud
from src.database.models import (
    Analyte, Synonym, LabVariant, MatchDecision,
    AnalyteType, SynonymType, ValidationConfidence,
)
from tests.fixtures.test_data import CRUD_TEST_ANALYTE, CRUD_TEST_SYNONYM


# ============================================================================
# ANALYTE CRUD TESTS
# ============================================================================

class TestAnalyteCRUD:
    """Tests for Analyte CRUD operations."""
    
    def test_insert_analyte_basic(self, test_db_session):
        """Test basic analyte insertion."""
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        
        test_db_session.commit()
        
        assert analyte.analyte_id == "TEST_001"
        assert analyte.preferred_name == "Test Chemical"
        assert analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE
    
    def test_insert_analyte_with_cas(self, test_db_session):
        """Test analyte insertion with CAS number."""
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_002",
            preferred_name="Test Chemical 2",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="123-45-6",
        )
        
        test_db_session.commit()
        
        assert analyte.cas_number == "123-45-6"
    
    def test_insert_analyte_duplicate_id(self, test_db_session):
        """Test that duplicate analyte_id raises error."""
        crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        test_db_session.commit()
        
        # Try to insert duplicate
        with pytest.raises(IntegrityError):
            crud.insert_analyte(
                test_db_session,
                analyte_id="TEST_001",
                preferred_name="Different Name",
                analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            )
            test_db_session.commit()
    
    def test_get_analyte_by_id(self, test_db_session):
        """Test retrieving analyte by ID."""
        crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        test_db_session.commit()
        
        analyte = crud.get_analyte_by_id(test_db_session, "TEST_001")
        
        assert analyte is not None
        assert analyte.analyte_id == "TEST_001"
        assert analyte.preferred_name == "Test Chemical"
    
    def test_get_analyte_by_cas(self, test_db_session):
        """Test retrieving analyte by CAS number."""
        crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="123-45-6",
        )
        test_db_session.commit()
        
        analyte = crud.get_analyte_by_cas(test_db_session, "123-45-6")
        
        assert analyte is not None
        assert analyte.cas_number == "123-45-6"
    
    def test_get_all_analytes(self, preloaded_analytes):
        """Test retrieving all analytes."""
        analytes = crud.get_all_analytes(preloaded_analytes)
        
        assert len(analytes) > 0
        # Should have the preloaded analytes
        assert any(a.analyte_id == "REG153_VOCS_001" for a in analytes)
    
    def test_update_analyte(self, test_db_session):
        """Test updating an analyte."""
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        test_db_session.commit()
        
        # Update the analyte
        analyte.preferred_name = "Updated Name"
        test_db_session.commit()
        
        # Retrieve and verify
        updated = crud.get_analyte_by_id(test_db_session, "TEST_001")
        assert updated.preferred_name == "Updated Name"
    
    def test_delete_analyte(self, test_db_session):
        """Test deleting an analyte."""
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="TEST_001",
            preferred_name="Test Chemical",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        test_db_session.commit()
        
        # Delete the analyte
        test_db_session.delete(analyte)
        test_db_session.commit()
        
        # Verify deletion
        deleted = crud.get_analyte_by_id(test_db_session, "TEST_001")
        assert deleted is None


# ============================================================================
# SYNONYM CRUD TESTS
# ============================================================================

class TestSynonymCRUD:
    """Tests for Synonym CRUD operations."""
    
    def test_insert_synonym(self, preloaded_analytes):
        """Test synonym insertion."""
        synonym = crud.insert_synonym(
            preloaded_analytes,
            analyte_id="REG153_VOCS_001",
            synonym_raw="Test Synonym",
            synonym_norm="test synonym",
            synonym_type=SynonymType.COMMON,
            confidence_score=0.95,
            source="test",
        )
        
        preloaded_analytes.commit()
        
        assert synonym.analyte_id == "REG153_VOCS_001"
        assert synonym.synonym_raw == "Test Synonym"
        assert synonym.synonym_norm == "test synonym"
        assert synonym.confidence_score == 0.95
    
    def test_get_synonyms_by_analyte(self, sample_synonyms):
        """Test retrieving synonyms for an analyte."""
        synonyms = crud.get_synonyms_by_analyte(sample_synonyms, "REG153_VOCS_001")
        
        assert len(synonyms) > 0
        for syn in synonyms:
            assert syn.analyte_id == "REG153_VOCS_001"
    
    def test_search_synonym(self, sample_synonyms):
        """Test searching for a synonym."""
        synonym = crud.search_synonym(sample_synonyms, "benzene")
        
        assert synonym is not None
        assert synonym.analyte_id == "REG153_VOCS_001"
    
    def test_bulk_insert_synonyms(self, preloaded_analytes):
        """Test bulk synonym insertion."""
        synonyms_data = [
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_raw': 'Syn1',
                'synonym_norm': 'syn1',
                'synonym_type': SynonymType.COMMON,
                'confidence_score': 0.9,
                'source': 'test',
            },
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_raw': 'Syn2',
                'synonym_norm': 'syn2',
                'synonym_type': SynonymType.COMMON,
                'confidence_score': 0.85,
                'source': 'test',
            },
        ]
        
        for data in synonyms_data:
            crud.insert_synonym(preloaded_analytes, **data)
        
        preloaded_analytes.commit()
        
        # Verify bulk insertion
        synonyms = crud.get_synonyms_by_analyte(preloaded_analytes, "REG153_VOCS_001")
        syn_raws = [s.synonym_raw for s in synonyms]
        assert 'Syn1' in syn_raws
        assert 'Syn2' in syn_raws
    
    def test_delete_synonym(self, sample_synonyms):
        """Test synonym deletion."""
        # Get a synonym
        synonym = crud.search_synonym(sample_synonyms, "benzene")
        assert synonym is not None
        
        syn_id = synonym.id
        
        # Delete it
        sample_synonyms.delete(synonym)
        sample_synonyms.commit()
        
        # Verify deletion
        deleted = sample_synonyms.query(Synonym).filter(Synonym.id == syn_id).first()
        assert deleted is None


# ============================================================================
# LAB VARIANT CRUD TESTS
# ============================================================================

class TestLabVariantCRUD:
    """Tests for LabVariant CRUD operations."""
    
    def test_insert_lab_variant(self, preloaded_analytes):
        """Test lab variant insertion."""
        variant = crud.insert_lab_variant(
            preloaded_analytes,
            lab_variant="Benzene (test lab)",
            analyte_id="REG153_VOCS_001",
            validation_confidence=ValidationConfidence.HIGH,
            lab_name="Test Lab",
        )
        
        preloaded_analytes.commit()
        
        assert variant.lab_variant == "Benzene (test lab)"
        assert variant.analyte_id == "REG153_VOCS_001"
        assert variant.validation_confidence == ValidationConfidence.HIGH
    
    def test_get_lab_variants_by_analyte(self, preloaded_analytes):
        """Test retrieving lab variants for an analyte."""
        # Insert a variant first
        crud.insert_lab_variant(
            preloaded_analytes,
            lab_variant="Benzene (test)",
            analyte_id="REG153_VOCS_001",
            validation_confidence=ValidationConfidence.HIGH,
        )
        preloaded_analytes.commit()
        
        variants = crud.get_lab_variants_by_analyte(preloaded_analytes, "REG153_VOCS_001")
        
        assert len(variants) > 0
        for variant in variants:
            assert variant.analyte_id == "REG153_VOCS_001"
    
    def test_search_lab_variant(self, preloaded_analytes):
        """Test searching for a lab variant."""
        # Insert a variant
        crud.insert_lab_variant(
            preloaded_analytes,
            lab_variant="Benzene Test",
            analyte_id="REG153_VOCS_001",
            validation_confidence=ValidationConfidence.HIGH,
        )
        preloaded_analytes.commit()
        
        variant = crud.search_lab_variant(preloaded_analytes, "Benzene Test")
        
        assert variant is not None
        assert variant.lab_variant == "Benzene Test"


# ============================================================================
# MATCH DECISION CRUD TESTS
# ============================================================================

class TestMatchDecisionCRUD:
    """Tests for MatchDecision CRUD operations."""
    
    def test_insert_match_decision(self, preloaded_analytes):
        """Test match decision insertion."""
        decision = crud.insert_match_decision(
            preloaded_analytes,
            lab_variant="Benzene Test",
            analyte_id="REG153_VOCS_001",
            confidence=0.95,
            method="exact",
            validated=True,
            reviewer="test_user",
        )
        
        preloaded_analytes.commit()
        
        assert decision.lab_variant == "Benzene Test"
        assert decision.analyte_id == "REG153_VOCS_001"
        assert decision.confidence == 0.95
        assert decision.validated is True
    
    def test_get_validated_decisions(self, preloaded_analytes):
        """Test retrieving validated decisions."""
        # Insert some decisions
        crud.insert_match_decision(
            preloaded_analytes,
            lab_variant="Benzene Test 1",
            analyte_id="REG153_VOCS_001",
            confidence=0.95,
            method="exact",
            validated=True,
        )
        crud.insert_match_decision(
            preloaded_analytes,
            lab_variant="Benzene Test 2",
            analyte_id="REG153_VOCS_001",
            confidence=0.85,
            method="fuzzy",
            validated=False,
        )
        preloaded_analytes.commit()
        
        validated = crud.get_validated_decisions(preloaded_analytes)
        
        assert len(validated) > 0
        for decision in validated:
            assert decision.validated is True
    
    def test_get_decisions_by_analyte(self, preloaded_analytes):
        """Test retrieving decisions for an analyte."""
        crud.insert_match_decision(
            preloaded_analytes,
            lab_variant="Benzene Test",
            analyte_id="REG153_VOCS_001",
            confidence=0.95,
            method="exact",
            validated=True,
        )
        preloaded_analytes.commit()
        
        decisions = crud.get_match_decisions_by_analyte(preloaded_analytes, "REG153_VOCS_001")
        
        assert len(decisions) > 0
        for decision in decisions:
            assert decision.analyte_id == "REG153_VOCS_001"
    
    def test_update_match_decision_validation(self, preloaded_analytes):
        """Test updating a match decision validation status."""
        decision = crud.insert_match_decision(
            preloaded_analytes,
            lab_variant="Benzene Test",
            analyte_id="REG153_VOCS_001",
            confidence=0.85,
            method="fuzzy",
            validated=False,
        )
        preloaded_analytes.commit()
        
        # Update validation
        decision.validated = True
        decision.reviewer = "test_reviewer"
        decision.review_date = datetime.utcnow()
        preloaded_analytes.commit()
        
        # Verify update
        updated = preloaded_analytes.query(MatchDecision).filter(
            MatchDecision.id == decision.id
        ).first()
        
        assert updated.validated is True
        assert updated.reviewer == "test_reviewer"


# ============================================================================
# BULK OPERATIONS TESTS
# ============================================================================

class TestBulkOperations:
    """Tests for bulk database operations."""
    
    def test_bulk_insert_analytes(self, test_db_session):
        """Test bulk analyte insertion."""
        analytes_data = [
            {
                'analyte_id': f'BULK_TEST_{i:03d}',
                'preferred_name': f'Bulk Test Chemical {i}',
                'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
            }
            for i in range(10)
        ]
        
        for data in analytes_data:
            crud.insert_analyte(test_db_session, **data)
        
        test_db_session.commit()
        
        # Verify all inserted
        for i in range(10):
            analyte = crud.get_analyte_by_id(test_db_session, f'BULK_TEST_{i:03d}')
            assert analyte is not None
    
    def test_bulk_insert_synonyms(self, preloaded_analytes):
        """Test bulk synonym insertion."""
        synonyms_data = [
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_raw': f'Bulk Synonym {i}',
                'synonym_norm': f'bulk synonym {i}',
                'synonym_type': SynonymType.COMMON,
                'confidence_score': 0.9,
                'source': 'bulk_test',
            }
            for i in range(20)
        ]
        
        for data in synonyms_data:
            crud.insert_synonym(preloaded_analytes, **data)
        
        preloaded_analytes.commit()
        
        # Verify count
        synonyms = crud.get_synonyms_by_analyte(preloaded_analytes, "REG153_VOCS_001")
        bulk_synonyms = [s for s in synonyms if 'Bulk Synonym' in s.synonym_raw]
        assert len(bulk_synonyms) == 20
    
    def test_bulk_query_performance(self, sample_synonyms):
        """Test bulk query performance."""
        import time
        
        analyte_ids = [
            "REG153_VOCS_001",
            "REG153_VOCS_002",
            "REG153_VOCS_003",
            "REG153_METALS_001",
            "REG153_METALS_002",
        ]
        
        start = time.perf_counter()
        
        for analyte_id in analyte_ids:
            analyte = crud.get_analyte_by_id(sample_synonyms, analyte_id)
            assert analyte is not None
        
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        # Should be fast (< 50ms for 5 queries in memory db)
        assert elapsed_ms < 100, f"Bulk queries too slow: {elapsed_ms:.2f}ms"


# ============================================================================
# TRANSACTION AND ROLLBACK TESTS
# ============================================================================

class TestTransactions:
    """Tests for transaction handling."""
    
    def test_commit_transaction(self, test_db_session):
        """Test successful transaction commit."""
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="TRANS_001",
            preferred_name="Transaction Test",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        
        test_db_session.commit()
        
        # Verify persistence
        retrieved = crud.get_analyte_by_id(test_db_session, "TRANS_001")
        assert retrieved is not None
    
    def test_rollback_transaction(self, test_db_session):
        """Test transaction rollback."""
        crud.insert_analyte(
            test_db_session,
            analyte_id="ROLLBACK_001",
            preferred_name="Rollback Test",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        
        # Rollback instead of commit
        test_db_session.rollback()
        
        # Should not be in database
        retrieved = crud.get_analyte_by_id(test_db_session, "ROLLBACK_001")
        assert retrieved is None
    
    def test_cascade_delete(self, test_db_session):
        """Test cascade deletion of analyte with synonyms."""
        # Insert analyte
        analyte = crud.insert_analyte(
            test_db_session,
            analyte_id="CASCADE_001",
            preferred_name="Cascade Test",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        )
        
        # Insert synonym
        crud.insert_synonym(
            test_db_session,
            analyte_id="CASCADE_001",
            synonym_raw="Cascade Synonym",
            synonym_norm="cascade synonym",
            synonym_type=SynonymType.COMMON,
            confidence_score=0.9,
            source="test",
        )
        
        test_db_session.commit()
        
        # Delete analyte (should cascade to synonyms)
        test_db_session.delete(analyte)
        test_db_session.commit()
        
        # Verify both are deleted
        assert crud.get_analyte_by_id(test_db_session, "CASCADE_001") is None
        assert crud.search_synonym(test_db_session, "cascade synonym") is None


# ============================================================================
# QUERY FILTER TESTS
# ============================================================================

class TestQueryFilters:
    """Tests for database query filtering."""
    
    def test_filter_by_chemical_group(self, preloaded_analytes):
        """Test filtering analytes by chemical group."""
        vocs = crud.get_analytes_by_group(preloaded_analytes, "VOCs")
        
        assert len(vocs) > 0
        for analyte in vocs:
            assert analyte.chemical_group == "VOCs"
    
    def test_filter_by_analyte_type(self, preloaded_analytes):
        """Test filtering by analyte type."""
        single_substances = crud.get_analytes_by_type(
            preloaded_analytes,
            AnalyteType.SINGLE_SUBSTANCE
        )
        
        assert len(single_substances) > 0
        for analyte in single_substances:
            assert analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE
    
    def test_search_analyte_by_name(self, preloaded_analytes):
        """Test searching analytes by name."""
        results = crud.search_analytes_by_name(preloaded_analytes, "Benzene")
        
        assert len(results) > 0
        assert any("Benzene" in a.preferred_name for a in results)
