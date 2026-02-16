"""
Quick verification test for database module.

Tests basic functionality of all components.
Run with: python -m pytest tests/test_database_quick.py -v
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
from src.database import (
    create_test_db,
    Analyte,
    Synonym,
    LabVariant,
    MatchDecision,
    SynonymType,
)
from src.database.crud import (
    create_analyte,
    create_synonym,
    create_lab_variant,
    create_match_decision,
    get_analyte_by_cas,
    get_synonyms_for_analyte,
    bulk_insert_synonyms,
    synonym_exists,
    count_analytes,
    delete_analyte,
)


@pytest.fixture
def db():
    """Create in-memory database for testing."""
    database = create_test_db()
    yield database
    database.close()


def test_analyte_creation(db):
    """Test basic analyte CRUD operations."""
    with db.session_scope() as session:
        # Create
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_001",
            cas_number="67-64-1",
            preferred_name="Acetone",
            analyte_type="single_substance",
            molecular_formula="C3H6O",
        )
        
        assert analyte.analyte_id == "REG153_TEST_001"
        assert analyte.cas_number == "67-64-1"
        assert analyte.preferred_name == "Acetone"
        
    # Verify persistence
    with db.session_scope() as session:
        retrieved = get_analyte_by_cas(session, "67-64-1")
        assert retrieved is not None
        assert retrieved.preferred_name == "Acetone"
        assert retrieved.molecular_formula == "C3H6O"


def test_synonym_operations(db):
    """Test synonym creation and queries."""
    with db.session_scope() as session:
        # Create analyte
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_002",
            cas_number="71-43-2",
            preferred_name="Benzene",
            analyte_type="single_substance",
        )
        
        # Create synonyms
        create_synonym(
            session,
            analyte_id=analyte.analyte_id,
            synonym_raw="Benzol",
            synonym_norm="benzol",
            harvest_source="pubchem",
            confidence=0.9,
        )
        
        create_synonym(
            session,
            analyte_id=analyte.analyte_id,
            synonym_raw="Phenyl hydride",
            synonym_norm="phenyl hydride",
            harvest_source="echa",
            confidence=0.85,
        )
        
    # Query synonyms
    with db.session_scope() as session:
        synonyms = get_synonyms_for_analyte(session, "REG153_TEST_002")
        assert len(synonyms) == 2
        
        # Check ordering (by confidence desc)
        assert synonyms[0].confidence == 0.9
        assert synonyms[1].confidence == 0.85
        
        # Test existence check
        exists = synonym_exists(session, "REG153_TEST_002", "benzol")
        assert exists is True
        
        not_exists = synonym_exists(session, "REG153_TEST_002", "nonexistent")
        assert not_exists is False


def test_bulk_synonym_insert(db):
    """Test bulk insertion of synonyms."""
    with db.session_scope() as session:
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_003",
            cas_number="108-88-3",
            preferred_name="Toluene",
            analyte_type="single_substance",
        )
        
        # Prepare bulk data
        synonyms_data = [
            {
                "analyte_id": analyte.analyte_id,
                "synonym_raw": f"Synonym {i}",
                "synonym_norm": f"synonym {i}",
                "synonym_type": SynonymType.COMMON,
                "harvest_source": "test",
                "confidence": 0.8,
            }
            for i in range(100)
        ]
        
        # Bulk insert
        count = bulk_insert_synonyms(session, synonyms_data, chunk_size=25)
        assert count == 100
        
    # Verify
    with db.session_scope() as session:
        synonyms = get_synonyms_for_analyte(session, "REG153_TEST_003")
        assert len(synonyms) == 100


def test_lab_variant(db):
    """Test lab variant operations."""
    with db.session_scope() as session:
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_004",
            cas_number="67-64-1",
            preferred_name="Acetone",
            analyte_type="single_substance",
        )
        
        variant = create_lab_variant(
            session,
            validated_match_id=analyte.analyte_id,
            lab_vendor="ALS Canada",
            observed_text="Acetone (2-Propanone)",
            method="EPA 8260D",
            matrix="Soil",
            units="µg/kg",
            frequency_count=15,
        )
        
        assert variant.id is not None
        assert variant.lab_vendor == "ALS Canada"
        assert variant.frequency_count == 15
        assert variant.observed_text == "Acetone (2-Propanone)"


def test_match_decision(db):
    """Test match decision recording."""
    with db.session_scope() as session:
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_005",
            cas_number="67-64-1",
            preferred_name="Acetone",
            analyte_type="single_substance",
        )
        
        decision = create_match_decision(
            session,
            input_text="acetone",
            matched_analyte_id=analyte.analyte_id,
            match_method="exact",
            confidence_score=0.98,
            top_k_candidates=[
                {"rank": 1, "analyte_id": analyte.analyte_id, "name": "Acetone", "score": 0.98}
            ],
            signals_used={
                "exact_match": 1.0,
                "levenshtein": 0.95,
            },
            corpus_snapshot_hash="test_v1",
            model_hash="model_v1",
            disagreement_flag=False,
        )
        
        assert decision.id is not None
        assert decision.confidence_score == 0.98
        assert len(decision.top_k_candidates) == 1
        assert "exact_match" in decision.signals_used


def test_relationships(db):
    """Test model relationships."""
    with db.session_scope() as session:
        # Create analyte with related records
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_006",
            cas_number="67-64-1",
            preferred_name="Acetone",
            analyte_type="single_substance",
        )
        
        create_synonym(
            session,
            analyte_id=analyte.analyte_id,
            synonym_raw="Propanone",
            synonym_norm="propanone",
            harvest_source="test",
            confidence=1.0,
        )
        
        create_lab_variant(
            session,
            validated_match_id=analyte.analyte_id,
            lab_vendor="Test Lab",
            observed_text="Acetone",
            frequency_count=1,
        )
        
    # Test relationships
    with db.session_scope() as session:
        analyte = get_analyte_by_cas(session, "67-64-1")
        
        # Access relationships
        assert len(analyte.synonyms) == 1
        assert len(analyte.lab_variants) == 1
        
        assert analyte.synonyms[0].synonym_norm == "propanone"
        assert analyte.lab_variants[0].lab_vendor == "Test Lab"


def test_cascade_delete(db):
    """Test cascade deletion of related records."""
    with db.session_scope() as session:
        analyte = create_analyte(
            session,
            analyte_id="REG153_TEST_007",
            cas_number="999-99-9",
            preferred_name="Test Chemical",
            analyte_type="single_substance",
        )
        
        aid = analyte.analyte_id
        
        create_synonym(
            session,
            analyte_id=aid,
            synonym_raw="Test",
            synonym_norm="test",
            harvest_source="test",
            confidence=1.0,
        )
        
    with db.session_scope() as session:
        # Delete analyte
        deleted = delete_analyte(session, aid)
        assert deleted is True
        
    with db.session_scope() as session:
        # Verify synonyms were also deleted
        synonyms = get_synonyms_for_analyte(session, aid)
        assert len(synonyms) == 0


def test_counts(db):
    """Test counting functions."""
    with db.session_scope() as session:
        # Create multiple analytes
        for i in range(5):
            create_analyte(
                session,
                analyte_id=f"REG153_COUNT_{i:03d}",
                cas_number=f"{i}-00-0",
                preferred_name=f"Analyte {i}",
                analyte_type="single_substance" if i < 3 else "fraction_or_group",
            )
        
        total = count_analytes(session)
        assert total == 5
        
        substances = count_analytes(session, analyte_type="single_substance")
        assert substances == 3
        
        fractions = count_analytes(session, analyte_type="fraction_or_group")
        assert fractions == 2


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
