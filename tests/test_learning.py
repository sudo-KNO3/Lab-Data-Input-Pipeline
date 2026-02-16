"""
Tests for the learning infrastructure.

Tests synonym ingestion, threshold calibration, variant clustering,
and incremental embedding updates.
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import Base, Analyte, Synonym, MatchDecision, SynonymType, AnalyteType
from src.learning.synonym_ingestion import SynonymIngestor
from src.learning.threshold_calibrator import ThresholdCalibrator
from src.learning.variant_clustering import VariantClusterer
from src.utils.config_manager import ConfigManager


# Fixtures

@pytest.fixture
def db_session():
    """Create an in-memory database session for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    # Add test analytes
    test_analytes = [
        Analyte(
            analyte_id="REG153_001",
            preferred_name="Benzene",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="71-43-2"
        ),
        Analyte(
            analyte_id="REG153_002",
            preferred_name="Toluene",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="108-88-3"
        ),
        Analyte(
            analyte_id="REG153_003",
            preferred_name="Xylene (total)",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE
        )
    ]
    
    for analyte in test_analytes:
        session.add(analyte)
    
    session.commit()
    
    yield session
    
    session.close()


@pytest.fixture
def synonym_ingestor():
    """Create a SynonymIngestor instance."""
    return SynonymIngestor()


@pytest.fixture
def threshold_calibrator():
    """Create a ThresholdCalibrator instance."""
    return ThresholdCalibrator()


@pytest.fixture
def variant_clusterer():
    """Create a VariantClusterer instance."""
    return VariantClusterer(similarity_threshold=0.85)


@pytest.fixture
def config_manager():
    """Create a ConfigManager instance with temporary config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "test_config.yaml"
        manager = ConfigManager()
        manager.config_path = config_path
        yield manager


# SynonymIngestor Tests

def test_ingest_validated_synonym_new(db_session, synonym_ingestor):
    """Test ingesting a new validated synonym."""
    result = synonym_ingestor.ingest_validated_synonym(
        raw_text="Benzol",
        analyte_id="REG153_001",
        db_session=db_session,
        confidence=1.0
    )
    
    assert result is True
    
    # Verify synonym was added
    synonyms = db_session.query(Synonym).filter_by(analyte_id="REG153_001").all()
    assert len(synonyms) == 1
    assert synonyms[0].synonym_raw == "Benzol"
    assert synonyms[0].harvest_source == "validated_runtime"
    assert synonyms[0].confidence == 1.0


def test_ingest_validated_synonym_duplicate(db_session, synonym_ingestor):
    """Test that duplicate synonyms are not ingested."""
    # First ingestion
    result1 = synonym_ingestor.ingest_validated_synonym(
        raw_text="Benzol",
        analyte_id="REG153_001",
        db_session=db_session
    )
    assert result1 is True
    
    # Second ingestion should be rejected
    result2 = synonym_ingestor.ingest_validated_synonym(
        raw_text="Benzol",
        analyte_id="REG153_001",
        db_session=db_session
    )
    assert result2 is False
    
    # Verify only one synonym exists
    synonyms = db_session.query(Synonym).filter_by(analyte_id="REG153_001").all()
    assert len(synonyms) == 1


def test_ingest_invalid_confidence(db_session, synonym_ingestor):
    """Test that invalid confidence values are rejected."""
    with pytest.raises(ValueError, match="Confidence must be between 0 and 1"):
        synonym_ingestor.ingest_validated_synonym(
            raw_text="Test",
            analyte_id="REG153_001",
            db_session=db_session,
            confidence=1.5
        )


def test_check_duplicate(db_session, synonym_ingestor):
    """Test duplicate checking."""
    # No duplicate initially
    assert synonym_ingestor.check_duplicate("benzol", "REG153_001", db_session) is False
    
    # Add synonym
    synonym_ingestor.ingest_validated_synonym(
        raw_text="Benzol",
        analyte_id="REG153_001",
        db_session=db_session
    )
    
    # Now there is a duplicate (normalized form matches)
    norm_text = synonym_ingestor.normalizer.normalize("Benzol")
    assert synonym_ingestor.check_duplicate(norm_text, "REG153_001", db_session) is True


def test_bulk_ingest(db_session, synonym_ingestor):
    """Test bulk synonym ingestion."""
    synonym_list = [
        ("Benzol", "REG153_001"),
        ("Methylbenzene", "REG153_002"),
        ("Xylol", "REG153_003"),
        ("Benzol", "REG153_001"),  # Duplicate
    ]
    
    stats = synonym_ingestor.bulk_ingest(synonym_list, db_session)
    
    assert stats['added'] == 3
    assert stats['duplicates'] == 1
    assert stats['errors'] == 0


def test_get_ingestion_stats(db_session, synonym_ingestor):
    """Test getting ingestion statistics."""
    # Add some synonyms
    synonym_ingestor.ingest_validated_synonym(
        raw_text="Benzol",
        analyte_id="REG153_001",
        db_session=db_session,
        synonym_type=SynonymType.COMMON
    )
    synonym_ingestor.ingest_validated_synonym(
        raw_text="Methylbenzene",
        analyte_id="REG153_002",
        db_session=db_session,
        synonym_type=SynonymType.LAB_VARIANT
    )
    
    stats = synonym_ingestor.get_ingestion_stats(db_session)
    
    assert stats['total'] == 2
    assert stats['by_type']['common'] == 1
    assert stats['by_type']['lab_variant'] == 1


# ThresholdCalibrator Tests

def test_analyze_recent_decisions_empty(db_session, threshold_calibrator):
    """Test analyzing decisions when no data exists."""
    stats = threshold_calibrator.analyze_recent_decisions(db_session, days=30)
    
    assert stats['total_decisions'] == 0
    assert stats['validated_count'] == 0


def test_analyze_recent_decisions_with_data(db_session, threshold_calibrator):
    """Test analyzing decisions with sample data."""
    # Add sample match decisions
    for i in range(10):
        decision = MatchDecision(
            input_text=f"test_{i}",
            matched_analyte_id="REG153_001" if i < 7 else None,
            match_method="fuzzy" if i < 5 else "semantic",
            confidence_score=0.85 + (i * 0.01),
            top_k_candidates=[],
            signals_used={},
            corpus_snapshot_hash="test_hash",
            model_hash="model_hash",
            human_validated=True,
            disagreement_flag=(i >= 8)
        )
        db_session.add(decision)
    
    db_session.commit()
    
    stats = threshold_calibrator.analyze_recent_decisions(db_session, days=30)
    
    assert stats['total_decisions'] == 10
    assert stats['validated_count'] == 10
    assert stats['validation_rate'] == 1.0
    assert 'method_distribution' in stats
    assert stats['method_distribution']['fuzzy'] == 5
    assert stats['method_distribution']['semantic'] == 5


def test_calculate_optimal_thresholds(db_session, threshold_calibrator):
    """Test threshold calculation."""
    # Add sample validated decisions
    for i in range(20):
        decision = MatchDecision(
            input_text=f"term_{i}",
            matched_analyte_id="REG153_001",
            match_method="fuzzy",
            confidence_score=0.70 + (i * 0.015),  # Range 0.70 to 0.985
            top_k_candidates=[],
            signals_used={},
            corpus_snapshot_hash="hash",
            model_hash="hash",
            human_validated=True,
            disagreement_flag=(i < 2)  # First 2 are disagreements
        )
        db_session.add(decision)
    
    db_session.commit()
    
    decisions = db_session.query(MatchDecision).all()
    thresholds = threshold_calibrator.calculate_optimal_thresholds(decisions)
    
    assert 'auto_accept' in thresholds
    assert 'review' in thresholds
    assert 'unknown' in thresholds
    assert 'disagreement_cap' in thresholds
    
    # Thresholds should be in valid range
    for threshold in thresholds.values():
        assert 0.0 <= threshold <= 1.0


def test_get_statistics(threshold_calibrator):
    """Test getting statistics."""
    stats = threshold_calibrator.get_statistics()
    assert isinstance(stats, dict)


# VariantClusterer Tests

def test_cluster_similar_unknowns_empty(variant_clusterer):
    """Test clustering with empty input."""
    clusters = variant_clusterer.cluster_similar_unknowns([])
    assert clusters == []


def test_cluster_similar_unknowns(variant_clusterer):
    """Test clustering similar variants."""
    unknown_terms = [
        "Benzene",
        "Benzen",
        "Benzol",
        "Toluene",
        "Toluen",
        "Methylbenzene"
    ]
    
    clusters = variant_clusterer.cluster_similar_unknowns(unknown_terms)
    
    assert len(clusters) > 0
    
    # Check cluster structure
    for cluster in clusters:
        assert 'anchor' in cluster
        assert 'similar_variants' in cluster
        assert 'cluster_size' in cluster
        assert 'avg_similarity' in cluster


def test_compute_similarity_matrix(variant_clusterer):
    """Test similarity matrix computation."""
    terms = ["benzene", "benzen", "toluene"]
    matrix = variant_clusterer._compute_similarity_matrix(terms)
    
    assert matrix.shape == (3, 3)
    assert matrix[0, 0] == 1.0  # Diagonal should be 1
    assert 0.0 <= matrix[0, 1] <= 1.0  # All values in [0, 1]


def test_find_closest_analyte(db_session, variant_clusterer):
    """Test finding closest analyte matches."""
    matches = variant_clusterer.find_closest_analyte(
        "Benzol",
        db_session,
        top_k=3
    )
    
    assert len(matches) <= 3
    
    if matches:
        analyte_id, preferred_name, score = matches[0]
        assert isinstance(analyte_id, str)
        assert isinstance(preferred_name, str)
        assert 0.0 <= score <= 1.0


def test_enrich_clusters_with_suggestions(db_session, variant_clusterer):
    """Test enriching clusters with analyte suggestions."""
    clusters = [
        {
            'anchor': 'Benzol',
            'similar_variants': [],
            'cluster_size': 1,
            'avg_similarity': 1.0
        }
    ]
    
    enriched = variant_clusterer.enrich_clusters_with_suggestions(
        clusters,
        db_session,
        top_k=2
    )
    
    assert len(enriched) == 1
    assert 'suggested_analytes' in enriched[0]


def test_get_clustering_statistics(variant_clusterer):
    """Test clustering statistics."""
    clusters = [
        {'cluster_size': 3, 'avg_similarity': 0.9},
        {'cluster_size': 2, 'avg_similarity': 0.85},
        {'cluster_size': 1, 'avg_similarity': 1.0}
    ]
    
    stats = variant_clusterer.get_clustering_statistics(clusters)
    
    assert stats['total_clusters'] == 3
    assert stats['total_terms'] == 6
    assert stats['max_cluster_size'] == 3
    assert stats['min_cluster_size'] == 1
    assert stats['singleton_clusters'] == 1


# ConfigManager Tests

def test_config_manager_defaults(config_manager):
    """Test that default configuration is loaded."""
    assert 'thresholds' in config_manager.config
    assert 'learning' in config_manager.config
    assert config_manager.get_threshold('auto_accept') == 0.93


def test_get_threshold(config_manager):
    """Test getting threshold values."""
    threshold = config_manager.get_threshold('auto_accept')
    assert threshold == 0.93


def test_get_threshold_not_found(config_manager):
    """Test getting non-existent threshold."""
    with pytest.raises(KeyError):
        config_manager.get_threshold('nonexistent')


def test_update_threshold(config_manager):
    """Test updating a threshold."""
    config_manager.update_threshold('auto_accept', 0.95)
    assert config_manager.get_threshold('auto_accept') == 0.95


def test_update_threshold_invalid_value(config_manager):
    """Test updating threshold with invalid value."""
    with pytest.raises(ValueError):
        config_manager.update_threshold('auto_accept', 1.5)


def test_update_thresholds_bulk(config_manager):
    """Test bulk threshold update."""
    new_thresholds = {
        'auto_accept': 0.95,
        'review': 0.80,
        'unknown': 0.70
    }
    
    config_manager.update_thresholds_bulk(new_thresholds)
    
    assert config_manager.get_threshold('auto_accept') == 0.95
    assert config_manager.get_threshold('review') == 0.80
    assert config_manager.get_threshold('unknown') == 0.70


def test_save_and_load_config(config_manager):
    """Test saving and loading configuration."""
    config_manager.update_threshold('auto_accept', 0.95)
    config_manager.save_config()
    
    # Load in new instance
    new_manager = ConfigManager(config_manager.config_path)
    assert new_manager.get_threshold('auto_accept') == 0.95


def test_reset_to_defaults(config_manager):
    """Test resetting configuration to defaults."""
    config_manager.update_threshold('auto_accept', 0.99)
    config_manager.reset_to_defaults()
    
    assert config_manager.get_threshold('auto_accept') == 0.93


def test_validate_config_valid(config_manager):
    """Test configuration validation with valid config."""
    errors = config_manager.validate_config()
    assert errors == []


def test_validate_config_invalid_threshold(config_manager):
    """Test configuration validation with invalid threshold."""
    config_manager.config['thresholds']['auto_accept'] = 1.5
    errors = config_manager.validate_config()
    
    assert len(errors) > 0
    assert any('auto_accept' in error for error in errors)


def test_get_learning_param(config_manager):
    """Test getting learning parameters."""
    param = config_manager.get_learning_param('retraining_trigger_count')
    assert param == 2000


def test_get_matching_param(config_manager):
    """Test getting matching parameters."""
    param = config_manager.get_matching_param('semantic_model')
    assert param == 'all-MiniLM-L6-v2'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
