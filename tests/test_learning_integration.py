"""
Integration tests for the learning system.

Tests:
- End-to-end synonym ingestion (SynonymIngestor)
- Threshold calibration initialization (ThresholdCalibrator)
- Variant clustering (VariantClusterer)
- Configuration management (ConfigManager)
- Maturity metrics
"""

import pytest
import os
from pathlib import Path
from datetime import datetime

from src.learning.synonym_ingestion import SynonymIngestor
from src.learning.threshold_calibrator import ThresholdCalibrator
from src.learning.variant_clustering import VariantClusterer
from src.utils.config_manager import ConfigManager


# ============================================================================
# SYNONYM INGESTION TESTS
# ============================================================================

class TestSynonymIngestion:
    """Tests for SynonymIngestor (Layer 1 learning)."""

    def test_ingestor_initializes(self):
        """SynonymIngestor can be created with no args."""
        ingestor = SynonymIngestor()
        assert ingestor.normalizer is not None

    def test_check_duplicate_returns_false_for_new(self, sample_synonyms):
        """Non-existing synonym is not flagged as duplicate."""
        ingestor = SynonymIngestor()
        is_dup = ingestor.check_duplicate(
            "never seen variant", "REG153_VOCS_001", sample_synonyms
        )
        assert is_dup is False

    def test_check_duplicate_returns_true_for_existing(self, sample_synonyms):
        """Known synonym is detected as duplicate."""
        ingestor = SynonymIngestor()
        # 'benzene' is loaded as normalized form in conftest
        is_dup = ingestor.check_duplicate(
            "benzene", "REG153_VOCS_001", sample_synonyms
        )
        assert is_dup is True

    def test_ingest_blocked_without_cascade_confirmation(self, sample_synonyms):
        """Dual-gate blocks ingestion when cascade_confirmed=False."""
        ingestor = SynonymIngestor()
        added = ingestor.ingest_validated_synonym(
            raw_text="Test Variant Blocked",
            analyte_id="REG153_VOCS_001",
            db_session=sample_synonyms,
            confidence=0.95,
            cascade_confirmed=False,
            cascade_margin=0.10,
        )
        assert added is False

    def test_ingest_blocked_with_low_margin(self, sample_synonyms):
        """Dual-gate blocks ingestion when margin is below threshold."""
        ingestor = SynonymIngestor()
        added = ingestor.ingest_validated_synonym(
            raw_text="Test Variant Low Margin",
            analyte_id="REG153_VOCS_001",
            db_session=sample_synonyms,
            confidence=0.95,
            cascade_confirmed=True,
            cascade_margin=0.01,  # below default 0.06
        )
        assert added is False

    def test_ingest_duplicate_skipped(self, sample_synonyms):
        """Duplicate synonym (already in DB) is skipped."""
        ingestor = SynonymIngestor()
        # First, ingest one successfully
        added1 = ingestor.ingest_validated_synonym(
            raw_text="Novel Dupe Test",
            analyte_id="REG153_VOCS_001",
            db_session=sample_synonyms,
            confidence=1.0,
            cascade_confirmed=True,
            cascade_margin=0.20,
        )
        # Second insert of the same text should be blocked as duplicate
        added2 = ingestor.ingest_validated_synonym(
            raw_text="Novel Dupe Test",
            analyte_id="REG153_VOCS_001",
            db_session=sample_synonyms,
            confidence=1.0,
            cascade_confirmed=True,
            cascade_margin=0.20,
        )
        assert added2 is False  # duplicate

    def test_ingest_invalid_confidence_raises(self, sample_synonyms):
        """Out-of-range confidence raises ValueError."""
        ingestor = SynonymIngestor()
        with pytest.raises(ValueError, match="Confidence must be between"):
            ingestor.ingest_validated_synonym(
                raw_text="Bad Confidence",
                analyte_id="REG153_VOCS_001",
                db_session=sample_synonyms,
                confidence=1.5,
                cascade_confirmed=True,
                cascade_margin=0.10,
            )

    def test_get_ingestion_stats(self, sample_synonyms):
        """Ingestion stats returns a dict."""
        ingestor = SynonymIngestor()
        stats = ingestor.get_ingestion_stats(sample_synonyms)
        assert isinstance(stats, dict)


# ============================================================================
# THRESHOLD CALIBRATION TESTS
# ============================================================================

class TestThresholdCalibration:
    """Tests for ThresholdCalibrator (Layer 3 learning)."""

    def test_calibrator_initializes(self):
        """ThresholdCalibrator can be created with no args."""
        calibrator = ThresholdCalibrator()
        assert calibrator.statistics == {}
        assert calibrator.optimal_thresholds == {}

    def test_analyze_recent_decisions_empty(self, sample_synonyms):
        """With no match decisions, returns empty statistics gracefully."""
        calibrator = ThresholdCalibrator()
        result = calibrator.analyze_recent_decisions(sample_synonyms, days=30)
        assert isinstance(result, dict)

    def test_get_statistics_empty(self):
        """get_statistics returns a dict even before analysis."""
        calibrator = ThresholdCalibrator()
        stats = calibrator.get_statistics()
        assert isinstance(stats, dict)


# ============================================================================
# VARIANT CLUSTERING TESTS
# ============================================================================

class TestVariantClustering:
    """Tests for VariantClusterer (Layer 4 learning)."""

    def test_clusterer_initializes_default(self):
        """VariantClusterer creates with default threshold."""
        clusterer = VariantClusterer()
        assert clusterer.similarity_threshold == 0.85

    def test_clusterer_custom_threshold(self):
        """VariantClusterer accepts a custom similarity threshold."""
        clusterer = VariantClusterer(similarity_threshold=0.90)
        assert clusterer.similarity_threshold == 0.90

    def test_clusterer_invalid_threshold_raises(self):
        """Out-of-range threshold raises ValueError."""
        with pytest.raises(ValueError):
            VariantClusterer(similarity_threshold=1.5)

    def test_cluster_empty_list(self):
        """Empty input returns empty clusters."""
        clusterer = VariantClusterer()
        clusters = clusterer.cluster_similar_unknowns([])
        assert clusters == []

    def test_cluster_single_term(self):
        """Single term returns one trivial cluster."""
        clusterer = VariantClusterer()
        clusters = clusterer.cluster_similar_unknowns(["Benzene"])
        assert len(clusters) >= 1

    def test_cluster_obvious_variants(self):
        """Closely related variants cluster together."""
        clusterer = VariantClusterer(similarity_threshold=0.70)
        clusters = clusterer.cluster_similar_unknowns([
            "Benzene",
            "benzene",
            "BENZENE",
            "Toluene",
            "toluene",
        ])
        assert len(clusters) >= 1
        # At least one cluster should contain multiple variants
        multi = [c for c in clusters if c.get('cluster_size', 0) > 1]
        assert len(multi) >= 1

    def test_clustering_statistics(self):
        """get_clustering_statistics returns a summary dict."""
        clusterer = VariantClusterer()
        clusters = clusterer.cluster_similar_unknowns(["Benzene", "benzene", "benzol"])
        stats = clusterer.get_clustering_statistics(clusters)
        assert isinstance(stats, dict)


# ============================================================================
# CONFIGURATION MANAGEMENT TESTS
# ============================================================================

class TestConfigurationManagement:
    """Tests for ConfigManager."""

    def test_default_config_loaded(self):
        """ConfigManager loads defaults when no file provided."""
        cm = ConfigManager()
        assert 'thresholds' in cm.config
        assert cm.config['thresholds']['auto_accept'] == 0.93

    def test_load_from_yaml(self, temp_dir):
        """ConfigManager loads a YAML file."""
        config_path = temp_dir / "test_config.yaml"
        config_path.write_text(
            "learning:\\n"
            "  synonym_ingestion:\\n"
            "    min_confidence: 0.75\\n"
        )
        cm = ConfigManager(config_path=config_path)
        assert cm.config is not None

    def test_get_nested_value(self):
        """Accessing nested config keys works."""
        cm = ConfigManager()
        auto_accept = cm.config.get('thresholds', {}).get('auto_accept')
        assert auto_accept == 0.93
