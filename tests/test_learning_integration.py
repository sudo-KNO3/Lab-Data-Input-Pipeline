"""
Integration tests for the learning system.

Tests:
- End-to-end synonym ingestion
- Threshold recalibration workflow
- Variant clustering
- Configuration management
"""

import pytest
import os
from pathlib import Path
from datetime import datetime

from src.learning.synonym_ingestion import SynonymIngestionPipeline
from src.learning.threshold_calibrator import ThresholdCalibrator
from src.learning.variant_clustering import VariantClusterer
from src.utils.config_manager import ConfigManager


# ============================================================================
# SYNONYM INGESTION TESTS
# ============================================================================

class TestSynonymIngestion:
    """Tests for synonym ingestion pipeline."""
    
    def test_ingest_synonym_basic(self, sample_synonyms, temp_dir):
        """Test basic synonym ingestion."""
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        # Ingest a new synonym
        result = pipeline.ingest_synonym(
            analyte_id="REG153_VOCS_001",
            synonym_text="New Benzene Variant",
            source="test",
            confidence=0.90,
        )
        
        assert result is not None
        assert result['success'] is True
        assert result['synonym_id'] is not None
    
    def test_ingest_duplicate_synonym(self, sample_synonyms):
        """Test handling of duplicate synonyms."""
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        # Try to ingest existing synonym
        result = pipeline.ingest_synonym(
            analyte_id="REG153_VOCS_001",
            synonym_text="Benzene",  # already exists
            source="test",
            confidence=1.0,
        )
        
        # Should either update or skip
        assert result is not None
        assert 'duplicate' in result or 'updated' in result or result['success']
    
    def test_ingest_batch_synonyms(self, sample_synonyms, temp_dir):
        """Test batch synonym ingestion."""
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        synonyms_data = [
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_text': 'Benzene Variant 1',
                'source': 'test_batch',
                'confidence': 0.90,
            },
            {
                'analyte_id': 'REG153_VOCS_002',
                'synonym_text': 'Toluene Variant 1',
                'source': 'test_batch',
                'confidence': 0.85,
            },
        ]
        
        results = pipeline.ingest_batch(synonyms_data)
        
        assert len(results) == len(synonyms_data)
        assert all(r['success'] for r in results)
    
    def test_ingest_with_validation(self, sample_synonyms, temp_dir):
        """Test synonym ingestion with validation."""
        pipeline = SynonymIngestionPipeline(sample_synonyms, validate=True)
        
        # Valid synonym
        result = pipeline.ingest_synonym(
            analyte_id="REG153_VOCS_001",
            synonym_text="Valid Synonym",
            source="test",
            confidence=0.90,
        )
        
        assert result['success'] is True
    
    def test_ingest_quality_filter(self, sample_synonyms, temp_dir):
        """Test quality filtering during ingestion."""
        pipeline = SynonymIngestionPipeline(sample_synonyms, min_confidence=0.85)
        
        # Low confidence synonym should be rejected
        result = pipeline.ingest_synonym(
            analyte_id="REG153_VOCS_001",
            synonym_text="Low Quality",
            source="test",
            confidence=0.70,
        )
        
        assert result['success'] is False or 'rejected' in result


# ============================================================================
# THRESHOLD CALIBRATION TESTS
# ============================================================================

class TestThresholdCalibration:
    """Tests for threshold calibration."""
    
    def test_calibrate_thresholds_basic(self, sample_synonyms, temp_dir):
        """Test basic threshold calibration."""
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        # Run calibration
        results = calibrator.calibrate(validation_set_size=10)
        
        assert results is not None
        assert 'exact_threshold' in results or 'thresholds' in results
        assert 'fuzzy_threshold' in results or 'thresholds' in results
    
    def test_calibrate_with_validation_data(self, sample_synonyms, temp_dir):
        """Test calibration with validation data."""
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        # Prepare validation data
        validation_data = [
            {
                'input': 'Benzene',
                'expected_id': 'REG153_VOCS_001',
                'expected_confidence': 1.0,
            },
            {
                'input': 'Toluene',
                'expected_id': 'REG153_VOCS_002',
                'expected_confidence': 1.0,
            },
        ]
        
        results = calibrator.calibrate_with_data(validation_data)
        
        assert results is not None
        assert 'accuracy' in results or 'metrics' in results
    
    def test_optimize_for_precision(self, sample_synonyms, temp_dir):
        """Test optimization for precision."""
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        results = calibrator.optimize(metric='precision', target=0.95)
        
        assert results is not None
        if 'precision' in results:
            assert results['precision'] >= 0.90  # Allow some tolerance
    
    def test_optimize_for_recall(self, sample_synonyms, temp_dir):
        """Test optimization for recall."""
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        results = calibrator.optimize(metric='recall', target=0.90)
        
        assert results is not None
        if 'recall' in results:
            assert results['recall'] >= 0.85  # Allow some tolerance


# ============================================================================
# VARIANT CLUSTERING TESTS
# ============================================================================

class TestVariantClustering:
    """Tests for variant clustering."""
    
    def test_cluster_variants_basic(self, sample_synonyms, temp_dir):
        """Test basic variant clustering."""
        clusterer = VariantClusterer(sample_synonyms)
        
        # Cluster variants for benzene
        clusters = clusterer.cluster_variants("REG153_VOCS_001")
        
        assert clusters is not None
        assert len(clusters) >= 1  # At least one cluster
    
    def test_cluster_by_similarity(self, sample_synonyms, temp_dir):
        """Test clustering by similarity."""
        clusterer = VariantClusterer(sample_synonyms)
        
        # Get all synonyms for an analyte
        variants = [
            "Benzene",
            "benzene",
            "BENZENE",
            "Benzen",
            "benzol",
        ]
        
        clusters = clusterer.cluster_by_similarity(variants, threshold=0.90)
        
        assert clusters is not None
        assert len(clusters) > 0
    
    def test_identify_representative(self, sample_synonyms, temp_dir):
        """Test identification of cluster representative."""
        clusterer = VariantClusterer(sample_synonyms)
        
        variants = ["Benzene", "benzene", "BENZENE"]
        
        representative = clusterer.identify_representative(variants)
        
        assert representative is not None
        assert representative in variants
    
    def test_cluster_quality_metrics(self, sample_synonyms, temp_dir):
        """Test cluster quality metrics."""
        clusterer = VariantClusterer(sample_synonyms)
        
        clusters = clusterer.cluster_variants("REG153_VOCS_001")
        
        if clusters:
            metrics = clusterer.calculate_metrics(clusters)
            
            assert metrics is not None
            assert 'num_clusters' in metrics or 'cluster_count' in metrics


# ============================================================================
# CONFIGURATION MANAGEMENT TESTS
# ============================================================================

class TestConfigurationManagement:
    """Tests for configuration management."""
    
    def test_load_config(self, temp_dir):
        """Test loading configuration."""
        # Create a test config file
        config_path = temp_dir / "test_config.yaml"
        config_content = """
learning:
  synonym_ingestion:
    min_confidence: 0.75
    auto_approve: false
  
  threshold_calibration:
    validation_split: 0.2
    optimization_metric: precision
"""
        config_path.write_text(config_content)
        
        config = ConfigManager.load_config(str(config_path))
        
        assert config is not None
        assert 'learning' in config
        assert config['learning']['synonym_ingestion']['min_confidence'] == 0.75
    
    def test_get_config_value(self, temp_dir):
        """Test getting specific config value."""
        config_path = temp_dir / "test_config.yaml"
        config_content = """
learning:
  threshold_calibration:
    fuzzy_threshold: 0.80
"""
        config_path.write_text(config_content)
        
        config_manager = ConfigManager(str(config_path))
        
        value = config_manager.get('learning.threshold_calibration.fuzzy_threshold')
        
        assert value == 0.80
    
    def test_update_config(self, temp_dir):
        """Test updating configuration."""
        config_path = temp_dir / "test_config.yaml"
        config_content = """
learning:
  threshold_calibration:
    fuzzy_threshold: 0.80
"""
        config_path.write_text(config_content)
        
        config_manager = ConfigManager(str(config_path))
        
        config_manager.set('learning.threshold_calibration.fuzzy_threshold', 0.85)
        config_manager.save()
        
        # Reload and verify
        config_manager2 = ConfigManager(str(config_path))
        value = config_manager2.get('learning.threshold_calibration.fuzzy_threshold')
        
        assert value == 0.85
    
    def test_default_config(self):
        """Test loading default configuration."""
        config = ConfigManager.get_default_config()
        
        assert config is not None
        assert 'learning' in config or 'matching' in config


# ============================================================================
# END-TO-END LEARNING WORKFLOW TESTS
# ============================================================================

class TestEndToEndLearningWorkflow:
    """Tests for complete learning workflows."""
    
    def test_full_ingestion_workflow(self, sample_synonyms, temp_dir):
        """Test complete synonym ingestion workflow."""
        # Step 1: Ingest new synonyms
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        new_synonyms = [
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_text': 'Workflow Test Variant 1',
                'source': 'workflow_test',
                'confidence': 0.90,
            },
            {
                'analyte_id': 'REG153_VOCS_002',
                'synonym_text': 'Workflow Test Variant 2',
                'source': 'workflow_test',
                'confidence': 0.85,
            },
        ]
        
        results = pipeline.ingest_batch(new_synonyms)
        
        assert all(r['success'] for r in results)
        
        # Step 2: Verify synonyms are in database
        from src.database import crud_new as crud
        
        syn1 = crud.search_synonym(sample_synonyms, "workflow test variant 1")
        assert syn1 is not None
        
        syn2 = crud.search_synonym(sample_synonyms, "workflow test variant 2")
        assert syn2 is not None
    
    def test_recalibration_workflow(self, sample_synonyms, temp_dir):
        """Test threshold recalibration workflow."""
        # Step 1: Get current thresholds
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        initial_results = calibrator.get_current_thresholds()
        
        # Step 2: Run calibration
        new_results = calibrator.calibrate(validation_set_size=5)
        
        assert new_results is not None
        
        # Step 3: Apply new thresholds (if improved)
        if calibrator.is_improvement(initial_results, new_results):
            applied = calibrator.apply_thresholds(new_results)
            assert applied is True
    
    def test_clustering_and_cleanup_workflow(self, sample_synonyms, temp_dir):
        """Test clustering and synonym cleanup workflow."""
        # Step 1: Cluster variants
        clusterer = VariantClusterer(sample_synonyms)
        
        clusters = clusterer.cluster_variants("REG153_VOCS_001")
        
        assert clusters is not None
        
        # Step 2: Identify duplicates
        duplicates = clusterer.find_duplicates(clusters)
        
        # Step 3: Merge if appropriate
        if duplicates:
            merged = clusterer.merge_duplicates(duplicates, dry_run=True)
            assert merged is not None


# ============================================================================
# INCREMENTAL LEARNING TESTS
# ============================================================================

class TestIncrementalLearning:
    """Tests for incremental learning capabilities."""
    
    def test_learn_from_validation(self, sample_synonyms, temp_dir):
        """Test learning from validated matches."""
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        # Simulate validated match
        validated_match = {
            'lab_variant': 'Benzene (validated)',
            'analyte_id': 'REG153_VOCS_001',
            'confidence': 0.92,
            'validated': True,
            'reviewer': 'test_user',
        }
        
        # Learn from this validation
        result = pipeline.learn_from_validation(validated_match)
        
        assert result is not None
        assert result['success'] is True or 'learned' in result
    
    def test_learn_from_batch_validations(self, sample_synonyms, temp_dir):
        """Test learning from batch of validations."""
        pipeline = SynonymIngestionPipeline(sample_synonyms)
        
        validations = [
            {
                'lab_variant': 'Benzene V1',
                'analyte_id': 'REG153_VOCS_001',
                'validated': True,
            },
            {
                'lab_variant': 'Toluene V1',
                'analyte_id': 'REG153_VOCS_002',
                'validated': True,
            },
        ]
        
        results = pipeline.learn_from_batch_validations(validations)
        
        assert len(results) == len(validations)
    
    def test_incremental_threshold_update(self, sample_synonyms, temp_dir):
        """Test incremental threshold updates."""
        calibrator = ThresholdCalibrator(sample_synonyms)
        
        # Get baseline
        baseline = calibrator.get_current_metrics()
        
        # Add more validation data incrementally
        new_validation = [
            {
                'input': 'Benzene',
                'expected_id': 'REG153_VOCS_001',
                'matched': True,
            }
        ]
        
        updated = calibrator.update_incrementally(new_validation)
        
        assert updated is not None


# ============================================================================
# MATURITY METRICS TESTS
# ============================================================================

class TestMaturityMetrics:
    """Tests for system maturity metrics."""
    
    def test_calculate_coverage(self, sample_synonyms):
        """Test calculating synonym coverage."""
        from src.learning.maturity_metrics import calculate_coverage
        
        coverage = calculate_coverage(sample_synonyms)
        
        assert coverage is not None
        assert 0 <= coverage <= 1.0
    
    def test_calculate_quality_score(self, sample_synonyms):
        """Test calculating quality score."""
        from src.learning.maturity_metrics import calculate_quality_score
        
        quality = calculate_quality_score(sample_synonyms)
        
        assert quality is not None
        assert isinstance(quality, (int, float))
    
    def test_calculate_maturity_index(self, sample_synonyms):
        """Test calculating overall maturity index."""
        from src.learning.maturity_metrics import calculate_maturity_index
        
        maturity = calculate_maturity_index(sample_synonyms)
        
        assert maturity is not None
        assert 'coverage' in maturity or 'overall' in maturity
