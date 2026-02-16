"""
Learning infrastructure for continuous vocabulary expansion.

This package provides incremental learning capabilities for the chemical matcher:

Layer 1 - Immediate Learning (No Retraining):
- Synonym ingestion from validated runtime decisions
- Direct insertion into synonym database

Layer 2 - Incremental Embedding Updates:
- Add new terms to FAISS index without full rebuild
- Periodic embedding expansion

Layer 3 - Threshold Calibration:
- Analyze match decision statistics
- Dynamically adjust confidence thresholds
- Track method performance

Layer 4 - Variant Clustering:
- Group similar unknown variants
- Facilitate batch validation
"""

from .synonym_ingestion import SynonymIngestor
from .incremental_embedder import IncrementalEmbedder
from .threshold_calibrator import ThresholdCalibrator
from .variant_clustering import VariantClusterer

__all__ = [
    "SynonymIngestor",
    "IncrementalEmbedder",
    "ThresholdCalibrator",
    "VariantClusterer",
]

__all__ = [
    'ingest_validated_synonym',
    'batch_ingest_validations',
    'IncrementalEmbedder',
    'analyze_match_decisions',
    'find_optimal_threshold',
    'recalibrate_thresholds',
    'cluster_similar_unknowns',
    'detect_typo_groups',
    'suggest_batch_validation',
    'calculate_corpus_maturity',
    'detect_plateau',
    'should_retrain_model',
]
