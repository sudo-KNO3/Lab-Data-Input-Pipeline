# Incremental Learning System

This module implements a multi-layer incremental learning system for continuous vocabulary expansion without full model retraining.

## Architecture

The learning system has four layers:

### Layer 1: Immediate Learning (Synonym Ingestion)
**No retraining required** - New terms added directly to database

```python
from src.learning import SynonymIngestor

ingestor = SynonymIngestor()
success = ingestor.ingest_validated_synonym(
    raw_text="Benzol",
    analyte_id="REG153_001",
    db_session=session,
    confidence=1.0
)
```

**Benefits:**
- Instant vocabulary expansion
- Zero downtime
- Exact matches work immediately

### Layer 2: Incremental Embeddings
**Partial retraining** - Add vectors to FAISS index without full rebuild

```python
from src.learning import IncrementalEmbedder
from pathlib import Path

embedder = IncrementalEmbedder(
    model_name="all-MiniLM-L6-v2",
    faiss_index_path=Path("data/embeddings/index.faiss"),
    vectors_path=Path("data/embeddings/vectors.npy"),
    save_frequency=100
)

embedder.load_existing_index()
embedder.add_term("Benzol", "REG153_001", session)
embedder.save_incremental_update()
```

**Benefits:**
- Semantic matching with new terms
- Auto-save every N additions
- No full re-embedding

### Layer 3: Threshold Calibration
**Self-tuning** - Dynamically adjust confidence thresholds

```python
from src.learning import ThresholdCalibrator

calibrator = ThresholdCalibrator()
stats = calibrator.analyze_recent_decisions(
    db_session=session,
    days=30
)

thresholds = calibrator.calculate_optimal_thresholds(
    decisions=validated_decisions,
    target_precision=0.98,
    target_recall=0.90
)
```

**Metrics tracked:**
- Acceptance rate (top-1 matches)
- Override frequency (human corrections)
- Unknown rate
- Method performance (exact/fuzzy/semantic)

### Layer 4: Variant Clustering
**Batch validation** - Group similar unknowns for efficient review

```python
from src.learning import VariantClusterer

clusterer = VariantClusterer(similarity_threshold=0.85)
clusters = clusterer.cluster_similar_unknowns(unknown_terms)

# Enrich with suggestions
enriched = clusterer.enrich_clusters_with_suggestions(
    clusters,
    db_session=session,
    top_k=3
)
```

**Output format:**
```python
{
    'anchor': 'Benzol',
    'similar_variants': [
        ('Benzen', 0.92),
        ('Benzene', 0.87)
    ],
    'cluster_size': 3,
    'suggested_analytes': [
        {
            'analyte_id': 'REG153_001',
            'preferred_name': 'Benzene',
            'similarity': 0.93
        }
    ]
}
```

## Configuration Management

```python
from src.utils.config_manager import ConfigManager
from pathlib import Path

config = ConfigManager(Path("config/learning_config.yaml"))

# Get thresholds
auto_accept = config.get_threshold('auto_accept')  # 0.93

# Update thresholds
config.update_threshold('auto_accept', 0.95)
config.save_config()

# Bulk update
config.update_thresholds_bulk({
    'auto_accept': 0.95,
    'review': 0.80,
    'unknown': 0.70
})
```

## Complete Workflow Example

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.learning import (
    SynonymIngestor,
    IncrementalEmbedder,
    ThresholdCalibrator,
    VariantClusterer
)
from src.utils.config_manager import ConfigManager

# Setup
engine = create_engine("sqlite:///data/reg153_matcher.db")
Session = sessionmaker(bind=engine)
session = Session()

config = ConfigManager(Path("config/learning_config.yaml"))

# 1. Ingest validated synonym (Layer 1)
ingestor = SynonymIngestor()
if ingestor.ingest_validated_synonym(
    raw_text="Benzol",
    analyte_id="REG153_001",
    db_session=session,
    confidence=1.0
):
    print("✓ Synonym added to database")

# 2. Update embeddings (Layer 2)
embedder = IncrementalEmbedder(
    faiss_index_path=Path("data/embeddings/index.faiss")
)
embedder.load_existing_index()
embedder.add_term("Benzol", "REG153_001", session)
print(f"✓ Embedding added (total: {embedder.faiss_index.ntotal})")

# 3. Calibrate thresholds (Layer 3)
calibrator = ThresholdCalibrator()
stats = calibrator.analyze_recent_decisions(session, days=30)
print(f"Analyzed {stats['total_decisions']} decisions")

if stats['validated_count'] >= 100:
    thresholds = calibrator.calculate_optimal_thresholds(
        validated_decisions
    )
    config.update_thresholds_bulk(thresholds)
    config.save_config()
    print("✓ Thresholds updated")

# 4. Cluster unknowns (Layer 4)
unknown_terms = ["Benzol", "Benzen", "Toluol", "Toluen"]
clusterer = VariantClusterer()
clusters = clusterer.cluster_similar_unknowns(unknown_terms)
enriched = clusterer.enrich_clusters_with_suggestions(
    clusters, session
)
print(f"✓ Formed {len(enriched)} clusters")

session.close()
```

## Performance Targets

| Operation | Target | Achievable |
|-----------|--------|------------|
| Synonym ingestion | <5ms | ✓ |
| Embedding addition | <50ms | ✓ |
| FAISS search | <10ms | ✓ |
| Threshold calibration | <1s for 1000 decisions | ✓ |
| Clustering 100 terms | <2s | ✓ |

## Retraining Triggers

Full model retraining is triggered when:

1. **Synonym count threshold** reached (default: 2000 new synonyms)
2. **Manual trigger** by admin
3. **Monthly scheduled** retraining (optional)

Check if retraining is needed:
```python
stats = ingestor.get_ingestion_stats(session)
trigger_count = config.get_learning_param('retraining_trigger_count')

if stats['total'] >= trigger_count:
    print("⚠ Full retraining recommended")
```

## Database Schema Updates

The learning system uses these tables:

- **synonyms**: Now includes `harvest_source='validated_runtime'`
- **embeddings_metadata**: Tracks vector indices and model versions
- **match_decisions**: Audit trail with `human_validated` and `ingested` flags

## Error Handling

All learning operations include:
- Transaction rollback on errors
- Comprehensive logging
- Duplicate detection
- Validation of inputs

```python
try:
    result = ingestor.ingest_validated_synonym(...)
    if not result:
        print("Duplicate detected, skipped")
except ValueError as e:
    print(f"Validation error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Testing

Run the test suite:
```bash
pytest tests/test_learning.py -v
```

Test categories:
- Synonym ingestion (6 tests)
- Threshold calibration (4 tests)
- Variant clustering (6 tests)
- Configuration management (12 tests)

## Configuration File

Default configuration in `config/learning_config.yaml`:

```yaml
thresholds:
  auto_accept: 0.93
  review: 0.75
  unknown: 0.75

learning:
  retraining_trigger_count: 2000
  incremental_save_frequency: 100
  calibration_period_days: 30

clustering:
  similarity_threshold: 0.85
```

## Logging

Enable detailed logging:
```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Dependencies

- `sqlalchemy>=2.0.25` - Database ORM
- `numpy>=1.26.3` - Array operations
- `faiss-cpu>=1.7.4` - Vector similarity search
- `sentence-transformers>=2.3.1` - Text embeddings
- `python-Levenshtein>=0.23.0` - String similarity
- `pyyaml>=6.0.1` - Configuration files

## Monitoring

Track learning system health:

```python
# Ingestion rate
stats = ingestor.get_ingestion_stats(session)
print(f"Runtime synonyms: {stats['total']}")

# Embedding index size
index_stats = embedder.get_index_stats()
print(f"Vectors: {index_stats['total_vectors']}")

# Threshold performance
calibrator_stats = calibrator.get_statistics()
print(f"Acceptance rate: {calibrator_stats['acceptance_rate_top1']:.2%}")

# Clustering efficiency
cluster_stats = clusterer.get_clustering_statistics(clusters)
print(f"Avg cluster size: {cluster_stats['avg_cluster_size']:.1f}")
```

## See Also

- [Matching Engine Documentation](../matching/README.md)
- [Database Schema](../database/README.md)
- [API Documentation](../../docs/API.md)
