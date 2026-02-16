# Database Module Documentation

## Overview

The database module provides a complete SQLite-based persistence layer for the Reg 153 Chemical Matcher project using SQLAlchemy 2.0.

## Quick Start

### 1. Initialize Database

```python
from src.database import init_db

# Initialize with default path (data/reg153_matcher.db)
db = init_db()
db.create_all_tables()

# Or specify custom path
db = init_db(db_path="path/to/custom.db", echo=True)
db.create_all_tables()
```

### 2. Using Sessions

```python
from src.database import session_scope
from src.database.crud import create_analyte

# Context manager automatically handles commit/rollback
with session_scope() as session:
    analyte = create_analyte(
        session,
        cas_number="67-64-1",
        preferred_name="Acetone",
        analyte_type="single_substance",
        molecular_formula="C3H6O"
    )
    print(f"Created analyte with ID: {analyte.id}")
```

### 3. Setup Script

```bash
# Initialize database with test data
python scripts/setup_database.py --seed-test-data

# Drop and recreate database
python scripts/setup_database.py --drop-existing --seed-test-data

# Custom database path
python scripts/setup_database.py --db-path "path/to/db.db" --echo
```

## Database Schema

### Core Tables

#### 1. **analytes** - Canonical Chemical Truth
Primary table for authoritative chemical substances.

Fields:
- `id`: Primary key
- `cas_number`: CAS registry number (nullable for fractions/groups)
- `preferred_name`: Official analyte name
- `iupac_name`: IUPAC nomenclature
- `analyte_type`: `single_substance`, `fraction_or_group`, `suite`, `parameter`
- `molecular_formula`, `molecular_weight`: Basic chemistry
- `smiles`, `inchi`, `inchi_key`: Structure identifiers
- `reg153_category`: Regulatory classification
- `notes`: Additional context

**Indexes**: cas_number, preferred_name, analyte_type, inchi_key

#### 2. **synonyms** - Alternative Chemical Names
Stores all known synonyms with confidence scores.

Fields:
- `id`: Primary key
- `analyte_id`: Foreign key to analytes
- `synonym_raw`: Original synonym text
- `synonym_norm`: Normalized (lowercase, trimmed) text
- `harvest_source`: Origin (`pubchem`, `cas_common_chemistry`, `echa`, `manual`)
- `confidence`: Score 0.0-1.0
- `language`: Default `en`

**Indexes**: synonym_norm, analyte_id, harvest_source, unique(synonym_norm + analyte_id)

#### 3. **lab_variants** - Ontario Lab Behavioral Corpus
Tracks lab-specific reporting variations.

Fields:
- `id`: Primary key
- `analyte_id`: Foreign key to analytes
- `lab_vendor`: Lab name
- `reported_name`: Lab's specific naming
- `method`, `matrix`, `units`: Analytical context
- `frequency`: Number of observations
- `first_observed`, `last_observed`: Temporal tracking

**Indexes**: lab_vendor, reported_name, vendor+name

#### 4. **match_decisions** - ML Audit Trail
Complete decision history for continuous improvement.

Fields:
- `id`: Primary key
- `query_text`, `query_norm`: Input query
- `analyte_id`: Matched analyte (nullable)
- `confidence_score`: Overall confidence
- `top_k_candidates`: JSON array of candidates with scores
- `signals_used`: JSON object of signal contributions
- `corpus_snapshot_hash`, `model_hash`: Version tracking
- `embedding_model_name`: Model used
- `disagreement_flag`: True if signals disagree
- `manual_review`, `reviewed_by`, `review_notes`: Human verification

**Indexes**: query_norm, analyte_id, corpus_snapshot_hash, model_hash, disagreement_flag, created_at

#### 5. **embeddings_metadata** - Vector Storage Tracking
Metadata for embeddings stored on disk (FAISS/numpy).

Fields:
- `id`: Primary key
- `analyte_id` OR `synonym_id`: Source (XOR constraint)
- `text_embedded`: Original text
- `model_name`, `model_version`: Model identification
- `embedding_dim`: Vector dimension
- `file_path`: Relative path to binary file
- `vector_index`: Index in FAISS or array

**Indexes**: analyte_id, synonym_id, model_name+version

#### 6. **api_harvest_metadata** - Bootstrap Audit
Tracks API harvesting operations.

Fields:
- `id`: Primary key
- `api_source`, `api_endpoint`: API identification
- `query_type`: Type of query
- `query_params`: JSON parameters
- `analyte_id`: Optional related analyte
- `status_code`, `success`: HTTP response
- `synonyms_harvested`: Count of synonyms collected
- `error_message`: Error details
- `response_time_ms`, `rate_limited`: Performance tracking

**Indexes**: api_source, success, created_at

#### 7. **snapshot_registry** - Version Control
Tracks corpus and model versions for reproducibility.

Fields:
- `id`: Primary key
- `snapshot_hash`: SHA256 hash
- `snapshot_type`: `corpus`, `model`, `embeddings`
- `version_tag`: Human-readable version
- `description`, `file_path`, `file_size_bytes`
- `analyte_count`, `synonym_count`: Statistics
- `metadata`: JSON for additional info
- `is_active`: Current active version flag

**Indexes**: snapshot_hash (unique), snapshot_type, version_tag, is_active

## CRUD Operations

### Analytes

```python
from src.database import session_scope
from src.database.crud import *

with session_scope() as session:
    # Create
    analyte = create_analyte(
        session,
        cas_number="108-88-3",
        preferred_name="Toluene",
        analyte_type="single_substance"
    )
    
    # Read
    analyte = get_analyte_by_cas(session, "108-88-3")
    analyte = get_analyte_by_name(session, "Toluene")
    analytes = search_analytes_by_name(session, "tol", limit=10)
    analytes = list_analytes(session, analyte_type="single_substance")
    
    # Update
    update_analyte(session, analyte.id, molecular_weight=92.14)
    
    # Delete
    delete_analyte(session, analyte.id)
    
    # Count
    count = count_analytes(session, analyte_type="single_substance")
```

### Synonyms

```python
with session_scope() as session:
    # Create single
    synonym = create_synonym(
        session,
        analyte_id=1,
        synonym_raw="Methylbenzene",
        synonym_norm="methylbenzene",
        harvest_source="pubchem",
        confidence=0.95
    )
    
    # Bulk insert
    synonyms_data = [
        {
            "analyte_id": 1,
            "synonym_raw": "Toluol",
            "synonym_norm": "toluol",
            "harvest_source": "pubchem",
            "confidence": 0.9,
            "language": "en"
        },
        # ... more synonyms
    ]
    count = bulk_insert_synonyms(session, synonyms_data, chunk_size=1000)
    
    # Check existence
    exists = synonym_exists(session, analyte_id=1, synonym_norm="toluol")
    
    # Get all for analyte
    synonyms = get_synonyms_for_analyte(session, analyte_id=1, min_confidence=0.8)
    
    # Search
    synonyms = search_synonyms(session, "toluene", exact=True)
    synonyms = search_synonyms(session, "tol", exact=False, limit=20)
    
    # Delete by source
    count = delete_synonyms_by_source(session, "pubchem", analyte_id=1)
```

### Lab Variants

```python
with session_scope() as session:
    # Create
    variant = create_lab_variant(
        session,
        analyte_id=1,
        lab_vendor="ALS Canada",
        reported_name="Toluene (Methylbenzene)",
        method="EPA 8260D",
        matrix="Soil",
        units="µg/kg",
        frequency=25
    )
    
    # Get by vendor
    variants = get_lab_variants_by_vendor(session, "ALS Canada")
    
    # Increment frequency (when observed again)
    variant = increment_lab_variant_frequency(session, variant.id)
    
    # Search
    variants = search_lab_variants(session, "toluene", lab_vendor="ALS Canada")
```

### Match Decisions

```python
with session_scope() as session:
    # Record decision
    decision = create_match_decision(
        session,
        query_text="toluene",
        query_norm="toluene",
        analyte_id=1,
        confidence_score=0.98,
        top_k_candidates=[
            {"rank": 1, "analyte_id": 1, "name": "Toluene", "score": 0.98},
            {"rank": 2, "analyte_id": 5, "name": "Xylene", "score": 0.45}
        ],
        signals_used={
            "exact_match": 1.0,
            "levenshtein": 0.95,
            "embedding_cosine": 0.98
        },
        corpus_snapshot_hash="v1_abc123",
        model_hash="model_v1_def456",
        embedding_model_name="all-MiniLM-L6-v2",
        disagreement_flag=False
    )
    
    # Get decisions for review
    decisions = get_decisions_for_review(
        session,
        disagreement_only=True,
        not_reviewed=True,
        limit=50
    )
    
    # Mark reviewed
    decision = mark_decision_reviewed(
        session,
        decision_id=1,
        reviewed_by="john_doe",
        review_notes="Confirmed correct match"
    )
    
    # Get statistics
    stats = get_match_statistics(
        session,
        corpus_snapshot_hash="v1_abc123",
        model_hash="model_v1_def456"
    )
    # Returns: {total_decisions, avg_confidence, disagreement_rate, review_rate}
```

### Specialized Queries

```python
with session_scope() as session:
    # Find nearest analyte match (simplified heuristic)
    analyte, score = get_nearest_analyte(session, "acetone", threshold=0.7)
    
    # Get all synonyms for corpus building
    corpus = get_all_synonyms_for_corpus(session, min_confidence=0.5)
    # Returns: [(analyte_id, synonym_norm, analyte_name), ...]
    
    # Get harvest statistics
    stats = get_harvest_statistics_by_source(session, "pubchem")
    # Returns: {total_requests, success_rate, total_synonyms, avg_response_time_ms, rate_limited_count}
    
    # Snapshot management
    snapshot = get_active_snapshot(session, "corpus")
    count = deactivate_snapshots(session, "corpus")
```

## Connection Management

### Database Manager

```python
from src.database.connection import DatabaseManager

# Create manager
db = DatabaseManager(
    db_path="data/reg153_matcher.db",
    echo=False,  # Set True for SQL logging
    check_same_thread=False,  # Allow multi-threading
    pool_size=5,
    max_overflow=10,
    pool_timeout=30
)

# Create tables
db.create_all_tables()

# Get session (manual management)
session = db.get_session()
try:
    # ... operations ...
    session.commit()
except:
    session.rollback()
    raise
finally:
    session.close()

# Context manager (preferred)
with db.session_scope() as session:
    # ... operations ...
    # Auto-commit on success, auto-rollback on exception

# Close connections
db.close()
```

### SQLite Optimizations

The connection module automatically configures:
- **Foreign key constraints**: Enabled (disabled by default in SQLite)
- **WAL mode**: Write-Ahead Logging for better concurrency
- **Synchronous=NORMAL**: Balance between safety and speed
- **64MB cache**: Faster queries
- **Memory temp tables**: Performance optimization

## Testing

### In-Memory Database

```python
from src.database import create_test_db

# Create in-memory database for tests
db = create_test_db()

with db.session_scope() as session:
    # ... test operations ...
```

### Test Data Seeding

```python
# Via setup script
python scripts/setup_database.py --db-path ":memory:" --seed-test-data

# Programmatically
from scripts.setup_database import seed_test_data
seed_test_data(db)
```

## Best Practices

### 1. Always Use Context Managers

```python
# ✅ Good
with session_scope() as session:
    analyte = create_analyte(session, ...)

# ❌ Avoid
session = get_session()
analyte = create_analyte(session, ...)
session.commit()
session.close()
```

### 2. Bulk Operations for Performance

```python
# For large datasets, use bulk operations
synonyms = [{"analyte_id": 1, ...} for _ in range(10000)]
bulk_insert_synonyms(session, synonyms, chunk_size=1000)
```

### 3. Check for Duplicates

```python
# Before inserting synonyms
if not synonym_exists(session, analyte_id, normalized_text):
    create_synonym(session, ...)
```

### 4. Use Transactions

```python
with session_scope() as session:
    # Multiple operations in single transaction
    analyte = create_analyte(session, ...)
    create_synonym(session, analyte.id, ...)
    create_lab_variant(session, analyte.id, ...)
    # All commit together or all rollback
```

### 5. Query Optimization

```python
# Use eager loading for relationships
from sqlalchemy.orm import joinedload

analytes = session.query(Analyte).options(
    joinedload(Analyte.synonyms)
).all()

# Use filters efficiently
analytes = session.query(Analyte).filter(
    Analyte.analyte_type == "single_substance",
    Analyte.cas_number.isnot(None)
).limit(100).all()
```

## Migration Guide

### From Old Schema

If migrating from an older schema:

1. **Export data**: Export existing analytes and synonyms to JSON/CSV
2. **Drop old tables**: `python scripts/setup_database.py --drop-existing`
3. **Create new schema**: Tables automatically created
4. **Import data**: Use bulk operations to reimport

### Using Alembic (Future)

For production migrations, consider using Alembic:

```bash
# Initialize alembic
alembic init alembic

# Create migration
alembic revision --autogenerate -m "Add new field"

# Apply migration
alembic upgrade head
```

## Performance Tips

1. **Index usage**: All foreign keys and frequently queried fields are indexed
2. **Bulk inserts**: Use `bulk_insert_synonyms()` for large datasets
3. **Connection pooling**: Configured automatically for file-based databases
4. **Query batching**: Process large result sets in chunks
5. **Transaction batching**: Group related operations in single transactions

## Troubleshooting

### Database Locked

```python
# Increase timeout
db = DatabaseManager(pool_timeout=60)
```

### Foreign Key Violations

```python
# Ensure analyte exists before adding synonym
analyte = get_analyte_by_id(session, analyte_id)
if analyte:
    create_synonym(session, analyte_id, ...)
```

### Memory Issues with Large Queries

```python
# Use limit and offset for pagination
for page in range(0, total_pages):
    analytes = list_analytes(session, offset=page*100, limit=100)
    process(analytes)
```

## See Also

- [SQLAlchemy 2.0 Documentation](https://docs.sqlalchemy.org/en/20/)
- [SQLite Performance Tuning](https://www.sqlite.org/pragma.html)
- Main project README for overall architecture
