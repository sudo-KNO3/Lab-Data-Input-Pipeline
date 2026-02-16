# Chemical Matching Engine

Multi-strategy chemical name matching using exact, fuzzy, and semantic (FAISS) approaches.

## Overview

The matching engine provides a **cascaded resolution system** that intelligently combines multiple matching strategies to find the best match for chemical names:

1. **Exact Matching** (confidence = 1.0)
   - CAS number direct lookup
   - InChIKey identity matching
   - Normalized synonym exact match

2. **Fuzzy Matching** (confidence 0.75-0.95)
   - Levenshtein ratio for edit distance
   - Token set ratio for order-invariant matching
   - Configurable thresholds

3. **Semantic Matching** (confidence 0.75-0.95)
   - Sentence-transformers embeddings (all-MiniLM-L6-v2)
   - FAISS IndexFlatIP for fast cosine similarity search
   - Vector-based semantic understanding

4. **Resolution Engine**
   - Combines all strategies with intelligent cascade logic
   - Disagreement detection between fuzzy and semantic
   - Confidence calibration and thresholds
   - Full provenance tracking
   - Automated logging to `match_decisions` table

## Architecture

```
ResolutionEngine (orchestrator)
    │
    ├─▶ CAS Extraction
    │       └─▶ Exact Match (if CAS found) → RETURN
    │
    ├─▶ Exact Matcher
    │       ├─ CAS lookup
    │       ├─ InChIKey lookup
    │       └─ Synonym exact match → RETURN if found
    │
    ├─▶ Fuzzy Matcher (parallel)
    │       └─ Levenshtein + Token Set Ratio
    │
    ├─▶ Semantic Matcher (parallel)
    │       └─ FAISS vector search
    │
    └─▶ Resolution Logic
        ├─ Disagreement detection
        ├─ Confidence calibration
        ├─ Threshold filtering
        └─ Best match selection → RETURN
```

## Quick Start

### 1. Generate Embeddings

First, generate embeddings for all synonyms in the database:

```bash
python scripts/09_generate_embeddings.py
```

This will:
- Load all synonyms from the database
- Encode them using sentence-transformers
- Create FAISS index (IndexFlatIP)
- Save to `data/embeddings/`
- Update `embeddings_metadata` table

### 2. Basic Usage

```python
from sqlalchemy.orm import Session
from src.matching import ResolutionEngine
from src.database.connection import get_session

# Get database session
session = next(get_session())

# Initialize resolution engine
engine = ResolutionEngine(session)

# Resolve a single query
result = engine.resolve("benzene")

if result.matched:
    print(f"Matched: {result.best_match.analyte_name}")
    print(f"CAS: {result.best_match.cas_number}")
    print(f"Confidence: {result.best_match.confidence}")
    print(f"Method: {result.best_match.method.value}")
else:
    print("No match found")

# Batch resolution
queries = ["toluene", "xylene", "ethylbenzene"]
results = engine.resolve_batch(queries)

# Export results
engine.export_results_csv(results, "output/matches.csv")
engine.export_results_json(results, "output/matches.json")
```

### 3. Direct Matcher Access

You can also use individual matchers directly:

```python
from src.matching import match_exact, match_fuzzy, SemanticMatcher

# Exact matching
match = match_exact("71-43-2", session)

# Fuzzy matching
matches = match_fuzzy("benzen", session, top_k=5, threshold=0.75)

# Semantic matching
semantic_matcher = SemanticMatcher()
matches = semantic_matcher.match_semantic("aromatic hydrocarbon", top_k=5)
```

## Configuration

Configure matching behavior through `MatcherConfig`:

```python
from src.matching.types import MatcherConfig

config = MatcherConfig(
    # Exact matching
    exact_cas_enabled=True,
    exact_synonym_enabled=True,
    exact_inchikey_enabled=True,
    
    # Fuzzy matching
    fuzzy_enabled=True,
    fuzzy_threshold=0.75,
    fuzzy_top_k=5,
    
    # Semantic matching
    semantic_enabled=True,
    semantic_threshold=0.75,
    semantic_top_k=5,
    
    # Resolution
    disagreement_penalty=0.1,
    disagreement_threshold=0.15,
    manual_review_threshold=0.80,
)

engine = ResolutionEngine(session, config=config)
```

## Performance

- **Target**: < 50ms per query
- **Exact matching**: ~1-5ms
- **Fuzzy matching**: ~10-30ms (depends on corpus size)
- **Semantic matching**: ~5-15ms with FAISS

### Optimization Tips

1. **Pre-filter by CAS**: If query contains CAS, exact match returns immediately
2. **FAISS index**: Keep in memory for fastest search
3. **Batch processing**: Use `resolve_batch()` for multiple queries
4. **Threshold tuning**: Higher thresholds = faster (fewer candidates)

## Data Structures

### Match

Represents a single match candidate:

```python
@dataclass
class Match:
    analyte_id: int
    analyte_name: str
    cas_number: Optional[str]
    confidence: float  # 0.0-1.0
    method: MatchMethod
    synonym_matched: Optional[str]
    synonym_id: Optional[int]
    distance_score: Optional[float]  # Fuzzy
    similarity_score: Optional[float]  # Semantic
    metadata: Dict[str, Any]
```

### MatchResult

Complete resolution result:

```python
@dataclass
class MatchResult:
    query_text: str
    query_norm: str
    best_match: Optional[Match]
    all_candidates: List[Match]
    methods_used: List[MatchMethod]
    signals: Dict[str, Any]
    disagreement_detected: bool
    disagreement_penalty: float
    cas_extracted: Optional[str]
    processing_time_ms: Optional[float]
    manual_review_recommended: bool
    review_reason: Optional[str]
```

## Confidence Levels

Confidence scores are mapped to categorical levels:

- **HIGH** (>= 0.95): Exact or very high confidence
- **MEDIUM** (>= 0.85): Good confidence
- **LOW** (>= 0.75): Acceptable, may need review
- **VERY_LOW** (< 0.75): Below threshold, requires review

## Disagreement Detection

When fuzzy and semantic matchers disagree on the top result:

1. **Detection**: Top fuzzy analyte ≠ top semantic analyte
2. **Severity**: Score difference > `disagreement_threshold` (default 0.15)
3. **Penalty**: Apply `disagreement_penalty` (default 0.1) to fuzzy matches
4. **Flagging**: Set `manual_review_recommended = True`

This helps identify ambiguous or problematic queries that need human review.

## Database Logging

All match decisions are logged to the `match_decisions` table:

```sql
CREATE TABLE match_decisions (
    id INTEGER PRIMARY KEY,
    query_text VARCHAR(500),
    query_norm VARCHAR(500),
    analyte_id INTEGER,  -- Best match (if any)
    confidence_score FLOAT,
    top_k_candidates JSON,  -- Top 10 candidates
    signals_used JSON,  -- Signal contributions
    corpus_snapshot_hash VARCHAR(64),
    model_hash VARCHAR(64),
    disagreement_flag BOOLEAN,
    manual_review BOOLEAN,
    created_at TIMESTAMP
);
```

This enables:
- Continuous improvement analysis
- A/B testing of different models
- Quality monitoring
- Training data collection

## Incremental Learning

The semantic matcher supports incremental additions:

```python
# Add new synonyms to existing index
new_texts = ["new synonym 1", "new synonym 2"]
new_metadata = [
    {'analyte_id': 1, 'synonym_id': 100},
    {'analyte_id': 2, 'synonym_id': 101},
]

semantic_matcher.add_embeddings(new_texts, new_metadata)

# Save updated index
semantic_matcher.save_index()
```

This is thread-safe and allows online learning without rebuilding the entire index.

## Testing

Run the test suite:

```bash
# Run all matching tests
pytest tests/test_matching.py -v

# Run specific test class
pytest tests/test_matching.py::TestExactMatching -v

# Run with coverage
pytest tests/test_matching.py --cov=src/matching
```

## File Structure

```
src/matching/
    __init__.py              # Package exports
    types.py                 # Type definitions
    exact_matcher.py         # Exact matching logic
    fuzzy_matcher.py         # Fuzzy string matching
    semantic_matcher.py      # FAISS semantic matching
    resolution_engine.py     # Orchestration layer

scripts/
    09_generate_embeddings.py  # Embedding generation script

tests/
    test_matching.py         # Comprehensive test suite

data/embeddings/
    faiss_index.bin          # FAISS index (binary)
    synonym_vectors.npy      # Raw embeddings (numpy)
    index_metadata.json      # Index -> synonym mapping
```

## Troubleshooting

### FAISS index not found

```
WARNING - FAISS index not found at data/embeddings/faiss_index.bin
```

**Solution**: Run `python scripts/09_generate_embeddings.py` to generate the index.

### Semantic matcher disabled

```
WARNING - Failed to load semantic matcher: [error]
```

**Solution**: Check that sentence-transformers and faiss-cpu are installed:

```bash
pip install sentence-transformers faiss-cpu
```

### Slow fuzzy matching

Fuzzy matching computes similarity against all synonyms. For large corpora:

1. Increase thresholds to filter more aggressively
2. Use exact matching first (faster)
3. Consider pre-filtering by first letter or other heuristics

## Future Enhancements

- [ ] GPU acceleration for FAISS (faiss-gpu)
- [ ] Hierarchical index for faster fuzzy matching
- [ ] Phonetic matching (Soundex, Metaphone)
- [ ] Structure-aware matching (RDKit fingerprints)
- [ ] Active learning from manual reviews
- [ ] Multi-model ensemble

## License

See project root LICENSE file.
