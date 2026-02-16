# Learning System Quick Start Guide

Get up and running with the incremental learning system in 5 minutes.

## Prerequisites

```bash
# Install dependencies (if not already done)
pip install -r requirements.txt

# Ensure database is set up
python scripts/setup_database.py
```

## Step 1: Initialize the Embedder (One-Time Setup)

```python
from src.learning.incremental_embedder import IncrementalEmbedder
from src.database.connection import get_session_factory

# Create embedder
embedder = IncrementalEmbedder(
    model_name='all-MiniLM-L6-v2',
    base_dir='data/embeddings',
    auto_save_interval=100
)

print(f"Embedder initialized: {embedder.get_stats()}")
```

## Step 2: Ingest Your First Synonym

```python
from src.learning.synonym_ingestion import ingest_validated_synonym

# Get database session
SessionFactory = get_session_factory()
session = SessionFactory()

# Ingest a validated synonym
result = ingest_validated_synonym(
    raw_text="Tert-Butyl Alcohol",
    analyte_id=42,  # Your analyte ID
    session=session,
    embedder=embedder,
    confidence=1.0
)

if result['success']:
    print(f"✓ Added synonym_id={result['synonym_id']}")
else:
    print(f"✗ Failed: {result['message']}")

session.close()
```

## Step 3: Set Up Daily Automation

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task
3. Name: "Chemical Matcher - Daily Validations"
4. Trigger: Daily at 2:00 AM
5. Action: Start a program
   - Program: `python`
   - Arguments: `scripts/09_ingest_daily_validations.py`
   - Start in: `n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher`

### Linux/Mac (Cron)

```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 2 AM)
0 2 * * * cd /path/to/project && python scripts/09_ingest_daily_validations.py >> logs/cron_daily.log 2>&1
```

## Step 4: Run Your First Calibration

```bash
# Dry run to see what would happen
python scripts/10_monthly_calibration.py --days 30 --output reports/

# Check the generated reports
cat reports/monthly_calibration_*.txt
```

## Step 5: Enable Human Validation in Your UI

When a user confirms a match is correct:

```python
from src.database.models import MatchDecision

# After user confirms match
decision = session.get(MatchDecision, decision_id)
decision.human_validated = True
session.commit()

# The daily script will automatically ingest this
```

## Testing the System

```bash
# Run tests to verify everything works
pytest tests/test_learning.py -v

# Test just synonym ingestion
pytest tests/test_learning.py::TestSynonymIngestion -v

# Test with coverage
pytest tests/test_learning.py --cov=src/learning
```

## Example Workflow

### Complete Daily Cycle (Automated)

```python
# This runs automatically via scheduled task

from src.learning.synonym_ingestion import batch_ingest_validations
from src.learning.incremental_embedder import IncrementalEmbedder
from src.database.connection import get_session_factory
from src.database.models import MatchDecision
from sqlalchemy import select, and_
from datetime import datetime, timedelta

# Initialize
SessionFactory = get_session_factory()
session = SessionFactory()
embedder = IncrementalEmbedder(model_name='all-MiniLM-L6-v2')

# Get validated decisions (last 7 days)
cutoff = datetime.utcnow() - timedelta(days=7)
decisions = session.execute(
    select(MatchDecision).where(
        and_(
            MatchDecision.human_validated == True,
            MatchDecision.ingested == False,
            MatchDecision.created_at >= cutoff
        )
    )
).scalars().all()

# Prepare validations
validations = [
    (d.query_text, d.analyte_id) 
    for d in decisions 
    if d.analyte_id is not None
]

# Batch ingest
results = batch_ingest_validations(
    validations=validations,
    session=session,
    embedder=embedder
)

print(f"✓ Ingested {results['successful']} synonyms")
print(f"⊗ Skipped {results['duplicates']} duplicates")
print(f"✗ Errors: {results['errors']}")

# Mark as ingested
from sqlalchemy import update
session.execute(
    update(MatchDecision)
    .where(MatchDecision.id.in_([d.id for d in decisions]))
    .values(ingested=True, ingested_at=datetime.utcnow())
)
session.commit()
session.close()
```

### Complete Monthly Cycle (Manual/Scheduled)

```bash
# Run monthly calibration
python scripts/10_monthly_calibration.py --days 30 --output reports/

# Review the report
cat reports/monthly_calibration_*.txt

# If thresholds changed, update config
python scripts/10_monthly_calibration.py --days 30 --update-config config/matching.yaml

# Check if retraining is recommended
# (Review the "RETRAINING RECOMMENDATION" section in report)
```

## Common Operations

### Check Corpus Health

```python
from src.learning.maturity_metrics import calculate_corpus_maturity
from src.database.connection import get_session_factory

session = get_session_factory()()
metrics = calculate_corpus_maturity(session, history_days=90)

print(f"Synonyms: {metrics['overall']['total_synonyms']}")
print(f"Analytes: {metrics['overall']['total_analytes']}")
print(f"Unknown rate: {metrics['overall']['unknown_rate']:.2%}")
print(f"Semantic reliance: {metrics['overall']['semantic_reliance']:.2%}")

session.close()
```

### Find Unknown Clusters

```python
from src.learning.variant_clustering import (
    get_recent_unknowns,
    cluster_similar_unknowns,
    suggest_batch_validation
)
from src.database.connection import get_session_factory

session = get_session_factory()()

# Get recent unknowns
unknowns = get_recent_unknowns(session, days_back=30, min_frequency=2)
print(f"Found {len(unknowns)} unknown terms")

# Cluster similar unknowns
clusters = cluster_similar_unknowns(unknowns, threshold=0.85)
print(f"Found {len(clusters)} clusters")

# Get suggestions
df = suggest_batch_validation(clusters, session, top_k=3)
print("\nTop suggestions:")
print(df[df['rank'] == 1][['anchor_term', 'suggested_name', 'confidence']])

session.close()
```

### Manual Threshold Tuning

```python
from src.learning.threshold_calibrator import (
    analyze_match_decisions,
    recalibrate_thresholds
)
from src.database.connection import get_session_factory

session = get_session_factory()()

# Analyze recent performance
stats = analyze_match_decisions(session, days_back=30)
print(f"Acceptance rate: {stats['acceptance_rate_top1']:.2%}")
print(f"Unknown rate: {stats['unknown_rate']:.2%}")

# Recalibrate for higher precision
results = recalibrate_thresholds(
    session,
    days_back=30,
    target_precision=0.99  # Very strict
)

if results['success']:
    print("\nNew thresholds:")
    for method, threshold in results['recommended_thresholds'].items():
        print(f"  {method}: {threshold:.3f}")

session.close()
```

## Monitoring Dashboard (Example)

Create a simple monitoring script:

```python
# scripts/learning_dashboard.py

from src.learning.maturity_metrics import calculate_corpus_maturity
from src.learning.threshold_calibrator import analyze_match_decisions
from src.database.connection import get_session_factory
from datetime import datetime

session = get_session_factory()()

print("="*60)
print("LEARNING SYSTEM DASHBOARD")
print("="*60)
print(f"Generated: {datetime.now()}")
print()

# Corpus status
metrics = calculate_corpus_maturity(session, history_days=90)
print("Corpus Status:")
print(f"  Total synonyms: {metrics['overall']['total_synonyms']:,}")
print(f"  Total analytes: {metrics['overall']['total_analytes']:,}")
print(f"  Avg synonyms/analyte: {metrics['overall']['avg_synonyms_per_analyte']:.1f}")
print()

# Growth
print("Growth (Last 7 days):")
print(f"  New synonyms: {metrics['growth']['synonyms_added_7d']}")
print(f"  Weekly rate: {metrics['growth']['growth_rate_weekly']:.1f}/week")
print()

# Performance
print("Match Performance (Last 30 days):")
print(f"  Exact match rate: {metrics['overall']['exact_match_rate']:.2%}")
print(f"  Unknown rate: {metrics['overall']['unknown_rate']:.2%}")
print(f"  Semantic reliance: {metrics['overall']['semantic_reliance']:.2%}")
print()

# Decisions
stats = analyze_match_decisions(session, days_back=30)
print("Decision Quality:")
print(f"  Total decisions: {stats['total_decisions']}")
print(f"  Acceptance rate: {stats['acceptance_rate_top1']:.2%}")
print(f"  Override rate: {stats['override_frequency']:.2%}")

session.close()
```

Run it:
```bash
python scripts/learning_dashboard.py
```

## Troubleshooting Quick Reference

| Problem | Quick Fix |
|---------|-----------|
| Import errors | `pip install -r requirements.txt` |
| Database errors | Check connection settings |
| FAISS save fails | Check disk space & permissions |
| No validations found | Set `human_validated=TRUE` on decisions |
| Slow ingestion | Use batch mode, increase auto_save_interval |
| High duplicate rate | Review normalization consistency |

## Next Steps

1. ✓ Set up daily automation
2. ✓ Test with a few manual validations
3. ✓ Run first monthly calibration
4. ⬜ Monitor for 1-2 weeks
5. ⬜ Adjust thresholds based on performance
6. ⬜ Set up monitoring alerts
7. ⬜ Document any custom workflows

## Support

- **Documentation:** See `src/learning/README.md`
- **Tests:** Run `pytest tests/test_learning.py -v`
- **Logs:** Check `logs/` directory
- **Config:** Review `config/matching.yaml`

---

**Ready to learn!** 🚀
