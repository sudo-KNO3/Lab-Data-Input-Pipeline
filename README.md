# Ontario Reg 153/04 Chemical Name Normalization System

A self-training chemical name matcher that normalizes lab analyte names to canonical Ontario Regulation 153/04 identities.

## Architecture

**Three-Layer Learning System:**
- **Layer 1 (Primary):** Continuous synonym accretion - every validation adds knowledge immediately
- **Layer 2 (Calibration):** Monthly statistical threshold tuning
- **Layer 3 (Neural):** Rare model retraining (6-12 months, only when signal plateaus)

**Key Innovation:** System matures through vocabulary growth, not perpetual AI retraining. After 12 months, becomes an Ontario environmental language registry with 80%+ exact match rate.

## Project Structure

```
reg153-chemical-matcher/
├── data/
│   ├── raw/                      # Original source files
│   │   ├── reg153/               # Regulation PDFs
│   │   ├── api_harvest/          # API response dumps
│   │   └── lab_edds/             # Lab reports
│   ├── processed/
│   │   ├── canonical/            # reg153_master.csv (ground truth)
│   │   └── synonyms/             # Merged synonym corpus
│   ├── training/                 # Labeled ground truth
│   ├── embeddings/               # Vectors + FAISS indices
│   └── snapshots/                # Versioned releases
├── src/
│   ├── bootstrap/                # API harvesters (one-time use)
│   ├── extraction/               # PDF parsing + validation
│   ├── normalization/            # Text cleaning (analyte-aware)
│   ├── matching/                 # Resolution engine
│   ├── learning/                 # Incremental learning system
│   └── database/                 # SQLite models
├── scripts/                      # Numbered workflow scripts
├── tests/                        # Comprehensive test suite
└── docs/                         # Architecture + lineage docs
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Setup database
python scripts/setup_database.py

# Load synthetic demo data (or connect your own lab data)
python scripts/generate_synthetic_edds.py

# Bootstrap API synonyms (PubChem + NCI - no auth needed)
python scripts/04_harvest_api_synonyms.py

# Generate embeddings
python scripts/09_generate_embeddings.py

# Match chemicals from lab report
python scripts/22_batch_ingest.py --input lab_data.xlsx
```

## APIs Used (Bootstrap Phase Only)

- **PubChem** - No authentication required, 5 req/sec
- **NCI Chemical Identifier Resolver** - Open service for structure conversion
- **NPRI (Canada)** - Open government data
- **CompTox (EPA)** - Environmental chemical database
- **CAS Common Chemistry** - Free registration required

APIs used ONCE for synonym harvesting, then system runs locally.

## Maturity Trajectory

| Metric | Month 1 | Month 6 | Month 12 |
|--------|---------|---------|----------|
| Exact match rate | 40% | 71% | 82%+ |
| Semantic reliance | 45% | 24% | <20% |
| Unknown rate | 15% | 5% | <2% |
| Validation effort | 15 hrs/wk | 8 hrs/wk | 2-3 hrs/wk |

## License

Internal tool for environmental consulting - Ontario-specific implementation.

## Version

v0.1.0 - Initial implementation (2026-02-12)
