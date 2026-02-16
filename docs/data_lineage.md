# Data Lineage Documentation

*Last Updated: 2026-02-12*

## Purpose

This document tracks the complete provenance of all data artifacts in the Ontario Reg 153 Chemical Matcher system. Every data transformation, API harvest, and model generation is documented with timestamps, sources, and reproducibility information.

---

## Data Sources

### 1. Regulatory Ground Truth

**Source:** Ontario Regulation 153/04 - Records of Site Condition  
**Type:** PDF document from e-Laws Ontario  
**URL:** https://www.ontario.ca/laws/regulation/040153  
**Last Retrieved:** [DATE]  
**Extraction Method:** [Manual/Automated PDF parsing]  
**Output:** `data/raw/reg153/schedule_d.csv`  
**Row Count:** ~170 substances  
**Columns:** substance_name, cas_number, table_number, regulatory_notes  

**Transformations:**
- Chemical name standardization
- CAS number validation
- Duplicate removal
- Manual curation of ambiguous entries

**Final Artifact:** `data/processed/canonical/reg153_master.csv`  
**Hash (SHA-256):** [HASH]  
**Version:** v1.0 (2026-02-12)

---

### 2. API-Harvested Synonyms

#### PubChem Harvest

**Harvest Date:** [DATE]  
**API Version:** REST PUG API  
**Input:** reg153_master.csv (CAS numbers + canonical names)  
**Method:** Batch queries via CAS and compound name  
**Rate Limit:** 5 requests/second  
**Request Count:** [COUNT]  
**Success Rate:** [PERCENT]%  
**Cache Location:** `data/raw/api_harvest/pubchem/`  
**Raw Files:** JSONL format, one file per chemical  

**Quality Filters Applied:**
- Minimum length: 3 characters
- Maximum length: 200 characters
- Excluded: Pure numeric IDs, UNII codes, DTXSID identifiers
- Deduplicated against existing synonyms

**Output:** `data/processed/synonyms/pubchem_synonyms.csv`  
**Synonym Count:** [COUNT]  
**Hash:** [HASH]

#### CompTox Dashboard Harvest

**Harvest Date:** [DATE]  
**API Endpoint:** EPA CompTox Chemicals Dashboard  
**Input:** reg153_master.csv  
**Request Count:** [COUNT]  
**Success Rate:** [PERCENT]%  
**Cache Location:** `data/raw/api_harvest/comptox/`  
**Output:** `data/processed/synonyms/comptox_synonyms.csv`  
**Synonym Count:** [COUNT]  
**Hash:** [HASH]

#### NCI Chemical Identifier Resolver

**Harvest Date:** [DATE]  
**Service:** NCI/CADD Group Chemical Identifier Resolver  
**Input:** reg153_master.csv (CAS numbers)  
**Request Count:** [COUNT]  
**Success Rate:** [PERCENT]%  
**Cache Location:** `data/raw/api_harvest/cir/`  
**Output:** `data/processed/synonyms/cir_synonyms.csv`  
**Synonym Count:** [COUNT]  
**Hash:** [HASH]

#### NPRI Canada

**Harvest Date:** [DATE]  
**Source:** National Pollutant Release Inventory  
**Method:** Manual download + parsing  
**File:** `data/raw/api_harvest/npri/substances_list.xlsx`  
**Output:** `data/processed/synonyms/npri_synonyms.csv`  
**Synonym Count:** [COUNT]  
**Hash:** [HASH]

---

### 3. Lab EDDS Data (Validation Source)

**Source:** Environmental lab analytical reports  
**Format:** Excel (.xlsx), PDF  
**Location:** `data/raw/lab_edds/`  
**Update Frequency:** Daily during active remediation projects  

**Ingestion Process:**
1. Manual upload or automated folder watch
2. Text extraction (pdfplumber for PDFs, pandas for Excel)
3. Column mapping to standard schema
4. Normalization pipeline
5. Matching + validation

**Schema:**
- sample_id
- analyte_name (raw)
- analyte_name_normalized
- concentration
- units
- detection_limit
- lab_name
- report_date

**Privacy:** No personally identifiable information stored. Sample IDs anonymized.

---

### 4. Manual Validation Records

**Source:** Human expert validation during matching  
**Location:** `data/training/ontario_variants_known.csv`  
**Update Frequency:** Continuous (after each validation session)  
**Purpose:** Ground truth for threshold calibration and semantic matcher training  

**Schema:**
- raw_name: Original lab analyte text
- matched_substance: Reg 153 canonical name
- match_type: exact | fuzzy | semantic | manual override
- confidence: 0.0-1.0
- validator: Person ID or "system"
- validation_date: ISO timestamp
- notes: Free text

**Current Count:** [COUNT] validated pairs  
**Hash:** [HASH]

---

## Transformations

### Synonym Corpus Merging

**Script:** `scripts/05_merge_synonym_corpus.py`  
**Execution Date:** [DATE]  
**Input Files:**
- pubchem_synonyms.csv
- comptox_synonyms.csv
- cir_synonyms.csv
- npri_synonyms.csv
- manual_curation.csv

**Process:**
1. Load all synonym sources
2. Deduplicate by (canonical_name, synonym) pairs
3. Apply quality filters
4. Normalize text (lowercase, strip whitespace)
5. Link to reg153_master via CAS or canonical name

**Output:** `data/processed/synonyms/merged_corpus.csv`  
**Total Unique Synonyms:** [COUNT]  
**Hash:** [HASH]

---

### Embedding Generation

**Script:** `scripts/09_generate_embeddings.py`  
**Execution Date:** [DATE]  
**Model:** sentence-transformers/all-MiniLM-L6-v2  
**Embedding Dimension:** 384  

**Input:** `data/processed/synonyms/merged_corpus.csv`  
**Process:**
1. Extract unique text variants
2. Batch encode using SentenceTransformer
3. Normalize vectors (L2 norm)
4. Build FAISS index (IndexFlatIP for cosine similarity)

**Outputs:**
- `data/embeddings/synonym_embeddings.npy` - Vector matrix
- `data/embeddings/synonym_index.faiss` - FAISS index
- `data/embeddings/synonym_metadata.csv` - ID to text mapping

**Vector Count:** [COUNT]  
**Index Type:** Flat (exact search)  
**Hash (embeddings.npy):** [HASH]  
**Hash (faiss index):** [HASH]

---

## Artifact Hashes

Use these hashes to verify data integrity and reproducibility.

| Artifact | SHA-256 Hash | Size | Updated |
|----------|--------------|------|---------|
| reg153_master.csv | [HASH] | [SIZE] | 2026-02-12 |
| merged_corpus.csv | [HASH] | [SIZE] | [DATE] |
| synonym_embeddings.npy | [HASH] | [SIZE] | [DATE] |
| synonym_index.faiss | [HASH] | [SIZE] | [DATE] |
| ontario_variants_known.csv | [HASH] | [SIZE] | [DATE] |
| database.db | [HASH] | [SIZE] | [DATE] |

---

## Version History

### v0.1.0 - Initial Bootstrap (2026-02-12)
- Extracted Reg 153 Schedule D substances
- Harvested synonyms from PubChem, CompTox, CIR, NPRI
- Generated initial embeddings
- Established data lineage tracking

### v0.2.0 - First Production Harvest ([DATE])
- [Describe changes]
- [New data sources]
- [Updates to canonical list]

---

## Reproducibility Notes

To reproduce the full data pipeline from scratch:

```bash
# 1. Extract regulatory ground truth
python scripts/01_extract_reg153.py

# 2. Bootstrap API synonyms
python scripts/04_harvest_api_synonyms.py

# 3. Merge synonym corpus
python scripts/05_merge_synonym_corpus.py

# 4. Generate embeddings
python scripts/09_generate_embeddings.py

# 5. Setup database
python scripts/setup_database.py
```

**Total Runtime:** Approximately [TIME] hours  
**Storage Required:** ~[SIZE] GB

---

## Data Governance

**Owner:** [Your Name/Department]  
**Steward:** [Technical Contact]  
**Classification:** Internal Use - Environmental Consulting  
**Retention:** Indefinite (regulatory reference data)  
**Backup Frequency:** Weekly snapshots to `snapshots/` directory  
**Access Control:** Read/write restricted to project team

---

## Contact

For questions about data provenance or to report data quality issues:
- **Email:** [email]
- **Slack:** [channel]
- **Issue Tracker:** [link]

---

*This document is automatically updated by data pipeline scripts. Manual edits should include a note in this section.*
