# Synonym Corpus Snapshots

This directory stores versioned snapshots of the merged synonym corpus.

## Purpose

Track the evolution of the synonym vocabulary over time as new variants are discovered through validation.

## Contents

- **synonym_corpus_YYYY-MM-DD.csv** - Timestamped corpus snapshots
- **growth_metrics.json** - Synonym count over time
- **source_distribution.json** - Percentage from each source (API vs. validation)

## Snapshot Strategy

- **Weekly:** Automatic snapshot every Friday night
- **On-Demand:** Before major system updates or threshold recalibrations
- **Retention:** Keep all snapshots indefinitely (small files, critical audit trail)

## Schema

Each snapshot CSV contains:
- canonical_name: Reg 153 substance name
- synonym: Variant text
- source: pubchem | comptox | cir | npri | validation
- first_seen_date: When synonym was added
- usage_count: How many times matched (if from validation)

## Restoration

To restore a previous synonym corpus:
```bash
cp snapshots/synonym_corpus/synonym_corpus_2026-01-15.csv data/processed/synonyms/merged_corpus.csv
python scripts/09_generate_embeddings.py  # Regenerate embeddings
```
