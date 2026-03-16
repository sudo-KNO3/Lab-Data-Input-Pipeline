"""
Local synonym bootstrap using ExhaustiveVariantGenerator.

Generates name variants for every analyte WITHOUT any network calls, then
loads supplemental synonyms from local data files:
  - data/pubchem_cache.json        (cached PubChem synonyms)
  - data/training/ontario_variants_known.csv  (observed lab variants)

Typical yield: 20-50 variants per analyte from name generation alone,
plus any cached/curated synonyms.

Usage:
    python scripts/05_local_variant_bootstrap.py [--db PATH] [--dry-run]
"""
import argparse
import csv
import json
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import select

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.bootstrap.quality_filters import filter_synonyms
from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, SynonymType, AnalyteType
from src.normalization.chemical_parser import ChemicalNameParser, ExhaustiveVariantGenerator
from src.normalization.text_normalizer import TextNormalizer


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def _get_existing_norms(session, analyte_id: str) -> set:
    rows = session.execute(
        select(Synonym.synonym_norm).where(Synonym.analyte_id == analyte_id)
    ).scalars().all()
    return set(rows)


def _insert_synonyms(session, analyte_id, analyte_type, raw_synonyms,
                     source: str, dry_run: bool) -> int:
    """Filter and insert synonyms; return count of new rows."""
    if not raw_synonyms:
        return 0

    cleaned = filter_synonyms(
        list(raw_synonyms),
        analyte_type=analyte_type,
        max_length=120,
        require_ascii=True,
    )
    existing = _get_existing_norms(session, analyte_id)

    added = 0
    for text in cleaned:
        norm = text.lower().strip()
        if norm in existing:
            continue
        if not dry_run:
            session.add(Synonym(
                analyte_id=analyte_id,
                synonym_raw=text,
                synonym_norm=norm,
                synonym_type=SynonymType.COMMON,
                harvest_source=source,
                confidence=0.9,
            ))
        existing.add(norm)
        added += 1
    return added


# ------------------------------------------------------------------ #
# Source 1: ExhaustiveVariantGenerator (local, no network)           #
# ------------------------------------------------------------------ #

def load_generated_variants(session, analytes, dry_run: bool) -> int:
    parser = ChemicalNameParser()
    generator = ExhaustiveVariantGenerator(parser)
    normalizer = TextNormalizer()

    total = 0
    for analyte in analytes:
        variants = generator.generate_all_variants(analyte.preferred_name)
        # Also feed any normalised form back in
        norm = normalizer.normalize(analyte.preferred_name)
        if norm:
            variants.add(norm)

        added = _insert_synonyms(
            session, analyte.analyte_id, analyte.analyte_type,
            variants, source="local_generator", dry_run=dry_run,
        )
        total += added
        logger.debug(f"{analyte.preferred_name}: +{added} generated variants")

    return total


# ------------------------------------------------------------------ #
# Source 2: pubchem_cache.json                                       #
# ------------------------------------------------------------------ #

def load_pubchem_cache(session, analytes, cache_path: Path, dry_run: bool) -> int:
    if not cache_path.exists():
        logger.warning(f"PubChem cache not found: {cache_path}")
        return 0

    with open(cache_path) as f:
        cache = json.load(f)

    # Build lookup: preferred_name.lower() -> analyte
    name_index = {a.preferred_name.lower(): a for a in analytes}
    # Also index by CAS
    cas_index = {a.cas_number: a for a in analytes if a.cas_number}

    total = 0
    for key, data in cache.items():
        synonyms = data.get("synonyms", [])
        if not synonyms:
            continue

        analyte = name_index.get(key.lower()) or cas_index.get(key)
        if not analyte:
            logger.debug(f"PubChem cache key '{key}' not matched to any analyte")
            continue

        added = _insert_synonyms(
            session, analyte.analyte_id, analyte.analyte_type,
            synonyms, source="pubchem_cache", dry_run=dry_run,
        )
        total += added
        logger.debug(f"PubChem cache '{key}': +{added} synonyms")

    return total


# ------------------------------------------------------------------ #
# Source 3: ontario_variants_known.csv                               #
# ------------------------------------------------------------------ #

def load_ontario_variants(session, analytes, csv_path: Path, dry_run: bool) -> int:
    if not csv_path.exists():
        logger.warning(f"Ontario variants file not found: {csv_path}")
        return 0

    name_index = {a.preferred_name.lower(): a for a in analytes}

    total = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            observed = (row.get("observed_text") or "").strip()
            canonical = (row.get("canonical_analyte_id") or "").strip()
            if not observed or not canonical:
                continue

            analyte = name_index.get(canonical.lower())
            if not analyte:
                continue

            added = _insert_synonyms(
                session, analyte.analyte_id, analyte.analyte_type,
                [observed], source="ontario_variants", dry_run=dry_run,
            )
            total += added

    return total


# ------------------------------------------------------------------ #
# Main                                                                #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap synonyms from local data (no network required)"
    )
    parser.add_argument("--db", default="data/reg153_matcher.db", help="Database path")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show counts without writing to database")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <level>{message}</level>",
               level="INFO")

    logger.info("=" * 70)
    logger.info("LOCAL SYNONYM BOOTSTRAP")
    logger.info(f"Database : {args.db}")
    logger.info(f"Dry-run  : {args.dry_run}")
    logger.info("=" * 70)

    db = DatabaseManager(db_path=args.db)

    with db.session_scope() as session:
        analytes = session.execute(
            select(Analyte).where(Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE)
        ).scalars().all()
        logger.info(f"Analytes to process: {len(analytes)}")

        # 1. Name variants (local generator)
        logger.info("Generating name variants (local, no network)...")
        n1 = load_generated_variants(session, analytes, args.dry_run)
        logger.info(f"  Generated variants : +{n1:,} synonyms")

        # 2. PubChem cache
        cache_path = Path("data/pubchem_cache.json")
        logger.info(f"Loading PubChem cache ({cache_path})...")
        n2 = load_pubchem_cache(session, analytes, cache_path, args.dry_run)
        logger.info(f"  PubChem cache      : +{n2:,} synonyms")

        # 3. Ontario lab variants
        variants_path = Path("data/training/ontario_variants_known.csv")
        logger.info(f"Loading Ontario variants ({variants_path})...")
        n3 = load_ontario_variants(session, analytes, variants_path, args.dry_run)
        logger.info(f"  Ontario variants   : +{n3:,} synonyms")

        if not args.dry_run:
            session.commit()

    total = n1 + n2 + n3
    logger.info("=" * 70)
    logger.info(f"TOTAL new synonyms added: {total:,}")
    if args.dry_run:
        logger.info("(dry-run — nothing written)")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
