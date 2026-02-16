"""
Exhaustive Synonym Harvest Script

This script orchestrates an exhaustive synonym harvesting process for analytes
with insufficient synonym coverage. It:
1. Identifies analytes with <= 5 synonyms
2. Generates exhaustive name variants using ExhaustiveVariantGenerator
3. Queries PubChem in parallel for each variant
4. Deduplicates and filters results
5. Inserts high-quality synonyms into the database

Usage:
    python scripts/exhaustive_synonym_harvest.py --database data/reg153_matcher.db
    python scripts/exhaustive_synonym_harvest.py --limit 10 --dry-run
    python scripts/exhaustive_synonym_harvest.py --analyte-id 123
    python scripts/exhaustive_synonym_harvest.py --max-workers 5
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

from loguru import logger
from sqlalchemy import func
from sqlalchemy.orm import Session
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalization.chemical_parser import ChemicalNameParser, ExhaustiveVariantGenerator
from src.normalization.text_normalizer import TextNormalizer
from src.bootstrap.parallel_harvester import harvest_synonyms_parallel, deduplicate_synonyms
from src.bootstrap.api_harvesters import PubChemHarvester
from src.bootstrap.quality_filters import filter_synonyms
from src.database.models import Analyte, Synonym, SynonymType
from src.database.connection import DatabaseManager


def setup_logging():
    """Configure loguru logger for the harvest process."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        "logs/exhaustive_harvest_{time}.log",
        rotation="10 MB",
        retention="30 days",
        level="DEBUG"
    )


def get_target_analytes(
    session: Session,
    limit: int = None,
    analyte_id: str = None,
    max_synonyms: int = 5
) -> List[Analyte]:
    """
    Query database for analytes that need more synonyms.
    
    Args:
        session: Database session
        limit: Maximum number of analytes to process
        analyte_id: Specific analyte ID to process (overrides other filters)
        max_synonyms: Only select analytes with this many or fewer synonyms
        
    Returns:
        List of Analyte objects
    """
    if analyte_id:
        analyte = session.query(Analyte).filter(Analyte.analyte_id == analyte_id).first()
        if analyte:
            logger.info(f"Processing specific analyte: {analyte.preferred_name} (ID: {analyte_id})")
            return [analyte]
        else:
            logger.error(f"Analyte with ID {analyte_id} not found")
            return []
    
    # Query for analytes with few synonyms
    query = (
        session.query(Analyte)
        .outerjoin(Synonym)
        .group_by(Analyte.analyte_id)
        .having(func.count(Synonym.id) <= max_synonyms)
        .order_by(func.count(Synonym.id), Analyte.preferred_name)
    )
    
    if limit:
        query = query.limit(limit)
    
    analytes = query.all()
    logger.info(f"Found {len(analytes)} analytes with <= {max_synonyms} synonyms")
    
    return analytes


def get_existing_synonyms(session: Session, analyte_id: str) -> Set[str]:
    """
    Get set of existing synonym names for an analyte.
    
    Args:
        session: Database session
        analyte_id: Analyte ID
        
    Returns:
        Set of normalized synonym names
    """
    synonyms = (
        session.query(Synonym.synonym_norm)
        .filter(Synonym.analyte_id == analyte_id)
        .all()
    )
    return {syn.synonym_norm.lower().strip() for syn in synonyms}


def process_analyte(
    analyte: Analyte,
    session: Session,
    harvester: PubChemHarvester,
    variant_generator: ExhaustiveVariantGenerator,
    normalizer: TextNormalizer,
    max_workers: int,
    dry_run: bool,
    stats: Dict[str, int]
) -> Dict[str, int]:
    """
    Process a single analyte: generate variants, harvest synonyms, insert into database.
    
    Args:
        analyte: Analyte object to process
        session: Database session
        harvester: PubChemHarvester instance
        variant_generator: ExhaustiveVariantGenerator instance
        max_workers: Number of parallel workers
        dry_run: If True, don't insert into database
        stats: Global statistics dictionary to update
        
    Returns:
        Dictionary with per-analyte statistics
    """
    analyte_stats = {
        'variants_generated': 0,
        'variants_successful': 0,
        'synonyms_found': 0,
        'synonyms_inserted': 0
    }
    
    logger.info(f"Processing: {analyte.preferred_name} (ID: {analyte.analyte_id})")
    stats['attempted'] += 1
    
    # Generate variants
    variants = variant_generator.generate_all_variants(analyte.preferred_name)
    analyte_stats['variants_generated'] = len(variants)
    stats['variants_generated'] += len(variants)
    
    logger.debug(f"Generated {len(variants)} variants for {analyte.preferred_name}")
    
    if not variants:
        logger.warning(f"No variants generated for {analyte.preferred_name}")
        return analyte_stats
    
    # Harvest synonyms in parallel
    raw_results = harvest_synonyms_parallel(
        analyte,
        variants,
        harvester,
        max_workers=max_workers
    )
    
    # Count successful variant matches
    successful_variants = len(raw_results)
    analyte_stats['variants_successful'] = successful_variants
    stats['variants_successful'] += successful_variants
    
    # Deduplicate across all variant results
    all_synonyms = deduplicate_synonyms(raw_results)
    analyte_stats['synonyms_found'] = len(all_synonyms)
    stats['synonyms_found'] += len(all_synonyms)
    
    logger.info(
        f"  Variants: {len(variants)} generated, {successful_variants} matched"
    )
    logger.info(f"  Found {len(all_synonyms)} unique synonyms")
    
    if not all_synonyms:
        logger.warning(f"No synonyms found for {analyte.preferred_name}")
        return analyte_stats
    
    # Get existing synonyms to avoid duplicates
    existing_synonyms = get_existing_synonyms(session, analyte.analyte_id)
    logger.debug(f"Analyte has {len(existing_synonyms)} existing synonyms")
    
    # Filter out existing synonyms
    new_synonyms = [
        syn for syn in all_synonyms
        if syn.lower().strip() not in existing_synonyms
    ]
    
    logger.info(f"  {len(new_synonyms)} new synonyms after deduplication")
    
    if not new_synonyms:
        logger.info(f"All synonyms already exist for {analyte.preferred_name}")
        return analyte_stats
    
    # Apply quality filters
    filtered_synonyms = filter_synonyms(
        new_synonyms,
        analyte_type=analyte.analyte_type.value,
        max_length=120,
        require_ascii=True
    )
    
    logger.info(f"  {len(filtered_synonyms)} synonyms after quality filtering")
    
    if not filtered_synonyms:
        logger.warning(f"All synonyms filtered out for {analyte.preferred_name}")
        return analyte_stats
    
    # Insert into database (unless dry run)
    if dry_run:
        logger.info(f"  [DRY RUN] Would insert {len(filtered_synonyms)} synonyms")
        analyte_stats['synonyms_inserted'] = len(filtered_synonyms)
        stats['synonyms_inserted'] += len(filtered_synonyms)
    else:
        inserted_count = 0
        for synonym_name in filtered_synonyms:
            try:
                synonym = Synonym(
                    analyte_id=analyte.analyte_id,
                    synonym_raw=synonym_name,
                    synonym_norm=normalizer.normalize(synonym_name),
                    synonym_type=SynonymType.COMMON,
                    harvest_source='pubchem_exhaustive'
                )
                session.add(synonym)
                inserted_count += 1
            except Exception as e:
                logger.error(f"Error inserting synonym '{synonym_name}': {e}")
                continue
        
        try:
            session.commit()
            analyte_stats['synonyms_inserted'] = inserted_count
            stats['synonyms_inserted'] += inserted_count
            logger.success(f"  Inserted {inserted_count} new synonyms")
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing synonyms for {analyte.preferred_name}: {e}")
    
    return analyte_stats


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Exhaustive synonym harvesting for analytes with insufficient coverage"
    )
    parser.add_argument(
        '--database',
        type=str,
        default='data/reg153_matcher.db',
        help='Path to SQLite database (default: data/reg153_matcher.db)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Process only N analytes (default: all)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=3,
        help='Number of parallel workers for API requests (default: 3)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without inserting into database (for testing)'
    )
    parser.add_argument(
        '--analyte-id',
        type=str,
        default=None,
        help='Process specific analyte ID only (e.g., REG153_METALS_006)'
    )
    parser.add_argument(
        '--max-synonyms',
        type=int,
        default=5,
        help='Target analytes with this many or fewer synonyms (default: 5)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logger.info("=" * 80)
    logger.info("EXHAUSTIVE SYNONYM HARVEST")
    logger.info("=" * 80)
    logger.info(f"Database: {args.database}")
    logger.info(f"Max workers: {args.max_workers}")
    logger.info(f"Dry run: {args.dry_run}")
    if args.limit:
        logger.info(f"Limit: {args.limit} analytes")
    if args.analyte_id:
        logger.info(f"Target analyte ID: {args.analyte_id}")
    else:
        logger.info(f"Target: analytes with <= {args.max_synonyms} synonyms")
    logger.info("=" * 80)
    
    # Initialize components
    db_manager = DatabaseManager(args.database)
    harvester = PubChemHarvester()
    parser = ChemicalNameParser()
    variant_generator = ExhaustiveVariantGenerator(parser)
    normalizer = TextNormalizer()
    
    # Global statistics
    stats = {
        'attempted': 0,
        'variants_generated': 0,
        'variants_successful': 0,
        'synonyms_found': 0,
        'synonyms_inserted': 0
    }
    
    # Get target analytes
    with db_manager.session_scope() as session:
        analytes = get_target_analytes(
            session,
            limit=args.limit,
            analyte_id=args.analyte_id,
            max_synonyms=args.max_synonyms
        )
        
        if not analytes:
            logger.warning("No analytes to process")
            return
        
        # Process each analyte with progress bar
        logger.info(f"\nProcessing {len(analytes)} analytes...\n")
        
        for analyte in tqdm(analytes, desc="Harvesting synonyms", unit="analyte"):
            try:
                analyte_stats = process_analyte(
                    analyte,
                    session,
                    harvester,
                    variant_generator,
                    normalizer,
                    args.max_workers,
                    args.dry_run,
                    stats
                )
                
                # Log detailed per-analyte results
                logger.debug(
                    f"Completed {analyte.preferred_name}: "
                    f"variants={analyte_stats['variants_generated']}, "
                    f"matched={analyte_stats['variants_successful']}, "
                    f"found={analyte_stats['synonyms_found']}, "
                    f"inserted={analyte_stats['synonyms_inserted']}"
                )
                
            except KeyboardInterrupt:
                logger.warning("\nProcess interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error processing {analyte.preferred_name}: {e}")
                continue
    
    # Report final results
    logger.info("\n" + "=" * 80)
    logger.info("HARVEST COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Analytes processed:      {stats['attempted']}")
    logger.info(f"Variants generated:      {stats['variants_generated']}")
    logger.info(f"Variants matched:        {stats['variants_successful']}")
    logger.info(f"Unique synonyms found:   {stats['synonyms_found']}")
    logger.info(f"{'[DRY RUN] Would insert:' if args.dry_run else 'New synonyms inserted:'} {stats['synonyms_inserted']}")
    logger.info("=" * 80)
    
    # Calculate some metrics
    if stats['attempted'] > 0:
        avg_variants = stats['variants_generated'] / stats['attempted']
        avg_synonyms = stats['synonyms_inserted'] / stats['attempted']
        logger.info(f"Average variants per analyte:    {avg_variants:.1f}")
        logger.info(f"Average new synonyms per analyte: {avg_synonyms:.1f}")
    
    if stats['variants_generated'] > 0:
        match_rate = (stats['variants_successful'] / stats['variants_generated']) * 100
        logger.info(f"Variant match rate:              {match_rate:.1f}%")
    
    logger.success("\n✓ Exhaustive synonym harvest completed successfully")


if __name__ == '__main__':
    main()
