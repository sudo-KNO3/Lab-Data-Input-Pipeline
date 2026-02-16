"""
Harvest chemical synonyms from public APIs.

This script queries multiple public chemical databases to harvest synonyms
for all analytes in the database. It applies quality filters and inserts
the synonyms into the database for later use in matching.

APIs used:
- PubChem (NCBI) - No authentication required
- NCI Chemical Identifier Resolver - Free public service
- NPRI (Canada) - Open government data

Usage:
    python scripts/04_harvest_api_synonyms.py [--limit N] [--source SOURCE]
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Set

from loguru import logger
from sqlalchemy import select
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bootstrap import create_harvesters, filter_synonyms
from src.database import Analyte, APIHarvestMetadata, Synonym, AnalyteType, SynonymType
from src.database.connection import DatabaseManager


def setup_logging(log_file: Path = None):
    """Configure logging."""
    logger.remove()  # Remove default handler

    # Console handler with INFO level
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )

    # File handler with DEBUG level
    if log_file is None:
        log_file = Path("logs") / f"harvest_api_synonyms_{datetime.now():%Y%m%d_%H%M%S}.log"

    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        level="DEBUG",
        rotation="10 MB",
    )

    logger.info(f"Logging to {log_file}")


def load_analytes_to_process(session, limit: int = None, include_no_cas: bool = False) -> List[Analyte]:
    """
    Load analytes that need synonym harvesting.
    
    Args:
        session: Database session
        limit: Optional limit on number of analytes
        include_no_cas: If True, also process analytes without CAS numbers
        
    Returns:
        List of analyte records
    """
    if include_no_cas:
        # Process all single substances, even without CAS
        query = select(Analyte).where(
            Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE
        )
    else:
        # Only process analytes with CAS numbers
        query = select(Analyte).where(
            Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE,
            Analyte.cas_number.isnot(None),
        )

    if limit:
        query = query.limit(limit)

    analytes = session.execute(query).scalars().all()
    logger.info(f"Loaded {len(analytes)} analytes for processing")
    return analytes


def get_existing_synonyms(session, analyte_id: int, source: str) -> Set[str]:
    """
    Get existing synonyms for an analyte from a specific source.
    
    Args:
        session: Database session
        analyte_id: Analyte ID
        source: Harvest source name
        
    Returns:
        Set of normalized synonym texts
    """
    query = select(Synonym.synonym_norm).where(
        Synonym.analyte_id == analyte_id,
        Synonym.harvest_source == source,
    )

    results = session.execute(query).scalars().all()
    return set(results)


def harvest_for_analyte(
    analyte: Analyte,
    harvesters: dict,
    session,
    source_filter: str = None,
    update_cas: bool = False,
) -> dict:
    """
    Harvest synonyms for a single analyte.
    
    Args:
        analyte: Analyte record
        harvesters: Dictionary of harvester instances
        session: Database session
        source_filter: Optional source to harvest from
        update_cas: If True, also fetch CAS number from PubChem if missing
        
    Returns:
        Dictionary with harvest statistics
    """
    stats = {
        "analyte_id": analyte.analyte_id,
        "cas_number": analyte.cas_number,
        "by_source": {},
        "total_raw": 0,
        "total_filtered": 0,
        "total_new": 0,
        "total_duplicate": 0,
        "errors": [],
        "cas_updated": False,
    }

    # Try to fetch CAS number from PubChem if missing
    if update_cas and not analyte.cas_number and "pubchem" in harvesters:
        try:
            pubchem = harvesters["pubchem"]
            cas_number = pubchem.get_cas_number(analyte.preferred_name)
            
            if cas_number:
                logger.info(f"Found CAS {cas_number} for '{analyte.preferred_name}'")
                analyte.cas_number = cas_number
                stats["cas_number"] = cas_number
                stats["cas_updated"] = True
                
                # Update database immediately
                session.add(analyte)
                session.flush()
        except Exception as e:
            error_msg = f"Error fetching CAS from PubChem: {e}"
            logger.warning(error_msg)
            stats["errors"].append(error_msg)

    # Filter harvesters if requested
    active_harvesters = harvesters
    if source_filter:
        if source_filter in harvesters:
            active_harvesters = {source_filter: harvesters[source_filter]}
        else:
            logger.error(f"Unknown source: {source_filter}")
            return stats

    # Harvest from each source
    for source_name, harvester in active_harvesters.items():
        source_stats = {
            "raw_count": 0,
            "filtered_count": 0,
            "new_count": 0,
            "duplicate_count": 0,
            "error": None,
        }

        try:
            # Get existing synonyms for this source
            existing = get_existing_synonyms(session, analyte.analyte_id, source_name)

            # Harvest synonyms
            start_time = time.time()
            raw_synonyms = harvester.harvest_synonyms(
                analyte.cas_number, analyte.preferred_name
            )
            duration_ms = int((time.time() - start_time) * 1000)

            source_stats["raw_count"] = len(raw_synonyms)
            stats["total_raw"] += len(raw_synonyms)

            if not raw_synonyms:
                logger.debug(f"No synonyms from {source_name} for {analyte.cas_number}")
                # Record empty harvest in metadata
                record_harvest_metadata(
                    session,
                    source_name,
                    analyte.analyte_id,
                    0,
                    duration_ms,
                    success=True,
                )
                continue

            # Apply quality filters
            filtered_synonyms = filter_synonyms(
                raw_synonyms,
                analyte.analyte_type,
                max_length=120,
                require_ascii=True,
            )

            source_stats["filtered_count"] = len(filtered_synonyms)
            stats["total_filtered"] += len(filtered_synonyms)

            # Insert new synonyms
            new_count = 0
            duplicate_count = 0

            for synonym_text in filtered_synonyms:
                # Normalize for comparison
                normalized = synonym_text.lower().strip()

                if normalized in existing:
                    duplicate_count += 1
                    continue

                # Insert new synonym
                synonym = Synonym(
                    analyte_id=analyte.analyte_id,
                    synonym_raw=synonym_text,
                    synonym_norm=normalized,
                    synonym_type=SynonymType.COMMON,  # API synonyms are common names
                    harvest_source=source_name,
                    confidence=1.0,
                )
                session.add(synonym)
                existing.add(normalized)
                new_count += 1

            source_stats["new_count"] = new_count
            source_stats["duplicate_count"] = duplicate_count
            stats["total_new"] += new_count
            stats["total_duplicate"] += duplicate_count

            # Record harvest metadata
            record_harvest_metadata(
                session,
                source_name,
                analyte.analyte_id,
                new_count,
                duration_ms,
                success=True,
            )

            logger.debug(
                f"{source_name} for {analyte.cas_number}: "
                f"{source_stats['raw_count']} raw → "
                f"{source_stats['filtered_count']} filtered → "
                f"{source_stats['new_count']} new"
            )

        except Exception as e:
            error_msg = f"Error harvesting from {source_name}: {e}"
            logger.error(error_msg)
            source_stats["error"] = str(e)
            stats["errors"].append(error_msg)

            # Record failed harvest
            record_harvest_metadata(
                session,
                source_name,
                analyte.analyte_id,
                0,
                0,
                success=False,
                error_message=str(e),
            )

        stats["by_source"][source_name] = source_stats

    return stats


def record_harvest_metadata(
    session,
    api_source: str,
    analyte_id: str,
    synonyms_count: int,
    duration_ms: int,
    success: bool = True,
    error_message: str = None,
):
    """
    Record API harvest attempt in metadata table.
    
    Args:
        session: Database session
        api_source: Source name
        analyte_id: Analyte ID
        synonyms_count: Number of synonyms harvested
        duration_ms: Request duration in milliseconds
        success: Whether harvest succeeded
        error_message: Optional error message
    """
    # Simplified metadata recording
    try:
        from datetime import date
        metadata = APIHarvestMetadata(
            api_name=api_source,
            harvest_date=date.today(),
            analytes_queried=1,
            synonyms_obtained=synonyms_count,
            synonyms_filtered=synonyms_count,  # Approximate
            errors_encountered=0 if success else 1,
            notes=error_message if error_message else f"Duration: {duration_ms}ms",
        )
        session.add(metadata)
    except Exception as e:
        logger.debug(f"Could not record metadata: {e}")


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Harvest chemical synonyms from public APIs"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of analytes to process (for testing)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        choices=["pubchem", "nci", "npri"],
        help="Only harvest from specific source",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="data/reg153_matcher.db",
        help="Database path",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path",
    )
    parser.add_argument(
        "--update-cas",
        action="store_true",
        help="Also fetch and update missing CAS numbers from PubChem",
    )
    parser.add_argument(
        "--include-no-cas",
        action="store_true",
        help="Process analytes without CAS numbers (requires --update-cas)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file)

    logger.info("=" * 80)
    logger.info("API SYNONYM HARVESTER")
    logger.info("=" * 80)

    # Initialize database
    logger.info(f"Connecting to database: {args.database}")
    db_manager = DatabaseManager(db_path=args.database)
    session = db_manager.SessionLocal()

    # Load analytes
    analytes = load_analytes_to_process(
        session, 
        limit=args.limit, 
        include_no_cas=args.include_no_cas
    )
    if not analytes:
        logger.warning("No analytes to process!")
        return

    # Create harvesters
    logger.info("Initializing API harvesters...")
    harvesters = create_harvesters()

    if args.source:
        logger.info(f"Harvesting only from: {args.source}")
        
    if args.update_cas:
        logger.info("CAS number updating enabled")

    # Process each analyte
    start_time = time.time()
    overall_stats = {
        "total_raw": 0,
        "total_filtered": 0,
        "total_new": 0,
        "total_duplicate": 0,
        "total_errors": 0,
        "cas_updated_count": 0,
    }

    with tqdm(total=len(analytes), desc="Harvesting synonyms") as pbar:
        for i, analyte in enumerate(analytes):
            display_name = analyte.cas_number or analyte.preferred_name
            pbar.set_description(f"Processing {display_name[:40]}")

            try:
                stats = harvest_for_analyte(
                    analyte,
                    harvesters,
                    session,
                    source_filter=args.source,
                    update_cas=args.update_cas,
                )

                overall_stats["total_raw"] += stats["total_raw"]
                overall_stats["total_filtered"] += stats["total_filtered"]
                overall_stats["total_new"] += stats["total_new"]
                overall_stats["total_duplicate"] += stats["total_duplicate"]
                overall_stats["total_errors"] += len(stats["errors"])
                
                if stats.get("cas_updated", False):
                    overall_stats["cas_updated_count"] += 1

                # Commit every 10 analytes
                if (i + 1) % 10 == 0:
                    session.commit()
                    logger.debug(f"Committed batch at {i + 1} analytes")

            except Exception as e:
                logger.error(f"Fatal error processing {analyte.preferred_name}: {e}")
                overall_stats["total_errors"] += 1
                session.rollback()

            pbar.update(1)

    # Final commit
    session.commit()

    # Close harvesters
    for harvester in harvesters.values():
        harvester.close()

    # Report statistics
    duration = time.time() - start_time
    logger.info("=" * 80)
    logger.info("HARVEST COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info(f"Analytes processed: {len(analytes)}")
    logger.info(f"Raw synonyms harvested: {overall_stats['total_raw']}")
    logger.info(f"After quality filters: {overall_stats['total_filtered']}")
    logger.info(f"New synonyms inserted: {overall_stats['total_new']}")
    logger.info(f"Duplicates skipped: {overall_stats['total_duplicate']}")
    logger.info(f"Errors encountered: {overall_stats['total_errors']}")
    
    if args.update_cas:
        logger.info(f"CAS numbers updated: {overall_stats['cas_updated_count']}")

    if overall_stats["total_filtered"] > 0:
        retention_rate = (
            overall_stats["total_filtered"] / overall_stats["total_raw"]
        ) * 100
        logger.info(f"Quality filter retention: {retention_rate:.1f}%")

    if overall_stats["total_new"] > 0:
        avg_per_analyte = overall_stats["total_new"] / len(analytes)
        logger.info(f"Average new synonyms per analyte: {avg_per_analyte:.1f}")

    logger.info("=" * 80)

    session.close()


if __name__ == "__main__":
    main()
