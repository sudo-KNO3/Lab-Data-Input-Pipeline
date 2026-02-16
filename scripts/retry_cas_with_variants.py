"""
Retry PubChem CAS lookup using name variants.

For analytes without CAS numbers, uses the chemical name parser to generate
alternative forms (locant reordering, aromatic position variants, etc) and
retry PubChem API lookups.

This addresses Ontario lab-specific notations that PubChem doesn't recognize:
- "Methylnaphthalene 1-" → try "1-Methylnaphthalene"
- "Dimethylphenol 2,4-" → try "2,4-Dimethylphenol"
- Qualifier text → try name without qualifiers
"""
import argparse
import sys
import time
from pathlib import Path

from loguru import logger
from sqlalchemy import select
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bootstrap.api_harvesters import PubChemHarvester
from src.database import Analyte, AnalyteType
from src.database.connection import DatabaseManager
from src.normalization.chemical_parser import ChemicalNameParser


def setup_logging():
    """Configure logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )


def clean_qualifier_text(name: str) -> list[str]:
    """
    Remove qualifier text like (total), (hot water soluble), etc.
    
    Args:
        name: Chemical name potentially with qualifiers
        
    Returns:
        List of name variants without qualifiers
    """
    variants = [name]
    
    # Remove text in parentheses
    import re
    if '(' in name and ')' in name:
        base = re.sub(r'\s*\([^)]+\)', '', name).strip()
        if base and base != name:
            variants.append(base)
    
    # Remove Roman numerals (e.g., "Chromium VI" → "Chromium")
    if re.search(r'\b(I{1,3}|IV|V|VI|VII|VIII|IX|X)\b', name):
        base = re.sub(r'\s+(I{1,3}|IV|V|VI|VII|VIII|IX|X)\b', '', name).strip()
        if base and base != name:
            variants.append(base)
    
    return list(set(variants))


def retry_cas_lookup_with_variants(
    analyte: Analyte,
    pubchem: PubChemHarvester,
    parser: ChemicalNameParser,
    session,
) -> tuple[bool, str | None]:
    """
    Retry CAS lookup using name variants.
    
    Args:
        analyte: Analyte record
        pubchem: PubChem harvester instance
        parser: Chemical name parser
        session: Database session
        
    Returns:
        Tuple of (success, cas_number)
    """
    # Start with qualifier cleanup
    base_variants = clean_qualifier_text(analyte.preferred_name)
    
    # Generate structural variants
    all_variants = set(base_variants)
    for base_name in base_variants:
        structural_variants = parser.generate_variants(base_name)
        all_variants.update(structural_variants)
    
    # Remove the original (already tried)
    all_variants.discard(analyte.preferred_name)
    
    # Try each variant
    logger.debug(f"Trying {len(all_variants)} variants for '{analyte.preferred_name}'")
    
    for variant in all_variants:
        try:
            cas = pubchem.get_cas_number(variant)
            if cas:
                logger.info(f"✓ Found CAS {cas} for '{analyte.preferred_name}' using variant '{variant}'")
                
                # Update database
                analyte.cas_number = cas
                session.add(analyte)
                session.flush()
                
                return True, cas
        except Exception as e:
            logger.debug(f"Variant '{variant}' failed: {e}")
            continue
    
    return False, None


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Retry PubChem CAS lookup using name variants"
    )
    parser.add_argument(
        "--database",
        type=str,
        default="data/reg153_matcher.db",
        help="Database path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of analytes to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't update database, just show what would be found",
    )
    
    args = parser.parse_args()
    
    setup_logging()
    
    logger.info("=" * 80)
    logger.info("RETRY CAS LOOKUP WITH NAME VARIANTS")
    logger.info("=" * 80)
    
    # Initialize
    db_manager = DatabaseManager(db_path=args.database)
    session = db_manager.SessionLocal()
    
    pubchem = PubChemHarvester()
    name_parser = ChemicalNameParser()
    
    # Get analytes without CAS
    query = select(Analyte).where(
        Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE,
        Analyte.cas_number.is_(None)
    )
    
    if args.limit:
        query = query.limit(args.limit)
    
    analytes = session.execute(query).scalars().all()
    
    logger.info(f"Found {len(analytes)} analytes without CAS numbers")
    
    if not analytes:
        logger.info("Nothing to process!")
        return
    
    # Process each analyte
    stats = {
        "attempted": 0,
        "found": 0,
        "not_found": 0,
    }
    
    found_mappings = []
    
    with tqdm(total=len(analytes), desc="Retrying CAS lookups") as pbar:
        for analyte in analytes:
            pbar.set_description(f"Processing {analyte.preferred_name[:40]}")
            
            stats["attempted"] += 1
            
            try:
                success, cas = retry_cas_lookup_with_variants(
                    analyte, pubchem, name_parser, session
                )
                
                if success:
                    stats["found"] += 1
                    found_mappings.append((analyte.preferred_name, cas))
                    
                    # Commit every 10 successes
                    if stats["found"] % 10 == 0 and not args.dry_run:
                        session.commit()
                else:
                    stats["not_found"] += 1
                
            except Exception as e:
                logger.error(f"Error processing {analyte.preferred_name}: {e}")
                stats["not_found"] += 1
            
            pbar.update(1)
            time.sleep(0.2)  # Rate limiting
    
    # Final commit
    if not args.dry_run:
        session.commit()
        logger.info("Changes committed to database")
    else:
        session.rollback()
        logger.info("Dry run - no changes made to database")
    
    # Report results
    logger.info("=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)
    logger.info(f"Analytes attempted: {stats['attempted']}")
    logger.info(f"CAS numbers found: {stats['found']}")
    logger.info(f"Still without CAS: {stats['not_found']}")
    
    if found_mappings:
        logger.info("")
        logger.info("CAS Numbers Found:")
        for name, cas in found_mappings:
            logger.info(f"  {cas:15s} → {name}")
    
    logger.info("=" * 80)
    
    pubchem.close()
    session.close()


if __name__ == "__main__":
    main()
