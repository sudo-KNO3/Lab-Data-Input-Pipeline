"""
Bootstrap synonyms table with canonical analyte names.

This script populates the synonyms table with the preferred_name from each
analyte as the first synonym entry. This enables exact matching of canonical
names immediately.

Usage:
    python scripts/bootstrap_synonyms.py
    python scripts/bootstrap_synonyms.py --db path/to/database.db
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, SynonymType
from src.normalization.text_normalizer import TextNormalizer


def bootstrap_synonyms(db_path: str) -> dict:
    """
    Populate synonyms table with canonical analyte names.
    
    For each analyte in the database:
    1. Get preferred_name and analyte_id
    2. Normalize the preferred_name using TextNormalizer
    3. Insert into synonyms table with:
       - synonym_raw = preferred_name
       - synonym_norm = normalized version
       - analyte_id = analyte_id
       - synonym_type = 'COMMON' (canonical names)
       - harvest_source = 'bootstrap'
       - confidence = 1.0
    
    Args:
        db_path: Path to the database file
    
    Returns:
        Dictionary with statistics:
        - total_analytes: Total number of analytes processed
        - synonyms_added: Number of synonyms successfully added
        - synonyms_skipped: Number of synonyms skipped (already exist)
        - errors: Number of errors encountered
    """
    # Initialize database manager and text normalizer
    db_manager = DatabaseManager(db_path=db_path)
    normalizer = TextNormalizer()
    
    # Statistics
    stats = {
        'total_analytes': 0,
        'synonyms_added': 0,
        'synonyms_skipped': 0,
        'errors': 0
    }
    
    print(f"\n{'='*70}")
    print(f"Bootstrapping Synonyms Table")
    print(f"{'='*70}")
    print(f"Database: {db_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Create session
        with db_manager.get_session() as session:
            # Query all analytes
            analytes = session.query(Analyte).all()
            stats['total_analytes'] = len(analytes)
            
            print(f"Found {stats['total_analytes']} analytes in database\n")
            print("Processing analytes...")
            
            # Process each analyte
            for i, analyte in enumerate(analytes, 1):
                try:
                    # Get preferred name
                    preferred_name = analyte.preferred_name
                    analyte_id = analyte.analyte_id
                    
                    # Normalize the name
                    normalized_name = normalizer.normalize(preferred_name)
                    
                    # Check if this synonym already exists
                    existing = session.query(Synonym).filter(
                        Synonym.analyte_id == analyte_id,
                        Synonym.synonym_norm == normalized_name
                    ).first()
                    
                    if existing:
                        stats['synonyms_skipped'] += 1
                        continue
                    
                    # Create new synonym entry
                    synonym = Synonym(
                        analyte_id=analyte_id,
                        synonym_raw=preferred_name,
                        synonym_norm=normalized_name,
                        synonym_type=SynonymType.COMMON,  # Using COMMON for canonical names
                        harvest_source='bootstrap',
                        confidence=1.0
                    )
                    
                    session.add(synonym)
                    stats['synonyms_added'] += 1
                    
                    # Progress indicator
                    if i % 25 == 0:
                        print(f"  Processed {i}/{stats['total_analytes']} analytes...")
                    
                except Exception as e:
                    stats['errors'] += 1
                    print(f"  ERROR processing analyte {analyte_id}: {e}")
                    continue
            
            # Commit all changes
            session.commit()
            print(f"  Processed {stats['total_analytes']}/{stats['total_analytes']} analytes...")
            
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        stats['errors'] += 1
        return stats
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"Bootstrap Complete")
    print(f"{'='*70}")
    print(f"Total analytes:      {stats['total_analytes']}")
    print(f"Synonyms added:      {stats['synonyms_added']}")
    print(f"Synonyms skipped:    {stats['synonyms_skipped']} (already exist)")
    print(f"Errors:              {stats['errors']}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return stats


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Bootstrap synonyms table with canonical analyte names'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='data/reg153_matcher.db',
        help='Path to the database file (default: data/reg153_matcher.db)'
    )
    
    args = parser.parse_args()
    
    # Verify database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: Database file not found: {args.db}")
        print("Please run setup_database.py first to create the database.")
        sys.exit(1)
    
    # Run bootstrap
    stats = bootstrap_synonyms(args.db)
    
    # Exit with appropriate code
    if stats['errors'] > 0:
        sys.exit(1)
    elif stats['synonyms_added'] == 0:
        print("NOTE: No synonyms were added. Database may already be bootstrapped.")
        sys.exit(0)
    else:
        print("✓ Bootstrap successful!")
        sys.exit(0)


if __name__ == '__main__':
    main()
