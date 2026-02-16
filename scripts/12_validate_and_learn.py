"""
Human validation ingestion for Layer 1 learning.

Reads completed review queue Excel files and ingests validated
synonym mappings into the database for immediate vocabulary expansion.

Usage:
    python scripts/12_validate_and_learn.py --review-queue reports/review_queue_validated.xlsx
    python scripts/12_validate_and_learn.py --auto-ingest
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database import crud_new
from src.database.models import SynonymType
from src.learning.synonym_ingestion import SynonymIngestor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/validation_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def detect_validation_format(df: pd.DataFrame) -> str:
    """
    Detect which validation format is being used.
    
    Formats:
    - 'review_queue': From generate_review_queue.py with chosen_match column
    - 'simple': Simple two-column format (variant, validated_match)
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Format type string
    """
    if 'chosen_match' in df.columns and 'validation_confidence' in df.columns:
        return 'review_queue'
    elif 'validated_match' in df.columns or 'preferred_name' in df.columns:
        return 'simple'
    else:
        raise ValueError(
            f"Cannot detect validation format. Required columns not found.\n"
            f"Available columns: {', '.join(df.columns)}"
        )


def load_validation_file(file_path: str) -> pd.DataFrame:
    """
    Load validation file (Excel or CSV).
    
    Args:
        file_path: Path to validation file
    
    Returns:
        DataFrame with validation data
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Validation file not found: {file_path}")
    
    logger.info(f"Loading validation file: {file_path}")
    
    if file_path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif file_path.suffix.lower() == '.csv':
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    logger.info(f"Loaded {len(df)} validation records")
    
    return df


def parse_review_queue_format(df: pd.DataFrame, session: Session) -> List[Dict]:
    """
    Parse review queue format validation file.
    
    Expected columns:
    - raw_variant: Original text
    - chosen_match: Preferred name selected by human
    - validation_confidence: HIGH, MEDIUM, LOW
    
    Args:
        df: DataFrame with review queue format
        session: Database session
    
    Returns:
        List of validation dictionaries
    """
    validations = []
    
    for idx, row in df.iterrows():
        raw_variant = str(row.get('raw_variant', '')).strip()
        chosen_match = str(row.get('chosen_match', '')).strip()
        confidence_str = str(row.get('validation_confidence', 'MEDIUM')).upper()
        
        # Skip empty or UNKNOWN matches
        if not raw_variant or not chosen_match or chosen_match.upper() == 'UNKNOWN':
            continue
        
        # Look up analyte by preferred name
        analyte = crud_new.get_analyte_by_name(session, chosen_match)
        
        if not analyte:
            logger.warning(f"Analyte not found for chosen_match '{chosen_match}' (variant: '{raw_variant}')")
            continue
        
        # Map confidence string to score
        confidence_map = {
            'HIGH': 1.0,
            'MEDIUM': 0.9,
            'LOW': 0.8,
        }
        confidence_score = confidence_map.get(confidence_str, 0.9)
        
        validations.append({
            'raw_text': raw_variant,
            'analyte_id': analyte.analyte_id,
            'preferred_name': analyte.preferred_name,
            'confidence': confidence_score
        })
    
    return validations


def parse_simple_format(df: pd.DataFrame, session: Session) -> List[Dict]:
    """
    Parse simple two-column validation format.
    
    Expected columns:
    - variant/raw_text: Original text
    - validated_match/preferred_name: Canonical name
    
    Args:
        df: DataFrame with simple format
        session: Database session
    
    Returns:
        List of validation dictionaries
    """
    validations = []
    
    # Detect column names
    variant_col = None
    match_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'variant' in col_lower or 'raw' in col_lower or 'original' in col_lower:
            variant_col = col
        if 'match' in col_lower or 'preferred' in col_lower or 'canonical' in col_lower:
            match_col = col
    
    if not variant_col or not match_col:
        raise ValueError(
            f"Cannot identify columns. Expected variant and match columns.\n"
            f"Available: {', '.join(df.columns)}"
        )
    
    logger.info(f"Using columns: variant='{variant_col}', match='{match_col}'")
    
    for idx, row in df.iterrows():
        raw_variant = str(row[variant_col]).strip()
        match_name = str(row[match_col]).strip()
        
        if not raw_variant or not match_name:
            continue
        
        # Look up analyte
        analyte = crud_new.get_analyte_by_name(session, match_name)
        
        if not analyte:
            logger.warning(f"Analyte not found for '{match_name}' (variant: '{raw_variant}')")
            continue
        
        validations.append({
            'raw_text': raw_variant,
            'analyte_id': analyte.analyte_id,
            'preferred_name': analyte.preferred_name,
            'confidence': 1.0  # Default high confidence for simple format
        })
    
    return validations


def ingest_validations(
    validations: List[Dict],
    session: Session,
    mark_decisions: bool = True
) -> Dict:
    """
    Ingest validated synonyms into database.
    
    Args:
        validations: List of validation dictionaries
        session: Database session
        mark_decisions: Whether to mark match_decisions as validated
    
    Returns:
        Summary statistics dictionary
    """
    ingestor = SynonymIngestor()
    
    stats = {
        'total': len(validations),
        'new_synonyms': 0,
        'duplicates': 0,
        'errors': 0,
        'decisions_marked': 0
    }
    
    logger.info(f"Ingesting {len(validations)} validated synonyms")
    
    for validation in tqdm(validations, desc="Ingesting synonyms"):
        try:
            # Ingest the synonym
            is_new = ingestor.ingest_validated_synonym(
                raw_text=validation['raw_text'],
                analyte_id=validation['analyte_id'],
                db_session=session,
                confidence=validation['confidence'],
                synonym_type=SynonymType.LAB_VARIANT
            )
            
            if is_new:
                stats['new_synonyms'] += 1
            else:
                stats['duplicates'] += 1
            
            # Mark corresponding match_decision as validated
            if mark_decisions:
                from src.database.models import MatchDecision
                from sqlalchemy import select, update
                
                # Find unvalidated decisions for this input
                stmt = select(MatchDecision).where(
                    MatchDecision.input_text == validation['raw_text'],
                    MatchDecision.human_validated == False
                ).limit(1)
                
                decision = session.execute(stmt).scalar_one_or_none()
                
                if decision:
                    decision.human_validated = True
                    decision.ingested = True
                    decision.validation_notes = f"Validated as {validation['preferred_name']}"
                    stats['decisions_marked'] += 1
            
        except Exception as e:
            logger.error(f"Error ingesting validation for '{validation['raw_text']}': {e}")
            stats['errors'] += 1
    
    # Commit all changes
    session.commit()
    
    return stats


def check_retraining_trigger(session: Session) -> Dict:
    """
    Check if retraining trigger conditions are met.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary with trigger status
    """
    from src.database.models import MatchDecision
    from sqlalchemy import select, func
    
    # Count validated decisions since last training
    validated_count = session.execute(
        select(func.count(MatchDecision.id)).where(
            MatchDecision.human_validated == True,
            MatchDecision.ingested == True
        )
    ).scalar_one()
    
    trigger_threshold = 2000
    trigger_met = validated_count >= trigger_threshold
    
    return {
        'validated_count': validated_count,
        'trigger_threshold': trigger_threshold,
        'trigger_met': trigger_met,
        'progress_percent': (validated_count / trigger_threshold * 100) if trigger_threshold > 0 else 0
    }


def auto_ingest_mode(session: Session) -> Dict:
    """
    Auto-ingest mode: Look for validation files in standard location.
    
    Args:
        session: Database session
    
    Returns:
        Summary statistics
    """
    # Look for validation files in reports/daily/
    validation_dir = Path("reports/daily/validations")
    
    if not validation_dir.exists():
        logger.warning(f"Validation directory not found: {validation_dir}")
        return {'total': 0, 'files_processed': 0}
    
    # Find unprocessed validation files
    validation_files = list(validation_dir.glob("*_validated.xlsx"))
    processed_marker = validation_dir / ".processed"
    
    if processed_marker.exists():
        with open(processed_marker, 'r') as f:
            processed_files = set(f.read().splitlines())
    else:
        processed_files = set()
    
    new_files = [f for f in validation_files if f.name not in processed_files]
    
    if not new_files:
        logger.info("No new validation files to process")
        return {'total': 0, 'files_processed': 0}
    
    logger.info(f"Found {len(new_files)} new validation files")
    
    combined_stats = {
        'total': 0,
        'new_synonyms': 0,
        'duplicates': 0,
        'errors': 0,
        'decisions_marked': 0,
        'files_processed': 0
    }
    
    for file_path in new_files:
        logger.info(f"Processing: {file_path.name}")
        
        try:
            df = load_validation_file(str(file_path))
            format_type = detect_validation_format(df)
            
            if format_type == 'review_queue':
                validations = parse_review_queue_format(df, session)
            else:
                validations = parse_simple_format(df, session)
            
            stats = ingest_validations(validations, session)
            
            # Accumulate stats
            for key in ['total', 'new_synonyms', 'duplicates', 'errors', 'decisions_marked']:
                combined_stats[key] += stats.get(key, 0)
            
            combined_stats['files_processed'] += 1
            
            # Mark as processed
            processed_files.add(file_path.name)
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")
    
    # Update processed marker
    with open(processed_marker, 'w') as f:
        f.write('\n'.join(sorted(processed_files)))
    
    return combined_stats


def main():
    """Main entry point for validation ingestion."""
    parser = argparse.ArgumentParser(
        description="Ingest validated chemical name synonyms",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest specific validation file
  python scripts/12_validate_and_learn.py --review-queue reports/review_validated.xlsx
  
  # Auto-ingest mode (daily automation)
  python scripts/12_validate_and_learn.py --auto-ingest
        """
    )
    
    parser.add_argument('--review-queue', '-r', help='Path to validated review queue file')
    parser.add_argument('--auto-ingest', '-a', action='store_true', 
                        help='Auto-ingest mode: process all new validation files')
    parser.add_argument('--database', '-d', help='Path to database file')
    parser.add_argument('--skip-decision-marking', action='store_true',
                        help='Skip marking match_decisions as validated')
    
    args = parser.parse_args()
    
    if not args.review_queue and not args.auto_ingest:
        parser.error("Either --review-queue or --auto-ingest must be specified")
    
    try:
        # Initialize database
        db_manager = DatabaseManager(db_path=args.database, echo=False)
        
        with db_manager.get_session() as session:
            if args.auto_ingest:
                stats = auto_ingest_mode(session)
            else:
                # Load validation file
                df = load_validation_file(args.review_queue)
                
                # Detect format and parse
                format_type = detect_validation_format(df)
                logger.info(f"Detected validation format: {format_type}")
                
                if format_type == 'review_queue':
                    validations = parse_review_queue_format(df, session)
                else:
                    validations = parse_simple_format(df, session)
                
                # Ingest validations
                stats = ingest_validations(
                    validations, 
                    session, 
                    mark_decisions=not args.skip_decision_marking
                )
        
            # Check retraining trigger
            trigger_status = check_retraining_trigger(session)
        
        # Print summary report
        print("\n" + "="*60)
        print("VALIDATION INGESTION SUMMARY")
        print("="*60)
        if args.auto_ingest:
            print(f"Files processed:      {stats['files_processed']}")
        print(f"Total validations:    {stats['total']}")
        print(f"New synonyms added:   {stats['new_synonyms']}")
        print(f"Duplicates skipped:   {stats['duplicates']}")
        print(f"Errors:               {stats['errors']}")
        print(f"Decisions marked:     {stats['decisions_marked']}")
        print("-"*60)
        print(f"Retraining progress:  {trigger_status['validated_count']}/{trigger_status['trigger_threshold']} "
              f"({trigger_status['progress_percent']:.1f}%)")
        
        if trigger_status['trigger_met']:
            print("\n🔔 RETRAINING TRIGGER MET! Consider running neural model training.")
        
        print("="*60)
        
        logger.info("Validation ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Validation ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
