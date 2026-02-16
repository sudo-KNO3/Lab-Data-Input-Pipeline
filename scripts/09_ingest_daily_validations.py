"""
Daily validation ingestion script.

Queries validated match decisions and ingests them as new synonyms.
Marks decisions as ingested to avoid duplicates.
Generates daily learning report.

Usage:
    python scripts/09_ingest_daily_validations.py [--dry-run] [--days N]
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import select, and_, update
from sqlalchemy.orm import Session

from src.database.connection import get_session_factory
from src.database.models import MatchDecision
from src.learning.synonym_ingestion import batch_ingest_validations
from src.learning.incremental_embedder import IncrementalEmbedder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/daily_validations.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_unvalidated_decisions(
    session: Session,
    days_back: int = 7
) -> list:
    """
    Get match decisions that are validated but not yet ingested.
    
    Queries for decisions where:
    - human_validated = TRUE
    - ingested = FALSE
    - created within last N days
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days_back)
    
    # Query for manually reviewed decisions that have an analyte match
    query = select(MatchDecision).where(
        and_(
            MatchDecision.human_validated == True,
            MatchDecision.ingested == False,
            MatchDecision.analyte_id.is_not(None),
            MatchDecision.created_at >= cutoff_date
        )
    )
    
    decisions = session.execute(query).scalars().all()
    
    logger.info(f"Found {len(decisions)} validated decisions to potentially ingest")
    
    return decisions


def prepare_validations(decisions: list) -> list:
    """
    Extract (raw_text, analyte_id) pairs from decisions.
    
    Args:
        decisions: List of MatchDecision objects
    
    Returns:
        List of (raw_text, analyte_id) tuples
    """
    validations = []
    
    for decision in decisions:
        if decision.query_text and decision.analyte_id:
            validations.append((decision.query_text, decision.analyte_id))
    
    return validations


def mark_as_ingested(session: Session, decision_ids: list) -> int:
    """
    Mark decisions as ingested.
    
    Note: This would update the 'ingested' field.
    For now, we'll add a note to review_notes.
    """
    # In production, you'd do:
    # session.execute(
    Updates the 'ingested' field and sets ingested_at timestamp.
    """
    try:
        session.execute(
            update(MatchDecision)
            .where(MatchDecision.id.in_(decision_ids))
            .values(ingested=True, ingested_at=datetime.utcnow())
        )
        session.commit()
        logger.info(f"Marked {len(decision_ids)} decisions as ingested")
        return len(decision_ids)
    except Exception as e:
        logger.error(f"Failed to mark decisions as ingested: {e}")
        session.rollback()
        return 0
    elapsed_seconds: float
) -> dict:
    """
    Generate daily learning report.
    
    Shows:
    - Synonyms added
    - Analytes affected
    - Current corpus size
    - FAISS index size
    - Trend comparison (vs last week/month)
    """
    from src.database.models import Synonym, Analyte
    from sqlalchemy import func
    
    # Current corpus stats
    total_analytes = session.execute(select(func.count(Analyte.id))).scalar_one()
    total_synonyms = session.execute(select(func.count(Synonym.id))).scalar_one()
    
    # Synonyms added in last 7 days
    cutoff_7d = datetime.utcnow() - timedelta(days=7)
    synonyms_7d = session.execute(
        select(func.count(Synonym.id)).where(Synonym.created_at >= cutoff_7d)
    ).scalar_one()
    
    # Synonyms added in last 30 days
    cutoff_30d = datetime.utcnow() - timedelta(days=30)
    synonyms_30d = session.execute(
        select(func.count(Synonym.id)).where(Synonym.created_at >= cutoff_30d)
    ).scalar_one()
    
    # Unique analytes affected today
    if results['synonym_ids']:
        affected_analytes = set()
        for syn_id in results['synonym_ids']:
            synonym = session.get(Synonym, syn_id)
            if synonym:
                affected_analytes.add(synonym.analyte_id)
        analytes_affected = len(affected_analytes)
    else:
        analytes_affected = 0
    
    report = {
        'timestamp': datetime.utcnow().isoformat(),
        'ingestion_summary': {
            'total_processed': results['total_processed'],
            'successful': results['successful'],
            'duplicates': results['duplicates'],
            'errors': results['errors'],
            'analytes_affected': analytes_affected,
            'processing_time_seconds': round(elapsed_seconds, 2)
        },
        'corpus_status': {
            'total_analytes': total_analytes,
            'total_synonyms': total_synonyms,
            'avg_synonyms_per_analyte': round(total_synonyms / total_analytes, 1) if total_analytes > 0 else 0
        },
        'trends': {
            'synonyms_added_7d': synonyms_7d,
            'synonyms_added_30d': synonyms_30d,
            'daily_rate_7d': round(synonyms_7d / 7, 1),
            'daily_rate_30d': round(synonyms_30d / 30, 1)
        },
        'errors': results.get('error_details', [])
    }
    
    return report


def save_report(report: dict, output_dir: str = 'logs'):
    """Save report to JSON file."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = output_path / f'daily_validation_report_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Report saved to {filename}")
    
    return filename


def print_report(report: dict):
    """Print human-readable report."""
    print("\n" + "="*70)
    print("DAILY VALIDATION INGESTION REPORT")
    print("="*70)
    print(f"Timestamp: {report['timestamp']}")
    print()
    
    summary = report['ingestion_summary']
    print(f"Processed: {summary['total_processed']} validations")
    print(f"  ✓ Successful: {summary['successful']}")
    print(f"  ⊗ Duplicates: {summary['duplicates']}")
    print(f"  ✗ Errors: {summary['errors']}")
    print(f"  Analytes affected: {summary['analytes_affected']}")
    print(f"  Processing time: {summary['processing_time_seconds']}s")
    print()
    
    corpus = report['corpus_status']
    print("Current Corpus:")
    print(f"  Analytes: {corpus['total_analytes']}")
    print(f"  Synonyms: {corpus['total_synonyms']}")
    print(f"  Avg synonyms/analyte: {corpus['avg_synonyms_per_analyte']}")
    print()
    
    trends = report['trends']
    print("Growth Trends:")
    print(f"  Last 7 days: {trends['synonyms_added_7d']} synonyms ({trends['daily_rate_7d']}/day)")
    print(f"  Last 30 days: {trends['synonyms_added_30d']} synonyms ({trends['daily_rate_30d']}/day)")
    print()
    
    if report['errors']:
        print("Errors:")
        for i, error in enumerate(report['errors'][:5], 1):  # Show first 5
            print(f"  {i}. {error.get('raw_text', 'N/A')}: {error.get('message', 'Unknown error')}")
        if len(report['errors']) > 5:
            print(f"  ... and {len(report['errors']) - 5} more")
        print()
    
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Ingest daily validated synonyms')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview validations without ingesting'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Look back N days for validations (default: 7)'
    )
    parser.add_argument(
        '--model',
        type=str,
        default='all-MiniLM-L6-v2',
        help='Sentence transformer model name'
    )
    parser.add_argument(
        '--index-path',
        type=str,
        default=None,
        help='Path to FAISS index (creates new if not specified)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info("Starting daily validation ingestion")
    logger.info(f"Looking back {args.days} days")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("="*70)
    
    start_time = datetime.utcnow()
    
    try:
        # Initialize database session
        SessionFactory = get_session_factory()
        session = SessionFactory()
        
        # Get unvalidated decisions
        decisions = get_unvalidated_decisions(session, days_back=args.days)
        
        if not decisions:
            logger.info("No new validations found")
            session.close()
            return
        
        # Prepare validations
        validations = prepare_validations(decisions)
        logger.info(f"Prepared {len(validations)} validation pairs")
        
        if args.dry_run:
            print("\nDRY RUN - Would ingest the following:")
            for i, (text, analyte_id) in enumerate(validations[:10], 1):
                print(f"  {i}. '{text}' → analyte_id={analyte_id}")
            if len(validations) > 10:
                print(f"  ... and {len(validations) - 10} more")
            session.close()
            return
        
        # Initialize embedder
        logger.info(f"Initializing embedder with model: {args.model}")
        embedder = IncrementalEmbedder(
            model_name=args.model,
            index_path=args.index_path,
            auto_save_interval=100
        )
        
        # Batch ingest
        logger.info("Starting batch ingestion...")
        results = batch_ingest_validations(
            validations=validations,
            session=session,
            embedder=embedder,
            confidence=1.0,
            harvest_source='validated_runtime'
        )
        
        # Mark as ingested (would update database field in production)
        decision_ids = [d.id for d in decisions]
        mark_as_ingested(session, decision_ids)
        
        # Generate report
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        report = generate_report(session, results, elapsed)
        
        # Save and print report
        save_report(report)
        print_report(report)
        
        session.close()
        
        logger.info("Daily validation ingestion complete")
        
    except Exception as e:
        logger.error(f"Fatal error during ingestion: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
