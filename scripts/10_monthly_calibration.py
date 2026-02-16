"""
Monthly threshold calibration script.

Analyzes match decisions from the last 30 days, calculates maturity metrics,
recalibrates thresholds, and generates comprehensive reports.

Usage:
    python scripts/10_monthly_calibration.py [--days N] [--output DIR] [--update-config]
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import json
import yaml

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_session_factory
from src.learning.threshold_calibrator import (
    analyze_match_decisions,
    recalibrate_thresholds,
    update_config_thresholds
)
from src.learning.maturity_metrics import (
    calculate_corpus_maturity,
    should_retrain_model,
    calculate_time_since_training
)
from src.learning.variant_clustering import get_recent_unknowns, cluster_similar_unknowns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/monthly_calibration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def generate_comprehensive_report(
    session,
    days_back: int = 30
) -> dict:
    """
    Generate comprehensive monthly calibration report.
    
    Includes:
    - Match decision analysis
    - Threshold recalibration results
    - Corpus maturity metrics
    - Retraining recommendations
    - Unknown term clustering
    """
    logger.info(f"Generating comprehensive report for last {days_back} days")
    
    report = {
        'timestamp': datetime.utcnow().isoformat(),
        'period_days': days_back,
        'sections': {}
    }
    
    # === Section 1: Match Decision Analysis ===
    logger.info("Analyzing match decisions...")
    decision_analysis = analyze_match_decisions(session, days_back=days_back)
    report['sections']['decision_analysis'] = decision_analysis
    
    # === Section 2: Threshold Recalibration ===
    logger.info("Recalibrating thresholds...")
    recalibration = recalibrate_thresholds(
        session,
        days_back=days_back,
        target_precision=0.98
    )
    report['sections']['threshold_recalibration'] = recalibration
    
    # === Section 3: Corpus Maturity ===
    logger.info("Calculating corpus maturity...")
    maturity = calculate_corpus_maturity(session, history_days=90)
    report['sections']['corpus_maturity'] = maturity
    
    # === Section 4: Retraining Recommendation ===
    logger.info("Evaluating retraining triggers...")
    should_retrain, retrain_reasons = should_retrain_model(maturity)
    
    # Get time since last training
    time_since_training = calculate_time_since_training(session)
    if time_since_training:
        days_since = time_since_training.days
    else:
        days_since = None
    
    report['sections']['retraining'] = {
        'recommended': should_retrain,
        'reasons': retrain_reasons,
        'days_since_last_training': days_since
    }
    
    # === Section 5: Unknown Term Clustering ===
    logger.info("Clustering unknown terms...")
    unknowns = get_recent_unknowns(session, days_back=days_back, min_frequency=2)
    
    if unknowns:
        clusters = cluster_similar_unknowns(unknowns, threshold=0.85, min_cluster_size=2)
        report['sections']['unknown_clustering'] = {
            'total_unknowns': len(unknowns),
            'clusters_found': len(clusters),
            'clusters': [
                {
                    'anchor_term': c.anchor_term,
                    'variant_count': len(c.variants),
                    'variants': c.variants[:5],  # First 5
                    'avg_similarity': c.avg_similarity
                }
                for c in clusters[:10]  # Top 10 clusters
            ]
        }
    else:
        report['sections']['unknown_clustering'] = {
            'total_unknowns': 0,
            'clusters_found': 0,
            'clusters': []
        }
    
    logger.info("Report generation complete")
    
    return report


def save_json_report(report: dict, output_dir: str = 'logs') -> Path:
    """Save detailed JSON report."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = output_path / f'monthly_calibration_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"JSON report saved to {filename}")
    
    return filename


def save_human_readable_report(report: dict, output_dir: str = 'logs') -> Path:
    """Save human-readable text report."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = output_path / f'monthly_calibration_{timestamp}.txt'
    
    with open(filename, 'w') as f:
        f.write("="*80 + "\n")
        f.write("MONTHLY CALIBRATION REPORT\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {report['timestamp']}\n")
        f.write(f"Analysis period: Last {report['period_days']} days\n")
        f.write("\n")
        
        # Decision Analysis
        f.write("-" * 80 + "\n")
        f.write("MATCH DECISION ANALYSIS\n")
        f.write("-" * 80 + "\n")
        analysis = report['sections']['decision_analysis']
        f.write(f"Total decisions: {analysis['total_decisions']}\n")
        f.write(f"Acceptance rate (top-1): {analysis['acceptance_rate_top1']:.2%}\n")
        f.write(f"Override frequency: {analysis['override_frequency']:.2%}\n")
        f.write(f"Disagreement rate: {analysis['disagreement_frequency']:.2%}\n")
        f.write(f"Unknown rate: {analysis['unknown_rate']:.2%}\n")
        f.write("\nMethod distribution:\n")
        for method, pct in analysis['method_distribution'].items():
            f.write(f"  {method}: {pct:.2%}\n")
        f.write("\nDecisions by confidence:\n")
        for range_name, count in analysis['decisions_by_confidence'].items():
            f.write(f"  {range_name}: {count}\n")
        f.write("\n")
        
        # Threshold Recalibration
        f.write("-" * 80 + "\n")
        f.write("THRESHOLD RECALIBRATION\n")
        f.write("-" * 80 + "\n")
        recalib = report['sections']['threshold_recalibration']
        if recalib['success']:
            f.write("✓ Recalibration successful\n")
            f.write(f"Message: {recalib['message']}\n\n")
            f.write("Recommended thresholds:\n")
            for method, threshold in recalib['recommended_thresholds'].items():
                f.write(f"  {method}: {threshold:.3f}\n")
            f.write("\nSample sizes:\n")
            for method, size in recalib['method_sample_sizes'].items():
                f.write(f"  {method}: {size} decisions\n")
        else:
            f.write("✗ Recalibration failed\n")
            f.write(f"Reason: {recalib['message']}\n")
        f.write("\n")
        
        # Corpus Maturity
        f.write("-" * 80 + "\n")
        f.write("CORPUS MATURITY\n")
        f.write("-" * 80 + "\n")
        maturity = report['sections']['corpus_maturity']
        overall = maturity['overall']
        f.write(f"Total analytes: {overall['total_analytes']}\n")
        f.write(f"Total synonyms: {overall['total_synonyms']}\n")
        f.write(f"Avg synonyms/analyte: {overall['avg_synonyms_per_analyte']:.1f}\n")
        f.write(f"\nExact match rate: {overall['exact_match_rate']:.2%}\n")
        f.write(f"Fuzzy match rate: {overall['fuzzy_match_rate']:.2%}\n")
        f.write(f"Semantic reliance: {overall['semantic_reliance']:.2%}\n")
        f.write(f"Unknown rate: {overall['unknown_rate']:.2%}\n")
        
        growth = maturity['growth']
        f.write(f"\nSynonyms added:\n")
        f.write(f"  Last 7 days: {growth['synonyms_added_7d']}\n")
        f.write(f"  Last 30 days: {growth['synonyms_added_30d']}\n")
        f.write(f"  Last 90 days: {growth['synonyms_added_90d']}\n")
        f.write(f"  Weekly rate: {growth['growth_rate_weekly']}\n")
        f.write("\n")
        
        # Retraining Recommendation
        f.write("-" * 80 + "\n")
        f.write("RETRAINING RECOMMENDATION\n")
        f.write("-" * 80 + "\n")
        retrain = report['sections']['retraining']
        
        if retrain['days_since_last_training']:
            f.write(f"Days since last training: {retrain['days_since_last_training']}\n\n")
        else:
            f.write("No previous training found\n\n")
        
        if retrain['recommended']:
            f.write("✓ RETRAINING RECOMMENDED\n")
        else:
            f.write("✗ Retraining not needed at this time\n")
        
        reasons = retrain['reasons']
        f.write(f"\nActive triggers: {reasons['num_active_triggers']} "
                f"(requires {reasons['min_required']})\n")
        
        if reasons['active_triggers']:
            f.write("\nTriggered conditions:\n")
            for trigger in reasons['active_triggers']:
                f.write(f"  • {trigger}\n")
                if trigger in reasons['trigger_details']:
                    details = reasons['trigger_details'][trigger]
                    for key, value in details.items():
                        f.write(f"      {key}: {value}\n")
        f.write("\n")
        
        # Unknown Clustering
        f.write("-" * 80 + "\n")
        f.write("UNKNOWN TERM CLUSTERING\n")
        f.write("-" * 80 + "\n")
        clustering = report['sections']['unknown_clustering']
        f.write(f"Total unknown terms: {clustering['total_unknowns']}\n")
        f.write(f"Clusters found: {clustering['clusters_found']}\n")
        
        if clustering['clusters']:
            f.write("\nTop clusters:\n")
            for i, cluster in enumerate(clustering['clusters'], 1):
                f.write(f"\n  {i}. Anchor: {cluster['anchor_term']}\n")
                f.write(f"     Variants ({cluster['variant_count']}): {', '.join(cluster['variants'])}\n")
                f.write(f"     Avg similarity: {cluster['avg_similarity']:.3f}\n")
        f.write("\n")
        
        f.write("="*80 + "\n")
        f.write("END OF REPORT\n")
        f.write("="*80 + "\n")
    
    logger.info(f"Human-readable report saved to {filename}")
    
    return filename


def print_summary(report: dict):
    """Print brief summary to console."""
    print("\n" + "="*80)
    print("MONTHLY CALIBRATION SUMMARY")
    print("="*80)
    print(f"Timestamp: {report['timestamp']}")
    print()
    
    # Key metrics
    analysis = report['sections']['decision_analysis']
    print(f"Decisions analyzed: {analysis['total_decisions']}")
    print(f"Unknown rate: {analysis['unknown_rate']:.2%}")
    print()
    
    # Thresholds
    recalib = report['sections']['threshold_recalibration']
    if recalib['success']:
        print("New recommended thresholds:")
        for method, threshold in recalib['recommended_thresholds'].items():
            print(f"  {method}: {threshold:.3f}")
    else:
        print(f"Threshold recalibration failed: {recalib['message']}")
    print()
    
    # Retraining
    retrain = report['sections']['retraining']
    if retrain['recommended']:
        print("⚠️  MODEL RETRAINING RECOMMENDED")
        print(f"   Active triggers: {', '.join(retrain['reasons']['active_triggers'])}")
    else:
        print("✓ Model retraining not needed")
    print()
    
    # Corpus health
    maturity = report['sections']['corpus_maturity']
    print(f"Corpus: {maturity['overall']['total_synonyms']} synonyms, "
          f"{maturity['overall']['total_analytes']} analytes")
    print(f"Growth: {maturity['growth']['synonyms_added_30d']} synonyms in last 30 days")
    print()
    
    print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Monthly threshold calibration and analysis')
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Analysis period in days (default: 30)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='logs',
        help='Output directory for reports (default: logs)'
    )
    parser.add_argument(
        '--update-config',
        type=str,
        default=None,
        help='Path to config file to update with new thresholds'
    )
    parser.add_argument(
        '--min-decisions',
        type=int,
        default=100,
        help='Minimum decisions required for calibration (default: 100)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("Starting monthly calibration")
    logger.info(f"Analysis period: {args.days} days")
    logger.info("="*80)
    
    try:
        # Initialize database session
        SessionFactory = get_session_factory()
        session = SessionFactory()
        
        # Generate comprehensive report
        report = generate_comprehensive_report(session, days_back=args.days)
        
        # Save reports
        json_file = save_json_report(report, output_dir=args.output)
        text_file = save_human_readable_report(report, output_dir=args.output)
        
        # Print summary
        print_summary(report)
        
        # Update config if requested
        if args.update_config:
            recalib = report['sections']['threshold_recalibration']
            if recalib['success']:
                success = update_config_thresholds(
                    args.update_config,
                    recalib['recommended_thresholds']
                )
                if success:
                    logger.info(f"Updated config file: {args.update_config}")
                else:
                    logger.error("Failed to update config file")
            else:
                logger.warning("Skipping config update (recalibration failed)")
        
        session.close()
        
        logger.info("Monthly calibration complete")
        logger.info(f"Reports saved to {args.output}/")
        
    except Exception as e:
        logger.error(f"Fatal error during calibration: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
