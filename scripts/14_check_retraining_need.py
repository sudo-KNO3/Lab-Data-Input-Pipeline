"""
Retraining need assessment tool.

Evaluates whether neural model retraining is warranted based on
multiple trigger conditions and system metrics.

Usage:
    python scripts/14_check_retraining_need.py
    python scripts/14_check_retraining_need.py --output reports/retraining_assessment.txt
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import MatchDecision, Synonym
from sqlalchemy import select, func

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/retraining_assessment.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def check_validation_volume_trigger(session: Session) -> Tuple[bool, Dict]:
    """
    Check if sufficient validations have been collected since last training.
    
    Trigger: >= 2000 validated and ingested decisions
    
    Args:
        session: Database session
    
    Returns:
        Tuple of (trigger_met, details_dict)
    """
    logger.info("Checking validation volume trigger...")
    
    # Count validated and ingested decisions
    validated_count = session.execute(
        select(func.count(MatchDecision.id)).where(
            MatchDecision.human_validated == True,
            MatchDecision.ingested == True
        )
    ).scalar_one()
    
    # Get count since last training (would normally check training timestamp)
    # For now, assume all validated decisions are "since last training"
    new_validations = validated_count
    
    threshold = 2000
    trigger_met = new_validations >= threshold
    
    details = {
        'validated_decisions': validated_count,
        'new_since_training': new_validations,
        'threshold': threshold,
        'progress_percent': (new_validations / threshold * 100) if threshold > 0 else 0,
        'trigger_met': trigger_met
    }
    
    logger.info(f"Validation volume: {new_validations}/{threshold} ({details['progress_percent']:.1f}%)")
    
    return trigger_met, details


def check_unknown_rate_plateau_trigger(session: Session, days: int = 90) -> Tuple[bool, Dict]:
    """
    Check if unknown rate has plateaued (not decreasing).
    
    Trigger: Unknown rate in last 30 days is not significantly lower than 60-90 days ago
    
    Args:
        session: Database session
        days: Number of days to analyze
    
    Returns:
        Tuple of (trigger_met, details_dict)
    """
    logger.info("Checking unknown rate plateau trigger...")
    
    now = datetime.utcnow()
    
    # Get unknown rate for last 30 days
    cutoff_30d = now - timedelta(days=30)
    recent_decisions = session.execute(
        select(MatchDecision).where(
            MatchDecision.decision_timestamp >= cutoff_30d
        )
    ).scalars().all()
    
    if not recent_decisions:
        return False, {'trigger_met': False, 'reason': 'Insufficient recent data'}
    
    recent_unknown_count = sum(1 for d in recent_decisions if d.matched_analyte_id is None)
    recent_unknown_rate = recent_unknown_count / len(recent_decisions)
    
    # Get unknown rate for 60-90 days ago
    cutoff_60d = now - timedelta(days=60)
    cutoff_90d = now - timedelta(days=90)
    
    historical_decisions = session.execute(
        select(MatchDecision).where(
            MatchDecision.decision_timestamp >= cutoff_90d,
            MatchDecision.decision_timestamp < cutoff_60d
        )
    ).scalars().all()
    
    if not historical_decisions:
        return False, {'trigger_met': False, 'reason': 'Insufficient historical data'}
    
    historical_unknown_count = sum(1 for d in historical_decisions if d.matched_analyte_id is None)
    historical_unknown_rate = historical_unknown_count / len(historical_decisions)
    
    # Check if unknown rate has NOT decreased by at least 2 percentage points
    improvement = historical_unknown_rate - recent_unknown_rate
    plateau_threshold = 0.02  # 2 percentage points
    
    trigger_met = improvement < plateau_threshold
    
    details = {
        'recent_unknown_rate': recent_unknown_rate,
        'historical_unknown_rate': historical_unknown_rate,
        'improvement': improvement,
        'plateau_threshold': plateau_threshold,
        'trigger_met': trigger_met
    }
    
    logger.info(f"Unknown rate: {recent_unknown_rate*100:.1f}% (recent) vs {historical_unknown_rate*100:.1f}% (historical)")
    
    return trigger_met, details


def check_semantic_reliance_trigger(session: Session, days: int = 30) -> Tuple[bool, Dict]:
    """
    Check if semantic matching usage is high.
    
    Trigger: > 30% of recent matches used semantic matching
    
    Args:
        session: Database session  
        days: Number of days to analyze
    
    Returns:
        Tuple of (trigger_met, details_dict)
    """
    logger.info("Checking semantic reliance trigger...")
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    
    recent_decisions = session.execute(
        select(MatchDecision).where(
            MatchDecision.decision_timestamp >= cutoff
        )
    ).scalars().all()
    
    if not recent_decisions:
        return False, {'trigger_met': False, 'reason': 'No recent decisions'}
    
    # Count decisions that used semantic matching
    semantic_count = sum(
        1 for d in recent_decisions
        if d.signals_used.get('semantic_match', False) or 
           d.signals_used.get('semantic_score', 0) > 0
    )
    
    semantic_reliance = semantic_count / len(recent_decisions)
    threshold = 0.30
    
    trigger_met = semantic_reliance > threshold
    
    details = {
        'semantic_count': semantic_count,
        'total_decisions': len(recent_decisions),
        'semantic_reliance': semantic_reliance,
        'threshold': threshold,
        'trigger_met': trigger_met
    }
    
    logger.info(f"Semantic reliance: {semantic_reliance*100:.1f}% ({semantic_count}/{len(recent_decisions)})")
    
    return trigger_met, details


def check_low_confidence_prevalence_trigger(session: Session, days: int = 30) -> Tuple[bool, Dict]:
    """
    Check if many matches have low confidence scores.
    
    Trigger: > 20% of matches have confidence between 0.75-0.85
    
    Args:
        session: Database session
        days: Number of days to analyze
    
    Returns:
        Tuple of (trigger_met, details_dict)
    """
    logger.info("Checking low confidence prevalence trigger...")
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    
    recent_decisions = session.execute(
        select(MatchDecision).where(
            MatchDecision.decision_timestamp >= cutoff,
            MatchDecision.matched_analyte_id.isnot(None)  # Only matched cases
        )
    ).scalars().all()
    
    if not recent_decisions:
        return False, {'trigger_met': False, 'reason': 'No recent matched decisions'}
    
    # Count low confidence matches (0.75 - 0.85)
    low_confidence_count = sum(
        1 for d in recent_decisions
        if 0.75 <= d.confidence_score <= 0.85
    )
    
    low_confidence_rate = low_confidence_count / len(recent_decisions)
    threshold = 0.20
    
    trigger_met = low_confidence_rate > threshold
    
    details = {
        'low_confidence_count': low_confidence_count,
        'total_matched': len(recent_decisions),
        'low_confidence_rate': low_confidence_rate,
        'threshold': threshold,
        'trigger_met': trigger_met
    }
    
    logger.info(f"Low confidence rate: {low_confidence_rate*100:.1f}% ({low_confidence_count}/{len(recent_decisions)})")
    
    return trigger_met, details


def assess_retraining_need(session: Session) -> Dict:
    """
    Comprehensive retraining need assessment.
    
    Requires >= 2 triggers to recommend retraining.
    
    Args:
        session: Database session
    
    Returns:
        Assessment dictionary with recommendation
    """
    logger.info("=" * 70)
    logger.info("RETRAINING NEED ASSESSMENT")
    logger.info("=" * 70)
    
    # Check all triggers
    triggers = {}
    
    trigger_met, details = check_validation_volume_trigger(session)
    triggers['validation_volume'] = details
    
    trigger_met, details = check_unknown_rate_plateau_trigger(session)
    triggers['unknown_rate_plateau'] = details
    
    trigger_met, details = check_semantic_reliance_trigger(session)
    triggers['semantic_reliance'] = details
    
    trigger_met, details = check_low_confidence_prevalence_trigger(session)
    triggers['low_confidence_prevalence'] = details
    
    # Count how many triggers are met
    triggers_met = sum(1 for t in triggers.values() if t.get('trigger_met', False))
    
    # Determine recommendation
    if triggers_met >= 2:
        recommendation = "RECOMMENDED"
        reasoning = f"{triggers_met} out of 4 retraining triggers are met. Neural model retraining is recommended."
    elif triggers_met == 1:
        recommendation = "CONSIDER"
        reasoning = f"1 out of 4 retraining triggers is met. Consider retraining if manual validation load is high."
    else:
        recommendation = "NOT NEEDED"
        reasoning = "No significant triggers met. Continue with current model and corpus expansion."
    
    # Prepare assessment
    assessment = {
        'timestamp': datetime.utcnow(),
        'triggers': triggers,
        'triggers_met_count': triggers_met,
        'recommendation': recommendation,
        'reasoning': reasoning
    }
    
    logger.info(f"Triggers met: {triggers_met}/4")
    logger.info(f"Recommendation: {recommendation}")
    
    return assessment


def format_assessment_report(assessment: Dict) -> str:
    """
    Format assessment as human-readable report.
    
    Args:
        assessment: Assessment dictionary
    
    Returns:
        Formatted report string
    """
    triggers = assessment['triggers']
    
    report = f"""
╔══════════════════════════════════════════════════════════════════════════╗
║            NEURAL MODEL RETRAINING NEED ASSESSMENT                       ║
╚══════════════════════════════════════════════════════════════════════════╝

Assessment Date: {assessment['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}

┌─ RECOMMENDATION ─────────────────────────────────────────────────────────┐
│                                                                          │
│  Status: {assessment['recommendation']:<60} │
│                                                                          │
│  {assessment['reasoning']:<70}│
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ TRIGGER ANALYSIS ───────────────────────────────────────────────────────┐
│                                                                          │
│  [{'✅' if triggers['validation_volume']['trigger_met'] else '❌'}] Trigger 1: Validation Volume                                     │
│      Validated decisions: {triggers['validation_volume']['validated_decisions']:>8,}                              │
│      Threshold:           {triggers['validation_volume']['threshold']:>8,}                              │
│      Progress:            {triggers['validation_volume']['progress_percent']:>7.1f}%                               │
│                                                                          │
│  [{'✅' if triggers['unknown_rate_plateau'].get('trigger_met') else '❌'}] Trigger 2: Unknown Rate Plateau                                  │
│      Recent unknown rate: {triggers['unknown_rate_plateau'].get('recent_unknown_rate', 0)*100:>7.1f}%                               │
│      Historical rate:     {triggers['unknown_rate_plateau'].get('historical_unknown_rate', 0)*100:>7.1f}%                               │
│      Improvement:         {triggers['unknown_rate_plateau'].get('improvement', 0)*100:>7.1f} pp                              │
│                                                                          │
│  [{'✅' if triggers['semantic_reliance']['trigger_met'] else '❌'}] Trigger 3: Semantic Reliance High                               │
│      Semantic usage:      {triggers['semantic_reliance']['semantic_reliance']*100:>7.1f}%                               │
│      Threshold:           {triggers['semantic_reliance']['threshold']*100:>7.1f}%                               │
│                                                                          │
│  [{'✅' if triggers['low_confidence_prevalence']['trigger_met'] else '❌'}] Trigger 4: Low Confidence Prevalence                            │
│      Low confidence rate: {triggers['low_confidence_prevalence']['low_confidence_rate']*100:>7.1f}%                               │
│      Threshold:           {triggers['low_confidence_prevalence']['threshold']*100:>7.1f}%                               │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ SUMMARY ────────────────────────────────────────────────────────────────┐
│                                                                          │
│  Triggers Met:  {assessment['triggers_met_count']}/4                                                      │
│                                                                          │
│  Required:      ≥ 2 triggers for retraining recommendation              │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ NEXT STEPS ─────────────────────────────────────────────────────────────┐
│                                                                          │
{_format_next_steps(assessment)}
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Report generated by Chemical Matcher Retraining Assessment Tool v1.0
"""
    
    return report


def _format_next_steps(assessment: Dict) -> str:
    """Format next steps based on recommendation."""
    recommendation = assessment['recommendation']
    
    lines = []
    
    if recommendation == "RECOMMENDED":
        lines.append("│  1. Review validation quality in match_decisions table               │")
        lines.append("│  2. Create snapshot of current corpus                                │")
        lines.append("│  3. Prepare training dataset from validated decisions                │")
        lines.append("│  4. Train new neural embeddings model                                │")
        lines.append("│  5. Evaluate model on held-out test set                              │")
        lines.append("│  6. Deploy new model if metrics improve                              │")
        
    elif recommendation == "CONSIDER":
        lines.append("│  1. Monitor manual validation workload                               │")
        lines.append("│  2. Continue corpus expansion through Layer 1 learning               │")
        lines.append("│  3. Re-assess in 2-4 weeks                                           │")
        
    else:  # NOT NEEDED
        lines.append("│  1. Continue current operations                                      │")
        lines.append("│  2. Focus on synonym ingestion (Layer 1 learning)                    │")
        lines.append("│  3. Process review queues regularly                                  │")
        lines.append("│  4. Re-assess monthly                                                │")
    
    return '\n'.join(lines)


def main():
    """Main entry point for retraining assessment."""
    parser = argparse.ArgumentParser(
        description="Assess neural model retraining need",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run assessment and display results
  python scripts/14_check_retraining_need.py
  
  # Save assessment to file
  python scripts/14_check_retraining_need.py --output reports/retraining_assessment.txt
        """
    )
    
    parser.add_argument('--output', '-o', help='Output file path for assessment report')
    parser.add_argument('--database', '-d', help='Path to database file')
    
    args = parser.parse_args()
    
    try:
        # Initialize database
        db_manager = DatabaseManager(db_path=args.database, echo=False)
        
        with db_manager.get_session() as session:
            # Run assessment
            assessment = assess_retraining_need(session)
        
        # Format report
        report = format_assessment_report(assessment)
        
        # Display report
        print(report)
        
        # Save to file if specified
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                f.write(report)
            
            logger.info(f"Assessment report saved to: {output_path}")
        
        # Exit with appropriate code
        if assessment['recommendation'] == "RECOMMENDED":
            logger.info("Retraining is RECOMMENDED")
            sys.exit(2)  # Special exit code for automation
        elif assessment['recommendation'] == "CONSIDER":
            logger.info("Retraining should be CONSIDERED")
            sys.exit(1)
        else:
            logger.info("Retraining is NOT NEEDED")
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"Retraining assessment failed: {e}", exc_info=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
