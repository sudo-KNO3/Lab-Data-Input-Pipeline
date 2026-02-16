"""
Maturity metrics for corpus and model health.

Tracks system maturity through:
- Corpus growth and coverage
- Match method distribution trends
- Unknown rate evolution
- Retraining trigger detection
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_

from src.database.models import MatchDecision, Synonym, Analyte

logger = logging.getLogger(__name__)


def calculate_corpus_maturity(
    session: Session,
    history_days: int = 90
) -> Dict[str, Any]:
    """
    Calculate comprehensive corpus maturity metrics.
    
    Tracks:
    - Exact match rate over time
    - Semantic reliance (percentage using embeddings)
    - Unknown rate trend
    - Synonyms per analyte (average)
    - New synonyms per week
    - Corpus growth rate
    
    Args:
        session: Database session
        history_days: Number of days to analyze (default 90)
    
    Returns:
        Dictionary with metrics:
        {
            'overall': {
                'exact_match_rate': float,
                'fuzzy_match_rate': float,
                'semantic_reliance': float,
                'unknown_rate': float,
                'avg_synonyms_per_analyte': float,
                'total_analytes': int,
                'total_synonyms': int
            },
            'trends': {
                'exact_match_rate_30d': List[float],
                'unknown_rate_30d': List[float],
                'new_synonyms_per_week': List[int]
            },
            'growth': {
                'synonyms_added_7d': int,
                'synonyms_added_30d': int,
                'synonyms_added_90d': int,
                'growth_rate_weekly': float
            },
            'timestamp': datetime
        }
    """
    now = datetime.utcnow()
    
    logger.info(f"Calculating corpus maturity (last {history_days} days)")
    
    # === Overall Statistics ===
    
    # Total counts
    total_analytes = session.execute(select(func.count(Analyte.id))).scalar_one()
    total_synonyms = session.execute(select(func.count(Synonym.id))).scalar_one()
    
    avg_synonyms_per_analyte = total_synonyms / total_analytes if total_analytes > 0 else 0.0
    
    # Match method distribution (last 30 days)
    cutoff_30d = now - timedelta(days=30)
    recent_decisions = session.execute(
        select(MatchDecision).where(MatchDecision.created_at >= cutoff_30d)
    ).scalars().all()
    
    if recent_decisions:
        exact_count = sum(
            1 for d in recent_decisions
            if d.signals_used.get('exact_match', False)
        )
        fuzzy_count = sum(
            1 for d in recent_decisions
            if d.signals_used.get('fuzzy_score', 0) > 0
            and not d.signals_used.get('exact_match', False)
        )
        semantic_count = sum(
            1 for d in recent_decisions
            if d.signals_used.get('semantic_score', 0) > 0
        )
        unknown_count = sum(1 for d in recent_decisions if d.analyte_id is None)
        
        total = len(recent_decisions)
        exact_match_rate = exact_count / total if total > 0 else 0.0
        fuzzy_match_rate = fuzzy_count / total if total > 0 else 0.0
        semantic_reliance = semantic_count / total if total > 0 else 0.0
        unknown_rate = unknown_count / total if total > 0 else 0.0
    else:
        exact_match_rate = 0.0
        fuzzy_match_rate = 0.0
        semantic_reliance = 0.0
        unknown_rate = 0.0
    
    # === Trends Over Time ===
    
    # Analyze in weekly buckets
    weeks = min(history_days // 7, 12)  # Up to 12 weeks
    
    exact_match_trend = []
    unknown_rate_trend = []
    new_synonyms_per_week = []
    
    for week in range(weeks):
        week_start = now - timedelta(days=(week + 1) * 7)
        week_end = now - timedelta(days=week * 7)
        
        # Match decisions in this week
        week_decisions = session.execute(
            select(MatchDecision).where(
                and_(
                    MatchDecision.created_at >= week_start,
                    MatchDecision.created_at < week_end
                )
            )
        ).scalars().all()
        
        if week_decisions:
            exact = sum(1 for d in week_decisions if d.signals_used.get('exact_match', False))
            unknown = sum(1 for d in week_decisions if d.analyte_id is None)
            total = len(week_decisions)
            
            exact_match_trend.append(exact / total if total > 0 else 0.0)
            unknown_rate_trend.append(unknown / total if total > 0 else 0.0)
        else:
            exact_match_trend.append(0.0)
            unknown_rate_trend.append(0.0)
        
        # New synonyms in this week
        new_syns = session.execute(
            select(func.count(Synonym.id)).where(
                and_(
                    Synonym.created_at >= week_start,
                    Synonym.created_at < week_end
                )
            )
        ).scalar_one()
        
        new_synonyms_per_week.append(new_syns)
    
    # Reverse to chronological order
    exact_match_trend.reverse()
    unknown_rate_trend.reverse()
    new_synonyms_per_week.reverse()
    
    # === Growth Metrics ===
    
    cutoff_7d = now - timedelta(days=7)
    cutoff_90d = now - timedelta(days=90)
    
    synonyms_added_7d = session.execute(
        select(func.count(Synonym.id)).where(Synonym.created_at >= cutoff_7d)
    ).scalar_one()
    
    synonyms_added_30d = session.execute(
        select(func.count(Synonym.id)).where(Synonym.created_at >= cutoff_30d)
    ).scalar_one()
    
    synonyms_added_90d = session.execute(
        select(func.count(Synonym.id)).where(Synonym.created_at >= cutoff_90d)
    ).scalar_one()
    
    # Weekly growth rate (synonyms per week)
    growth_rate_weekly = synonyms_added_7d  # Last week
    
    # === Compile Results ===
    
    results = {
        'overall': {
            'exact_match_rate': exact_match_rate,
            'fuzzy_match_rate': fuzzy_match_rate,
            'semantic_reliance': semantic_reliance,
            'unknown_rate': unknown_rate,
            'avg_synonyms_per_analyte': avg_synonyms_per_analyte,
            'total_analytes': total_analytes,
            'total_synonyms': total_synonyms
        },
        'trends': {
            'exact_match_rate_trend': exact_match_trend,
            'unknown_rate_trend': unknown_rate_trend,
            'new_synonyms_per_week': new_synonyms_per_week
        },
        'growth': {
            'synonyms_added_7d': synonyms_added_7d,
            'synonyms_added_30d': synonyms_added_30d,
            'synonyms_added_90d': synonyms_added_90d,
            'growth_rate_weekly': growth_rate_weekly
        },
        'timestamp': now
    }
    
    logger.info(
        f"Maturity metrics: exact={exact_match_rate:.2%}, "
        f"unknown={unknown_rate:.2%}, synonyms={total_synonyms}"
    )
    
    return results


def detect_plateau(
    history: List[float],
    window: int = 4,
    threshold: float = 0.02
) -> bool:
    """
    Detect if a metric has plateaued (stopped improving).
    
    Uses linear regression on recent window to detect flat trend.
    
    Args:
        history: List of metric values over time (chronological)
        window: Number of recent points to analyze (default 4)
        threshold: Maximum slope to consider plateau (default 0.02)
    
    Returns:
        True if plateaued, False if still improving
        
    Example:
        >>> unknown_rates = [0.15, 0.12, 0.10, 0.09, 0.09, 0.09]
        >>> detect_plateau(unknown_rates, window=3)
        True  # Last 3 values are flat
    """
    if not history or len(history) < window:
        return False
    
    # Get recent window
    recent = history[-window:]
    
    # Calculate linear regression slope
    x = np.arange(len(recent))
    y = np.array(recent)
    
    # Avoid division by zero
    if np.std(x) == 0:
        return True
    
    # Calculate slope
    slope = np.corrcoef(x, y)[0, 1] * (np.std(y) / np.std(x))
    
    # Plateau if slope is near zero (or slightly positive for metrics we want to decrease)
    is_plateau = abs(slope) < threshold
    
    logger.debug(f"Plateau detection: slope={slope:.4f}, threshold={threshold}, plateau={is_plateau}")
    
    return is_plateau


def should_retrain_model(
    stats: Dict[str, Any],
    triggers: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Determine if model retraining is needed based on triggers.
    
    Retraining triggers:
    1. validated_since_last_train >= 2000 (new data available)
    2. unknown_rate_plateau (not decreasing despite growth)
    3. semantic_reliance > 0.30 (over-relying on embeddings)
    4. new_chemical_groups_added > 0 (structural changes)
    
    Requires at least 2 triggers to recommend retraining.
    
    Args:
        stats: Maturity statistics from calculate_corpus_maturity()
        triggers: Custom trigger thresholds (optional)
    
    Returns:
        Tuple of (should_retrain: bool, reasons: Dict)
        
    Example:
        >>> should_retrain, reasons = should_retrain_model(stats)
        >>> if should_retrain:
        ...     print(f"Retrain triggered: {reasons['active_triggers']}")
    """
    # Default trigger thresholds
    default_triggers = {
        'validated_since_last_train': 2000,
        'unknown_rate_plateau_threshold': 0.02,
        'semantic_reliance_max': 0.30,
        'new_chemical_groups': 0,
        'min_triggers_required': 2
    }
    
    if triggers:
        default_triggers.update(triggers)
    
    trigger_config = default_triggers
    
    # Track active triggers
    active_triggers = []
    trigger_details = {}
    
    # === Trigger 1: Sufficient new validated data ===
    # Get count of validated synonyms since last model snapshot
    # For now, use 30-day synonym additions as proxy
    validated_count = stats['growth']['synonyms_added_30d']
    
    if validated_count >= trigger_config['validated_since_last_train']:
        active_triggers.append('validated_data_threshold')
        trigger_details['validated_data_threshold'] = {
            'count': validated_count,
            'threshold': trigger_config['validated_since_last_train']
        }
    
    # === Trigger 2: Unknown rate plateau ===
    unknown_trend = stats['trends'].get('unknown_rate_trend', [])
    
    if unknown_trend and detect_plateau(
        unknown_trend,
        window=4,
        threshold=trigger_config['unknown_rate_plateau_threshold']
    ):
        active_triggers.append('unknown_rate_plateau')
        trigger_details['unknown_rate_plateau'] = {
            'recent_trend': unknown_trend[-4:] if len(unknown_trend) >= 4 else unknown_trend,
            'current_rate': stats['overall']['unknown_rate']
        }
    
    # === Trigger 3: High semantic reliance ===
    semantic_reliance = stats['overall'].get('semantic_reliance', 0.0)
    
    if semantic_reliance > trigger_config['semantic_reliance_max']:
        active_triggers.append('high_semantic_reliance')
        trigger_details['high_semantic_reliance'] = {
            'reliance': semantic_reliance,
            'threshold': trigger_config['semantic_reliance_max']
        }
    
    # === Trigger 4: New chemical groups (would need custom tracking) ===
    # This would require tracking new analyte types or categories
    # For now, we'll check if new analytes were added
    # This is a placeholder - would need proper implementation
    
    # Determine if retraining needed
    num_active = len(active_triggers)
    should_retrain = num_active >= trigger_config['min_triggers_required']
    
    reasons = {
        'should_retrain': should_retrain,
        'active_triggers': active_triggers,
        'num_active_triggers': num_active,
        'min_required': trigger_config['min_triggers_required'],
        'trigger_details': trigger_details,
        'stats_snapshot': {
            'unknown_rate': stats['overall']['unknown_rate'],
            'semantic_reliance': semantic_reliance,
            'total_synonyms': stats['overall']['total_synonyms'],
            'validated_30d': validated_count
        }
    }
    
    if should_retrain:
        logger.warning(
            f"Retraining recommended: {num_active} triggers active "
            f"({', '.join(active_triggers)})"
        )
    else:
        logger.info(
            f"Retraining not needed: {num_active} triggers active "
            f"(requires {trigger_config['min_triggers_required']})"
        )
    
    return should_retrain, reasons


def get_last_training_date(session: Session) -> Optional[datetime]:
    """
    Get the date of the last model training.
    
    Looks for most recent snapshot with type='model'.
    
    Args:
        session: Database session
    
    Returns:
        Datetime of last training, or None if never trained
    """
    from src.database.models import SnapshotRegistry
    
    snapshot = session.execute(
        select(SnapshotRegistry)
        .where(SnapshotRegistry.snapshot_type == 'model')
        .order_by(SnapshotRegistry.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    
    if snapshot:
        return snapshot.created_at
    
    return None


def calculate_time_since_training(session: Session) -> Optional[timedelta]:
    """
    Calculate time elapsed since last model training.
    
    Args:
        session: Database session
    
    Returns:
        Timedelta since last training, or None if never trained
    """
    last_training = get_last_training_date(session)
    
    if last_training:
        return datetime.utcnow() - last_training
    
    return None
