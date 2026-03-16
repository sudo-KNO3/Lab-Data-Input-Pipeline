"""
Threshold calibration for Layer 3 learning.

Analyzes match decision statistics to dynamically adjust confidence
thresholds based on observed precision and recall.
"""

import logging
import yaml
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..database.models import MatchDecision

logger = logging.getLogger(__name__)


class ThresholdCalibrator:
    """
    Calibrates confidence thresholds based on match decision statistics.
    
    This enables Layer 3 learning: dynamic threshold adjustment based
    on observed precision and recall of the matching system.
    """
    
    def __init__(self):
        """Initialize the threshold calibrator."""
        self.statistics: dict[str, Any] = {}
        self.optimal_thresholds: dict[str, float] = {}
        logger.info("ThresholdCalibrator initialized")
    
    def analyze_recent_decisions(
        self,
        db_session: Session,
        days: int = 30,
        min_confidence: float = 0.0
    ) -> dict[str, Any]:
        """
        Analyze recent match decisions to compute statistics.
        
        Args:
            db_session: Database session
            days: Number of days to analyze
            min_confidence: Minimum confidence to include
        
        Returns:
            Dictionary containing comprehensive statistics
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Query recent decisions
        stmt = select(MatchDecision).where(
            MatchDecision.decision_timestamp >= cutoff_date,
            MatchDecision.confidence_score >= min_confidence
        )
        
        decisions = db_session.execute(stmt).scalars().all()
        
        if not decisions:
            logger.warning(f"No decisions found in the last {days} days")
            return self._empty_statistics()
        
        logger.info(f"Analyzing {len(decisions)} decisions from the last {days} days")
        
        # Compute statistics
        self.statistics = self._compute_statistics(decisions)
        
        return self.statistics
    
    def _compute_statistics(self, decisions: List[MatchDecision]) -> dict[str, Any]:
        """
        Compute comprehensive statistics from decisions.
        
        Args:
            decisions: List of match decisions
        
        Returns:
            Statistics dictionary
        """
        total_decisions = len(decisions)
        validated_decisions = [d for d in decisions if d.human_validated]
        
        # Basic counts
        stats = {
            'total_decisions': total_decisions,
            'validated_count': len(validated_decisions),
            'validation_rate': len(validated_decisions) / total_decisions if total_decisions > 0 else 0,
            'analysis_period_days': (
                (max(d.decision_timestamp for d in decisions) - 
                 min(d.decision_timestamp for d in decisions)).days
                if decisions else 0
            )
        }
        
        # Method distribution
        method_counts = defaultdict(int)
        for decision in decisions:
            method_counts[decision.match_method] += 1
        
        stats['method_distribution'] = dict(method_counts)
        stats['method_percentages'] = {
            method: (count / total_decisions * 100)
            for method, count in method_counts.items()
        }
        
        # Validation statistics (only for validated decisions)
        if validated_decisions:
            # Acceptance rate for top-1 candidate
            top1_correct = sum(
                1 for d in validated_decisions
                if d.matched_analyte_id is not None
            )
            stats['acceptance_rate_top1'] = top1_correct / len(validated_decisions)
            
            # Override frequency (when user chose different candidate)
            disagreements = sum(1 for d in validated_decisions if d.disagreement_flag)
            stats['override_frequency'] = disagreements / len(validated_decisions)
            
            # Unknown rate (no match found)
            no_match = sum(
                1 for d in validated_decisions
                if d.matched_analyte_id is None
            )
            stats['unknown_rate'] = no_match / len(validated_decisions)
            
            # Disagreement by method
            stats['disagreement_by_method'] = self._compute_disagreement_by_method(validated_decisions)
            
        else:
            stats['acceptance_rate_top1'] = None
            stats['override_frequency'] = None
            stats['unknown_rate'] = None
            stats['disagreement_by_method'] = {}
        
        # Confidence distribution
        stats['confidence_distribution'] = self._compute_confidence_distribution(decisions)
        
        # Ingestion statistics
        ingested_count = sum(1 for d in validated_decisions if d.ingested)
        stats['ingested_count'] = ingested_count
        stats['ingestion_rate'] = (
            ingested_count / len(validated_decisions)
            if validated_decisions else 0
        )
        
        return stats
    
    def _compute_disagreement_by_method(
        self,
        validated_decisions: List[MatchDecision]
    ) -> dict[str, float]:
        """Compute disagreement rates by matching method."""
        method_stats = defaultdict(lambda: {'total': 0, 'disagreements': 0})
        
        for decision in validated_decisions:
            method = decision.match_method
            method_stats[method]['total'] += 1
            if decision.disagreement_flag:
                method_stats[method]['disagreements'] += 1
        
        return {
            method: (
                stats['disagreements'] / stats['total']
                if stats['total'] > 0 else 0
            )
            for method, stats in method_stats.items()
        }
    
    def _compute_confidence_distribution(
        self,
        decisions: List[MatchDecision]
    ) -> dict[str, int]:
        """Compute distribution of confidence scores in bins."""
        bins = {
            '0.0-0.5': 0,
            '0.5-0.7': 0,
            '0.7-0.8': 0,
            '0.8-0.9': 0,
            '0.9-0.95': 0,
            '0.95-1.0': 0
        }
        
        for decision in decisions:
            score = decision.confidence_score
            if score < 0.5:
                bins['0.0-0.5'] += 1
            elif score < 0.7:
                bins['0.5-0.7'] += 1
            elif score < 0.8:
                bins['0.7-0.8'] += 1
            elif score < 0.9:
                bins['0.8-0.9'] += 1
            elif score < 0.95:
                bins['0.9-0.95'] += 1
            else:
                bins['0.95-1.0'] += 1
        
        return bins
    
    def calculate_optimal_thresholds(
        self,
        decisions: List[MatchDecision],
        target_precision: float = 0.98,
        target_recall: float = 0.90
    ) -> dict[str, float]:
        """
        Calculate optimal thresholds based on precision/recall targets.
        
        Args:
            decisions: List of validated match decisions
            target_precision: Target precision for auto-accept threshold
            target_recall: Target recall for review threshold
        
        Returns:
            Dictionary of optimal thresholds
        """
        # Filter to only validated decisions
        validated_decisions = [d for d in decisions if d.human_validated]
        
        if not validated_decisions:
            logger.warning("No validated decisions to calibrate thresholds")
            return self._default_thresholds()
        
        # Sort by confidence score
        sorted_decisions = sorted(
            validated_decisions,
            key=lambda d: d.confidence_score,
            reverse=True
        )
        
        # Find auto-accept threshold (high precision)
        auto_accept_threshold = self._find_precision_threshold(
            sorted_decisions,
            target_precision
        )
        
        # Find review threshold (balanced precision/recall)
        review_threshold = self._find_balanced_threshold(
            sorted_decisions,
            target_precision=0.90,
            target_recall=target_recall
        )
        
        self.optimal_thresholds = {
            'auto_accept': auto_accept_threshold,
            'review': review_threshold,
            'unknown': review_threshold,  # Same as review
            'disagreement_cap': auto_accept_threshold - 0.05  # Slightly below auto-accept
        }
        
        logger.info(f"Calculated optimal thresholds: {self.optimal_thresholds}")
        
        return self.optimal_thresholds
    
    def _find_precision_threshold(
        self,
        sorted_decisions: List[MatchDecision],
        target_precision: float
    ) -> float:
        """Find threshold that achieves target precision."""
        if not sorted_decisions:
            return 0.93  # Default
        
        for i in range(len(sorted_decisions)):
            # Count correct predictions at this threshold
            correct = sum(
                1 for d in sorted_decisions[:i+1]
                if not d.disagreement_flag and d.matched_analyte_id is not None
            )
            total = i + 1
            precision = correct / total if total > 0 else 0
            
            if precision >= target_precision and total >= 10:  # Require at least 10 samples
                return sorted_decisions[i].confidence_score
        
        # If target not achieved, return high default
        return 0.95
    
    def _find_balanced_threshold(
        self,
        sorted_decisions: List[MatchDecision],
        target_precision: float,
        target_recall: float
    ) -> float:
        """Find threshold that balances precision and recall."""
        if not sorted_decisions:
            return 0.75  # Default
        
        # Total positive cases (actual matches)
        total_positives = sum(
            1 for d in sorted_decisions
            if d.matched_analyte_id is not None and not d.disagreement_flag
        )
        
        if total_positives == 0:
            return 0.75
        
        best_threshold = 0.75
        best_f1 = 0.0
        
        for i in range(len(sorted_decisions)):
            # Count correct predictions at this threshold
            true_positives = sum(
                1 for d in sorted_decisions[:i+1]
                if not d.disagreement_flag and d.matched_analyte_id is not None
            )
            
            total_predicted = i + 1
            precision = true_positives / total_predicted if total_predicted > 0 else 0
            recall = true_positives / total_positives if total_positives > 0 else 0
            
            # F1 score
            if precision + recall > 0:
                f1 = 2 * (precision * recall) / (precision + recall)
                
                # Check if meets targets
                if precision >= target_precision and recall >= target_recall:
                    if f1 > best_f1:
                        best_f1 = f1
                        best_threshold = sorted_decisions[i].confidence_score
        
        return best_threshold
    
    def get_statistics(self) -> dict:
        """
        Get the most recently computed statistics.
        
        Returns:
            Statistics dictionary
        """
        return self.statistics
    
    def _default_thresholds(self) -> dict[str, float]:
        """Return default thresholds."""
        return {
            'auto_accept': 0.93,
            'review': 0.75,
            'unknown': 0.75,
            'disagreement_cap': 0.84
        }
    
    def _empty_statistics(self) -> dict[str, Any]:
        """Return empty statistics structure."""
        return {
            'total_decisions': 0,
            'validated_count': 0,
            'validation_rate': 0,
            'analysis_period_days': 0,
            'method_distribution': {},
            'method_percentages': {},
            'acceptance_rate_top1': None,
            'override_frequency': None,
            'unknown_rate': None,
            'disagreement_by_method': {},
            'confidence_distribution': {},
            'ingested_count': 0,
            'ingestion_rate': 0
        }


# ============================================================================
# Module-level wrappers for script compatibility
# ============================================================================

def analyze_match_decisions(session: Session, days_back: int = 30) -> Dict[str, Any]:
    """
    Analyze recent match decisions and return stats in the format expected by
    scripts/10_monthly_calibration.py.
    """
    calibrator = ThresholdCalibrator()
    stats = calibrator.analyze_recent_decisions(session, days=days_back)
    total = stats['total_decisions']
    return {
        'total_decisions': total,
        'acceptance_rate_top1': stats.get('acceptance_rate_top1') or 0.0,
        'override_frequency': stats.get('override_frequency') or 0.0,
        'disagreement_frequency': stats.get('override_frequency') or 0.0,
        'unknown_rate': stats.get('unknown_rate') or 0.0,
        'method_distribution': {
            method: (count / total if total > 0 else 0.0)
            for method, count in stats.get('method_distribution', {}).items()
        },
        'decisions_by_confidence': stats.get('confidence_distribution', {}),
    }


def recalibrate_thresholds(
    session: Session,
    days_back: int = 30,
    target_precision: float = 0.98
) -> Dict[str, Any]:
    """
    Recalibrate thresholds from recent decisions. Returns result dict with
    keys: success, message, recommended_thresholds, method_sample_sizes.
    """
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    decisions = session.execute(
        select(MatchDecision).where(MatchDecision.decision_timestamp >= cutoff)
    ).scalars().all()

    if len(decisions) < 100:
        return {
            'success': False,
            'message': f'Insufficient data: {len(decisions)} decisions (need ≥100)',
            'recommended_thresholds': {},
            'method_sample_sizes': {},
        }

    calibrator = ThresholdCalibrator()
    optimal = calibrator.calculate_optimal_thresholds(decisions, target_precision=target_precision)

    method_counts: Dict[str, int] = {}
    for d in decisions:
        method_counts[d.match_method] = method_counts.get(d.match_method, 0) + 1

    return {
        'success': True,
        'message': f'Calibrated on {len(decisions)} decisions over last {days_back} days',
        'recommended_thresholds': optimal,
        'method_sample_sizes': method_counts,
    }


def update_config_thresholds(config_path: str, new_thresholds: Dict[str, float]) -> bool:
    """
    Update threshold values in a YAML config file.

    Only keys present in new_thresholds AND recognized threshold names
    (auto_accept, review, disagreement_cap) are written.
    """
    path = Path(config_path)
    try:
        cfg: dict = {}
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
        cfg.setdefault('thresholds', {}).update({
            k: v for k, v in new_thresholds.items()
            if k in ('auto_accept', 'review', 'disagreement_cap', 'margin_threshold')
        })
        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(cfg, f, default_flow_style=False)
        logger.info(f"Updated thresholds in {config_path}: {new_thresholds}")
        return True
    except Exception as e:
        logger.error(f"Failed to update config {config_path}: {e}")
        return False
