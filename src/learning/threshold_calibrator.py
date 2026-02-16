"""
Threshold calibration for Layer 3 learning.

Analyzes match decision statistics to dynamically adjust confidence
thresholds based on observed precision and recall.
"""

import logging
from typing import Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

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
