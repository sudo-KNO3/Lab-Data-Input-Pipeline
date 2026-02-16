"""
Data structures for matching results.

Defines the output format for chemical name matching operations,
including individual match results and resolution results from the cascade engine.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class MatchResult:
    """
    Result from a single matching method.
    
    Attributes:
        analyte_id: Unique identifier for the matched analyte (REG153_XXX)
        preferred_name: Canonical name of the matched analyte
        confidence: Confidence score [0.0, 1.0]
        method: Matching method used ('exact', 'fuzzy', 'semantic', 'cas_extracted', 'hybrid')
        score: Raw similarity score from the matching algorithm
        metadata: Additional information about the match (synonym used, distance, etc.)
    """
    analyte_id: str
    preferred_name: str
    confidence: float
    method: str
    score: float
    metadata: Dict[str, any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate the match result."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confidence}")
        
        valid_methods = {'exact', 'fuzzy', 'semantic', 'cas_extracted', 'hybrid', 'unknown',
                         'vendor_cache', 'vendor_cache_stale'}
        if self.method not in valid_methods:
            raise ValueError(f"Invalid method '{self.method}', must be one of {valid_methods}")


@dataclass
class ResolutionResult:
    """
    Complete resolution result from the cascade matching engine.
    
    Attributes:
        input_text: Original input text
        best_match: Top matching result (None if no match above threshold)
        all_candidates: List of all candidate matches (up to top 5)
        signals_used: Dictionary indicating which matching methods were triggered
        disagreement_flag: True if fuzzy matches disagree significantly
        confidence_band: Classification of confidence ('AUTO_ACCEPT', 'REVIEW', 'UNKNOWN')
        resolution_time_ms: Time taken for resolution in milliseconds
    """
    input_text: str
    best_match: Optional[MatchResult]
    all_candidates: List[MatchResult] = field(default_factory=list)
    signals_used: Dict[str, bool] = field(default_factory=dict)
    disagreement_flag: bool = False
    confidence_band: str = "UNKNOWN"
    resolution_time_ms: float = 0.0
    margin: float = 0.0
    
    @property
    def is_resolved(self) -> bool:
        """Check if input was successfully resolved."""
        return self.best_match is not None
    
    @property
    def requires_review(self) -> bool:
        """Check if result requires human review."""
        return self.confidence_band == "REVIEW" or self.disagreement_flag
    
    @property
    def is_novel(self) -> bool:
        """Check if input was classified as out-of-distribution / novel compound."""
        return self.confidence_band == "NOVEL_COMPOUND"
    
    @property
    def confidence(self) -> float:
        """Get confidence score of best match."""
        return self.best_match.confidence if self.best_match else 0.0
