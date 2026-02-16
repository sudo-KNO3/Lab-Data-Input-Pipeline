"""
Chemical name matching engine package.

Provides cascade matching logic for chemical name resolution using:
- Exact matching (normalized synonym lookup)
- CAS number extraction and lookup
- Fuzzy matching (Levenshtein distance)
- Semantic matching (embeddings, to be added later)

The resolution engine coordinates these methods with confidence thresholds
and disagreement detection.
"""

from src.matching.match_result import MatchResult, ResolutionResult
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.resolution_engine import ResolutionEngine

__all__ = [
    "MatchResult",
    "ResolutionResult",
    "ExactMatcher",
    "FuzzyMatcher",
    "ResolutionEngine",
]
