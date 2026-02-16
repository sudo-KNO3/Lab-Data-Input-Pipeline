"""
Chemical name matching engine package.

Provides cascade matching logic for chemical name resolution using:
- Exact matching (normalized synonym lookup)
- CAS number extraction and lookup
- Fuzzy matching (Levenshtein distance)
- Semantic matching (FAISS + sentence-transformers embeddings)

The resolution engine coordinates these methods with confidence thresholds
and disagreement detection.
"""

import logging
from pathlib import Path
from typing import Optional

from src.matching.match_result import MatchResult, ResolutionResult
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.resolution_engine import ResolutionEngine
from src.matching.semantic_matcher import SemanticMatcher
from src.matching.types import EmbeddingConfig

_logger = logging.getLogger(__name__)


def build_engine(
    db_session,
    normalizer=None,
    enable_semantic: bool = True,
    base_path: Optional[str] = None,
    **engine_kwargs,
) -> ResolutionEngine:
    """
    Build a ResolutionEngine with all available signals wired.

    Loads the SemanticMatcher if the FAISS index exists on disk.
    Falls back to fuzzy-only mode if embeddings are missing or
    if ``enable_semantic=False``.

    Args:
        db_session: SQLAlchemy session.
        normalizer: TextNormalizer (created internally if None).
        enable_semantic: If False, skip semantic matcher entirely.
        base_path: Project root for resolving relative paths.
        **engine_kwargs: Extra kwargs forwarded to ResolutionEngine.

    Returns:
        Fully-wired ResolutionEngine instance.
    """
    if base_path is None:
        # default: two levels up from src/matching/
        base_path = str(Path(__file__).resolve().parent.parent.parent)

    semantic_matcher = None
    if enable_semantic:
        config = EmbeddingConfig()
        index_path = Path(base_path) / config.faiss_index_path
        if index_path.exists():
            try:
                semantic_matcher = SemanticMatcher(config=config, base_path=base_path)
                _logger.info(
                    "SemanticMatcher loaded: %d vectors",
                    semantic_matcher.index.ntotal if semantic_matcher.index else 0,
                )
            except Exception as exc:
                _logger.warning("SemanticMatcher unavailable: %s", exc)
        else:
            _logger.info(
                "FAISS index not found at %s — running without semantic signal",
                index_path,
            )

    return ResolutionEngine(
        db_session=db_session,
        normalizer=normalizer,
        semantic_matcher=semantic_matcher,
        **engine_kwargs,
    )


__all__ = [
    "MatchResult",
    "ResolutionResult",
    "ExactMatcher",
    "FuzzyMatcher",
    "ResolutionEngine",
    "SemanticMatcher",
    "EmbeddingConfig",
    "build_engine",
]
