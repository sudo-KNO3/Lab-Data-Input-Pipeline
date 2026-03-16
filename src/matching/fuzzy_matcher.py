"""
Fuzzy matching module for chemical names.

Uses Levenshtein distance for approximate string matching,
returning ranked candidates with confidence scores.
"""

from typing import List, Tuple, Optional
from sqlalchemy import select, func
from sqlalchemy.orm import Session
import Levenshtein

from src.database.models import Synonym, Analyte
from src.normalization.text_normalizer import TextNormalizer
from src.matching.match_result import MatchResult


class FuzzyMatcher:
    """
    Fuzzy matching engine using Levenshtein distance.
    
    Compares normalized input against all synonyms in the database,
    returning top-K matches above a similarity threshold.
    """
    
    # Confidence mapping based on similarity score
    CONFIDENCE_THRESHOLDS = {
        0.95: 0.95,
        0.85: 0.85,
        0.75: 0.75,
    }
    
    def __init__(self, normalizer: Optional[TextNormalizer] = None):
        """
        Initialize the fuzzy matcher.
        
        Args:
            normalizer: TextNormalizer instance (creates new if None)
        """
        self.normalizer = normalizer or TextNormalizer()
    
    def match(self, text: str, db_session: Session, 
              threshold: float = 0.75, top_k: int = 5,
              vendor: Optional[str] = None,
              vendor_boost: float = 0.0) -> List[MatchResult]:
        """
        Find fuzzy matches for input text.
        
        Args:
            text: Input chemical name
            db_session: SQLAlchemy database session
            threshold: Minimum similarity score (0.0-1.0)
            top_k: Maximum number of results to return
            vendor: Lab vendor name for tiebreak boost
            vendor_boost: Additive score boost for vendor-matching synonyms
            
        Returns:
            List of MatchResult objects sorted by score (highest first)
        """
        if not text or not isinstance(text, str):
            return []
        
        # Normalize input
        normalized_input = self.normalizer.normalize(text)
        if not normalized_input:
            return []
        
        # Single JOIN query: fetch synonyms + their analytes in one round trip.
        # A length-based SQL pre-filter reduces the candidate pool before
        # Python-level Levenshtein scoring. For very short inputs (<=3 chars)
        # the filter is skipped to avoid over-pruning abbreviations.
        n = len(normalized_input)
        stmt = select(Synonym, Analyte).join(Analyte, Synonym.analyte_id == Analyte.analyte_id)
        if n > 3:
            # Conservative bounds: a Levenshtein ratio >= threshold requires
            # |len_a - len_b| <= (1 - threshold) * (len_a + len_b) / threshold.
            # Using ±20% slack prevents false exclusions near the boundary.
            min_len = max(1, int(n * threshold * 0.80))
            max_len = int(n / threshold * 1.20) + 1
            stmt = stmt.where(func.length(Synonym.synonym_norm).between(min_len, max_len))

        rows = db_session.execute(stmt).all()

        # Score candidates and filter by threshold
        matches: List[Tuple[float, Synonym, Analyte, bool]] = []
        for synonym, analyte in rows:
            similarity = self._calculate_similarity(normalized_input, synonym.synonym_norm)

            # Vendor tiebreak: boost synonyms from the same lab vendor
            vendor_match = False
            if vendor and vendor_boost > 0.0 and hasattr(synonym, 'lab_vendor'):
                if synonym.lab_vendor and synonym.lab_vendor == vendor:
                    similarity = min(similarity + vendor_boost, 1.0)
                    vendor_match = True

            if similarity >= threshold:
                matches.append((similarity, synonym, analyte, vendor_match))

        # Sort by similarity (descending), take top-K
        matches.sort(key=lambda x: x[0], reverse=True)
        matches = matches[:top_k]

        # Build MatchResult objects (analyte already loaded from JOIN — no extra queries)
        results = []
        for similarity, synonym, analyte, vendor_match in matches:
            confidence = self._map_confidence(similarity)
            result = MatchResult(
                analyte_id=analyte.analyte_id,
                preferred_name=analyte.preferred_name,
                confidence=confidence,
                method='fuzzy',
                score=similarity,
                metadata={
                    'synonym_raw': synonym.synonym_raw,
                    'synonym_norm': synonym.synonym_norm,
                    'synonym_type': synonym.synonym_type.value,
                    'normalized_input': normalized_input,
                    'levenshtein_ratio': similarity,
                    'vendor_boosted': vendor_match
                }
            )
            results.append(result)

        return results
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate Levenshtein similarity ratio between two strings.
        
        Args:
            text1: First string
            text2: Second string
            
        Returns:
            Similarity ratio [0.0, 1.0], where 1.0 is identical
        """
        if not text1 or not text2:
            return 0.0
        
        # Use Levenshtein ratio (normalized by string lengths)
        ratio = Levenshtein.ratio(text1, text2)
        return ratio
    
    def _map_confidence(self, similarity: float) -> float:
        """
        Map similarity score to confidence score.
        
        Returns raw Levenshtein ratio directly to preserve score
        discrimination for downstream margin-based gating.
        Step-function binning was removed to maintain distance
        geometry and relative ordering of candidates.
        
        Args:
            similarity: Levenshtein ratio [0.0, 1.0]
            
        Returns:
            Confidence score [0.0, 1.0] (raw score passthrough)
        """
        return similarity
