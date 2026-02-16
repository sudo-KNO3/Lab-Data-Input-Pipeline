"""
Fuzzy matching module for chemical names.

Uses Levenshtein distance for approximate string matching,
returning ranked candidates with confidence scores.
"""

from typing import List, Tuple, Optional
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
        
        # Get all synonyms from database
        synonyms = db_session.query(Synonym).all()
        
        # Calculate similarities
        matches: List[Tuple[float, Synonym, bool]] = []
        for synonym in synonyms:
            similarity = self._calculate_similarity(normalized_input, synonym.synonym_norm)
            
            # Vendor tiebreak: boost synonyms from the same lab vendor
            vendor_match = False
            if vendor and vendor_boost > 0.0 and hasattr(synonym, 'lab_vendor'):
                if synonym.lab_vendor and synonym.lab_vendor == vendor:
                    similarity = min(similarity + vendor_boost, 1.0)
                    vendor_match = True
            
            if similarity >= threshold:
                matches.append((similarity, synonym, vendor_match))
        
        # Sort by similarity (descending)
        matches.sort(key=lambda x: x[0], reverse=True)
        
        # Take top-K
        matches = matches[:top_k]
        
        # Build MatchResult objects
        results = []
        for similarity, synonym, vendor_match in matches:
            # Get analyte
            analyte = db_session.query(Analyte).filter(
                Analyte.analyte_id == synonym.analyte_id
            ).first()
            
            if not analyte:
                continue
            
            # Map similarity to confidence
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
