"""
Exact matching module for chemical names.

Performs normalized exact matching against the synonyms database,
returning matches with 1.0 confidence when found.
"""

from typing import Optional, Any
from sqlalchemy.orm import Session

from src.database.models import Synonym, Analyte
from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.matching.match_result import MatchResult


class ExactMatcher:
    """
    Exact matching engine for chemical names.
    
    Uses normalized text matching against the synonyms database.
    Returns confidence of 1.0 for exact matches.
    Also checks for CAS numbers in the input text.
    """
    
    def __init__(self, normalizer: Optional[TextNormalizer] = None, 
                 cas_extractor: Optional[CASExtractor] = None):
        """
        Initialize the exact matcher.
        
        Args:
            normalizer: TextNormalizer instance (creates new if None)
            cas_extractor: CASExtractor instance (creates new if None)
        """
        self.normalizer = normalizer or TextNormalizer()
        self.cas_extractor = cas_extractor or CASExtractor()
    
    def match(self, text: str, db_session: Session) -> Optional[MatchResult]:
        """
        Attempt exact match on input text.
        
        First checks for CAS numbers, then performs normalized synonym lookup.
        
        Args:
            text: Input chemical name or CAS number
            db_session: SQLAlchemy database session
            
        Returns:
            MatchResult with confidence 1.0 if found, None otherwise
        """
        if not text or not isinstance(text, str):
            return None
        
        # Try CAS extraction first (highest priority)
        cas_result = self._match_by_cas(text, db_session)
        if cas_result:
            return cas_result
        
        # Try normalized synonym lookup
        synonym_result = self._normalize_and_lookup(text, db_session)
        if synonym_result:
            return synonym_result
        
        return None
    
    def _match_by_cas(self, text: str, db_session: Session) -> Optional[MatchResult]:
        """
        Extract CAS number from text and look up in database.
        
        Args:
            text: Input text potentially containing CAS number
            db_session: Database session
            
        Returns:
            MatchResult if CAS found and matched, None otherwise
        """
        # Extract CAS number
        cas_number = self.cas_extractor.extract_cas(text)
        if not cas_number:
            return None
        
        # Look up in database
        analyte = self.cas_extractor.lookup_by_cas(cas_number, db_session)
        if not analyte:
            return None
        
        # Return exact match result
        return MatchResult(
            analyte_id=analyte.analyte_id,
            preferred_name=analyte.preferred_name,
            confidence=1.0,
            method='cas_extracted',
            score=1.0,
            metadata={
                'cas_number': cas_number,
                'synonym_type': 'cas_number'
            }
        )
    
    def _normalize_and_lookup(self, text: str, db_session: Session) -> Optional[MatchResult]:
        """
        Normalize input text and look up in synonyms table.
        
        Args:
            text: Input chemical name
            db_session: Database session
            
        Returns:
            MatchResult if synonym found, None otherwise
        """
        # Normalize input text
        normalized = self.normalizer.normalize(text)
        if not normalized:
            return None
        
        # Query synonyms table for exact normalized match
        synonym = db_session.query(Synonym).filter(
            Synonym.synonym_norm == normalized
        ).first()
        
        if not synonym:
            return None
        
        # Get the analyte
        analyte = db_session.query(Analyte).filter(
            Analyte.analyte_id == synonym.analyte_id
        ).first()
        
        if not analyte:
            return None
        
        # Return exact match result
        return MatchResult(
            analyte_id=analyte.analyte_id,
            preferred_name=analyte.preferred_name,
            confidence=1.0,
            method='exact',
            score=1.0,
            metadata={
                'synonym_raw': synonym.synonym_raw,
                'synonym_norm': synonym.synonym_norm,
                'synonym_type': synonym.synonym_type.value,
                'normalized_input': normalized
            }
        )
