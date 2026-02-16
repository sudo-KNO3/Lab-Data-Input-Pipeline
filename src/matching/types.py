"""
Type definitions for the chemical matching engine.

Defines data structures and enums used across all matching modules.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime


class MatchMethod(Enum):
    """Methods used for matching chemicals."""
    EXACT = "exact"
    CAS_EXTRACTED = "cas_extracted"
    FUZZY = "fuzzy"
    SEMANTIC = "semantic"
    HYBRID = "hybrid"


class ConfidenceLevel(Enum):
    """Confidence level categories."""
    HIGH = "high"  # >= 0.95
    MEDIUM = "medium"  # >= 0.85
    LOW = "low"  # >= 0.75
    VERY_LOW = "very_low"  # < 0.75


@dataclass
class Match:
    """
    A single match candidate.
    
    Represents one potential match between a query and an analyte,
    including the analyte details, confidence score, and method used.
    """
    analyte_id: int
    analyte_name: str
    cas_number: Optional[str]
    confidence: float
    method: MatchMethod
    synonym_matched: Optional[str] = None
    synonym_id: Optional[int] = None
    distance_score: Optional[float] = None  # For fuzzy matching
    similarity_score: Optional[float] = None  # For semantic matching
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate confidence is in valid range."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")
    
    @property
    def confidence_level(self) -> ConfidenceLevel:
        """Get the confidence level category."""
        if self.confidence >= 0.95:
            return ConfidenceLevel.HIGH
        elif self.confidence >= 0.85:
            return ConfidenceLevel.MEDIUM
        elif self.confidence >= 0.75:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.VERY_LOW
    
    def to_match_result(self) -> 'MatchResultCompat':
        """
        Convert to match_result.MatchResult for resolution engine compatibility.
        
        This is an output projection — preserves internal state richness
        while constraining the interface to match the resolution engine's
        expected type. Avoids full type unification to prevent cascading
        reparameterization.
        
        Returns:
            match_result.MatchResult instance
        """
        from .match_result import MatchResult as MatchResultCompat
        return MatchResultCompat(
            analyte_id=str(self.analyte_id),
            preferred_name=self.analyte_name,
            confidence=self.confidence,
            method=self.method.value,
            score=self.similarity_score if self.similarity_score is not None 
                  else (self.distance_score if self.distance_score is not None 
                        else self.confidence),
            metadata={
                'cas_number': self.cas_number,
                'synonym_matched': self.synonym_matched,
                'synonym_id': self.synonym_id,
                'distance_score': self.distance_score,
                'similarity_score': self.similarity_score,
                **self.metadata,
            }
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "analyte_id": self.analyte_id,
            "analyte_name": self.analyte_name,
            "cas_number": self.cas_number,
            "confidence": self.confidence,
            "confidence_level": self.confidence_level.value,
            "method": self.method.value,
            "synonym_matched": self.synonym_matched,
            "synonym_id": self.synonym_id,
            "distance_score": self.distance_score,
            "similarity_score": self.similarity_score,
            "metadata": self.metadata,
        }


@dataclass
class MatchResult:
    """
    Complete result from the matching resolution engine.
    
    Contains the best match (if any), all candidates considered,
    decision provenance, and quality flags.
    """
    query_text: str
    query_norm: str
    best_match: Optional[Match] = None
    all_candidates: List[Match] = field(default_factory=list)
    
    # Decision metadata
    methods_used: List[MatchMethod] = field(default_factory=list)
    signals: Dict[str, Any] = field(default_factory=dict)
    disagreement_detected: bool = False
    disagreement_penalty: float = 0.0
    
    # CAS extraction
    cas_extracted: Optional[str] = None
    cas_extraction_attempted: bool = False
    
    # Timing
    processing_time_ms: Optional[float] = None
    
    # Quality flags
    manual_review_recommended: bool = False
    review_reason: Optional[str] = None
    
    # Timestamp
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def matched(self) -> bool:
        """Check if a match was found."""
        return self.best_match is not None
    
    @property
    def confidence(self) -> float:
        """Get confidence of best match, or 0.0 if no match."""
        return self.best_match.confidence if self.best_match else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "query_text": self.query_text,
            "query_norm": self.query_norm,
            "matched": self.matched,
            "best_match": self.best_match.to_dict() if self.best_match else None,
            "all_candidates": [m.to_dict() for m in self.all_candidates],
            "methods_used": [m.value for m in self.methods_used],
            "signals": self.signals,
            "disagreement_detected": self.disagreement_detected,
            "disagreement_penalty": self.disagreement_penalty,
            "cas_extracted": self.cas_extracted,
            "cas_extraction_attempted": self.cas_extraction_attempted,
            "processing_time_ms": self.processing_time_ms,
            "manual_review_recommended": self.manual_review_recommended,
            "review_reason": self.review_reason,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class EmbeddingConfig:
    """Configuration for semantic embedding model."""
    model_name: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384
    faiss_index_path: str = "data/embeddings/faiss_index.bin"
    vectors_path: str = "data/embeddings/synonym_vectors.npy"
    metadata_path: str = "data/embeddings/index_metadata.json"
    normalize_l2: bool = True


@dataclass
class MatcherConfig:
    """Configuration for matching thresholds and parameters."""
    # Exact matching
    exact_cas_enabled: bool = True
    exact_synonym_enabled: bool = True
    exact_inchikey_enabled: bool = True
    
    # Fuzzy matching
    fuzzy_enabled: bool = True
    fuzzy_threshold: float = 0.75
    fuzzy_top_k: int = 5
    fuzzy_high_confidence: float = 0.95
    fuzzy_medium_confidence: float = 0.85
    
    # Semantic matching
    semantic_enabled: bool = True
    semantic_threshold: float = 0.75
    semantic_top_k: int = 5
    semantic_high_confidence: float = 0.95
    semantic_medium_confidence: float = 0.85
    
    # Resolution engine
    disagreement_penalty: float = 0.1
    disagreement_threshold: float = 0.15  # If top fuzzy and semantic differ by > this
    manual_review_threshold: float = 0.80  # Flag for review if confidence < this
    
    # Performance
    max_processing_time_ms: float = 50.0
