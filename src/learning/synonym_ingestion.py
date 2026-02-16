"""
Synonym ingestion for Layer 1 learning.

Enables immediate vocabulary expansion by adding validated synonyms
to the database without requiring model retraining.
"""

import logging
from typing import Optional
from datetime import date as date_type
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..database.models import Synonym, SynonymType
from ..normalization.text_normalizer import TextNormalizer, NORMALIZATION_VERSION

logger = logging.getLogger(__name__)


class SynonymIngestor:
    """
    Handles ingestion of validated synonyms into the database.
    
    This enables Layer 1 learning: immediate vocabulary expansion
    without model retraining. Validated runtime decisions are added
    directly to the synonym table for future exact matching.
    """
    
    def __init__(self):
        """Initialize the synonym ingestor with text normalizer."""
        self.normalizer = TextNormalizer()
        logger.info("SynonymIngestor initialized")
    
    def ingest_validated_synonym(
        self,
        raw_text: str,
        analyte_id: str,
        db_session: Session,
        confidence: float = 1.0,
        synonym_type: SynonymType = SynonymType.LAB_VARIANT,
        lab_vendor: Optional[str] = None,
        cascade_confirmed: bool = False,
        cascade_margin: float = 0.0,
        dual_gate_margin: float = 0.06,
        max_global_synonyms_per_day: int = 20,
    ) -> bool:
        """
        Ingest a validated synonym into the database.
        
        Enforces a strict dual-confirmation gate: a global synonym is only
        created when cascade independently confirmed the match (not just
        vendor cache) AND the cascade margin exceeds the dual-gate threshold.
        Also enforces a daily cap on global synonym creation to maintain
        structural memory inertia.
        
        Args:
            raw_text: Raw text form of the synonym
            analyte_id: ID of the analyte this synonym maps to
            db_session: Database session
            confidence: Confidence score (default 1.0 for validated)
            synonym_type: Type classification of the synonym
            lab_vendor: Source lab vendor (None for non-vendor contexts)
            cascade_confirmed: True if cascade (not vendor cache) independently matched
            cascade_margin: Margin (s1-s2) from the cascade resolution
            dual_gate_margin: Minimum margin for global synonym creation
            max_global_synonyms_per_day: Daily cap on validated_runtime synonyms
        
        Returns:
            True if new synonym was added, False if skipped (duplicate, gate, or cap)
        
        Raises:
            ValueError: If confidence is out of range [0, 1]
        """
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {confidence}")
        
        # Normalize the raw text
        norm_text = self.normalizer.normalize(raw_text)
        
        logger.debug(f"Ingesting synonym: '{raw_text}' -> '{norm_text}' for {analyte_id}")
        
        # Dual-confirmation gate: require cascade agreement + sufficient margin
        # Global alias graph is high-inertia structural memory.
        # Vendor-local truth must not pollute global graph.
        if not cascade_confirmed:
            logger.info(
                f"Dual gate blocked synonym '{norm_text}': cascade did not independently confirm "
                f"(vendor cache bypass only)"
            )
            return False
        
        if cascade_margin < dual_gate_margin:
            logger.info(
                f"Dual gate blocked synonym '{norm_text}': cascade margin {cascade_margin:.3f} "
                f"< dual_gate_margin {dual_gate_margin:.3f}"
            )
            return False
        
        # Global synonym daily rate cap — structural velocity bound
        todays_count = db_session.execute(
            select(func.count(Synonym.id)).where(
                Synonym.harvest_source.like('validated_runtime%'),
                Synonym.created_at >= func.date('now')
            )
        ).scalar_one()
        
        if todays_count >= max_global_synonyms_per_day:
            logger.warning(
                f"Global synonym daily cap reached ({max_global_synonyms_per_day}). "
                f"Blocked: '{norm_text}' -> {analyte_id}"
            )
            return False
        
        # Check for duplicates
        if self.check_duplicate(norm_text, analyte_id, db_session):
            logger.info(f"Duplicate synonym detected, skipping: '{norm_text}' for {analyte_id}")
            return False
        
        # Insert new synonym
        try:
            # Determine harvest source with vendor tag
            if lab_vendor:
                harvest_source = f"validated_runtime:{lab_vendor}"
            else:
                harvest_source = "validated_runtime"
            
            new_synonym = Synonym(
                analyte_id=analyte_id,
                synonym_raw=raw_text,
                synonym_norm=norm_text,
                synonym_type=synonym_type,
                harvest_source=harvest_source,
                confidence=confidence,
                lab_vendor=lab_vendor,
                normalization_version=NORMALIZATION_VERSION,
            )
            
            db_session.add(new_synonym)
            db_session.commit()
            
            logger.info(
                f"Successfully ingested synonym: '{raw_text}' -> {analyte_id} "
                f"(confidence={confidence:.2f})"
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest synonym '{raw_text}': {e}")
            db_session.rollback()
            raise
    
    def check_duplicate(
        self,
        norm_text: str,
        analyte_id: str,
        db_session: Session
    ) -> bool:
        """
        Check if a synonym already exists for the given analyte.
        
        Args:
            norm_text: Normalized text of the synonym
            analyte_id: ID of the analyte
            db_session: Database session
        
        Returns:
            True if synonym already exists, False otherwise
        """
        stmt = select(Synonym).where(
            Synonym.synonym_norm == norm_text,
            Synonym.analyte_id == analyte_id
        )
        
        result = db_session.execute(stmt).first()
        return result is not None
    
    def bulk_ingest(
        self,
        synonym_list: list[tuple[str, str]],
        db_session: Session,
        confidence: float = 1.0,
        synonym_type: SynonymType = SynonymType.LAB_VARIANT,
        lab_vendor: Optional[str] = None,
        cascade_confirmed: bool = False,
        cascade_margin: float = 0.0,
    ) -> dict[str, int]:
        """
        Ingest multiple synonyms in bulk.
        
        Args:
            synonym_list: List of (raw_text, analyte_id) tuples
            db_session: Database session
            confidence: Confidence score for all synonyms
            synonym_type: Type classification for all synonyms
            lab_vendor: Source lab vendor
            cascade_confirmed: Whether cascade independently confirmed
            cascade_margin: Margin from cascade resolution
        
        Returns:
            Dictionary with statistics: {'added': count, 'duplicates': count, 'errors': count}
        """
        stats = {'added': 0, 'duplicates': 0, 'errors': 0}
        
        logger.info(f"Starting bulk ingest of {len(synonym_list)} synonyms")
        
        for raw_text, analyte_id in synonym_list:
            try:
                if self.ingest_validated_synonym(
                    raw_text, analyte_id, db_session, confidence, synonym_type,
                    lab_vendor=lab_vendor,
                    cascade_confirmed=cascade_confirmed,
                    cascade_margin=cascade_margin,
                ):
                    stats['added'] += 1
                else:
                    stats['duplicates'] += 1
            except Exception as e:
                logger.error(f"Error ingesting '{raw_text}' -> {analyte_id}: {e}")
                stats['errors'] += 1
        
        logger.info(
            f"Bulk ingest complete: {stats['added']} added, "
            f"{stats['duplicates']} duplicates, {stats['errors']} errors"
        )
        
        return stats
    
    def get_ingestion_stats(self, db_session: Session) -> dict[str, int]:
        """
        Get statistics on runtime-validated synonyms.
        
        Args:
            db_session: Database session
        
        Returns:
            Dictionary with counts by type
        """
        stmt = select(Synonym).where(Synonym.harvest_source == "validated_runtime")
        result = db_session.execute(stmt).scalars().all()
        
        stats = {
            'total': len(result),
            'by_type': {}
        }
        
        for synonym in result:
            type_name = synonym.synonym_type.value
            stats['by_type'][type_name] = stats['by_type'].get(type_name, 0) + 1
        
        return stats
