"""
CRUD operations for Reg 153 Chemical Matcher database.

Provides comprehensive database operations including:
- Create, Read, Update, Delete for all models
- Bulk operations for synonyms and lab variants
- Specialized query helpers for chemical matching
- Transaction-safe batch operations
"""

from datetime import datetime, date as date_type
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy import select, func, or_, and_, desc
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .models import (
    Analyte,
    Synonym,
    LabVariant,
    LabVariantConfirmation,
    MatchDecision,
    EmbeddingsMetadata as EmbeddingMetadata,
    APIHarvestMetadata as ApiHarvestMetadata,
    SnapshotRegistry,
    AnalyteType,
)


# ============================================================================
# ANALYTE CRUD OPERATIONS
# ============================================================================

def create_analyte(
    session: Session,
    analyte_id: str,
    preferred_name: str,
    analyte_type: str,
    cas_number: Optional[str] = None,
    molecular_formula: Optional[str] = None,
    smiles: Optional[str] = None,
    inchi_key: Optional[str] = None,
    group_code: Optional[str] = None,
    table_number: Optional[int] = None,
    chemical_group: Optional[str] = None,
) -> Analyte:
    """
    Create a new analyte.
    
    Args:
        session: Database session
        analyte_id: Primary key (e.g. 'REG153_VOCS_001')
        preferred_name: Official analyte name
        analyte_type: One of: single_substance, fraction_or_group, suite, parameter
        cas_number: CAS registry number (can be None for fractions/groups)
        molecular_formula: Molecular formula
        smiles: SMILES string
        inchi_key: InChI key
        group_code: Group code
        table_number: Reg 153 table number
        chemical_group: Chemical group
    
    Returns:
        Created Analyte instance
    """
    # Convert string to AnalyteType enum if needed
    if isinstance(analyte_type, str):
        analyte_type = AnalyteType(analyte_type)
    
    analyte = Analyte(
        analyte_id=analyte_id,
        cas_number=cas_number,
        preferred_name=preferred_name,
        analyte_type=analyte_type,
        molecular_formula=molecular_formula,
        smiles=smiles,
        inchi_key=inchi_key,
        group_code=group_code,
        table_number=table_number,
        chemical_group=chemical_group,
    )
    session.add(analyte)
    session.flush()
    return analyte


def get_analyte_by_id(session: Session, analyte_id: str) -> Optional[Analyte]:
    """Get analyte by primary key (analyte_id string)."""
    return session.get(Analyte, analyte_id)


def get_analyte_by_cas(session: Session, cas_number: str) -> Optional[Analyte]:
    """Get analyte by CAS number."""
    return session.execute(
        select(Analyte).where(Analyte.cas_number == cas_number)
    ).scalar_one_or_none()


def get_analyte_by_name(session: Session, name: str) -> Optional[Analyte]:
    """Get analyte by exact preferred name match."""
    return session.execute(
        select(Analyte).where(Analyte.preferred_name == name)
    ).scalar_one_or_none()


def search_analytes_by_name(
    session: Session,
    query: str,
    limit: int = 20
) -> List[Analyte]:
    """
    Search analytes by partial name match (case-insensitive).
    
    Args:
        session: Database session
        query: Search query
        limit: Maximum number of results
    
    Returns:
        List of matching analytes
    """
    search_pattern = f"%{query}%"
    return session.execute(
        select(Analyte)
        .where(
            Analyte.preferred_name.ilike(search_pattern)
        )
        .limit(limit)
    ).scalars().all()


def list_analytes(
    session: Session,
    analyte_type: Optional[str] = None,
    offset: int = 0,
    limit: int = 100
) -> List[Analyte]:
    """
    List analytes with optional filtering.
    
    Args:
        session: Database session
        analyte_type: Filter by analyte type
        offset: Number of records to skip
        limit: Maximum number of records to return
    
    Returns:
        List of analytes
    """
    stmt = select(Analyte)
    
    if analyte_type:
        stmt = stmt.where(Analyte.analyte_type == analyte_type)
    
    stmt = stmt.offset(offset).limit(limit).order_by(Analyte.preferred_name)
    
    return session.execute(stmt).scalars().all()


def update_analyte(
    session: Session,
    analyte_id: str,
    **kwargs
) -> Optional[Analyte]:
    """
    Update analyte fields.
    
    Args:
        session: Database session
        analyte_id: ID of analyte to update
        **kwargs: Fields to update
    
    Returns:
        Updated Analyte instance or None if not found
    """
    analyte = session.get(Analyte, analyte_id)
    if not analyte:
        return None
    
    for key, value in kwargs.items():
        if hasattr(analyte, key):
            setattr(analyte, key, value)
    
    analyte.updated_at = datetime.utcnow()
    session.flush()
    return analyte


def delete_analyte(session: Session, analyte_id: str) -> bool:
    """
    Delete an analyte and all related records (cascade).
    
    Args:
        session: Database session
        analyte_id: Primary key string (e.g. 'REG153_VOCS_001')
    
    Returns:
        True if deleted, False if not found
    """
    analyte = session.get(Analyte, analyte_id)
    if not analyte:
        return False
    
    session.delete(analyte)
    session.flush()
    return True


def count_analytes(session: Session, analyte_type: Optional[str] = None) -> int:
    """Count total analytes, optionally filtered by type."""
    stmt = select(func.count(Analyte.analyte_id))
    if analyte_type:
        # Convert string to AnalyteType enum if needed
        if isinstance(analyte_type, str):
            analyte_type = AnalyteType(analyte_type)
        stmt = stmt.where(Analyte.analyte_type == analyte_type)
    return session.execute(stmt).scalar_one()


# ============================================================================
# SYNONYM CRUD OPERATIONS
# ============================================================================

def create_synonym(
    session: Session,
    analyte_id: str,
    synonym_raw: str,
    synonym_norm: str,
    harvest_source: str,
    confidence: float = 1.0,
    synonym_type: str = "common",
) -> Synonym:
    """
    Create a new synonym.
    
    Args:
        session: Database session
        analyte_id: Foreign key to analyte (string PK)
        synonym_raw: Raw synonym text
        synonym_norm: Normalized synonym text
        harvest_source: Source of synonym (e.g., 'pubchem', 'manual')
        confidence: Confidence score (0.0 to 1.0)
        synonym_type: Type of synonym (iupac, common, abbreviation, etc.)
    
    Returns:
        Created Synonym instance
    """
    from .models import SynonymType as ST
    # Convert string to SynonymType enum if needed
    if isinstance(synonym_type, str):
        synonym_type = ST(synonym_type)
    
    synonym = Synonym(
        analyte_id=analyte_id,
        synonym_raw=synonym_raw,
        synonym_norm=synonym_norm,
        synonym_type=synonym_type,
        harvest_source=harvest_source,
        confidence=confidence,
    )
    session.add(synonym)
    session.flush()
    return synonym


def bulk_insert_synonyms(
    session: Session,
    synonyms: List[Dict[str, Any]],
    chunk_size: int = 1000
) -> int:
    """
    Bulk insert synonyms for performance.
    
    Args:
        session: Database session
        synonyms: List of synonym dictionaries
        chunk_size: Number of records per chunk
    
    Returns:
        Number of synonyms inserted
    """
    total_inserted = 0
    
    for i in range(0, len(synonyms), chunk_size):
        chunk = synonyms[i:i + chunk_size]
        session.bulk_insert_mappings(Synonym, chunk)
        session.flush()
        total_inserted += len(chunk)
    
    return total_inserted


def synonym_exists(
    session: Session,
    analyte_id: int,
    synonym_norm: str
) -> bool:
    """
    Check if a normalized synonym already exists for an analyte.
    
    Args:
        session: Database session
        analyte_id: Analyte ID
        synonym_norm: Normalized synonym text
    
    Returns:
        True if synonym exists, False otherwise
    """
    exists = session.execute(
        select(func.count(Synonym.id))
        .where(
            and_(
                Synonym.analyte_id == analyte_id,
                Synonym.synonym_norm == synonym_norm
            )
        )
    ).scalar_one()
    return exists > 0


def get_synonyms_for_analyte(
    session: Session,
    analyte_id: int,
    min_confidence: float = 0.0
) -> List[Synonym]:
    """
    Get all synonyms for an analyte.
    
    Args:
        session: Database session
        analyte_id: Analyte ID
        min_confidence: Minimum confidence threshold
    
    Returns:
        List of synonyms
    """
    return session.execute(
        select(Synonym)
        .where(
            and_(
                Synonym.analyte_id == analyte_id,
                Synonym.confidence >= min_confidence
            )
        )
        .order_by(desc(Synonym.confidence))
    ).scalars().all()


def search_synonyms(
    session: Session,
    query: str,
    exact: bool = True,
    limit: int = 20
) -> List[Synonym]:
    """
    Search synonyms by normalized text.
    
    Args:
        session: Database session
        query: Search query
        exact: If True, exact match; if False, partial match
        limit: Maximum number of results
    
    Returns:
        List of matching synonyms
    """
    if exact:
        stmt = select(Synonym).where(Synonym.synonym_norm == query)
    else:
        search_pattern = f"%{query}%"
        stmt = select(Synonym).where(Synonym.synonym_norm.ilike(search_pattern))
    
    stmt = stmt.limit(limit)
    return session.execute(stmt).scalars().all()


def delete_synonyms_by_source(
    session: Session,
    harvest_source: str,
    analyte_id: Optional[int] = None
) -> int:
    """
    Delete synonyms by harvest source.
    
    Args:
        session: Database session
        harvest_source: Source to delete
        analyte_id: Optional analyte ID to limit deletion
    
    Returns:
        Number of synonyms deleted
    """
    stmt = select(Synonym).where(Synonym.harvest_source == harvest_source)
    if analyte_id:
        stmt = stmt.where(Synonym.analyte_id == analyte_id)
    
    synonyms = session.execute(stmt).scalars().all()
    count = len(synonyms)
    
    for synonym in synonyms:
        session.delete(synonym)
    
    session.flush()
    return count


# ============================================================================
# LAB VARIANT CRUD OPERATIONS
# ============================================================================

def create_lab_variant(
    session: Session,
    validated_match_id: str,
    lab_vendor: str,
    observed_text: str,
    method: Optional[str] = None,
    matrix: Optional[str] = None,
    units: Optional[str] = None,
    frequency_count: int = 1,
) -> LabVariant:
    """
    Create a new lab variant.
    
    Args:
        session: Database session
        validated_match_id: FK to analytes.analyte_id
        lab_vendor: Lab vendor name
        observed_text: How the lab reports this analyte
        method: Analytical method
        matrix: Sample matrix
        units: Measurement units
        frequency_count: Number of times observed
    
    Returns:
        Created LabVariant instance
    """
    from datetime import date as date_type
    today = date_type.today()
    lab_variant = LabVariant(
        validated_match_id=validated_match_id,
        lab_vendor=lab_vendor,
        observed_text=observed_text,
        method=method,
        matrix=matrix,
        units=units,
        frequency_count=frequency_count,
        first_seen_date=today,
        last_seen_date=today,
    )
    session.add(lab_variant)
    session.flush()
    return lab_variant


def get_lab_variants_by_vendor(
    session: Session,
    lab_vendor: str,
    limit: int = 100
) -> List[LabVariant]:
    """Get all lab variants for a specific vendor."""
    return session.execute(
        select(LabVariant)
        .where(LabVariant.lab_vendor == lab_vendor)
        .order_by(desc(LabVariant.frequency_count))
        .limit(limit)
    ).scalars().all()


def increment_lab_variant_frequency(
    session: Session,
    variant_id: int
) -> Optional[LabVariant]:
    """
    Increment frequency counter and update last_seen_date.
    
    Args:
        session: Database session
        variant_id: Lab variant ID
    
    Returns:
        Updated LabVariant or None if not found
    """
    from datetime import date as date_type
    variant = session.get(LabVariant, variant_id)
    if not variant:
        return None
    
    variant.frequency_count += 1
    variant.last_seen_date = date_type.today()
    session.flush()
    return variant


def search_lab_variants(
    session: Session,
    observed_text: str,
    lab_vendor: Optional[str] = None,
    limit: int = 20
) -> List[LabVariant]:
    """
    Search lab variants by observed text.
    
    Args:
        session: Database session
        observed_text: Search query for observed text
        lab_vendor: Optional vendor filter
        limit: Maximum number of results
    
    Returns:
        List of matching lab variants
    """
    search_pattern = f"%{observed_text}%"
    stmt = select(LabVariant).where(LabVariant.observed_text.ilike(search_pattern))
    
    if lab_vendor:
        stmt = stmt.where(LabVariant.lab_vendor == lab_vendor)
    
    stmt = stmt.order_by(desc(LabVariant.frequency_count)).limit(limit)
    return session.execute(stmt).scalars().all()


# ============================================================================
# VENDOR CACHE CRUD OPERATIONS (Confirmation / Collision / Rate-limit)
# ============================================================================

def get_or_create_lab_variant(
    session: Session,
    lab_vendor: str,
    observed_text: str,
    validated_match_id: Optional[int] = None,
    confidence: Optional[float] = None,
) -> Tuple[LabVariant, bool]:
    """
    Get existing or create new LabVariant (UNIQUE on vendor+observed_text).
    
    Returns:
        (variant, created) where created is True if newly inserted.
    """
    variant = session.execute(
        select(LabVariant).where(
            LabVariant.lab_vendor == lab_vendor,
            LabVariant.observed_text == observed_text
        )
    ).scalar_one_or_none()
    
    if variant is not None:
        return variant, False
    
    variant = LabVariant(
        lab_vendor=lab_vendor,
        observed_text=observed_text,
        validated_match_id=validated_match_id,
        confidence=confidence,
        frequency_count=1,
        first_seen_date=date_type.today(),
        last_seen_date=date_type.today(),
        collision_count=0,
        normalization_version=1,
    )
    session.add(variant)
    session.flush()
    return variant, True


def create_lab_variant_confirmation(
    session: Session,
    variant_id: int,
    submission_id: str,
    confirmed_analyte_id: int,
) -> Optional[LabVariantConfirmation]:
    """
    Record a confirmation for a lab variant from a specific submission.
    
    Skips duplicate (variant_id, submission_id) pairs silently.
    
    Returns:
        LabVariantConfirmation or None if duplicate.
    """
    try:
        confirmation = LabVariantConfirmation(
            variant_id=variant_id,
            submission_id=submission_id,
            confirmed_analyte_id=confirmed_analyte_id,
        )
        session.add(confirmation)
        session.flush()
        return confirmation
    except IntegrityError:
        session.rollback()
        return None


def get_distinct_confirmation_count(
    session: Session,
    variant_id: int,
) -> int:
    """
    Count distinct submissions that confirmed this variant (valid_for_consensus only).
    """
    return session.execute(
        select(func.count(func.distinct(LabVariantConfirmation.submission_id))).where(
            LabVariantConfirmation.variant_id == variant_id,
            LabVariantConfirmation.valid_for_consensus == True  # noqa: E712
        )
    ).scalar() or 0


def check_variant_collision(
    session: Session,
    variant_id: int,
    proposed_analyte_id: int,
) -> bool:
    """
    Check if the proposed analyte conflicts with existing consensus.
    
    A collision exists when there are valid confirmations for a DIFFERENT
    analyte_id than the one being proposed.
    
    Returns:
        True if collision detected.
    """
    existing = session.execute(
        select(LabVariantConfirmation.confirmed_analyte_id).where(
            LabVariantConfirmation.variant_id == variant_id,
            LabVariantConfirmation.valid_for_consensus == True,  # noqa: E712
            LabVariantConfirmation.confirmed_analyte_id != proposed_analyte_id
        ).limit(1)
    ).scalar_one_or_none()
    return existing is not None


def clear_conflicting_confirmations(
    session: Session,
    variant_id: int,
    keep_analyte_id: int,
) -> int:
    """
    Soft-delete confirmations that disagree with the kept analyte.
    
    Sets valid_for_consensus=False on conflicting rows for audit trail.
    
    Returns:
        Number of rows invalidated.
    """
    # Get conflicting confirmations
    conflicts = session.execute(
        select(LabVariantConfirmation).where(
            LabVariantConfirmation.variant_id == variant_id,
            LabVariantConfirmation.valid_for_consensus == True,  # noqa: E712
            LabVariantConfirmation.confirmed_analyte_id != keep_analyte_id
        )
    ).scalars().all()
    
    count = 0
    for conf in conflicts:
        conf.valid_for_consensus = False
        count += 1
    
    if count > 0:
        session.flush()
    return count


def count_todays_hard_promotions(
    session: Session,
    lab_vendor: str,
) -> int:
    """
    Count vendor cache hard promotions today (rate limiter).
    
    A 'hard promotion' is a variant that crossed min_confirmations today.
    Approximated by counting confirmations created today for this vendor.
    """
    today = date_type.today()
    return session.execute(
        select(func.count(LabVariantConfirmation.id)).where(
            LabVariantConfirmation.confirmed_at >= datetime(today.year, today.month, today.day),
            LabVariantConfirmation.variant_id.in_(
                select(LabVariant.id).where(LabVariant.lab_vendor == lab_vendor)
            )
        )
    ).scalar() or 0


def count_todays_global_synonyms(
    session: Session,
    lab_vendor: Optional[str] = None,
) -> int:
    """
    Count global synonyms promoted from validated_runtime today.
    
    Used to enforce daily cap on synonym ingestion.
    """
    today_prefix = f"validated_runtime:{lab_vendor}" if lab_vendor else "validated_runtime%"
    return session.execute(
        select(func.count(Synonym.synonym_id)).where(
            Synonym.harvest_source.like(today_prefix if lab_vendor else "validated_runtime%"),
            Synonym.created_at >= datetime(date_type.today().year, date_type.today().month, date_type.today().day)
        )
    ).scalar() or 0


# ============================================================================
# MATCH DECISION CRUD OPERATIONS
# ============================================================================

def create_match_decision(
    session: Session,
    input_text: str,
    matched_analyte_id: Optional[str],
    match_method: str,
    confidence_score: float,
    top_k_candidates: Dict[str, Any],
    signals_used: Dict[str, Any],
    corpus_snapshot_hash: str,
    model_hash: str,
    disagreement_flag: bool = False,
    # Legacy kwargs (accepted but ignored for backward compatibility)
    **kwargs,
) -> MatchDecision:
    """
    Record a match decision for audit trail.
    
    Args:
        session: Database session
        input_text: Original input text
        matched_analyte_id: Matched analyte ID (None if no confident match)
        match_method: Method used (exact, fuzzy, semantic, cas_extracted, hybrid)
        confidence_score: Overall confidence score
        top_k_candidates: JSON of top-k candidate matches
        signals_used: JSON of signal contributions
        corpus_snapshot_hash: Version hash of corpus used
        model_hash: Version hash of model used
        disagreement_flag: True if signals disagree
    
    Returns:
        Created MatchDecision instance
    """
    decision = MatchDecision(
        input_text=input_text,
        matched_analyte_id=matched_analyte_id,
        match_method=match_method,
        confidence_score=confidence_score,
        top_k_candidates=top_k_candidates,
        signals_used=signals_used,
        corpus_snapshot_hash=corpus_snapshot_hash,
        model_hash=model_hash,
        disagreement_flag=disagreement_flag,
    )
    session.add(decision)
    session.flush()
    return decision


def get_decisions_for_review(
    session: Session,
    disagreement_only: bool = True,
    not_reviewed: bool = True,
    limit: int = 50
) -> List[MatchDecision]:
    """
    Get match decisions that need review.
    
    Args:
        session: Database session
        disagreement_only: Only return decisions with disagreement flag
        not_reviewed: Only return decisions not yet manually reviewed
        limit: Maximum number of results
    
    Returns:
        List of match decisions needing review
    """
    stmt = select(MatchDecision)
    
    if disagreement_only:
        stmt = stmt.where(MatchDecision.disagreement_flag == True)
    
    if not_reviewed:
        stmt = stmt.where(MatchDecision.manual_review == False)
    
    stmt = stmt.order_by(desc(MatchDecision.created_at)).limit(limit)
    return session.execute(stmt).scalars().all()


def mark_decision_reviewed(
    session: Session,
    decision_id: int,
    reviewed_by: str,
    review_notes: Optional[str] = None
) -> Optional[MatchDecision]:
    """
    Mark a match decision as manually reviewed.
    
    Args:
        session: Database session
        decision_id: Match decision ID
        reviewed_by: Username/identifier of reviewer
        review_notes: Optional review notes
    
    Returns:
        Updated MatchDecision or None if not found
    """
    decision = session.get(MatchDecision, decision_id)
    if not decision:
        return None
    
    decision.manual_review = True
    decision.reviewed_by = reviewed_by
    decision.review_notes = review_notes
    session.flush()
    return decision


def get_match_statistics(
    session: Session,
    corpus_snapshot_hash: Optional[str] = None,
    model_hash: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate match statistics.
    
    Args:
        session: Database session
        corpus_snapshot_hash: Optional filter by corpus version
        model_hash: Optional filter by model version
    
    Returns:
        Dictionary of statistics
    """
    stmt = select(MatchDecision)
    
    if corpus_snapshot_hash:
        stmt = stmt.where(MatchDecision.corpus_snapshot_hash == corpus_snapshot_hash)
    if model_hash:
        stmt = stmt.where(MatchDecision.model_hash == model_hash)
    
    decisions = session.execute(stmt).scalars().all()
    
    if not decisions:
        return {
            "total_decisions": 0,
            "avg_confidence": 0.0,
            "disagreement_rate": 0.0,
            "review_rate": 0.0,
        }
    
    total = len(decisions)
    avg_conf = sum(d.confidence_score for d in decisions) / total
    disagreements = sum(1 for d in decisions if d.disagreement_flag)
    reviewed = sum(1 for d in decisions if d.manual_review)
    
    return {
        "total_decisions": total,
        "avg_confidence": avg_conf,
        "disagreement_rate": disagreements / total,
        "review_rate": reviewed / total,
    }


# ============================================================================
# EMBEDDING METADATA CRUD OPERATIONS
# ============================================================================

def create_embedding_metadata(
    session: Session,
    text_embedded: str,
    model_name: str,
    model_version: str,
    embedding_dim: int,
    file_path: str,
    vector_index: int,
    analyte_id: Optional[int] = None,
    synonym_id: Optional[int] = None,
) -> EmbeddingMetadata:
    """
    Create embedding metadata record.
    
    Args:
        session: Database session
        text_embedded: Text that was embedded
        model_name: Embedding model name
        model_version: Model version
        embedding_dim: Dimension of embedding vector
        file_path: Relative path to embedding file
        vector_index: Index in FAISS or array
        analyte_id: Optional analyte ID
        synonym_id: Optional synonym ID
    
    Returns:
        Created EmbeddingMetadata instance
    """
    metadata = EmbeddingMetadata(
        analyte_id=analyte_id,
        synonym_id=synonym_id,
        text_embedded=text_embedded,
        model_name=model_name,
        model_version=model_version,
        embedding_dim=embedding_dim,
        file_path=file_path,
        vector_index=vector_index,
    )
    session.add(metadata)
    session.flush()
    return metadata


def get_embeddings_by_model(
    session: Session,
    model_name: str,
    model_version: Optional[str] = None
) -> List[EmbeddingMetadata]:
    """
    Get all embeddings for a specific model.
    
    Args:
        session: Database session
        model_name: Embedding model name
        model_version: Optional specific version
    
    Returns:
        List of embedding metadata records
    """
    stmt = select(EmbeddingMetadata).where(EmbeddingMetadata.model_name == model_name)
    
    if model_version:
        stmt = stmt.where(EmbeddingMetadata.model_version == model_version)
    
    return session.execute(stmt).scalars().all()


# ============================================================================
# API HARVEST METADATA CRUD OPERATIONS
# ============================================================================

def create_api_harvest_record(
    session: Session,
    api_source: str,
    api_endpoint: str,
    query_type: str,
    query_params: Dict[str, Any],
    status_code: int,
    success: bool,
    synonyms_harvested: int = 0,
    analyte_id: Optional[int] = None,
    error_message: Optional[str] = None,
    response_time_ms: Optional[int] = None,
    rate_limited: bool = False,
) -> ApiHarvestMetadata:
    """
    Record an API harvest attempt.
    
    Args:
        session: Database session
        api_source: API source identifier
        api_endpoint: API endpoint URL
        query_type: Type of query
        query_params: Query parameters as JSON
        status_code: HTTP status code
        success: Whether harvest was successful
        synonyms_harvested: Number of synonyms collected
        analyte_id: Optional analyte ID
        error_message: Optional error message
        response_time_ms: Response time in milliseconds
        rate_limited: Whether request was rate limited
    
    Returns:
        Created ApiHarvestMetadata instance
    """
    record = ApiHarvestMetadata(
        api_source=api_source,
        api_endpoint=api_endpoint,
        query_type=query_type,
        query_params=query_params,
        analyte_id=analyte_id,
        status_code=status_code,
        success=success,
        synonyms_harvested=synonyms_harvested,
        error_message=error_message,
        response_time_ms=response_time_ms,
        rate_limited=rate_limited,
    )
    session.add(record)
    session.flush()
    return record


def get_harvest_statistics_by_source(
    session: Session,
    api_source: str
) -> Dict[str, Any]:
    """
    Get harvest statistics for an API source.
    
    Args:
        session: Database session
        api_source: API source identifier
    
    Returns:
        Dictionary of harvest statistics
    """
    records = session.execute(
        select(ApiHarvestMetadata).where(ApiHarvestMetadata.api_source == api_source)
    ).scalars().all()
    
    if not records:
        return {
            "total_requests": 0,
            "success_rate": 0.0,
            "total_synonyms": 0,
            "avg_response_time_ms": 0.0,
            "rate_limited_count": 0,
        }
    
    total = len(records)
    successes = sum(1 for r in records if r.success)
    total_synonyms = sum(r.synonyms_harvested for r in records)
    response_times = [r.response_time_ms for r in records if r.response_time_ms]
    rate_limited = sum(1 for r in records if r.rate_limited)
    
    return {
        "total_requests": total,
        "success_rate": successes / total if total > 0 else 0.0,
        "total_synonyms": total_synonyms,
        "avg_response_time_ms": sum(response_times) / len(response_times) if response_times else 0.0,
        "rate_limited_count": rate_limited,
    }


# ============================================================================
# SNAPSHOT REGISTRY CRUD OPERATIONS
# ============================================================================

def create_snapshot(
    session: Session,
    snapshot_hash: str,
    snapshot_type: str,
    version_tag: str,
    file_path: str,
    file_size_bytes: int,
    description: Optional[str] = None,
    analyte_count: Optional[int] = None,
    synonym_count: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    is_active: bool = True,
) -> SnapshotRegistry:
    """
    Register a new snapshot.
    
    Args:
        session: Database session
        snapshot_hash: SHA256 hash of snapshot
        snapshot_type: One of: corpus, model, embeddings
        version_tag: Human-readable version tag
        file_path: Relative path to snapshot file
        file_size_bytes: Size of snapshot file
        description: Optional description
        analyte_count: Number of analytes in snapshot
        synonym_count: Number of synonyms in snapshot
        metadata: Additional metadata as JSON
        is_active: Whether snapshot is active
    
    Returns:
        Created SnapshotRegistry instance
    """
    snapshot = SnapshotRegistry(
        snapshot_hash=snapshot_hash,
        snapshot_type=snapshot_type,
        version_tag=version_tag,
        description=description,
        file_path=file_path,
        file_size_bytes=file_size_bytes,
        analyte_count=analyte_count,
        synonym_count=synonym_count,
        metadata=metadata,
        is_active=is_active,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def get_active_snapshot(
    session: Session,
    snapshot_type: str
) -> Optional[SnapshotRegistry]:
    """
    Get the currently active snapshot of a given type.
    
    Args:
        session: Database session
        snapshot_type: Type of snapshot
    
    Returns:
        Active SnapshotRegistry or None
    """
    return session.execute(
        select(SnapshotRegistry)
        .where(
            and_(
                SnapshotRegistry.snapshot_type == snapshot_type,
                SnapshotRegistry.is_active == True
            )
        )
        .order_by(desc(SnapshotRegistry.created_at))
        .limit(1)
    ).scalar_one_or_none()


def deactivate_snapshots(
    session: Session,
    snapshot_type: str
) -> int:
    """
    Deactivate all snapshots of a given type.
    
    Args:
        session: Database session
        snapshot_type: Type of snapshot
    
    Returns:
        Number of snapshots deactivated
    """
    snapshots = session.execute(
        select(SnapshotRegistry)
        .where(
            and_(
                SnapshotRegistry.snapshot_type == snapshot_type,
                SnapshotRegistry.is_active == True
            )
        )
    ).scalars().all()
    
    for snapshot in snapshots:
        snapshot.is_active = False
    
    session.flush()
    return len(snapshots)


# ============================================================================
# SPECIALIZED QUERY HELPERS
# ============================================================================

def get_nearest_analyte(
    session: Session,
    query: str,
    threshold: float = 0.7
) -> Tuple[Optional[Analyte], float]:
    """
    Find the nearest analyte match using simple heuristics.
    
    This is a simple implementation. In production, use embedding-based
    similarity or more sophisticated matching.
    
    Args:
        session: Database session
        query: Query string
        threshold: Minimum similarity threshold
    
    Returns:
        Tuple of (matched Analyte or None, similarity score)
    """
    # First try exact match on preferred name
    analyte = get_analyte_by_name(session, query)
    if analyte:
        return analyte, 1.0
    
    # Try exact match on synonyms
    synonyms = search_synonyms(session, query, exact=True, limit=1)
    if synonyms:
        return synonyms[0].analyte, 1.0
    
    # Try partial matches (simplified - in production use Levenshtein distance)
    analytes = search_analytes_by_name(session, query, limit=5)
    if analytes:
        # Return first match with a lower confidence
        return analytes[0], 0.8
    
    return None, 0.0


def get_all_synonyms_for_corpus(
    session: Session,
    min_confidence: float = 0.5
) -> List[Tuple[int, str, str]]:
    """
    Get all synonyms for building a matching corpus.
    
    Args:
        session: Database session
        min_confidence: Minimum confidence threshold
    
    Returns:
        List of tuples: (analyte_id, synonym_norm, analyte_name)
    """
    results = session.execute(
        select(
            Synonym.analyte_id,
            Synonym.synonym_norm,
            Analyte.preferred_name
        )
        .join(Analyte, Synonym.analyte_id == Analyte.analyte_id)
        .where(Synonym.confidence >= min_confidence)
        .order_by(Synonym.analyte_id)
    ).all()
    
    return [(r[0], r[1], r[2]) for r in results]
