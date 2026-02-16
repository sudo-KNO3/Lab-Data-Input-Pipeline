"""
CRUD operations for Reg 153 Chemical Matcher database.

Provides simplified database operations for:
- Analytes (canonical chemical truth)
- Synonyms (name variants)
- Lab variants (Ontario lab behavior)
- Match decisions (ML audit trail)
- Embeddings metadata
- API harvest metadata
- Snapshot registry
"""

from datetime import datetime, date
from typing import Optional, List, Dict, Any
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .models import (
    Analyte,
    Synonym,
    LabVariant,
    MatchDecision,
    EmbeddingsMetadata,
    APIHarvestMetadata,
    SnapshotRegistry,
    AnalyteType,
    SynonymType,
    ValidationConfidence,
)


# ============================================================================
# ANALYTE CRUD OPERATIONS
# ============================================================================

def insert_analyte(
    session: Session,
    analyte_id: str,
    preferred_name: str,
    analyte_type: AnalyteType,
    cas_number: Optional[str] = None,
    group_code: Optional[str] = None,
    table_number: Optional[int] = None,
    chemical_group: Optional[str] = None,
    smiles: Optional[str] = None,
    inchi_key: Optional[str] = None,
    molecular_formula: Optional[str] = None,
) -> Analyte:
    """
    Insert a new analyte into the database.
    
    Args:
        session: Database session
        analyte_id: Primary key in REG153_XXX format
        preferred_name: Canonical name
        analyte_type: Type enum (single_substance, fraction_or_group, suite, parameter)
        cas_number: CAS Registry Number (optional)
        group_code: PHC_F1, BTEX, etc. (optional)
        table_number: Reg 153 table 1-9 (optional)
        chemical_group: Metals, VOCs, PAHs, etc. (optional)
        smiles: SMILES notation (optional)
        inchi_key: InChI Key (optional)
        molecular_formula: Chemical formula (optional)
    
    Returns:
        Created Analyte instance
        
    Raises:
        IntegrityError: If analyte_id already exists
    """
    analyte = Analyte(
        analyte_id=analyte_id,
        preferred_name=preferred_name,
        analyte_type=analyte_type,
        cas_number=cas_number,
        group_code=group_code,
        table_number=table_number,
        chemical_group=chemical_group,
        smiles=smiles,
        inchi_key=inchi_key,
        molecular_formula=molecular_formula,
    )
    
    session.add(analyte)
    session.flush()
    return analyte


def get_analyte_by_id(session: Session, analyte_id: str) -> Optional[Analyte]:
    """
    Retrieve an analyte by its ID.
    
    Args:
        session: Database session
        analyte_id: Analyte primary key
    
    Returns:
        Analyte instance or None if not found
    """
    return session.get(Analyte, analyte_id)


def get_analyte_by_cas(session: Session, cas_number: str) -> Optional[Analyte]:
    """
    Retrieve an analyte by CAS number.
    
    Args:
        session: Database session
        cas_number: CAS Registry Number
    
    Returns:
        Analyte instance or None if not found
    """
    return session.execute(
        select(Analyte).where(Analyte.cas_number == cas_number)
    ).scalar_one_or_none()


def get_analyte_by_name(session: Session, preferred_name: str) -> Optional[Analyte]:
    """
    Retrieve an analyte by preferred name (case-insensitive).
    
    Args:
        session: Database session
        preferred_name: Preferred name to search for
    
    Returns:
        Analyte instance or None if not found
    """
    return session.execute(
        select(Analyte).where(func.lower(Analyte.preferred_name) == preferred_name.lower())
    ).scalar_one_or_none()


def get_all_analytes(session: Session) -> List[Analyte]:
    """
    Retrieve all analytes from the database.
    
    Args:
        session: Database session
    
    Returns:
        List of all Analyte instances
    """
    return session.execute(select(Analyte)).scalars().all()


def update_analyte(
    session: Session,
    analyte_id: str,
    **kwargs
) -> Optional[Analyte]:
    """
    Update analyte fields.
    
    Args:
        session: Database session
        analyte_id: Analyte primary key
        **kwargs: Fields to update
    
    Returns:
        Updated Analyte instance or None if not found
    """
    analyte = session.get(Analyte, analyte_id)
    if not analyte:
        return None
    
    for key, value in kwargs.items():
        if hasattr(analyte, key) and key not in ['analyte_id', 'created_at']:
            setattr(analyte, key, value)
    
    analyte.updated_at = datetime.utcnow()
    session.flush()
    return analyte


# ============================================================================
# SYNONYM CRUD OPERATIONS
# ============================================================================

def insert_synonym(
    session: Session,
    analyte_id: str,
    synonym_raw: str,
    synonym_norm: str,
    synonym_type: SynonymType,
    harvest_source: str,
    confidence: float = 1.0,
) -> Synonym:
    """
    Insert a single synonym.
    
    Args:
        session: Database session
        analyte_id: Foreign key to analyte
        synonym_raw: Original text
        synonym_norm: Normalized form (for matching)
        synonym_type: Type enum (iupac, common, abbreviation, etc.)
        harvest_source: Source (pubchem, comptox, cas, manual_validation, edd_observed)
        confidence: Confidence score 0.0-1.0 (default 1.0)
    
    Returns:
        Created Synonym instance
    """
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


def batch_insert_synonyms(
    session: Session,
    synonyms_data: List[Dict[str, Any]],
    chunk_size: int = 1000,
) -> int:
    """
    Batch insert multiple synonyms efficiently.
    
    Args:
        session: Database session
        synonyms_data: List of dictionaries with synonym fields
        chunk_size: Number of records per chunk
    
    Returns:
        Number of synonyms inserted
    
    Example:
        synonyms_data = [
            {
                'analyte_id': 'REG153_VOCS_001',
                'synonym_raw': 'Benzene',
                'synonym_norm': 'benzene',
                'synonym_type': SynonymType.COMMON,
                'harvest_source': 'pubchem',
                'confidence': 1.0,
            },
            ...
        ]
    """
    total_inserted = 0
    
    for i in range(0, len(synonyms_data), chunk_size):
        chunk = synonyms_data[i:i + chunk_size]
        session.bulk_insert_mappings(Synonym, chunk)
        session.flush()
        total_inserted += len(chunk)
    
    return total_inserted


def query_by_normalized_synonym(
    session: Session,
    normalized_text: str,
    min_confidence: float = 0.0,
) -> List[Synonym]:
    """
    Query synonyms by normalized text.
    
    This is the CORE MATCHING FUNCTION for exact lookups.
    
    Args:
        session: Database session
        normalized_text: Normalized query text
        min_confidence: Minimum confidence threshold
    
    Returns:
        List of matching synonyms (with analyte relationships loaded)
    """
    return session.execute(
        select(Synonym)
        .where(Synonym.synonym_norm == normalized_text)
        .where(Synonym.confidence >= min_confidence)
    ).scalars().all()


def get_synonyms_for_analyte(
    session: Session,
    analyte_id: str,
) -> List[Synonym]:
    """
    Get all synonyms for a specific analyte.
    
    Args:
        session: Database session
        analyte_id: Analyte primary key
    
    Returns:
        List of synonyms for the analyte
    """
    return session.execute(
        select(Synonym).where(Synonym.analyte_id == analyte_id)
    ).scalars().all()


# ============================================================================
# LAB VARIANT CRUD OPERATIONS
# ============================================================================

def insert_lab_variant(
    session: Session,
    observed_text: str,
    lab_vendor: Optional[str] = None,
    method: Optional[str] = None,
    matrix: Optional[str] = None,
    units: Optional[str] = None,
    raw_context: Optional[str] = None,
    frequency_count: int = 1,
    first_seen_date: Optional[date] = None,
    validated_match_id: Optional[str] = None,
    validation_confidence: Optional[ValidationConfidence] = None,
) -> LabVariant:
    """
    Insert a lab variant observation.
    
    Args:
        session: Database session
        observed_text: Text observed from lab
        lab_vendor: Lab name (ALS, SGS, Bureau_Veritas)
        method: Analytical method
        matrix: Sample matrix (soil, groundwater, sediment)
        units: Reporting units
        raw_context: Full column header or context
        frequency_count: Number of times observed
        first_seen_date: Date first observed
        validated_match_id: Matched analyte ID (if validated)
        validation_confidence: Confidence level (HIGH, MEDIUM, LOW, UNKNOWN)
    
    Returns:
        Created LabVariant instance
    """
    lab_variant = LabVariant(
        observed_text=observed_text,
        lab_vendor=lab_vendor,
        method=method,
        matrix=matrix,
        units=units,
        raw_context=raw_context,
        frequency_count=frequency_count,
        first_seen_date=first_seen_date or date.today(),
        validated_match_id=validated_match_id,
        validation_confidence=validation_confidence,
    )
    
    session.add(lab_variant)
    session.flush()
    return lab_variant


# ============================================================================
# MATCH DECISION CRUD OPERATIONS
# ============================================================================

def log_match_decision(
    session: Session,
    input_text: str,
    matched_analyte_id: Optional[str],
    match_method: str,
    confidence_score: float,
    top_k_candidates: List[Dict[str, Any]],
    signals_used: Dict[str, Any],
    corpus_snapshot_hash: str,
    model_hash: str,
    human_validated: bool = False,
    validation_notes: Optional[str] = None,
    disagreement_flag: bool = False,
) -> MatchDecision:
    """
    Log a match decision for audit and learning.
    
    Args:
        session: Database session
        input_text: Original query text
        matched_analyte_id: Matched analyte (None if no confident match)
        match_method: Method used (exact, fuzzy, semantic, cas_extracted, hybrid)
        confidence_score: Final confidence score 0.0-1.0
        top_k_candidates: List of top candidates with scores
        signals_used: Dictionary of signals (exact, fuzzy, semantic, cas)
        corpus_snapshot_hash: SHA256 of corpus version
        model_hash: SHA256 of model version
        human_validated: Whether human confirmed this match
        validation_notes: Human validation notes
        disagreement_flag: Whether ML and human disagree
    
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
        human_validated=human_validated,
        validation_notes=validation_notes,
        disagreement_flag=disagreement_flag,
    )
    
    session.add(decision)
    session.flush()
    return decision


def get_validated_decisions_since(
    session: Session,
    since_date: datetime,
    ingested_only: bool = False,
) -> List[MatchDecision]:
    """
    Get validated match decisions for learning loop ingestion.
    
    Args:
        session: Database session
        since_date: Get decisions since this timestamp
        ingested_only: If True, only return already-ingested decisions
    
    Returns:
        List of validated MatchDecision instances
    """
    stmt = select(MatchDecision).where(
        MatchDecision.human_validated == True,
        MatchDecision.decision_timestamp >= since_date,
    )
    
    if ingested_only:
        stmt = stmt.where(MatchDecision.ingested == True)
    else:
        stmt = stmt.where(MatchDecision.ingested == False)
    
    return session.execute(stmt).scalars().all()


def mark_decision_as_ingested(
    session: Session,
    decision_id: int,
) -> Optional[MatchDecision]:
    """
    Mark a match decision as ingested into the learning system.
    
    Args:
        session: Database session
        decision_id: MatchDecision ID
    
    Returns:
        Updated MatchDecision or None if not found
    """
    decision = session.get(MatchDecision, decision_id)
    if not decision:
        return None
    
    decision.ingested = True
    session.flush()
    return decision


# ============================================================================
# EMBEDDINGS METADATA CRUD OPERATIONS
# ============================================================================

def insert_embedding_metadata(
    session: Session,
    text_content: str,
    embedding_index: int,
    model_name: str,
    model_hash: str,
    analyte_id: Optional[str] = None,
    synonym_id: Optional[int] = None,
) -> EmbeddingsMetadata:
    """
    Insert embedding metadata record.
    
    Args:
        session: Database session
        text_content: Text that was embedded
        embedding_index: Index in .npy file or FAISS index
        model_name: Model name (e.g., 'all-MiniLM-L6-v2')
        model_hash: SHA256 of model weights
        analyte_id: Source analyte ID (XOR with synonym_id)
        synonym_id: Source synonym ID (XOR with analyte_id)
    
    Returns:
        Created EmbeddingsMetadata instance
        
    Raises:
        ValueError: If both or neither of analyte_id/synonym_id are provided
    """
    if (analyte_id is None) == (synonym_id is None):
        raise ValueError("Must provide exactly one of analyte_id or synonym_id")
    
    metadata = EmbeddingsMetadata(
        analyte_id=analyte_id,
        synonym_id=synonym_id,
        text_content=text_content,
        embedding_index=embedding_index,
        model_name=model_name,
        model_hash=model_hash,
    )
    
    session.add(metadata)
    session.flush()
    return metadata


# ============================================================================
# API HARVEST METADATA CRUD OPERATIONS
# ============================================================================

def insert_api_harvest_metadata(
    session: Session,
    api_name: str,
    harvest_date: date,
    analytes_queried: int,
    synonyms_obtained: int,
    synonyms_filtered: int,
    errors_encountered: int,
    notes: Optional[str] = None,
) -> APIHarvestMetadata:
    """
    Log API harvest statistics.
    
    Args:
        session: Database session
        api_name: API source name (pubchem, comptox, cas)
        harvest_date: Date of harvest
        analytes_queried: Number of analytes queried
        synonyms_obtained: Total synonyms obtained
        synonyms_filtered: Synonyms removed by quality filters
        errors_encountered: Number of errors
        notes: Additional notes
    
    Returns:
        Created APIHarvestMetadata instance
    """
    metadata = APIHarvestMetadata(
        api_name=api_name,
        harvest_date=harvest_date,
        analytes_queried=analytes_queried,
        synonyms_obtained=synonyms_obtained,
        synonyms_filtered=synonyms_filtered,
        errors_encountered=errors_encountered,
        notes=notes,
    )
    
    session.add(metadata)
    session.flush()
    return metadata


# ============================================================================
# SNAPSHOT REGISTRY CRUD OPERATIONS
# ============================================================================

def insert_snapshot_registry(
    session: Session,
    version: str,
    release_date: date,
    corpus_hash: str,
    db_file_path: Optional[str] = None,
    model_hash: Optional[str] = None,
    faiss_index_hash: Optional[str] = None,
    notes: Optional[str] = None,
) -> SnapshotRegistry:
    """
    Register a new corpus/model snapshot.
    
    Args:
        session: Database session
        version: Version tag (e.g., 'v1.0', 'v1. 1')
        release_date: Release date
        corpus_hash: SHA256 of corpus
        db_file_path: Path to database snapshot
        model_hash: SHA256 of model
        faiss_index_hash: SHA256 of FAISS index
        notes: Version notes
    
    Returns:
        Created SnapshotRegistry instance
    """
    snapshot = SnapshotRegistry(
        version=version,
        release_date=release_date,
        db_file_path=db_file_path,
        corpus_hash=corpus_hash,
        model_hash=model_hash,
        faiss_index_hash=faiss_index_hash,
        notes=notes,
    )
    
    session.add(snapshot)
    session.flush()
    return snapshot


def get_latest_snapshot(session: Session) -> Optional[SnapshotRegistry]:
    """
    Get the most recent snapshot registry entry.
    
    Args:
        session: Database session
    
    Returns:
        Latest SnapshotRegistry instance or None
    """
    return session.execute(
        select(SnapshotRegistry)
        .order_by(SnapshotRegistry.release_date.desc())
        .limit(1)
    ).scalar_one_or_none()


# ============================================================================
# STATISTICS AND REPORTING
# ============================================================================

def get_database_statistics(session: Session) -> Dict[str, Any]:
    """
    Get comprehensive database statistics.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary with counts and statistics
    """
    analyte_count = session.execute(select(func.count(Analyte.analyte_id))).scalar_one()
    synonym_count = session.execute(select(func.count(Synonym.id))).scalar_one()
    lab_variant_count = session.execute(select(func.count(LabVariant.id))).scalar_one()
    match_decision_count = session.execute(select(func.count(MatchDecision.id))).scalar_one()
    
    validated_count = session.execute(
        select(func.count(MatchDecision.id))
        .where(MatchDecision.human_validated == True)
    ).scalar_one()
    
    ingested_count = session.execute(
        select(func.count(MatchDecision.id))
        .where(MatchDecision.ingested == True)
    ).scalar_one()
    
    return {
        'analytes': analyte_count,
        'synonyms': synonym_count,
        'lab_variants': lab_variant_count,
        'match_decisions': match_decision_count,
        'validated_decisions': validated_count,
        'ingested_decisions': ingested_count,
    }
