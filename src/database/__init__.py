"""
Database package for Reg 153 Chemical Matcher.

This package provides:
- SQLAlchemy ORM models for all database tables
- Connection and session management
- CRUD operations for all models
- Specialized query helpers for chemical matching

Quick start:
    from src.database import DatabaseManager, get_session
    from src.database.crud_new import insert_analyte, query_by_normalized_synonym
    
    # Initialize database
    db = DatabaseManager("data/reg153_matcher.db")
    db.create_all_tables()
    
    # Use with context manager
    with db.session_scope() as session:
        analyte = insert_analyte(
            session,
            analyte_id="REG153_VOCS_001",
            preferred_name="Acetone",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="67-64-1",
        )
"""

from .connection import (
    DatabaseManager,
    get_session,
    get_db_manager,
    init_db,
)

from .models import (
    Base,
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

# Import new CRUD functions
from .crud_new import (
    insert_analyte,
    get_analyte_by_id,
    get_analyte_by_cas,
    get_all_analytes,
    update_analyte,
    insert_synonym,
    batch_insert_synonyms,
    query_by_normalized_synonym,
    get_synonyms_for_analyte,
    insert_lab_variant,
    log_match_decision,
    get_validated_decisions_since,
    mark_decision_as_ingested,
    insert_embedding_metadata,
    insert_api_harvest_metadata,
    insert_snapshot_registry,
    get_latest_snapshot,
    get_database_statistics,
)


__all__ = [
    # Connection
    "DatabaseManager",
    "get_session",
    "get_db_manager",
    "init_db",
    # Models
    "Base",
    "Analyte",
    "Synonym",
    "LabVariant",
    "MatchDecision",
    "EmbeddingsMetadata",
    "APIHarvestMetadata",
    "SnapshotRegistry",
    # Enums
    "AnalyteType",
    "SynonymType",
    "ValidationConfidence",
    # CRUD - Analytes
    "insert_analyte",
    "get_analyte_by_id",
    "get_analyte_by_cas",
    "get_all_analytes",
    "update_analyte",
    # CRUD - Synonyms
    "insert_synonym",
    "batch_insert_synonyms",
    "query_by_normalized_synonym",
    "get_synonyms_for_analyte",
    # CRUD - Lab Variants
    "insert_lab_variant",
    # CRUD - Match Decisions
    "log_match_decision",
    "get_validated_decisions_since",
    "mark_decision_as_ingested",
    # CRUD - Embeddings
    "insert_embedding_metadata",
    # CRUD - API Harvest
    "insert_api_harvest_metadata",
    # CRUD - Snapshots
    "insert_snapshot_registry",
    "get_latest_snapshot",
    # Statistics
    "get_database_statistics",
]


__version__ = "1.0.0"
