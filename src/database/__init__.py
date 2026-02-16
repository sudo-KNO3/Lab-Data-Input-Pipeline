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


def create_test_db() -> DatabaseManager:
    """Create an in-memory DatabaseManager for testing."""
    db = DatabaseManager(db_path=":memory:")
    db.create_all_tables()
    return db

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

# Import vendor micro-controller and matching helpers from crud.py
# These provide functions not duplicated in crud_new.py.
from .crud import (
    # Analyte extras
    search_analytes_by_name,
    list_analytes,
    delete_analyte,
    count_analytes,
    # Synonym extras
    synonym_exists,
    search_synonyms,
    delete_synonyms_by_source,
    # Vendor micro-controller (lab variant lifecycle)
    get_or_create_lab_variant,
    create_lab_variant_confirmation,
    get_distinct_confirmation_count,
    check_variant_collision,
    clear_conflicting_confirmations,
    count_todays_hard_promotions,
    count_todays_global_synonyms,
    get_lab_variants_by_vendor,
    increment_lab_variant_frequency,
    search_lab_variants,
    # Match decisions
    get_decisions_for_review,
    mark_decision_reviewed,
    get_match_statistics,
    # Embeddings
    get_embeddings_by_model,
    # API harvest
    get_harvest_statistics_by_source,
    # Snapshots
    deactivate_snapshots,
    # Matching helpers
    get_nearest_analyte,
    get_all_synonyms_for_corpus,
)


__all__ = [
    # Connection
    "DatabaseManager",
    "get_session",
    "get_db_manager",
    "init_db",
    "create_test_db",
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
    "search_analytes_by_name",
    "list_analytes",
    "delete_analyte",
    "count_analytes",
    # CRUD - Synonyms
    "insert_synonym",
    "batch_insert_synonyms",
    "query_by_normalized_synonym",
    "get_synonyms_for_analyte",
    "synonym_exists",
    "search_synonyms",
    "delete_synonyms_by_source",
    # CRUD - Lab Variants (vendor micro-controller)
    "insert_lab_variant",
    "get_or_create_lab_variant",
    "create_lab_variant_confirmation",
    "get_distinct_confirmation_count",
    "check_variant_collision",
    "clear_conflicting_confirmations",
    "count_todays_hard_promotions",
    "count_todays_global_synonyms",
    "get_lab_variants_by_vendor",
    "increment_lab_variant_frequency",
    "search_lab_variants",
    # CRUD - Match Decisions
    "log_match_decision",
    "get_validated_decisions_since",
    "mark_decision_as_ingested",
    "get_decisions_for_review",
    "mark_decision_reviewed",
    "get_match_statistics",
    # CRUD - Embeddings
    "insert_embedding_metadata",
    "get_embeddings_by_model",
    # CRUD - API Harvest
    "insert_api_harvest_metadata",
    "get_harvest_statistics_by_source",
    # CRUD - Snapshots
    "insert_snapshot_registry",
    "get_latest_snapshot",
    "deactivate_snapshots",
    # Matching helpers
    "get_nearest_analyte",
    "get_all_synonyms_for_corpus",
    # Statistics
    "get_database_statistics",
]


__version__ = "1.0.0"
