"""
SQLAlchemy ORM models for Reg 153 Chemical Matcher database.

This module defines the database schema including:
- Analytes (canonical chemical truth)
- Synonyms (name variants with confidence scores)
- Lab variants (Ontario lab behavioral corpus)
- Match decisions (audit trail for ML matching)
- Embeddings metadata (vector storage tracking)
- API harvest metadata (bootstrap audit)
- Snapshot registry (version tracking)
"""

from datetime import datetime, date
from typing import Optional
import enum
from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    Date,
    Text,
    JSON,
    Boolean,
    Index,
    ForeignKey,
    CheckConstraint,
    UniqueConstraint,
    Enum,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


class AnalyteType(enum.Enum):
    """Enum for analyte classification types."""
    SINGLE_SUBSTANCE = "single_substance"
    FRACTION_OR_GROUP = "fraction_or_group"
    SUITE = "suite"
    PARAMETER = "parameter"
    CALCULATED = "calculated"


class SynonymType(enum.Enum):
    """Enum for synonym classification."""
    IUPAC = "iupac"
    COMMON = "common"
    ABBREVIATION = "abbreviation"
    LAB_VARIANT = "lab_variant"
    TRADE = "trade"
    FRACTION_NOTATION = "fraction_notation"
    EXACT = "exact"


class ValidationConfidence(enum.Enum):
    """Enum for lab variant validation confidence."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"
    UNSTABLE = "UNSTABLE"


class Analyte(Base):
    """
    Canonical analyte truth table.
    
    Represents the authoritative chemical substances tracked in Reg 153,
    including single substances, fractions/groups, suites, and parameters.
    """
    __tablename__ = "analytes"
    
    # Primary Key - REG153_XXX format
    analyte_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    
    # Core identification
    preferred_name: Mapped[str] = mapped_column(String(500), nullable=False)
    analyte_type: Mapped[AnalyteType] = mapped_column(Enum(AnalyteType), nullable=False, index=True)
    
    # Chemical identifiers
    cas_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    group_code: Mapped[Optional[str]] = mapped_column(
        String(50), 
        nullable=True,
        index=True,
        comment="PHC_F1, PHC_F2, PHC_F3, PHC_F4, BTEX, VOC, PAH, METAL, OC, ABN, CP, PCB"
    )
    
    # Regulatory context
    table_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="Reg 153 table 1-9")
    chemical_group: Mapped[Optional[str]] = mapped_column(
        String(100), 
        nullable=True,
        index=True,
        comment="Metals, VOCs, PAHs, OCs, ABNs, CPs, PCBs, PHCs"
    )
    
    # Parent-child hierarchy (e.g. individual PCB congeners under "PCBs total")
    parent_analyte_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        ForeignKey("analytes.analyte_id"),
        nullable=True,
        index=True,
        comment="Parent suite/group analyte_id for congeners or sub-compounds",
    )
    
    # Chemical structure (for single substances)
    smiles: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inchi_key: Mapped[Optional[str]] = mapped_column(String(27), nullable=True, index=True)
    molecular_formula: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )
    
    # Relationships
    parent: Mapped[Optional["Analyte"]] = relationship(
        "Analyte", remote_side=[analyte_id], back_populates="children",
    )
    children: Mapped[list["Analyte"]] = relationship(
        "Analyte", back_populates="parent", cascade="all, delete-orphan",
    )
    synonyms: Mapped[list["Synonym"]] = relationship(back_populates="analyte", cascade="all, delete-orphan")
    lab_variants: Mapped[list["LabVariant"]] = relationship(back_populates="analyte", cascade="all, delete-orphan")
    match_decisions: Mapped[list["MatchDecision"]] = relationship(back_populates="analyte", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index("ix_analytes_preferred_name", "preferred_name"),
        Index("ix_analytes_group_chemical", "group_code", "chemical_group"),
    )
    
    def __repr__(self) -> str:
        return f"<Analyte(analyte_id='{self.analyte_id}', name='{self.preferred_name}')>"


class Synonym(Base):
    """
    Synonym table for alternative chemical names.
    
    Stores raw and normalized versions of synonyms with confidence scores,
    type classification, and harvest source tracking.
    """
    __tablename__ = "synonyms"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to analyte
    analyte_id: Mapped[str] = mapped_column(
        ForeignKey("analytes.analyte_id", ondelete="CASCADE"), 
        nullable=False, 
        index=True
    )
    
    # Synonym text (raw and normalized)
    synonym_raw: Mapped[str] = mapped_column(Text, nullable=False)
    synonym_norm: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Classification and metadata
    synonym_type: Mapped[SynonymType] = mapped_column(Enum(SynonymType), nullable=False, index=True)
    harvest_source: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="pubchem, comptox, cas, manual_validation, edd_observed"
    )
    confidence: Mapped[float] = mapped_column(
        Float, 
        nullable=False, 
        default=1.0,
        comment="1.0 for API-verified, 0.5-0.9 for lab variants"
    )
    
    # Vendor tracking (NULL for API-harvested legacy synonyms)
    lab_vendor: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        index=True,
        comment="Source lab vendor, NULL for API-harvested"
    )
    
    # Normalization version tracking
    normalization_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        comment="Version of normalization rules used to produce synonym_norm"
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    analyte: Mapped["Analyte"] = relationship(back_populates="synonyms")
    
    __table_args__ = (
        CheckConstraint("confidence >= 0.0 AND confidence <= 1.0", name="ck_synonym_confidence_range"),
        Index("ix_synonyms_norm_fast_lookup", "synonym_norm"),  # Critical for matching performance
        Index("ix_synonyms_source_type", "harvest_source", "synonym_type"),
        Index("ix_synonyms_vendor_norm", "lab_vendor", "synonym_norm"),
    )
    
    def __repr__(self) -> str:
        return f"<Synonym(id={self.id}, analyte_id='{self.analyte_id}', synonym='{self.synonym_norm[:50]}')>"


class LabVariant(Base):
    """
    Ontario lab behavioral corpus.
    
    Tracks how different labs report analytes with lab-specific naming,
    methods, matrices, and reporting frequencies.
    """
    __tablename__ = "lab_variants"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Observed text from lab
    observed_text: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Lab and method context
    lab_vendor: Mapped[Optional[str]] = mapped_column(
        String(100), 
        nullable=True,
        comment="ALS, SGS, Bureau_Veritas"
    )
    method: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    matrix: Mapped[Optional[str]] = mapped_column(
        String(100), 
        nullable=True,
        comment="soil, groundwater, sediment"
    )
    units: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    raw_context: Mapped[Optional[str]] = mapped_column(Text, nullable=True, comment="Column header or full context")
    
    # Frequency tracking
    frequency_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    first_seen_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    last_seen_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    
    # Collision tracking
    collision_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
        comment="Times canonical changed for this vendor+text pair"
    )
    last_collision_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    
    # Normalization version tracking
    normalization_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1,
        comment="Version of normalization rules used to produce observed_text"
    )
    
    # Validation (if matched to canonical analyte)
    validated_match_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("analytes.analyte_id", ondelete="SET NULL"),
        nullable=True,
        index=True
    )
    validation_confidence: Mapped[Optional[ValidationConfidence]] = mapped_column(
        Enum(ValidationConfidence), 
        nullable=True
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    analyte: Mapped[Optional["Analyte"]] = relationship(back_populates="lab_variants")
    confirmations: Mapped[list["LabVariantConfirmation"]] = relationship(
        back_populates="variant", cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("ix_lab_variants_observed_text", "observed_text"),
        Index("ix_lab_variants_vendor", "lab_vendor"),
        Index("ix_lab_variants_matrix", "matrix"),
        UniqueConstraint("lab_vendor", "observed_text", name="uq_lab_variant_vendor_text"),
        CheckConstraint("length(observed_text) > 0", name="ck_lab_variant_text_nonempty"),
    )
    
    def __repr__(self) -> str:
        return f"<LabVariant(id={self.id}, lab='{self.lab_vendor}', text='{self.observed_text[:50]}')>"


class LabVariantConfirmation(Base):
    """
    Junction table tracking distinct-submission confirmations of lab variants.
    
    Enables consensus counting (N=3 distinct submissions for hard cache),
    collision detection (differing confirmed_analyte_id values), 
    temporal decay window queries, and audit trail preservation.
    """
    __tablename__ = "lab_variant_confirmations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # FK to lab variant
    variant_id: Mapped[int] = mapped_column(
        ForeignKey("lab_variants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Which submission provided this confirmation
    submission_id: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # What analyte was confirmed (for collision detection)
    confirmed_analyte_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("analytes.analyte_id", ondelete="SET NULL"),
        nullable=True
    )
    
    # Timestamp
    confirmed_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    
    # Soft-delete for post-cooldown purge (preserves audit trail)
    valid_for_consensus: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True,
        comment="False for confirmations invalidated by collision/cooldown"
    )
    
    # Relationships
    variant: Mapped["LabVariant"] = relationship(back_populates="confirmations")
    
    __table_args__ = (
        UniqueConstraint("variant_id", "submission_id", name="uq_confirmation_variant_submission"),
        Index("ix_confirmations_variant_at", "variant_id", "confirmed_at"),
        Index("ix_confirmations_analyte", "confirmed_analyte_id"),
        Index("ix_confirmations_variant_analyte", "variant_id", "confirmed_analyte_id"),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LabVariantConfirmation(id={self.id}, variant={self.variant_id}, "
            f"submission={self.submission_id}, analyte='{self.confirmed_analyte_id}')>"
        )


class MatchDecision(Base):
    """
    Audit trail for ML-based matching decisions.
    
    Records the complete decision context including candidate rankings,
    signals used, model versions, and validation status for continuous
    improvement and human-in-the-loop learning.
    """
    __tablename__ = "match_decisions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Input query
    input_text: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    
    # Matched result (if confident)
    matched_analyte_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("analytes.analyte_id", ondelete="SET NULL"),
        nullable=True,
        index=True
    )
    
    # Decision metadata
    match_method: Mapped[str] = mapped_column(
        String(50), 
        nullable=False,
        comment="exact, fuzzy, semantic, cas_extracted, hybrid"
    )
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False)
    
    # Candidate details (JSON)
    top_k_candidates: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        comment="Array of {analyte_id, score, method}"
    )
    signals_used: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        comment="{exact: bool, fuzzy: score, semantic: score, cas: bool}"
    )
    
    # Version tracking
    corpus_snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    model_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    
    # Timestamps
    decision_timestamp: Mapped[datetime] = mapped_column(
        DateTime, 
        default=datetime.utcnow, 
        nullable=False, 
        index=True
    )
    
    # Human validation
    human_validated: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
        comment="True if human confirmed this match is correct"
    )
    validation_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Quality flags
    disagreement_flag: Mapped[bool] = mapped_column(
        Boolean, 
        nullable=False, 
        default=False, 
        index=True,
        comment="True if ML and human disagree"
    )
    
    # Phase B: Decision quality extensions
    margin: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Score gap between top-1 and top-2 candidates (s1 - s2)"
    )
    cross_method_conflict: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="True if fuzzy and semantic top-1 disagree on analyte_id"
    )
    correction_of: Mapped[Optional[int]] = mapped_column(
        ForeignKey("match_decisions.id", ondelete="SET NULL"),
        nullable=True,
        comment="FK to the original decision this correction supersedes"
    )
    is_corrected: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
        comment="True if this decision has been superseded by a correction"
    )
    
    # Context features (for context-conditioned thresholds)
    lab_vendor: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Lab vendor name (Caduceon, SGS, ALS, etc.)"
    )
    method: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Analytical method (EPA524.2, EPA8260, etc.)"
    )
    matrix: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Sample matrix (water, soil, air, etc.)"
    )
    
    # Learning loop tracking
    ingested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
        comment="True if this validation has been ingested as a synonym"
    )
    
    # Relationships
    analyte: Mapped[Optional["Analyte"]] = relationship(back_populates="match_decisions")
    
    __table_args__ = (
        CheckConstraint("confidence_score >= 0.0 AND confidence_score <= 1.0", name="ck_match_confidence_range"),
        Index("ix_match_decisions_corpus_model", "corpus_snapshot_hash", "model_hash"),
        Index("ix_match_decisions_validation", "human_validated", "ingested"),
    )
    
    def __repr__(self) -> str:
        return f"<MatchDecision(id={self.id}, input='{self.input_text[:50]}', confidence={self.confidence_score:.3f})>"


class EmbeddingsMetadata(Base):
    """
    Metadata for embeddings stored on disk.
    
    Vectors are stored in efficient binary formats (FAISS, numpy), while
    this table tracks the metadata and disk locations.
    """
    __tablename__ = "embeddings_metadata"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to source (one of these must be set)
    analyte_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("analytes.analyte_id", ondelete="CASCADE"),
        nullable=True,
        index=True
    )
    synonym_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("synonyms.id", ondelete="CASCADE"),
        nullable=True,
        index=True
    )
    
    # Text that was embedded
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Storage location
    embedding_index: Mapped[int] = mapped_column(
        Integer, 
        nullable=False, 
        comment="Index in .npy file or FAISS index"
    )
    
    # Model information
    model_name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    model_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        CheckConstraint(
            "(analyte_id IS NOT NULL AND synonym_id IS NULL) OR (analyte_id IS NULL AND synonym_id IS NOT NULL)",
            name="ck_embedding_source_xor"
        ),
        Index("ix_embeddings_model", "model_name", "model_hash"),
    )
    
    def __repr__(self) -> str:
        source = f"analyte_id='{self.analyte_id}'" if self.analyte_id else f"synonym_id={self.synonym_id}"
        return f"<EmbeddingsMetadata(id={self.id}, {source}, model='{self.model_name}')>"


class APIHarvestMetadata(Base):
    """
    Bootstrap audit trail for API harvesting.
    
    Tracks what data was harvested from which APIs, when, and with what
    success rates for debugging and re-harvesting.
    """
    __tablename__ = "api_harvest_metadata"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # API source information
    api_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    harvest_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    
    # Processing statistics
    analytes_queried: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    synonyms_obtained: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    synonyms_filtered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors_encountered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    
    # Notes
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    __table_args__ = (
        Index("ix_api_harvest_date_source", "harvest_date", "api_name"),
    )
    
    def __repr__(self) -> str:
        return f"<APIHarvestMetadata(id={self.id}, api='{self.api_name}', date={self.harvest_date})>"


class SnapshotRegistry(Base):
    """
    Version tracking for corpus and model snapshots.
    
    Maintains a registry of snapshot versions for reproducibility and
    A/B testing of different corpus/model combinations.
    """
    __tablename__ = "snapshot_registry"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Version identification
    version: Mapped[str] = mapped_column(String(100), nullable=False, index=True, comment="v1.0, v1.1, etc.")
    release_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    
    # File paths
    db_file_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    
    # Hashes for verification
    corpus_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    model_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    faiss_index_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    
    # Notes
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    __table_args__ = (
        Index("ix_snapshot_version", "version"),
    )
    
    def __repr__(self) -> str:
        return f"<SnapshotRegistry(id={self.id}, version='{self.version}', date={self.release_date})>"
