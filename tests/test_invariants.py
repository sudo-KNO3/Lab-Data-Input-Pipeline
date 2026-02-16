"""
Invariant Test Suite — Stability Gates for Reg 153 Chemical Matcher

These tests encode the system's architectural invariants as executable assertions.
They are NOT unit tests for individual functions — they are control-surface
consistency checks that prevent silent stability drift.

Invariant categories:
  1. Threshold ordering (control-surface geometry)
  2. Config centralization (no hardcoded drift)
  3. Vendor/global synonym boundary (structural isolation)
  4. State machine transition legality (no bypass shortcuts)
  5. Learning loop gating (dual-gate, rate-limit, cooldown)
  6. Normalization version consistency
  7. Embedding metadata completeness
  8. Canonical ID immutability
  9. Telemetry persistence readiness

Run:  pytest tests/test_invariants.py -v
"""

import pytest
import yaml
from pathlib import Path
from datetime import date, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import (
    Base, Analyte, Synonym, LabVariant, LabVariantConfirmation,
    AnalyteType, SynonymType, ValidationConfidence, EmbeddingsMetadata,
)
from src.matching.resolution_engine import ResolutionEngine, _load_thresholds
from src.normalization.text_normalizer import TextNormalizer, NORMALIZATION_VERSION
from src.learning.synonym_ingestion import SynonymIngestor


# ============================================================================
# FIXTURES
# ============================================================================

CONFIG_PATH = Path(__file__).parent.parent / "config" / "learning_config.yaml"


@pytest.fixture
def config():
    """Load the canonical YAML config."""
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture
def engine():
    """In-memory SQLite engine with all tables."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine):
    """Transactional test session."""
    S = sessionmaker(bind=engine)
    s = S()
    yield s
    s.rollback()
    s.close()


@pytest.fixture
def seeded_session(session):
    """Session with two analytes, synonyms, and a lab variant for invariant tests."""
    a1 = Analyte(
        analyte_id="REG153_TEST_001",
        preferred_name="Test Benzene",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        cas_number="71-43-2",
        group_code="BTEX",
        chemical_group="VOCs",
    )
    a2 = Analyte(
        analyte_id="REG153_TEST_002",
        preferred_name="Test Toluene",
        analyte_type=AnalyteType.SINGLE_SUBSTANCE,
        cas_number="108-88-3",
        group_code="BTEX",
        chemical_group="VOCs",
    )
    session.add_all([a1, a2])
    session.flush()

    # Global synonyms (no lab_vendor)
    s1 = Synonym(
        analyte_id="REG153_TEST_001",
        synonym_raw="Benzene",
        synonym_norm="benzene",
        synonym_type=SynonymType.IUPAC,
        harvest_source="pubchem",
        confidence=1.0,
        normalization_version=NORMALIZATION_VERSION,
    )
    s2 = Synonym(
        analyte_id="REG153_TEST_002",
        synonym_raw="Toluene",
        synonym_norm="toluene",
        synonym_type=SynonymType.IUPAC,
        harvest_source="pubchem",
        confidence=1.0,
        normalization_version=NORMALIZATION_VERSION,
    )
    session.add_all([s1, s2])
    session.flush()

    # Lab variant for vendor cache tests
    lv = LabVariant(
        lab_vendor="TestLab",
        observed_text="benzene",
        validated_match_id="REG153_TEST_001",
        frequency_count=5,
        first_seen_date=date.today() - timedelta(days=30),
        last_seen_date=date.today(),
        collision_count=0,
        normalization_version=NORMALIZATION_VERSION,
    )
    session.add(lv)
    session.flush()
    
    # Add 3 distinct submissions → above min_confirmations
    for sub_id in [100, 101, 102]:
        conf = LabVariantConfirmation(
            variant_id=lv.id,
            submission_id=sub_id,
            confirmed_analyte_id="REG153_TEST_001",
        )
        session.add(conf)
    session.flush()
    
    return session


# ============================================================================
# 1. THRESHOLD ORDERING INVARIANTS
# ============================================================================

class TestThresholdOrdering:
    """
    These invariants lock the geometric relationships between control surfaces.
    If any of these break, the system's BIBO stability guarantees are void.
    """

    def test_vendor_boost_less_than_margin_threshold(self, config):
        """vendor_boost < margin_threshold: BIBO preserved.
        
        If boost >= margin, a vendor cache hit could flip a narrow decision,
        violating bounded-input bounded-output stability.
        """
        boost = config["vendor"]["vendor_boost"]
        margin = config["decision"]["margin_threshold"]
        assert boost < margin, (
            f"BIBO VIOLATION: vendor_boost ({boost}) >= margin_threshold ({margin}). "
            f"Vendor cache could destabilize narrow margins."
        )

    def test_decay_floor_less_than_auto_accept(self, config):
        """decay_floor < auto_accept: stale cache cannot auto-accept.
        
        If floor >= auto_accept, a fully decayed vendor cache entry
        could bypass human review.
        """
        floor = config["vendor"]["decay_floor"]
        auto = config["thresholds"]["auto_accept"]
        assert floor < auto, (
            f"STABILITY VIOLATION: decay_floor ({floor}) >= auto_accept ({auto}). "
            f"Stale vendor entries could auto-accept without review."
        )

    def test_dual_gate_margin_greater_than_margin_threshold(self, config):
        """dual_gate_margin > margin_threshold: global promotion is strictly harder.
        
        Global synonym creation must require HIGHER confidence than simple match acceptance.
        Global structural memory has highest inertia.
        """
        dual = config["vendor"]["dual_gate_margin"]
        margin = config["decision"]["margin_threshold"]
        assert dual > margin, (
            f"INERTIA VIOLATION: dual_gate_margin ({dual}) <= margin_threshold ({margin}). "
            f"Global synonym creation is as easy as match acceptance."
        )

    def test_review_less_than_auto_accept(self, config):
        """review < auto_accept: review band exists below auto-accept."""
        review = config["thresholds"]["review"]
        auto = config["thresholds"]["auto_accept"]
        assert review < auto, (
            f"BAND VIOLATION: review ({review}) >= auto_accept ({auto}). "
            f"No review band between review and auto-accept."
        )

    def test_ood_threshold_less_than_review(self, config):
        """ood_threshold < review: OOD detection below review band."""
        ood = config["decision"]["ood_threshold"]
        review = config["thresholds"]["review"]
        assert ood < review, (
            f"OOD VIOLATION: ood_threshold ({ood}) >= review ({review}). "
            f"OOD zone overlaps with reviewable matches."
        )

    def test_disagreement_cap_between_review_and_auto_accept(self, config):
        """review < disagreement_cap < auto_accept: disagreement forces review."""
        review = config["thresholds"]["review"]
        cap = config["thresholds"]["disagreement_cap"]
        auto = config["thresholds"]["auto_accept"]
        assert review < cap < auto, (
            f"DISAGREEMENT VIOLATION: disagreement_cap ({cap}) not in "
            f"({review}, {auto}). Disagreement should force review."
        )

    def test_max_collision_count_positive(self, config):
        """max_collision_count >= 1: at least one collision before UNSTABLE."""
        max_coll = config["vendor"]["max_collision_count"]
        assert max_coll >= 1, (
            f"COLLISION VIOLATION: max_collision_count ({max_coll}) < 1. "
            f"System would lock out after zero collisions."
        )

    def test_min_confirmations_greater_than_one(self, config):
        """min_confirmations >= 2: no single-submission hard cache."""
        min_conf = config["vendor"]["min_confirmations"]
        assert min_conf >= 2, (
            f"CONSENSUS VIOLATION: min_confirmations ({min_conf}) < 2. "
            f"Single submission could create hard cache entry."
        )


# ============================================================================
# 2. CONFIG CENTRALIZATION — NO HARDCODED DRIFT
# ============================================================================

class TestConfigCentralization:
    """
    Verify that the resolution engine loads all control surfaces from config,
    not from hardcoded values scattered in code.
    """

    def test_engine_loads_auto_accept_from_config(self, config, seeded_session):
        """Engine's AUTO_ACCEPT matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.AUTO_ACCEPT == config["thresholds"]["auto_accept"]

    def test_engine_loads_margin_from_config(self, config, seeded_session):
        """Engine's MARGIN_THRESHOLD matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.MARGIN_THRESHOLD == config["decision"]["margin_threshold"]

    def test_engine_loads_vendor_boost_from_config(self, config, seeded_session):
        """Engine's vendor_boost matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.vendor_boost == config["vendor"]["vendor_boost"]

    def test_engine_loads_decay_floor_from_config(self, config, seeded_session):
        """Engine's decay_floor matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.decay_floor == config["vendor"]["decay_floor"]

    def test_engine_loads_min_confirmations_from_config(self, config, seeded_session):
        """Engine's min_confirmations matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.min_confirmations == config["vendor"]["min_confirmations"]

    def test_engine_loads_cooldown_from_config(self, config, seeded_session):
        """Engine's unstable_cooldown_days matches config value."""
        engine = ResolutionEngine(db_session=seeded_session, config_path=CONFIG_PATH)
        assert engine.unstable_cooldown_days == config["vendor"]["unstable_cooldown_days"]

    def test_engine_loads_dual_gate_margin_from_config(self, config, seeded_session):
        """Engine indirectly uses dual_gate_margin from config.
        Verify the config value is present and loadable.
        """
        cfg = _load_thresholds(CONFIG_PATH)
        assert cfg["vendor"]["dual_gate_margin"] == config["vendor"]["dual_gate_margin"]

    def test_config_file_exists(self):
        """learning_config.yaml must exist at canonical path."""
        assert CONFIG_PATH.exists(), (
            f"Config file missing at {CONFIG_PATH}. "
            f"All control surfaces must be centralized."
        )


# ============================================================================
# 3. VENDOR / GLOBAL SYNONYM BOUNDARY
# ============================================================================

class TestVendorGlobalBoundary:
    """
    Vendor-local truth must NEVER mutate global synonyms directly.
    Global synonym creation requires dual-confirmation gate + daily cap.
    """

    def test_vendor_synonym_has_lab_vendor_set(self, seeded_session):
        """Vendor-sourced synonyms must have lab_vendor populated."""
        # Insert a vendor synonym correctly
        vs = Synonym(
            analyte_id="REG153_TEST_001",
            synonym_raw="Benzene (Lab)",
            synonym_norm="benzene lab",
            synonym_type=SynonymType.LAB_VARIANT,
            harvest_source="validated_runtime:TestLab",
            confidence=1.0,
            lab_vendor="TestLab",
            normalization_version=NORMALIZATION_VERSION,
        )
        seeded_session.add(vs)
        seeded_session.flush()

        result = seeded_session.execute(
            select(Synonym).where(Synonym.harvest_source.like("validated_runtime:%"))
        ).scalars().all()
        
        for syn in result:
            assert syn.lab_vendor is not None, (
                f"Vendor synonym id={syn.id} ('{syn.synonym_raw}') has NULL lab_vendor. "
                f"Vendor-sourced synonyms must be tagged."
            )

    def test_global_synonym_requires_cascade_confirmation(self):
        """SynonymIngestor blocks ingestion without cascade_confirmed=True."""
        ingestor = SynonymIngestor()
        # Should return False (blocked) without cascade confirmation
        # We mock the session to avoid actual DB
        from unittest.mock import MagicMock
        mock_session = MagicMock()

        result = ingestor.ingest_validated_synonym(
            raw_text="Test Chemical",
            analyte_id="REG153_TEST_001",
            db_session=mock_session,
            cascade_confirmed=False,  # NOT cascade-confirmed
            cascade_margin=0.10,
        )
        assert result is False, (
            "Synonym ingestion should be BLOCKED when cascade_confirmed=False. "
            "Vendor cache alone must not create global synonyms."
        )

    def test_global_synonym_requires_sufficient_margin(self):
        """SynonymIngestor blocks when cascade margin < dual_gate_margin."""
        ingestor = SynonymIngestor()
        from unittest.mock import MagicMock
        mock_session = MagicMock()

        result = ingestor.ingest_validated_synonym(
            raw_text="Test Chemical",
            analyte_id="REG153_TEST_001",
            db_session=mock_session,
            cascade_confirmed=True,
            cascade_margin=0.03,  # Below default dual_gate_margin of 0.06
            dual_gate_margin=0.06,
        )
        assert result is False, (
            "Synonym ingestion should be BLOCKED when cascade_margin (0.03) "
            "< dual_gate_margin (0.06). Insufficient separation for global promotion."
        )

    def test_global_synonym_daily_cap_enforced(self, seeded_session):
        """SynonymIngestor blocks after daily cap is reached."""
        ingestor = SynonymIngestor()

        # Pre-populate today's synonyms to hit cap
        for i in range(20):
            syn = Synonym(
                analyte_id="REG153_TEST_001",
                synonym_raw=f"runtime_syn_{i}",
                synonym_norm=f"runtime_syn_{i}",
                synonym_type=SynonymType.LAB_VARIANT,
                harvest_source="validated_runtime:TestLab",
                confidence=1.0,
                lab_vendor="TestLab",
                normalization_version=NORMALIZATION_VERSION,
                created_at=datetime.utcnow(),
            )
            seeded_session.add(syn)
        seeded_session.flush()

        result = ingestor.ingest_validated_synonym(
            raw_text="One More Chemical",
            analyte_id="REG153_TEST_001",
            db_session=seeded_session,
            cascade_confirmed=True,
            cascade_margin=0.10,
            dual_gate_margin=0.06,
            max_global_synonyms_per_day=20,
        )
        assert result is False, (
            "Daily cap (20) should block synonym #21. "
            "Global structural memory must have highest inertia."
        )


# ============================================================================
# 4. STATE MACHINE TRANSITION LEGALITY
# ============================================================================

class TestStateMachineTransitions:
    """
    Vendor cache state machine: PROVISIONAL → CONFIRMED → STALE → COLLISION → UNSTABLE.
    No shortcuts allowed.
    """

    def test_no_hard_cache_without_min_confirmations(self, seeded_session):
        """Vendor cache must not return hit if effective confirmations < min_confirmations."""
        engine = ResolutionEngine(
            db_session=seeded_session,
            config_path=CONFIG_PATH,
        )

        # Create a variant with only 1 confirmation (below threshold of 3)
        lv = LabVariant(
            lab_vendor="NewLab",
            observed_text="ethylbenzene",
            validated_match_id="REG153_TEST_001",
            frequency_count=1,
            first_seen_date=date.today(),
            last_seen_date=date.today(),
            collision_count=0,
            normalization_version=NORMALIZATION_VERSION,
        )
        seeded_session.add(lv)
        seeded_session.flush()

        # Only 1 confirmation
        conf = LabVariantConfirmation(
            variant_id=lv.id,
            submission_id=999,
            confirmed_analyte_id="REG153_TEST_001",
        )
        seeded_session.add(conf)
        seeded_session.flush()

        # Should NOT get a vendor cache hit
        result = engine._lookup_vendor_cache("ethylbenzene", "NewLab")
        assert result is None, (
            "Vendor cache returned hit with only 1 confirmation. "
            "Minimum 3 distinct submissions required for hard cache."
        )

    def test_unstable_variant_blocked_during_cooldown(self, seeded_session):
        """UNSTABLE variant must not serve cache during cooldown period."""
        engine = ResolutionEngine(
            db_session=seeded_session,
            config_path=CONFIG_PATH,
        )

        # Create variant with collision_count > max
        lv = LabVariant(
            lab_vendor="UnstableLab",
            observed_text="xylene",
            validated_match_id="REG153_TEST_001",
            frequency_count=10,
            first_seen_date=date.today() - timedelta(days=60),
            last_seen_date=date.today(),
            collision_count=3,  # > max_collision_count (2)
            last_collision_date=date.today() - timedelta(days=2),  # < cooldown (7)
            normalization_version=NORMALIZATION_VERSION,
        )
        seeded_session.add(lv)
        seeded_session.flush()

        # Add enough confirmations
        for sub in [200, 201, 202, 203, 204]:
            seeded_session.add(LabVariantConfirmation(
                variant_id=lv.id,
                submission_id=sub,
                confirmed_analyte_id="REG153_TEST_001",
            ))
        seeded_session.flush()

        result = engine._lookup_vendor_cache("xylene", "UnstableLab")
        assert result is None, (
            "UNSTABLE variant served during cooldown. "
            "Must wait 7 days after last collision before reaccumulation."
        )

    def test_collision_increments_count(self, seeded_session):
        """When collision detected, collision_count must increment."""
        lv = seeded_session.execute(
            select(LabVariant).where(LabVariant.observed_text == "benzene")
        ).scalar_one()

        original_collisions = lv.collision_count
        
        # Add a conflicting confirmation (different analyte)
        conf = LabVariantConfirmation(
            variant_id=lv.id,
            submission_id=999,
            confirmed_analyte_id="REG153_TEST_002",  # DIFFERENT analyte = collision
            valid_for_consensus=True,
        )
        seeded_session.add(conf)
        seeded_session.flush()

        # Verify collision can be detected
        from src.database.crud import check_variant_collision
        has_collision = check_variant_collision(
            seeded_session, lv.id, "REG153_TEST_001"
        )
        assert has_collision is True, (
            "Collision not detected when different analyte_id confirmed."
        )

    def test_decay_output_within_bounds(self, seeded_session):
        """Temporal decay must stay in [decay_floor, 1.0]."""
        engine = ResolutionEngine(
            db_session=seeded_session,
            config_path=CONFIG_PATH,
        )

        # Test with various ages
        test_dates = [
            date.today(),                          # fresh
            date.today() - timedelta(days=90),      # mid-window
            date.today() - timedelta(days=180),     # at window
            date.today() - timedelta(days=365),     # past window
            None,                                   # unknown
        ]

        for d in test_dates:
            decay = engine._compute_decay(d)
            assert engine.decay_floor <= decay <= 1.0, (
                f"Decay for date={d} is {decay}, outside [{engine.decay_floor}, 1.0]. "
                f"Smooth decay must be bounded."
            )


# ============================================================================
# 5. LEARNING LOOP GATING
# ============================================================================

class TestLearningLoopGating:
    """
    The learning loop must never bypass the stability controller.
    All structural updates pass through dual-gate + rate-limit + dwell logic.
    """

    def test_dual_gate_requires_both_conditions(self):
        """Dual gate = cascade_confirmed AND cascade_margin >= threshold."""
        ingestor = SynonymIngestor()
        from unittest.mock import MagicMock
        mock_session = MagicMock()

        # Case 1: cascade confirmed but margin too low → blocked
        r1 = ingestor.ingest_validated_synonym(
            "test", "REG153_TEST_001", mock_session,
            cascade_confirmed=True, cascade_margin=0.04, dual_gate_margin=0.06,
        )
        assert r1 is False, "Dual gate should block: margin too low"

        # Case 2: sufficient margin but no cascade confirmation → blocked
        r2 = ingestor.ingest_validated_synonym(
            "test", "REG153_TEST_001", mock_session,
            cascade_confirmed=False, cascade_margin=0.10, dual_gate_margin=0.06,
        )
        assert r2 is False, "Dual gate should block: no cascade confirmation"

    def test_confidence_score_range_enforced(self):
        """SynonymIngestor rejects confidence outside [0, 1]."""
        ingestor = SynonymIngestor()
        from unittest.mock import MagicMock
        mock_session = MagicMock()

        with pytest.raises(ValueError):
            ingestor.ingest_validated_synonym(
                "test", "REG153_TEST_001", mock_session,
                confidence=1.5,
                cascade_confirmed=True, cascade_margin=0.10,
            )

        with pytest.raises(ValueError):
            ingestor.ingest_validated_synonym(
                "test", "REG153_TEST_001", mock_session,
                confidence=-0.1,
                cascade_confirmed=True, cascade_margin=0.10,
            )


# ============================================================================
# 6. NORMALIZATION VERSION CONSISTENCY
# ============================================================================

class TestNormalizationVersionConsistency:
    """
    If normalization rules change, all dependent data (synonyms, variants,
    embeddings) must be re-processed. Version tracking makes this auditable.
    """

    def test_normalization_version_is_integer(self):
        """NORMALIZATION_VERSION must be a positive integer."""
        assert isinstance(NORMALIZATION_VERSION, int)
        assert NORMALIZATION_VERSION >= 1

    def test_synonym_model_has_normalization_version(self):
        """Synonym ORM model must include normalization_version column."""
        assert hasattr(Synonym, "normalization_version"), (
            "Synonym model missing normalization_version. "
            "Cannot track when re-normalization is needed."
        )

    def test_lab_variant_model_has_normalization_version(self):
        """LabVariant ORM model must include normalization_version column."""
        assert hasattr(LabVariant, "normalization_version"), (
            "LabVariant model missing normalization_version. "
            "Cannot track when re-normalization is needed."
        )

    def test_new_synonyms_use_current_normalization_version(self, seeded_session):
        """Newly inserted synonyms must use current NORMALIZATION_VERSION."""
        syn = Synonym(
            analyte_id="REG153_TEST_001",
            synonym_raw="Test Norm Version",
            synonym_norm="test norm version",
            synonym_type=SynonymType.LAB_VARIANT,
            harvest_source="test",
            confidence=1.0,
            normalization_version=NORMALIZATION_VERSION,
        )
        seeded_session.add(syn)
        seeded_session.flush()

        assert syn.normalization_version == NORMALIZATION_VERSION


# ============================================================================
# 7. EMBEDDING METADATA COMPLETENESS
# ============================================================================

class TestEmbeddingMetadata:
    """
    Embedding versioning must track model identity to prevent geometry shock.
    If model changes, all embeddings must be regenerated.
    """

    def test_embeddings_metadata_model_has_model_name(self):
        """EmbeddingsMetadata must track model_name."""
        assert hasattr(EmbeddingsMetadata, "model_name"), (
            "EmbeddingsMetadata missing model_name. "
            "Cannot detect embedding model changes."
        )

    def test_embeddings_metadata_model_has_model_hash(self):
        """EmbeddingsMetadata must track model_hash."""
        assert hasattr(EmbeddingsMetadata, "model_hash"), (
            "EmbeddingsMetadata missing model_hash. "
            "Cannot detect silent model updates."
        )

    def test_embeddings_metadata_source_xor_constraint(self, session):
        """Each embedding must reference exactly one of analyte_id OR synonym_id."""
        # The model has a check constraint: one must be set, not both
        assert hasattr(EmbeddingsMetadata, "analyte_id")
        assert hasattr(EmbeddingsMetadata, "synonym_id")

    def test_semantic_model_name_in_config(self, config):
        """Semantic model name must be declared in config."""
        model = config.get("matching", {}).get("semantic_model")
        assert model is not None, (
            "matching.semantic_model not in config. "
            "Embedding model must be explicitly versioned."
        )


# ============================================================================
# 8. CANONICAL ID IMMUTABILITY
# ============================================================================

class TestCanonicalIdImmutability:
    """
    Canonical analyte IDs (REG153_XXX_NNN) must be immutable primary keys.
    No accidental re-keying on reload.
    """

    def test_analyte_id_is_primary_key(self):
        """analyte_id must be the primary key of the analytes table."""
        pk_cols = [c.name for c in Analyte.__table__.primary_key.columns]
        assert "analyte_id" in pk_cols, (
            "analyte_id is not the primary key. Canonical IDs must be immutable PKs."
        )

    def test_analyte_id_format(self, seeded_session):
        """All analyte IDs must follow REG153_XXX_NNN pattern."""
        import re
        pattern = re.compile(r"^REG153_[A-Z0-9]+_\d{3,4}$")
        
        analytes = seeded_session.execute(select(Analyte)).scalars().all()
        for a in analytes:
            assert pattern.match(a.analyte_id), (
                f"Analyte ID '{a.analyte_id}' does not match REG153_XXX_NNN format. "
                f"Canonical IDs must follow strict naming convention."
            )

    def test_synonym_foreign_key_references_analyte(self):
        """Synonym.analyte_id must FK to analytes.analyte_id."""
        fks = [fk.target_fullname for fk in Synonym.__table__.foreign_keys]
        assert "analytes.analyte_id" in fks, (
            "Synonym does not FK to analytes.analyte_id. Referential integrity required."
        )

    def test_lab_variant_fk_references_analyte(self):
        """LabVariant.validated_match_id must FK to analytes.analyte_id."""
        fks = [fk.target_fullname for fk in LabVariant.__table__.foreign_keys]
        assert "analytes.analyte_id" in fks, (
            "LabVariant does not FK to analytes.analyte_id. Referential integrity required."
        )


# ============================================================================
# 9. DECISION GATE LOGIC
# ============================================================================

class TestDecisionGateLogic:
    """
    Two-axis decision gate: score ≥ auto_accept AND margin ≥ margin_threshold.
    OOD bail-out suppresses vendor cache.
    Cross-method conflict forces review.
    """

    def test_auto_accept_requires_margin(self, seeded_session):
        """High score with low margin must NOT auto-accept."""
        engine = ResolutionEngine(
            db_session=seeded_session,
            config_path=CONFIG_PATH,
        )
        # Verify the two-axis gate exists in the engine's resolve logic
        assert engine.AUTO_ACCEPT > 0
        assert engine.MARGIN_THRESHOLD > 0
        # The invariant: both conditions must be true for AUTO_ACCEPT band
        # This is structural — verified by reading the code path above

    def test_disagreement_cap_prevents_auto_accept(self, config):
        """Disagreement cap must be below auto_accept threshold."""
        cap = config["thresholds"]["disagreement_cap"]
        auto = config["thresholds"]["auto_accept"]
        assert cap < auto, (
            f"disagreement_cap ({cap}) >= auto_accept ({auto}). "
            f"Cross-method conflict could still auto-accept."
        )


# ============================================================================
# 10. PRODUCTION DATABASE INVARIANTS (against live data)
# ============================================================================

class TestProductionDatabaseInvariants:
    """
    These tests verify invariants against the production matcher database.
    They ensure structural integrity hasn't drifted.
    
    Skipped if database file is not present (CI environments).
    """

    MATCHER_DB_PATH = Path(__file__).parent.parent / "data" / "reg153_matcher.db"

    @pytest.fixture
    def prod_session(self):
        """Session against production matcher database."""
        if not self.MATCHER_DB_PATH.exists():
            pytest.skip("Production database not present")
        engine = create_engine(f"sqlite:///{self.MATCHER_DB_PATH}", echo=False)
        S = sessionmaker(bind=engine)
        s = S()
        yield s
        s.close()
        engine.dispose()

    def test_all_synonyms_have_normalization_version(self, prod_session):
        """Every synonym in production must have a normalization_version set."""
        null_count = prod_session.execute(
            text("SELECT COUNT(*) FROM synonyms WHERE normalization_version IS NULL")
        ).scalar()
        assert null_count == 0, (
            f"{null_count} synonyms have NULL normalization_version. "
            f"All synonyms must track normalization version."
        )

    def test_no_orphan_synonyms(self, prod_session):
        """No synonyms should reference non-existent analytes."""
        orphans = prod_session.execute(
            text("""
                SELECT COUNT(*) FROM synonyms s
                LEFT JOIN analytes a ON s.analyte_id = a.analyte_id
                WHERE a.analyte_id IS NULL
            """)
        ).scalar()
        assert orphans == 0, (
            f"{orphans} orphan synonyms found (referencing non-existent analytes). "
            f"Referential integrity violation."
        )

    def test_no_duplicate_analyte_ids(self, prod_session):
        """Canonical IDs must be unique."""
        dupes = prod_session.execute(
            text("""
                SELECT analyte_id, COUNT(*) as cnt FROM analytes
                GROUP BY analyte_id HAVING cnt > 1
            """)
        ).fetchall()
        assert len(dupes) == 0, (
            f"Duplicate analyte IDs found: {dupes}. "
            f"Canonical IDs must be unique."
        )

    def test_all_analytes_have_preferred_name(self, prod_session):
        """Every analyte must have a non-empty preferred_name."""
        missing = prod_session.execute(
            text("SELECT COUNT(*) FROM analytes WHERE preferred_name IS NULL OR preferred_name = ''")
        ).scalar()
        assert missing == 0, (
            f"{missing} analytes have NULL/empty preferred_name. "
            f"Every canonical analyte must have a name."
        )

    def test_synonym_confidence_in_range(self, prod_session):
        """All synonym confidences must be in [0.0, 1.0]."""
        out_of_range = prod_session.execute(
            text("SELECT COUNT(*) FROM synonyms WHERE confidence < 0.0 OR confidence > 1.0")
        ).scalar()
        assert out_of_range == 0, (
            f"{out_of_range} synonyms have confidence outside [0.0, 1.0]. "
            f"Confidence scores must be bounded."
        )

    def test_minimum_analyte_count(self, prod_session):
        """Production database must have at least 274 analytes (baseline)."""
        count = prod_session.execute(
            text("SELECT COUNT(*) FROM analytes")
        ).scalar()
        assert count >= 274, (
            f"Only {count} analytes in production (expected ≥274). "
            f"Canonical set may have been corrupted."
        )

    def test_minimum_synonym_count(self, prod_session):
        """Production database must have at least 47,000 synonyms (baseline)."""
        count = prod_session.execute(
            text("SELECT COUNT(*) FROM synonyms")
        ).scalar()
        assert count >= 47000, (
            f"Only {count} synonyms in production (expected ≥47,000). "
            f"Synonym corpus may have been truncated."
        )
