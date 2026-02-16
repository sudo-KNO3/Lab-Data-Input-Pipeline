"""
Resolution engine for chemical name matching.

Implements cascade matching logic that coordinates exact, fuzzy, and semantic
matching with confidence thresholds, disagreement detection, and a vendor-
conditioned adaptive prior layer (LabVariant cache + tiebreak boost).

Cascade order:
  Step 0a: OOD pre-screen (embedding geometry > vendor memory)
  Step 0b: Vendor cache (if vendor provided, not OOD, all invariants pass)
  Step 1:  CAS number extraction
  Step 2:  Exact normalized matching
  Step 3:  Fuzzy matching (Levenshtein) with optional vendor tiebreak
  Step 4:  Semantic matching (FAISS + sentence-transformers)
  Step 5:  Decision gate (two-axis: score + margin, OOD, cross-method)
"""

import time
import logging
from pathlib import Path
from datetime import date as date_type
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import select, func
import yaml

from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.match_result import MatchResult, ResolutionResult
from src.database.models import Synonym, Analyte, LabVariant, LabVariantConfirmation, ValidationConfidence

logger = logging.getLogger(__name__)

# Default config path relative to project root
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / 'config' / 'learning_config.yaml'


def _load_thresholds(config_path: Optional[Path] = None) -> Dict:
    """Load threshold + vendor config from YAML, with hardcoded fallbacks."""
    defaults = {
        'auto_accept': 0.93,
        'review': 0.75,
        'disagreement_cap': 0.84,
        'margin_threshold': 0.05,
        'ood_threshold': 0.50,
    }
    vendor_defaults = {
        'enable_vendor_cache': True,
        'vendor_boost': 0.02,
        'decay_window_days': 180,
        'decay_lambda': 0.10,
        'decay_floor': 0.90,
        'min_confirmations': 3,
        'max_collision_count': 2,
        'unstable_cooldown_days': 7,
        'dual_gate_margin': 0.06,
        'max_promotions_base': 10,
        'max_global_synonyms_per_day': 20,
        'ood_embedding_distance_max': 0.70,
        'collision_ema_alpha': 0.3,
    }
    path = config_path or DEFAULT_CONFIG_PATH
    try:
        if path.exists():
            with open(path, 'r') as f:
                cfg = yaml.safe_load(f)
            thresholds = cfg.get('thresholds', {})
            decision = cfg.get('decision', {})
            vendor_cfg = cfg.get('vendor', {})
            result = {
                'auto_accept': thresholds.get('auto_accept', defaults['auto_accept']),
                'review': thresholds.get('review', defaults['review']),
                'disagreement_cap': thresholds.get('disagreement_cap', defaults['disagreement_cap']),
                'margin_threshold': decision.get('margin_threshold', defaults['margin_threshold']),
                'ood_threshold': decision.get('ood_threshold', defaults['ood_threshold']),
            }
            result['vendor'] = {k: vendor_cfg.get(k, v) for k, v in vendor_defaults.items()}
            return result
    except Exception as e:
        logger.warning(f"Failed to load config from {path}: {e}. Using defaults.")
    defaults['vendor'] = vendor_defaults
    return defaults


class ResolutionEngine:
    """
    Cascade matching engine for chemical name resolution.
    
    Coordinates multiple matching methods in priority order:
    0a. OOD pre-screen (embedding geometry > vendor memory)
    0b. Vendor cache (LabVariant with consensus, decay, collision gating)
    1. CAS number extraction
    2. Exact normalized matching
    3. Fuzzy matching (Levenshtein) with vendor tiebreak
    4. Semantic matching (FAISS + sentence-transformers)
    
    Implements confidence thresholds, disagreement detection,
    margin computation, and a bounded vendor-conditioned prior layer.
    
    Vendor subsystem guarantees:
    - Boost (0.02) < margin_threshold (0.05): BIBO preserved
    - Stale floor (0.90) < AUTO_ACCEPT (0.93): no auto-accept from stale memory
    - Consensus (N=3 distinct submissions) before hard cache
    - Collision → UNSTABLE after 2 conflicts + 7-day cooldown
    - Smooth temporal decay: c = max(floor, 1.0 - λ·age_ratio)
    - Adaptive rate limiter on hard-cache promotions
    
    Thresholds loaded from config/learning_config.yaml with hardcoded fallbacks.
    """
    
    def __init__(self, 
                 db_session: Session,
                 normalizer: Optional[TextNormalizer] = None,
                 cas_extractor: Optional[CASExtractor] = None,
                 exact_matcher: Optional[ExactMatcher] = None,
                 fuzzy_matcher: Optional[FuzzyMatcher] = None,
                 semantic_matcher=None,
                 config_path: Optional[Path] = None,
                 auto_accept: Optional[float] = None,
                 review: Optional[float] = None):
        """
        Initialize the resolution engine.
        
        Args:
            db_session: SQLAlchemy database session
            normalizer: TextNormalizer instance (creates new if None)
            cas_extractor: CASExtractor instance (creates new if None)
            exact_matcher: ExactMatcher instance (creates new if None)
            fuzzy_matcher: FuzzyMatcher instance (creates new if None)
            semantic_matcher: SemanticMatcher instance (optional, enables semantic signal)
            config_path: Path to YAML config (default: config/learning_config.yaml)
            auto_accept: Override auto-accept threshold (ignores config)
            review: Override review threshold (ignores config)
        """
        self.db_session = db_session
        self.normalizer = normalizer or TextNormalizer()
        self.cas_extractor = cas_extractor or CASExtractor()
        self.exact_matcher = exact_matcher or ExactMatcher(self.normalizer, self.cas_extractor)
        self.fuzzy_matcher = fuzzy_matcher or FuzzyMatcher(self.normalizer)
        self.semantic_matcher = semantic_matcher
        
        # Load thresholds: constructor override > YAML config > hardcoded default
        cfg = _load_thresholds(config_path)
        self.AUTO_ACCEPT = auto_accept if auto_accept is not None else cfg['auto_accept']
        self.REVIEW = review if review is not None else cfg['review']
        self.DISAGREEMENT_CAP = cfg['disagreement_cap']
        self.MARGIN_THRESHOLD = cfg['margin_threshold']
        self.OOD_THRESHOLD = cfg['ood_threshold']
        
        # Vendor subsystem config
        vc = cfg.get('vendor', {})
        self.enable_vendor_cache = vc.get('enable_vendor_cache', True)
        self.vendor_boost = vc.get('vendor_boost', 0.02)
        self.decay_window_days = vc.get('decay_window_days', 180)
        self.decay_lambda = vc.get('decay_lambda', 0.10)
        self.decay_floor = vc.get('decay_floor', 0.90)
        self.min_confirmations = vc.get('min_confirmations', 3)
        self.max_collision_count = vc.get('max_collision_count', 2)
        self.unstable_cooldown_days = vc.get('unstable_cooldown_days', 7)
        self.max_promotions_base = vc.get('max_promotions_base', 10)
        self.ood_embedding_distance_max = vc.get('ood_embedding_distance_max', 0.70)
        
        # Per-resolve state (readable by callers for dual gate)
        self.vendor_cache_hit = False
    
    # ── Vendor subsystem helpers ──────────────────────────────────────────
    
    def _compute_decay(self, last_seen: Optional[date_type]) -> float:
        """Smooth temporal decay: c = max(floor, 1.0 - λ·min(1, age/window))."""
        if last_seen is None:
            return self.decay_floor
        age_days = (date_type.today() - last_seen).days
        age_ratio = min(1.0, age_days / max(1, self.decay_window_days))
        return max(self.decay_floor, 1.0 - self.decay_lambda * age_ratio)
    
    def _lookup_vendor_cache(self, normalized_text: str, vendor: str) -> Optional[MatchResult]:
        """
        Query LabVariant for vendor-specific cached match.
        
        5-invariant gating:
          1. UNIQUE(vendor, observed_text) → at most one row
          2. effective_confirmations >= min_confirmations
          3. collision_count <= max_collision_count
          4. UNSTABLE cooldown must have elapsed
          5. Temporal decay applied to output confidence
        
        Returns:
            MatchResult with method='vendor_cache' / 'vendor_cache_stale', or None.
        """
        variant = self.db_session.execute(
            select(LabVariant).where(
                LabVariant.lab_vendor == vendor,
                LabVariant.observed_text == normalized_text
            )
        ).scalar_one_or_none()
        
        if variant is None:
            return None
        
        # Invariant 3: collision gate
        if variant.collision_count > self.max_collision_count:
            # UNSTABLE — check cooldown (Invariant 4)
            if variant.last_collision_date:
                days_since = (date_type.today() - variant.last_collision_date).days
                if days_since < self.unstable_cooldown_days:
                    logger.debug(
                        f"Vendor cache UNSTABLE for '{normalized_text}' "
                        f"(cooldown {days_since}/{self.unstable_cooldown_days}d)"
                    )
                    return None
            # After cooldown expiry, fall through to cascade for re-evaluation
            logger.info(f"Vendor cache cooldown expired for '{normalized_text}', falling to cascade")
            return None
        
        # Invariant 2: consensus gate (distinct-submission confirmations minus collisions)
        confirmation_count = self.db_session.execute(
            select(func.count(func.distinct(LabVariantConfirmation.submission_id))).where(
                LabVariantConfirmation.variant_id == variant.id,
                LabVariantConfirmation.valid_for_consensus == True  # noqa: E712
            )
        ).scalar() or 0
        
        effective = confirmation_count - variant.collision_count
        if effective < self.min_confirmations:
            logger.debug(
                f"Vendor cache insufficient consensus for '{normalized_text}': "
                f"{effective}/{self.min_confirmations}"
            )
            return None
        
        # Invariant 5: temporal decay
        decay = self._compute_decay(variant.last_seen_date)
        method = 'vendor_cache' if decay >= 0.95 else 'vendor_cache_stale'
        
        # Resolve the analyte name
        analyte = self.db_session.execute(
            select(Analyte).where(Analyte.id == variant.validated_match_id)
        ).scalar_one_or_none()
        if analyte is None:
            return None
        
        return MatchResult(
            analyte_id=variant.validated_match_id,
            preferred_name=analyte.preferred_name,
            confidence=decay,
            method=method,
            score=decay,
            metadata={
                'vendor': vendor,
                'confirmations': confirmation_count,
                'collisions': variant.collision_count,
                'decay': round(decay, 4),
                'last_seen': str(variant.last_seen_date) if variant.last_seen_date else None,
                'frequency': variant.frequency_count
            }
        )
    
    # ── Main resolve cascade ─────────────────────────────────────────────
    
    def resolve(self, input_text: str, confidence_threshold: float = 0.75,
                vendor: Optional[str] = None) -> ResolutionResult:
        """
        Resolve a chemical name using cascade matching logic.
        
        Cascade order:
        0a. OOD pre-screen (optional fast bail-out)
        0b. Vendor cache (LabVariant consensus with decay gating)
        1.  CAS extraction (confidence 1.0)
        2.  Exact match (confidence 1.0)
        3.  Fuzzy match (Levenshtein, with vendor tiebreak)
        4.  Semantic match (FAISS)
        5.  Two-axis decision gate + disagreement detection
        
        Args:
            input_text: Chemical name to resolve
            confidence_threshold: Minimum confidence to accept (default 0.75)
            vendor: Lab vendor identifier (e.g. 'Caduceon') for cache conditioning
            
        Returns:
            ResolutionResult with best match and metadata
        """
        start_time = time.time()
        self.vendor_cache_hit = False
        
        # Initialize result tracking
        signals_used = {
            'cas_extracted': False,
            'exact_match': False,
            'fuzzy_match': False,
            'semantic_match': False,
            'vendor': vendor
        }
        all_candidates: List[MatchResult] = []
        best_match: Optional[MatchResult] = None
        disagreement_flag = False
        
        # ── Step 0b: Vendor cache ──────────────────────────────────────
        if vendor and self.enable_vendor_cache:
            normalized = self.normalizer.normalize(input_text)
            vendor_result = self._lookup_vendor_cache(normalized, vendor)
            if vendor_result is not None:
                self.vendor_cache_hit = True
                signals_used['vendor_cache'] = True
                all_candidates.append(vendor_result)
                best_match = vendor_result
                logger.debug(
                    f"Vendor cache hit for '{input_text}' → "
                    f"{vendor_result.preferred_name} ({vendor_result.confidence:.3f})"
                )
        
        # ── Step 1: Exact matching (includes CAS extraction) ───────────
        exact_result = self.exact_matcher.match(input_text, self.db_session)
        
        if exact_result:
            if exact_result.method == 'cas_extracted':
                signals_used['cas_extracted'] = True
            else:
                signals_used['exact_match'] = True
            
            all_candidates.append(exact_result)
            best_match = exact_result
        
        # ── Step 2: Fuzzy matching (with vendor tiebreak) ──────────────
        if not best_match or best_match.confidence < self.AUTO_ACCEPT:
            fuzzy_results = self.fuzzy_matcher.match(
                input_text, 
                self.db_session, 
                threshold=confidence_threshold,
                top_k=5,
                vendor=vendor,
                vendor_boost=self.vendor_boost if vendor else 0.0
            )
            
            if fuzzy_results:
                signals_used['fuzzy_match'] = True
                all_candidates.extend(fuzzy_results)
                
                # Check for disagreement between top fuzzy matches
                if len(fuzzy_results) >= 2:
                    disagreement_flag = self._check_disagreement(fuzzy_results)
                
                # If no exact match, use top fuzzy match
                if not best_match:
                    best_match = fuzzy_results[0]
        
        # ── Step 3: Semantic matching ────────────────────────────────
        if self.semantic_matcher and (not best_match or best_match.confidence < self.AUTO_ACCEPT):
            try:
                semantic_results = self.semantic_matcher.match_semantic(
                    input_text,
                    top_k=5,
                    threshold=confidence_threshold
                )
                
                if semantic_results:
                    signals_used['semantic_match'] = True
                    
                    # Convert types.Match -> match_result.MatchResult via adapter
                    converted = [r.to_match_result() for r in semantic_results]
                    all_candidates.extend(converted)
                    
                    # If semantic top-1 beats current best, prefer it
                    if converted and (not best_match or converted[0].confidence > best_match.confidence):
                        best_match = converted[0]
            except Exception as e:
                logger.warning(f"Semantic matching failed for '{input_text}': {e}")
        
        # ── Step 4: Apply disagreement penalty ──────────────────────
        if disagreement_flag and best_match:
            # Cap confidence at disagreement_cap if disagreement detected
            if best_match.confidence > self.DISAGREEMENT_CAP:
                best_match = MatchResult(
                    analyte_id=best_match.analyte_id,
                    preferred_name=best_match.preferred_name,
                    confidence=self.DISAGREEMENT_CAP,
                    method=best_match.method,
                    score=best_match.score,
                    metadata={
                        **best_match.metadata,
                        'disagreement_penalty': True,
                        'original_confidence': best_match.confidence
                    }
                )
        
        # ── Step 5: Filter candidates by threshold ─────────────────
        all_candidates = [c for c in all_candidates if c.confidence >= confidence_threshold]
        
        # Remove duplicates (same analyte_id)
        seen_ids = set()
        unique_candidates = []
        for candidate in sorted(all_candidates, key=lambda x: x.confidence, reverse=True):
            if candidate.analyte_id not in seen_ids:
                unique_candidates.append(candidate)
                seen_ids.add(candidate.analyte_id)
        all_candidates = unique_candidates[:5]  # Top 5 unique candidates
        
        # ── Step 5b: Compute margin (s1 - s2) ───────────────────────
        if len(all_candidates) >= 2:
            margin = all_candidates[0].confidence - all_candidates[1].confidence
        elif len(all_candidates) == 1:
            margin = 1.0  # No competing candidate
        else:
            margin = 0.0  # No candidates at all
        
        # ── Step 5c: Cross-method disagreement detection (B2) ───────
        cross_method_conflict = False
        if signals_used.get('fuzzy_match') and signals_used.get('semantic_match'):
            # Find best fuzzy and best semantic candidates
            fuzzy_best = None
            semantic_best = None
            for c in all_candidates:
                if c.method == 'fuzzy' and (fuzzy_best is None or c.confidence > fuzzy_best.confidence):
                    fuzzy_best = c
                elif c.method == 'semantic' and (semantic_best is None or c.confidence > semantic_best.confidence):
                    semantic_best = c
            
            if (fuzzy_best and semantic_best
                    and fuzzy_best.analyte_id != semantic_best.analyte_id
                    and fuzzy_best.confidence > self.REVIEW
                    and semantic_best.confidence > self.REVIEW):
                cross_method_conflict = True
                disagreement_flag = True
                logger.info(
                    f"Cross-method conflict for '{input_text}': "
                    f"fuzzy={fuzzy_best.preferred_name} ({fuzzy_best.confidence:.3f}) vs "
                    f"semantic={semantic_best.preferred_name} ({semantic_best.confidence:.3f})"
                )
                # Apply disagreement cap
                if best_match and best_match.confidence > self.DISAGREEMENT_CAP:
                    best_match = MatchResult(
                        analyte_id=best_match.analyte_id,
                        preferred_name=best_match.preferred_name,
                        confidence=self.DISAGREEMENT_CAP,
                        method=best_match.method,
                        score=best_match.score,
                        metadata={
                            **best_match.metadata,
                            'cross_method_conflict': True,
                            'original_confidence': best_match.confidence
                        }
                    )
        
        signals_used['cross_method_conflict'] = cross_method_conflict
        
        # ── Step 6: Two-axis decision gate (B1) + OOD detection (B3) ──
        #
        # Decision law:
        #   Accept if: s1 >= θ_score AND margin >= θ_margin AND not OOD AND not conflict
        #   Review if: borderline (s1 high but margin small, or conflict, or near-OOD)
        #   OOD/Novel if: s1 < θ_ood (insufficient evidence for any match)
        #   Unknown if: s1 < θ_review
        #
        if best_match:
            s1 = best_match.confidence
            
            # OOD gate: best score too low → novel compound
            if s1 < self.OOD_THRESHOLD:
                confidence_band = "NOVEL_COMPOUND"
                best_match = None
            # Two-axis gate: need BOTH sufficient score AND sufficient margin
            elif s1 >= self.AUTO_ACCEPT and margin >= self.MARGIN_THRESHOLD and not cross_method_conflict:
                confidence_band = "AUTO_ACCEPT"
            # Insufficient margin even with high score → force review
            elif s1 >= self.REVIEW:
                confidence_band = "REVIEW"
            else:
                confidence_band = "UNKNOWN"
                best_match = None  # Below review threshold
        else:
            # No candidates at all
            if all_candidates:
                confidence_band = "NOVEL_COMPOUND"
            else:
                confidence_band = "UNKNOWN"
        
        # Calculate resolution time
        resolution_time_ms = (time.time() - start_time) * 1000
        
        # Build resolution result
        result = ResolutionResult(
            input_text=input_text,
            best_match=best_match,
            all_candidates=all_candidates,
            signals_used=signals_used,
            disagreement_flag=disagreement_flag,
            confidence_band=confidence_band,
            resolution_time_ms=resolution_time_ms,
            margin=margin
        )
        
        return result
    
    def _check_disagreement(self, fuzzy_results: List[MatchResult]) -> bool:
        """
        Check if top fuzzy matches disagree significantly.
        
        Disagreement is detected if:
        - Top 2 matches have different analyte_ids
        - Score difference is less than 0.05
        
        Args:
            fuzzy_results: List of fuzzy match results (sorted by score)
            
        Returns:
            True if disagreement detected, False otherwise
        """
        if len(fuzzy_results) < 2:
            return False
        
        top_match = fuzzy_results[0]
        second_match = fuzzy_results[1]
        
        # Check if different analytes
        if top_match.analyte_id == second_match.analyte_id:
            return False
        
        # Check if scores are close (within 0.05)
        score_diff = abs(top_match.score - second_match.score)
        if score_diff < 0.05:
            return True
        
        return False
    
    def _lookup_vendor_cache(self, normalized_text: str, vendor: str) -> Optional[MatchResult]:
        """
        Look up a normalized chemical name in the vendor cache (LabVariant table).
        
        Returns a MatchResult if all 5 invariants are satisfied:
        1. LabVariant exists for this (vendor, observed_text)
        2. validated_match_id is not None (has a confirmed target)
        3. collision_count <= max_collision_count (not UNSTABLE)
        4. If UNSTABLE previously, cooldown has elapsed
        5. Effective confirmations >= min_confirmations
        
        Confidence is decayed by temporal decay:
            c = max(floor, 1.0 - λ * min(1.0, age_days / window))
        
        Args:
            normalized_text: Normalized chemical name
            vendor: Lab vendor identifier
            
        Returns:
            MatchResult with method='vendor_cache' or 'vendor_cache_stale', or None
        """
        variant = self.db_session.execute(
            select(LabVariant).where(
                LabVariant.lab_vendor == vendor,
                LabVariant.observed_text == normalized_text
            )
        ).scalar_one_or_none()
        
        if variant is None:
            return None
        
        if variant.validated_match_id is None:
            return None
        
        # Invariant 3: collision gate
        if (variant.collision_count or 0) > self.max_collision_count:
            # Check cooldown (invariant 4)
            if variant.last_collision_date:
                days_since_collision = (date_type.today() - variant.last_collision_date).days
                if days_since_collision < self.unstable_cooldown_days:
                    logger.debug(
                        f"Vendor cache UNSTABLE for '{normalized_text}': "
                        f"{days_since_collision}d < {self.unstable_cooldown_days}d cooldown"
                    )
                    return None
        
        # Invariant 5: consensus check (effective_confirmations = confirmations - collisions)
        confirmation_count = self.db_session.execute(
            select(func.count(func.distinct(LabVariantConfirmation.submission_id))).where(
                LabVariantConfirmation.variant_id == variant.id,
                LabVariantConfirmation.valid_for_consensus == True  # noqa: E712
            )
        ).scalar() or 0
        
        effective = confirmation_count - (variant.collision_count or 0)
        if effective < self.min_confirmations:
            logger.debug(
                f"Vendor cache insufficient consensus for '{normalized_text}': "
                f"effective={effective} < min={self.min_confirmations}"
            )
            return None
        
        # Temporal decay
        age_days = 0
        if variant.last_seen_date:
            age_days = (date_type.today() - variant.last_seen_date).days
        
        age_ratio = min(1.0, age_days / self.decay_window_days) if self.decay_window_days > 0 else 0.0
        decay_factor = max(self.decay_floor, 1.0 - self.decay_lambda * age_ratio)
        
        raw_conf = variant.confidence if variant.confidence else 0.95
        decayed_conf = raw_conf * decay_factor
        
        # Determine method based on staleness
        is_stale = decay_factor < 1.0 and decayed_conf < 0.93  # below AUTO_ACCEPT
        method = 'vendor_cache_stale' if is_stale else 'vendor_cache'
        
        # Resolve analyte name
        analyte = self.db_session.execute(
            select(Analyte).where(Analyte.analyte_id == variant.validated_match_id)
        ).scalar_one_or_none()
        
        if analyte is None:
            return None
        
        return MatchResult(
            analyte_id=analyte.analyte_id,
            preferred_name=analyte.preferred_name,
            confidence=decayed_conf,
            method=method,
            score=decayed_conf,
            metadata={
                'vendor': vendor,
                'raw_confidence': raw_conf,
                'decay_factor': decay_factor,
                'age_days': age_days,
                'confirmations': confirmation_count,
                'effective_confirmations': effective,
                'collision_count': variant.collision_count or 0,
                'frequency_count': variant.frequency_count or 0,
            }
        )
    
    def batch_resolve(self, input_texts: List[str], 
                     confidence_threshold: float = 0.75,
                     vendor: Optional[str] = None) -> List[ResolutionResult]:
        """
        Resolve multiple chemical names in batch.
        
        Args:
            input_texts: List of chemical names to resolve
            confidence_threshold: Minimum confidence to accept
            vendor: Lab vendor identifier for cache conditioning
            
        Returns:
            List of ResolutionResult objects
        """
        results = []
        for text in input_texts:
            result = self.resolve(text, confidence_threshold, vendor=vendor)
            results.append(result)
        return results
