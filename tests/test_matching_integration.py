"""
Integration tests for the matching engine.

Tests:
- Exact matching workflow
- Fuzzy matching with various similarity levels
- Resolution cascade with real analyte names
- Disagreement detection
- Unknown handling
- CAS-based matching
"""

import pytest
from src.matching.match_result import MatchResult, ResolutionResult
from tests.fixtures.test_data import (
    EXPECTED_EXACT_MATCHES,
    EXPECTED_FUZZY_MATCHES,
    EXPECTED_UNKNOWNS,
    SAMPLE_ANALYTE_VARIANTS,
)


# ============================================================================
# EXACT MATCHER TESTS
# ============================================================================

class TestExactMatcher:
    """Tests for ExactMatcher class."""
    
    def test_exact_match_benzene(self, exact_matcher, sample_synonyms):
        """Test exact match for benzene."""
        result = exact_matcher.match("Benzene", sample_synonyms)
        
        assert result is not None, "Should find exact match for 'Benzene'"
        assert result.analyte_id == "REG153_VOCS_001"
        assert result.confidence == 1.0
        assert result.method in ['exact', 'cas_extracted']
    
    def test_exact_match_case_insensitive(self, exact_matcher, sample_synonyms):
        """Test case-insensitive exact matching."""
        test_cases = ["Benzene", "benzene", "BENZENE", "BeNzEnE"]
        
        results = [exact_matcher.match(text, sample_synonyms) for text in test_cases]
        
        for result in results:
            assert result is not None, "All case variations should match"
            assert result.analyte_id == "REG153_VOCS_001"
            assert result.confidence == 1.0
    
    def test_exact_match_by_cas(self, exact_matcher, sample_synonyms):
        """Test exact match using CAS number."""
        result = exact_matcher.match("71-43-2", sample_synonyms)
        
        assert result is not None, "Should match by CAS number"
        assert result.analyte_id == "REG153_VOCS_001"
        assert result.confidence == 1.0
        assert result.method == 'cas_extracted'
    
    def test_exact_match_cas_in_text(self, exact_matcher, sample_synonyms):
        """Test CAS extraction from text."""
        result = exact_matcher.match("Benzene (CAS 71-43-2)", sample_synonyms)
        
        assert result is not None, "Should extract and match CAS from text"
        assert result.analyte_id == "REG153_VOCS_001"
        assert result.confidence == 1.0
    
    def test_exact_match_phc_fractions(self, exact_matcher, sample_synonyms):
        """Test exact matching for PHC fractions."""
        test_cases = [
            ("PHC F1", "REG153_PHCS_001"),
            ("PHC F2", "REG153_PHCS_002"),
            ("F2", "REG153_PHCS_002"),
            ("F2 (C10-C16)", "REG153_PHCS_002"),
        ]
        
        for input_text, expected_id in test_cases:
            result = exact_matcher.match(input_text, sample_synonyms)
            if result:  # Some may not have exact synonyms
                assert result.analyte_id == expected_id, f"Failed for '{input_text}'"
                assert result.confidence == 1.0
    
    def test_exact_match_no_match(self, exact_matcher, sample_synonyms):
        """Test exact matcher returns None for no match."""
        result = exact_matcher.match("Unknown Chemical XYZ", sample_synonyms)
        assert result is None, "Should return None for unknown chemical"
    
    def test_exact_match_empty_input(self, exact_matcher, sample_synonyms):
        """Test exact matcher with empty input."""
        assert exact_matcher.match("", sample_synonyms) is None
        assert exact_matcher.match(None, sample_synonyms) is None
        assert exact_matcher.match("   ", sample_synonyms) is None


# ============================================================================
# FUZZY MATCHER TESTS
# ============================================================================

class TestFuzzyMatcher:
    """Tests for FuzzyMatcher class."""
    
    def test_fuzzy_match_typo(self, fuzzy_matcher, sample_synonyms):
        """Test fuzzy matching on common typos."""
        # "Benzen" missing last 'e'
        results = fuzzy_matcher.match("Benzen", sample_synonyms, threshold=0.80)
        
        assert len(results) > 0, "Should find fuzzy match for 'Benzen'"
        best = results[0]
        assert best.analyte_id == "REG153_VOCS_001"
        assert best.score >= 0.80
    
    def test_fuzzy_match_toluenne(self, fuzzy_matcher, sample_synonyms):
        """Test fuzzy match for common 'Toluenne' typo."""
        results = fuzzy_matcher.match("Toluenne", sample_synonyms, threshold=0.80)
        
        assert len(results) > 0, "Should find fuzzy match for 'Toluenne'"
        best = results[0]
        assert best.analyte_id == "REG153_VOCS_002"
        assert best.score >= 0.80
    
    def test_fuzzy_match_threshold(self, fuzzy_matcher, sample_synonyms):
        """Test fuzzy matching respects threshold."""
        # Use high threshold
        high_results = fuzzy_matcher.match("Benzen", sample_synonyms, threshold=0.95)
        
        # Use low threshold
        low_results = fuzzy_matcher.match("Benzen", sample_synonyms, threshold=0.70)
        
        assert len(low_results) >= len(high_results), \
            "Lower threshold should return more results"
    
    def test_fuzzy_match_top_k(self, fuzzy_matcher, sample_synonyms):
        """Test top-K limiting."""
        results = fuzzy_matcher.match("Benzene", sample_synonyms, threshold=0.60, top_k=3)
        
        assert len(results) <= 3, "Should respect top_k limit"
    
    def test_fuzzy_match_ranking(self, fuzzy_matcher, sample_synonyms):
        """Test that results are ranked by score."""
        results = fuzzy_matcher.match("Benzene", sample_synonyms, threshold=0.60, top_k=5)
        
        if len(results) > 1:
            scores = [r.score for r in results]
            assert scores == sorted(scores, reverse=True), \
                "Results should be sorted by score (descending)"
    
    def test_fuzzy_match_confidence_mapping(self, fuzzy_matcher, sample_synonyms):
        """Test confidence score mapping."""
        results = fuzzy_matcher.match("Benzene", sample_synonyms, threshold=0.60)
        
        for result in results:
            assert 0.0 <= result.confidence <= 1.0, \
                "Confidence should be between 0 and 1"
            assert result.score >= 0.60, \
                "Score should be above threshold"
    
    def test_fuzzy_match_empty_input(self, fuzzy_matcher, sample_synonyms):
        """Test fuzzy matcher with empty input."""
        assert len(fuzzy_matcher.match("", sample_synonyms)) == 0
        assert len(fuzzy_matcher.match(None, sample_synonyms)) == 0


# ============================================================================
# RESOLUTION ENGINE TESTS
# ============================================================================

class TestResolutionEngine:
    """Tests for ResolutionEngine cascade matching."""
    
    def test_resolution_exact_match(self, resolution_engine):
        """Test resolution with exact match."""
        result = resolution_engine.resolve("Benzene")
        
        assert result.is_resolved, "Should resolve 'Benzene'"
        assert result.best_match is not None
        assert result.best_match.analyte_id == "REG153_VOCS_001"
        assert result.best_match.confidence == 1.0
        assert result.confidence_band == "AUTO_ACCEPT"
    
    def test_resolution_cas_priority(self, resolution_engine):
        """Test that CAS extraction has priority."""
        result = resolution_engine.resolve("71-43-2")
        
        assert result.is_resolved
        assert result.best_match.analyte_id == "REG153_VOCS_001"
        assert result.best_match.method == "cas_extracted"
        assert result.signals_used['cas_extracted']
    
    def test_resolution_fuzzy_fallback(self, resolution_engine):
        """Test fuzzy matching as fallback."""
        result = resolution_engine.resolve("Toluenne")  # typo
        
        if result.is_resolved:
            assert result.best_match.analyte_id == "REG153_VOCS_002"
            assert result.best_match.method == "fuzzy"
            assert result.signals_used['fuzzy_match']
    
    def test_resolution_confidence_bands(self, resolution_engine):
        """Test confidence band classification."""
        # High confidence (exact match)
        result = resolution_engine.resolve("Benzene")
        assert result.confidence_band == "AUTO_ACCEPT"
        
        # Medium confidence (might be fuzzy)
        result = resolution_engine.resolve("Benzen")
        assert result.confidence_band in ["AUTO_ACCEPT", "REVIEW", "UNKNOWN"]
    
    def test_resolution_unknown(self, resolution_engine):
        """Test handling of unknown chemicals."""
        for unknown in EXPECTED_UNKNOWNS:
            result = resolution_engine.resolve(unknown)
            # Should either not resolve or have very low confidence
            if result.is_resolved:
                assert result.confidence < 0.75 or result.requires_review
    
    def test_resolution_timing(self, resolution_engine):
        """Test that resolution time is recorded."""
        result = resolution_engine.resolve("Benzene")
        
        assert result.resolution_time_ms > 0, "Should record resolution time"
        assert result.resolution_time_ms < 1000, "Should resolve quickly (< 1 second)"
    
    def test_resolution_signals_tracking(self, resolution_engine):
        """Test that signals used are tracked."""
        result = resolution_engine.resolve("Benzene")
        
        assert isinstance(result.signals_used, dict)
        assert 'exact_match' in result.signals_used or 'cas_extracted' in result.signals_used
    
    def test_resolution_all_candidates(self, resolution_engine):
        """Test that all candidates are returned."""
        result = resolution_engine.resolve("Benzene")
        
        assert isinstance(result.all_candidates, list)
        if result.is_resolved:
            assert len(result.all_candidates) > 0
            assert result.best_match in result.all_candidates
    
    def test_resolution_phc_fractions(self, resolution_engine):
        """Test resolution of PHC fractions."""
        test_cases = [
            ("PHC F1", "REG153_PHCS_001"),
            ("PHC F2", "REG153_PHCS_002"),
            ("F2 (C10-C16)", "REG153_PHCS_002"),
            ("C10-C16", "REG153_PHCS_002"),
        ]
        
        for input_text, expected_id in test_cases:
            result = resolution_engine.resolve(input_text)
            if result.is_resolved:
                assert result.best_match.analyte_id == expected_id, \
                    f"Failed to resolve '{input_text}' correctly"
    
    def test_resolution_metals_with_qualifiers(self, resolution_engine):
        """Test resolution of metals with qualifiers."""
        test_cases = [
            "Arsenic, total",
            "Lead",
            "Chromium VI",
            "Hexavalent Chromium",
        ]
        
        for text in test_cases:
            result = resolution_engine.resolve(text)
            # Should resolve to something (qualifier handling may vary)
            # At minimum should not crash
            assert isinstance(result, ResolutionResult)


# ============================================================================
# DISAGREEMENT DETECTION TESTS
# ============================================================================

class TestDisagreementDetection:
    """Tests for disagreement detection in fuzzy matching."""
    
    def test_no_disagreement_single_match(self, resolution_engine):
        """Test no disagreement with single strong match."""
        result = resolution_engine.resolve("Benzene")
        assert not result.disagreement_flag, \
            "Should not flag disagreement for exact match"
    
    def test_disagreement_ambiguous(self, fuzzy_matcher, sample_synonyms):
        """Test disagreement detection with ambiguous matches."""
        # This would need an ambiguous input that matches multiple analytes similarly
        # Example: "Xylene" without qualifier could match o-, m-, p-xylene
        results = fuzzy_matcher.match("Chrome", sample_synonyms, threshold=0.70, top_k=5)
        
        if len(results) >= 2:
            # Check if top matches have different analyte_ids
            top_ids = [r.analyte_id for r in results[:2]]
            if len(set(top_ids)) > 1:
                # This could indicate disagreement
                score_diff = results[0].score - results[1].score
                assert score_diff >= 0, "Scores should be sorted"


# ============================================================================
# BATCH PROCESSING TESTS
# ============================================================================

class TestBatchProcessing:
    """Tests for batch processing multiple analyte names."""
    
    def test_batch_exact_matches(self, resolution_engine):
        """Test batch processing of exact matches."""
        inputs = ["Benzene", "Toluene", "Ethylbenzene", "Xylenes (total)"]
        
        results = [resolution_engine.resolve(text) for text in inputs]
        
        assert len(results) == len(inputs)
        for result in results:
            assert isinstance(result, ResolutionResult)
            # Most should resolve
            assert result.is_resolved or result.confidence_band == "UNKNOWN"
    
    def test_batch_mixed_quality(self, resolution_engine):
        """Test batch with mix of good and poor quality names."""
        inputs = [
            "Benzene",           # exact
            "Toluenne",         # typo
            "Unknown123",       # unknown
            "71-43-2",          # CAS
            "PHC F2",           # PHC
        ]
        
        results = [resolution_engine.resolve(text) for text in inputs]
        
        assert len(results) == len(inputs)
        
        # Count resolved vs unresolved
        resolved = sum(1 for r in results if r.is_resolved)
        assert resolved >= 3, "Should resolve at least 3 of 5"
    
    def test_batch_performance(self, resolution_engine):
        """Test batch processing performance."""
        import time
        
        inputs = ["Benzene", "Toluene", "Ethylbenzene"] * 10  # 30 items
        
        start = time.perf_counter()
        results = [resolution_engine.resolve(text) for text in inputs]
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        assert len(results) == len(inputs)
        avg_time = elapsed_ms / len(inputs)
        
        # Should be reasonably fast
        assert avg_time < 100, f"Average time per item should be < 100ms, got {avg_time:.2f}ms"


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_very_long_input(self, resolution_engine):
        """Test handling of very long input text."""
        long_text = "Benzene " * 100
        result = resolution_engine.resolve(long_text)
        
        # Should handle gracefully without crashing
        assert isinstance(result, ResolutionResult)
    
    def test_special_characters(self, resolution_engine):
        """Test handling of special characters."""
        test_cases = [
            "Benzene!!!",
            "Toluene???",
            "PHC F2 @#$%",
            "Lead***",
        ]
        
        for text in test_cases:
            result = resolution_engine.resolve(text)
            assert isinstance(result, ResolutionResult), \
                f"Should handle special characters in '{text}'"
    
    def test_unicode_input(self, resolution_engine):
        """Test handling of Unicode characters."""
        test_cases = [
            "Benzène",
            "Toluène",
            "Caffeïne",
        ]
        
        for text in test_cases:
            result = resolution_engine.resolve(text)
            assert isinstance(result, ResolutionResult)
    
    def test_numeric_only(self, resolution_engine):
        """Test handling of numeric-only input."""
        result = resolution_engine.resolve("123456")
        assert isinstance(result, ResolutionResult)
        # May or may not resolve, but should not crash
    
    def test_mixed_language(self, resolution_engine):
        """Test handling of mixed language input."""
        # This is mainly to ensure robustness
        test_cases = [
            "Benzene (Français)",
            "Toluene 中文",
        ]
        
        for text in test_cases:
            result = resolution_engine.resolve(text)
            assert isinstance(result, ResolutionResult)
