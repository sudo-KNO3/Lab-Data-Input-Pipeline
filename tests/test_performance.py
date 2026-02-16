"""
Performance benchmarks for the chemical matcher system.

Tests:
- Exact matching speed (target: <10ms)
- Fuzzy matching speed (target: <50ms)
- Batch processing (target: 1000 rows in <30s)
- Memory usage
"""

import pytest
import time
import psutil
import os
from typing import List

from tests.fixtures.test_data import PERFORMANCE_BATCH_1000


# ============================================================================
# EXACT MATCHING PERFORMANCE
# ============================================================================

class TestExactMatchingPerformance:
    """Performance tests for exact matching."""
    
    def test_exact_match_speed_single(self, exact_matcher, sample_synonyms, performance_tracker):
        """Test single exact match speed (target: <10ms)."""
        iterations = 100
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                exact_matcher.match,
                "Benzene",
                sample_synonyms
            )
        
        avg_time = performance_tracker.avg_time()
        max_time = performance_tracker.max_time()
        
        print(f"\nExact match performance:")
        print(f"  Average: {avg_time:.2f}ms")
        print(f"  Max: {max_time:.2f}ms")
        
        assert avg_time < 10, f"Average exact match time {avg_time:.2f}ms exceeds 10ms target"
    
    def test_exact_match_speed_batch(self, exact_matcher, sample_synonyms):
        """Test batch exact matching speed."""
        inputs = ["Benzene", "Toluene", "Ethylbenzene", "Xylenes (total)"] * 25  # 100 items
        
        start = time.perf_counter()
        
        results = [exact_matcher.match(text, sample_synonyms) for text in inputs]
        
        elapsed_ms = (time.perf_counter() - start) * 1000
        avg_time = elapsed_ms / len(inputs)
        
        print(f"\nBatch exact match ({len(inputs)} items):")
        print(f"  Total: {elapsed_ms:.2f}ms")
        print(f"  Average: {avg_time:.2f}ms per item")
        
        assert avg_time < 10, f"Average batch exact match time {avg_time:.2f}ms exceeds 10ms target"
    
    def test_exact_match_cas_extraction(self, exact_matcher, sample_synonyms, performance_tracker):
        """Test CAS extraction performance."""
        iterations = 100
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                exact_matcher.match,
                "Benzene (CAS 71-43-2)",
                sample_synonyms
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nCAS extraction performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 15, f"CAS extraction time {avg_time:.2f}ms exceeds 15ms target"


# ============================================================================
# FUZZY MATCHING PERFORMANCE
# ============================================================================

class TestFuzzyMatchingPerformance:
    """Performance tests for fuzzy matching."""
    
    def test_fuzzy_match_speed_single(self, fuzzy_matcher, sample_synonyms, performance_tracker):
        """Test single fuzzy match speed (target: <50ms)."""
        iterations = 50
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                fuzzy_matcher.match,
                "Benzen",  # typo requiring fuzzy match
                sample_synonyms,
                threshold=0.80,
                top_k=5
            )
        
        avg_time = performance_tracker.avg_time()
        max_time = performance_tracker.max_time()
        
        print(f"\nFuzzy match performance:")
        print(f"  Average: {avg_time:.2f}ms")
        print(f"  Max: {max_time:.2f}ms")
        
        assert avg_time < 50, f"Average fuzzy match time {avg_time:.2f}ms exceeds 50ms target"
    
    def test_fuzzy_match_various_thresholds(self, fuzzy_matcher, sample_synonyms):
        """Test fuzzy matching with various thresholds."""
        thresholds = [0.95, 0.85, 0.75, 0.65]
        input_text = "Toluenne"  # typo
        
        results = {}
        
        for threshold in thresholds:
            start = time.perf_counter()
            matches = fuzzy_matcher.match(input_text, sample_synonyms, threshold=threshold, top_k=5)
            elapsed_ms = (time.perf_counter() - start) * 1000
            
            results[threshold] = {
                'time_ms': elapsed_ms,
                'matches': len(matches)
            }
        
        print(f"\nFuzzy match with varying thresholds:")
        for threshold, data in results.items():
            print(f"  Threshold {threshold}: {data['time_ms']:.2f}ms ({data['matches']} matches)")
        
        # All should be reasonably fast
        for data in results.values():
            assert data['time_ms'] < 100, "Fuzzy match too slow for any threshold"
    
    def test_fuzzy_match_top_k_impact(self, fuzzy_matcher, sample_synonyms):
        """Test impact of top_k parameter on performance."""
        top_k_values = [1, 3, 5, 10]
        input_text = "Benzene"
        
        results = {}
        
        for top_k in top_k_values:
            start = time.perf_counter()
            matches = fuzzy_matcher.match(input_text, sample_synonyms, threshold=0.75, top_k=top_k)
            elapsed_ms = (time.perf_counter() - start) * 1000
            
            results[top_k] = {
                'time_ms': elapsed_ms,
                'matches': len(matches)
            }
        
        print(f"\nFuzzy match with varying top_k:")
        for top_k, data in results.items():
            print(f"  top_k={top_k}: {data['time_ms']:.2f}ms ({data['matches']} matches)")
        
        # Performance should not degrade significantly
        assert all(data['time_ms'] < 100 for data in results.values())


# ============================================================================
# RESOLUTION ENGINE PERFORMANCE
# ============================================================================

class TestResolutionEnginePerformance:
    """Performance tests for the resolution engine."""
    
    def test_resolution_speed_exact(self, resolution_engine, performance_tracker):
        """Test resolution speed for exact matches."""
        iterations = 100
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                resolution_engine.resolve,
                "Benzene"
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nResolution (exact) performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 20, f"Resolution time {avg_time:.2f}ms exceeds 20ms target"
    
    def test_resolution_speed_fuzzy(self, resolution_engine, performance_tracker):
        """Test resolution speed for fuzzy matches."""
        iterations = 50
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                resolution_engine.resolve,
                "Toluenne"  # typo requiring fuzzy
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nResolution (fuzzy) performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 100, f"Fuzzy resolution time {avg_time:.2f}ms exceeds 100ms target"
    
    def test_resolution_timing_accuracy(self, resolution_engine):
        """Test accuracy of resolution timing measurement."""
        result = resolution_engine.resolve("Benzene")
        
        assert result.resolution_time_ms > 0, "Resolution time should be recorded"
        assert result.resolution_time_ms < 1000, "Resolution time seems unreasonably high"


# ============================================================================
# BATCH PROCESSING PERFORMANCE
# ============================================================================

class TestBatchProcessingPerformance:
    """Performance tests for batch processing."""
    
    def test_batch_100_items(self, resolution_engine):
        """Test processing 100 items (target: <5s)."""
        inputs = ["Benzene", "Toluene", "Ethylbenzene", "Xylenes (total)"] * 25
        
        start = time.perf_counter()
        results = [resolution_engine.resolve(text) for text in inputs]
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        avg_time = elapsed_ms / len(inputs)
        
        print(f"\nBatch processing (100 items):")
        print(f"  Total: {elapsed_ms:.2f}ms ({elapsed_ms/1000:.2f}s)")
        print(f"  Average: {avg_time:.2f}ms per item")
        print(f"  Throughput: {len(inputs)/(elapsed_ms/1000):.0f} items/sec")
        
        assert elapsed_ms < 5000, f"Batch processing {elapsed_ms:.2f}ms exceeds 5s target"
    
    @pytest.mark.slow
    def test_batch_1000_items(self, resolution_engine):
        """Test processing 1000 items (target: <30s)."""
        # Use standardized performance batch data
        inputs = [item['analyte'] for item in PERFORMANCE_BATCH_1000]
        
        start = time.perf_counter()
        results = [resolution_engine.resolve(text) for text in inputs]
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        avg_time = elapsed_ms / len(inputs)
        resolved_count = sum(1 for r in results if r.is_resolved)
        
        print(f"\nBatch processing (1000 items):")
        print(f"  Total: {elapsed_ms:.2f}ms ({elapsed_ms/1000:.2f}s)")
        print(f"  Average: {avg_time:.2f}ms per item")
        print(f"  Throughput: {len(inputs)/(elapsed_ms/1000):.0f} items/sec")
        print(f"  Resolved: {resolved_count}/{len(inputs)} ({resolved_count/len(inputs)*100:.1f}%)")
        
        assert elapsed_ms < 30000, f"Batch processing {elapsed_ms/1000:.2f}s exceeds 30s target"
    
    def test_batch_mixed_quality(self, resolution_engine):
        """Test batch with mixed quality inputs."""
        inputs = [
            "Benzene",           # exact - fast
            "Toluenne",         # fuzzy - slower
            "Unknown123",       # unknown - fast (no match)
            "PHC F2",           # exact - fast
            "71-43-2",          # CAS - fast
        ] * 20  # 100 items
        
        start = time.perf_counter()
        results = [resolution_engine.resolve(text) for text in inputs]
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        avg_time = elapsed_ms / len(inputs)
        
        print(f"\nBatch mixed quality (100 items):")
        print(f"  Total: {elapsed_ms:.2f}ms")
        print(f"  Average: {avg_time:.2f}ms per item")
        
        assert elapsed_ms < 10000, "Mixed batch too slow"


# ============================================================================
# MEMORY USAGE TESTS
# ============================================================================

class TestMemoryUsage:
    """Tests for memory usage and leaks."""
    
    def test_memory_usage_single_operation(self, resolution_engine):
        """Test memory usage for single operation."""
        process = psutil.Process(os.getpid())
        
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Perform operations
        for _ in range(100):
            resolution_engine.resolve("Benzene")
        
        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        mem_increase = mem_after - mem_before
        
        print(f"\nMemory usage (100 operations):")
        print(f"  Before: {mem_before:.2f} MB")
        print(f"  After: {mem_after:.2f} MB")
        print(f"  Increase: {mem_increase:.2f} MB")
        
        # Should not increase significantly
        assert mem_increase < 10, f"Memory increase {mem_increase:.2f}MB seems excessive"
    
    @pytest.mark.slow
    def test_memory_leak_detection(self, resolution_engine):
        """Test for memory leaks in batch processing."""
        process = psutil.Process(os.getpid())
        
        mem_initial = process.memory_info().rss / 1024 / 1024
        mem_samples = [mem_initial]
        
        # Run multiple batches
        batch_size = 100
        num_batches = 5
        
        for batch_num in range(num_batches):
            inputs = ["Benzene", "Toluene"] * (batch_size // 2)
            
            for text in inputs:
                resolution_engine.resolve(text)
            
            mem_current = process.memory_info().rss / 1024 / 1024
            mem_samples.append(mem_current)
        
        mem_final = mem_samples[-1]
        mem_total_increase = mem_final - mem_initial
        
        print(f"\nMemory leak test ({num_batches} batches of {batch_size}):")
        print(f"  Initial: {mem_initial:.2f} MB")
        print(f"  Final: {mem_final:.2f} MB")
        print(f"  Total increase: {mem_total_increase:.2f} MB")
        print(f"  Per batch: {mem_total_increase/num_batches:.2f} MB")
        
        # Memory should stabilize, not grow linearly
        avg_increase_per_batch = mem_total_increase / num_batches
        assert avg_increase_per_batch < 5, "Possible memory leak detected"
    
    def test_database_session_cleanup(self, sample_synonyms, exact_matcher):
        """Test that database sessions are properly cleaned up."""
        process = psutil.Process(os.getpid())
        
        mem_before = process.memory_info().rss / 1024 / 1024
        
        # Perform many database operations
        for _ in range(500):
            exact_matcher.match("Benzene", sample_synonyms)
        
        mem_after = process.memory_info().rss / 1024 / 1024
        mem_increase = mem_after - mem_before
        
        print(f"\nDatabase session cleanup test:")
        print(f"  Memory increase: {mem_increase:.2f} MB")
        
        # Should not accumulate sessions
        assert mem_increase < 20, f"Possible session leak: {mem_increase:.2f}MB increase"


# ============================================================================
# NORMALIZATION PERFORMANCE
# ============================================================================

class TestNormalizationPerformance:
    """Performance tests for text normalization."""
    
    def test_normalization_speed(self, text_normalizer, performance_tracker):
        """Test text normalization speed."""
        iterations = 1000
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                text_normalizer.normalize,
                "Benzene (CAS 71-43-2) - Total Dissolved"
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nNormalization performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 1, f"Normalization time {avg_time:.2f}ms exceeds 1ms target"
    
    def test_cas_extraction_speed(self, cas_extractor, performance_tracker):
        """Test CAS extraction speed."""
        iterations = 1000
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                cas_extractor.extract_cas,
                "Benzene (CAS 71-43-2)"
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nCAS extraction performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 1, f"CAS extraction time {avg_time:.2f}ms exceeds 1ms target"


# ============================================================================
# DATABASE QUERY PERFORMANCE
# ============================================================================

class TestDatabasePerformance:
    """Performance tests for database operations."""
    
    def test_query_analyte_by_id(self, sample_synonyms, performance_tracker):
        """Test analyte lookup by ID performance."""
        from src.database import crud_new as crud
        
        iterations = 1000
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                crud.get_analyte_by_id,
                sample_synonyms,
                "REG153_VOCS_001"
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nAnalyte lookup performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 1, f"Analyte lookup {avg_time:.2f}ms exceeds 1ms target"
    
    def test_query_synonyms(self, sample_synonyms, performance_tracker):
        """Test synonym query performance."""
        from src.database import crud_new as crud
        
        iterations = 500
        
        for _ in range(iterations):
            _, elapsed = performance_tracker.measure(
                crud.get_synonyms_by_analyte,
                sample_synonyms,
                "REG153_VOCS_001"
            )
        
        avg_time = performance_tracker.avg_time()
        
        print(f"\nSynonym query performance:")
        print(f"  Average: {avg_time:.2f}ms")
        
        assert avg_time < 5, f"Synonym query {avg_time:.2f}ms exceeds 5ms target"


# ============================================================================
# COMPREHENSIVE BENCHMARK
# ============================================================================

class TestComprehensiveBenchmark:
    """Comprehensive system benchmark."""
    
    @pytest.mark.slow
    def test_full_system_benchmark(self, resolution_engine):
        """Run comprehensive system benchmark."""
        test_cases = [
            ("Exact matches", ["Benzene", "Toluene", "Lead", "Arsenic"] * 25),
            ("Fuzzy matches", ["Benzen", "Toluenne", "Leed", "Arsnic"] * 25),
            ("CAS numbers", ["71-43-2", "108-88-3", "7439-92-1", "7440-38-2"] * 25),
            ("PHC fractions", ["PHC F1", "PHC F2", "PHC F3", "PHC F4"] * 25),
            ("Unknown", ["Unknown1", "Unknown2", "Unknown3", "Unknown4"] * 25),
        ]
        
        print("\n" + "="*60)
        print("COMPREHENSIVE SYSTEM BENCHMARK")
        print("="*60)
        
        total_items = 0
        total_time = 0
        
        for test_name, inputs in test_cases:
            start = time.perf_counter()
            results = [resolution_engine.resolve(text) for text in inputs]
            elapsed_ms = (time.perf_counter() - start) * 1000
            
            resolved = sum(1 for r in results if r.is_resolved)
            avg_time = elapsed_ms / len(inputs)
            
            print(f"\n{test_name}:")
            print(f"  Items: {len(inputs)}")
            print(f"  Total time: {elapsed_ms:.2f}ms ({elapsed_ms/1000:.2f}s)")
            print(f"  Avg per item: {avg_time:.2f}ms")
            print(f"  Throughput: {len(inputs)/(elapsed_ms/1000):.0f} items/sec")
            print(f"  Resolved: {resolved}/{len(inputs)} ({resolved/len(inputs)*100:.1f}%)")
            
            total_items += len(inputs)
            total_time += elapsed_ms/1000
        
        print(f"\nOVERALL:")
        print(f"  Total items: {total_items}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average throughput: {total_items/total_time:.0f} items/sec")
        print("="*60)
        
        # Overall performance target
        assert total_items / total_time > 50, "Overall throughput below 50 items/sec target"
