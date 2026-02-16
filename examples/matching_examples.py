"""
Example usage of the chemical matching engine.

This script demonstrates how to use the multi-strategy matching system
for resolving chemical names.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_session
from src.matching import ResolutionEngine, MatcherConfig
from src.matching.types import MatchMethod


def example_single_query():
    """Example: Resolve a single chemical name."""
    print("=" * 80)
    print("EXAMPLE 1: Single Query Resolution")
    print("=" * 80)
    
    # Get database session
    session = next(get_session())
    
    # Initialize resolution engine with custom config
    config = MatcherConfig(
        fuzzy_threshold=0.75,
        semantic_threshold=0.75,
        disagreement_penalty=0.1,
    )
    
    engine = ResolutionEngine(session, config=config)
    
    # Resolve a query
    query = "benzene"
    print(f"\nQuery: '{query}'")
    
    result = engine.resolve(query, threshold=0.75)
    
    # Display results
    if result.matched:
        print(f"\n✓ MATCHED")
        print(f"  Analyte: {result.best_match.analyte_name}")
        print(f"  CAS: {result.best_match.cas_number}")
        print(f"  Confidence: {result.best_match.confidence:.3f}")
        print(f"  Method: {result.best_match.method.value}")
        print(f"  Processing: {result.processing_time_ms:.2f}ms")
        
        if result.disagreement_detected:
            print(f"  ⚠ Disagreement detected between matchers")
        
        if result.manual_review_recommended:
            print(f"  ⚠ Manual review recommended: {result.review_reason}")
    else:
        print(f"\n✗ NO MATCH")
        print(f"  Processing: {result.processing_time_ms:.2f}ms")
    
    # Show all candidates
    if result.all_candidates:
        print(f"\n  Top Candidates:")
        for i, candidate in enumerate(result.all_candidates[:5], 1):
            print(f"    {i}. {candidate.analyte_name} ({candidate.confidence:.3f}) - {candidate.method.value}")
    
    session.close()


def example_batch_processing():
    """Example: Batch process multiple queries."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Batch Processing")
    print("=" * 80)
    
    session = next(get_session())
    engine = ResolutionEngine(session)
    
    # Multiple queries
    queries = [
        "benzene",
        "71-43-2",  # CAS number
        "toluene",
        "methyl ethyl ketone",
        "unknown chemical xyz",  # Should not match
    ]
    
    print(f"\nProcessing {len(queries)} queries...")
    
    results = engine.resolve_batch(queries, threshold=0.75)
    
    # Summary
    matched = sum(1 for r in results if r.matched)
    print(f"\nResults: {matched}/{len(results)} matched")
    
    # Display each result
    for i, (query, result) in enumerate(zip(queries, results), 1):
        status = "✓" if result.matched else "✗"
        match_info = f"{result.best_match.analyte_name} ({result.confidence:.2f})" if result.matched else "No match"
        print(f"  {status} {i}. '{query}' → {match_info}")
    
    session.close()


def example_export_results():
    """Example: Export results to CSV and JSON."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Export Results")
    print("=" * 80)
    
    session = next(get_session())
    engine = ResolutionEngine(session)
    
    queries = ["benzene", "toluene", "xylene"]
    results = engine.resolve_batch(queries, log_decisions=False)
    
    # Export to CSV
    csv_path = "output/example_matches.csv"
    engine.export_results_csv(results, csv_path)
    print(f"\n✓ Exported to CSV: {csv_path}")
    
    # Export to JSON
    json_path = "output/example_matches.json"
    engine.export_results_json(results, json_path)
    print(f"✓ Exported to JSON: {json_path}")
    
    session.close()


def example_direct_matchers():
    """Example: Use individual matchers directly."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Direct Matcher Access")
    print("=" * 80)
    
    from src.matching import match_exact, match_fuzzy, SemanticMatcher
    
    session = next(get_session())
    
    # Exact matching
    print("\n1. Exact Matching")
    match = match_exact("71-43-2", session)
    if match:
        print(f"   ✓ Found: {match.analyte_name} (CAS: {match.cas_number})")
    
    # Fuzzy matching
    print("\n2. Fuzzy Matching")
    query = "benzen"  # Typo
    matches = match_fuzzy(query, session, top_k=3, threshold=0.75)
    if matches:
        print(f"   Top {len(matches)} fuzzy matches for '{query}':")
        for i, m in enumerate(matches, 1):
            print(f"     {i}. {m.analyte_name} (score: {m.distance_score:.3f})")
    
    # Semantic matching
    print("\n3. Semantic Matching")
    try:
        semantic_matcher = SemanticMatcher()
        matches = semantic_matcher.match_semantic("aromatic hydrocarbon", top_k=3)
        if matches:
            print(f"   Top {len(matches)} semantic matches:")
            for i, m in enumerate(matches, 1):
                print(f"     {i}. {m.analyte_name} (similarity: {m.similarity_score:.3f})")
    except Exception as e:
        print(f"   ⚠ Semantic matcher not available: {e}")
    
    session.close()


def example_custom_config():
    """Example: Use custom configuration."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Custom Configuration")
    print("=" * 80)
    
    session = next(get_session())
    
    # Create custom config
    config = MatcherConfig(
        # Enable/disable matchers
        exact_cas_enabled=True,
        fuzzy_enabled=True,
        semantic_enabled=False,  # Disable semantic for this example
        
        # Adjust thresholds
        fuzzy_threshold=0.85,  # Higher threshold = more strict
        fuzzy_top_k=10,
        
        # Quality control
        disagreement_penalty=0.15,
        manual_review_threshold=0.90,
    )
    
    engine = ResolutionEngine(session, config=config)
    
    result = engine.resolve("benzene derivative")
    
    print(f"\nWith stricter config:")
    print(f"  Fuzzy threshold: {config.fuzzy_threshold}")
    print(f"  Semantic enabled: {config.semantic_enabled}")
    print(f"  Manual review threshold: {config.manual_review_threshold}")
    
    if result.matched:
        print(f"\n  ✓ Match: {result.best_match.analyte_name} ({result.confidence:.2f})")
    else:
        print(f"\n  ✗ No match (stricter thresholds)")
    
    session.close()


if __name__ == "__main__":
    """Run all examples."""
    
    print("\n" + "=" * 80)
    print("CHEMICAL MATCHING ENGINE - EXAMPLES")
    print("=" * 80)
    
    try:
        # Run examples
        example_single_query()
        example_batch_processing()
        # example_export_results()  # Uncomment if you want to test export
        example_direct_matchers()
        example_custom_config()
        
        print("\n" + "=" * 80)
        print("✓ All examples completed!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
        import traceback
        traceback.print_exc()
