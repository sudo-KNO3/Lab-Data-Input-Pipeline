"""
Example usage of the Reg 153 Chemical Matcher database.

This script demonstrates:
1. Database initialization
2. Creating analytes with synonyms
3. Recording lab variants
4. Logging match decisions
5. Querying and searching
6. Working with snapshots

Run this example:
    python examples/database_usage_example.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import init_db, session_scope
from src.database.crud import (
    create_analyte,
    create_synonym,
    create_lab_variant,
    create_match_decision,
    create_snapshot,
    get_analyte_by_cas,
    search_analytes_by_name,
    get_synonyms_for_analyte,
    search_synonyms,
    get_lab_variants_by_vendor,
    get_decisions_for_review,
    get_match_statistics,
    get_active_snapshot,
    count_analytes,
)


def main():
    print("=" * 70)
    print("Reg 153 Chemical Matcher - Database Usage Example")
    print("=" * 70)
    
    # 1. Initialize database
    print("\n1️⃣  Initializing database...")
    db = init_db("examples/example.db", echo=False)
    db.create_all_tables()
    print("   ✓ Database initialized")
    
    # 2. Create analytes
    print("\n2️⃣  Creating analytes...")
    
    with session_scope() as session:
        # Create a single substance
        acetone = create_analyte(
            session,
            cas_number="67-64-1",
            preferred_name="Acetone",
            iupac_name="Propan-2-one",
            analyte_type="single_substance",
            molecular_formula="C3H6O",
            molecular_weight=58.08,
            smiles="CC(=O)C",
            reg153_category="Volatile Organic Compounds",
            notes="Common solvent, regulated under Reg 153"
        )
        print(f"   ✓ Created: {acetone.preferred_name} (CAS: {acetone.cas_number})")
        
        # Create a fraction (no CAS number)
        tph_f2 = create_analyte(
            session,
            cas_number=None,
            preferred_name="Total Petroleum Hydrocarbons (F2)",
            analyte_type="fraction_or_group",
            reg153_category="Petroleum Hydrocarbons",
            notes="C10-C16 fraction"
        )
        print(f"   ✓ Created: {tph_f2.preferred_name} (Fraction)")
        
        # Create a suite
        btex_suite = create_analyte(
            session,
            cas_number=None,
            preferred_name="BTEX Suite",
            analyte_type="suite",
            reg153_category="BTEX",
            notes="Benzene, Toluene, Ethylbenzene, Xylenes"
        )
        print(f"   ✓ Created: {btex_suite.preferred_name} (Suite)")
    
    # 3. Add synonyms
    print("\n3️⃣  Adding synonyms...")
    
    with session_scope() as session:
        acetone = get_analyte_by_cas(session, "67-64-1")
        
        synonyms = [
            ("2-Propanone", "2-propanone", "pubchem", 1.0),
            ("Dimethyl ketone", "dimethyl ketone", "pubchem", 0.95),
            ("Propanone", "propanone", "cas_common_chemistry", 1.0),
            ("Dimethylketal", "dimethylketal", "echa", 0.85),
            ("Methyl ketone", "methyl ketone", "manual", 0.8),
        ]
        
        for raw, norm, source, conf in synonyms:
            create_synonym(
                session,
                analyte_id=acetone.id,
                synonym_raw=raw,
                synonym_norm=norm,
                harvest_source=source,
                confidence=conf,
            )
            print(f"   ✓ Added synonym: {raw} (confidence: {conf})")
    
    # 4. Add lab variants
    print("\n4️⃣  Recording lab behavioral variants...")
    
    with session_scope() as session:
        acetone = get_analyte_by_cas(session, "67-64-1")
        
        variants = [
            ("ALS Canada", "Acetone (2-Propanone)", "EPA 8260D", "Soil", "µg/kg", 42),
            ("Bureau Veritas", "Acetone", "EPA 8260C", "Water", "µg/L", 28),
            ("Eurofins", "2-Propanone", "TO-15", "Air", "µg/m³", 15),
        ]
        
        for vendor, name, method, matrix, units, freq in variants:
            create_lab_variant(
                session,
                analyte_id=acetone.id,
                lab_vendor=vendor,
                reported_name=name,
                method=method,
                matrix=matrix,
                units=units,
                frequency=freq,
            )
            print(f"   ✓ {vendor}: '{name}' (observed {freq} times)")
    
    # 5. Record match decisions
    print("\n5️⃣  Logging match decisions...")
    
    with session_scope() as session:
        acetone = get_analyte_by_cas(session, "67-64-1")
        
        # Successful match
        decision1 = create_match_decision(
            session,
            query_text="acetone",
            query_norm="acetone",
            analyte_id=acetone.id,
            confidence_score=0.98,
            top_k_candidates=[
                {"rank": 1, "analyte_id": acetone.id, "name": "Acetone", "score": 0.98},
            ],
            signals_used={
                "exact_match": 1.0,
                "levenshtein_distance": 0.0,
                "embedding_cosine": 0.98,
                "cas_match": 0.0,
            },
            corpus_snapshot_hash="v1.0.0_abc123",
            model_hash="model_v1_def456",
            embedding_model_name="all-MiniLM-L6-v2",
            disagreement_flag=False,
        )
        print(f"   ✓ Match: 'acetone' → {acetone.preferred_name} (confidence: 0.98)")
        
        # Ambiguous match with disagreement
        decision2 = create_match_decision(
            session,
            query_text="propanone",
            query_norm="propanone",
            analyte_id=acetone.id,
            confidence_score=0.72,
            top_k_candidates=[
                {"rank": 1, "analyte_id": acetone.id, "name": "Acetone", "score": 0.72},
                {"rank": 2, "analyte_id": 99, "name": "Propanal", "score": 0.68},
            ],
            signals_used={
                "exact_match": 0.0,
                "levenshtein_distance": 0.85,
                "embedding_cosine": 0.65,
            },
            corpus_snapshot_hash="v1.0.0_abc123",
            model_hash="model_v1_def456",
            embedding_model_name="all-MiniLM-L6-v2",
            disagreement_flag=True,  # Signals disagree
        )
        print(f"   ✓ Match: 'propanone' → {acetone.preferred_name} (confidence: 0.72, disagreement: True)")
    
    # 6. Create snapshot
    print("\n6️⃣  Creating corpus snapshot...")
    
    with session_scope() as session:
        analyte_count = count_analytes(session)
        
        snapshot = create_snapshot(
            session,
            snapshot_hash="v1.0.0_abc123",
            snapshot_type="corpus",
            version_tag="v1.0.0",
            file_path="snapshots/db/corpus_v1.0.0.db",
            file_size_bytes=1024000,
            description="Initial production corpus with 3 analytes",
            analyte_count=analyte_count,
            synonym_count=5,
            metadata={
                "created_by": "example_script",
                "notes": "Test snapshot"
            },
            is_active=True,
        )
        print(f"   ✓ Snapshot created: {snapshot.version_tag}")
    
    # 7. Query and search
    print("\n7️⃣  Querying database...")
    
    with session_scope() as session:
        # Search by name
        results = search_analytes_by_name(session, "acetone", limit=5)
        print(f"\n   Search 'acetone': {len(results)} result(s)")
        for analyte in results:
            print(f"      → {analyte.preferred_name} (CAS: {analyte.cas_number or 'N/A'})")
        
        # Get synonyms
        acetone = get_analyte_by_cas(session, "67-64-1")
        synonyms = get_synonyms_for_analyte(session, acetone.id, min_confidence=0.8)
        print(f"\n   Synonyms for {acetone.preferred_name} (confidence ≥ 0.8): {len(synonyms)}")
        for syn in synonyms[:3]:
            print(f"      → {syn.synonym_raw} ({syn.confidence:.2f}, source: {syn.harvest_source})")
        
        # Get lab variants
        variants = get_lab_variants_by_vendor(session, "ALS Canada")
        print(f"\n   Lab variants for ALS Canada: {len(variants)}")
        for var in variants:
            print(f"      → {var.reported_name} (method: {var.method}, freq: {var.frequency})")
        
        # Get decisions needing review
        review_decisions = get_decisions_for_review(
            session,
            disagreement_only=True,
            not_reviewed=True,
            limit=10
        )
        print(f"\n   Decisions needing review: {len(review_decisions)}")
        for dec in review_decisions:
            print(f"      → Query: '{dec.query_text}' (confidence: {dec.confidence_score:.2f})")
        
        # Get statistics
        stats = get_match_statistics(
            session,
            corpus_snapshot_hash="v1.0.0_abc123",
        )
        print(f"\n   Match statistics:")
        print(f"      Total decisions: {stats['total_decisions']}")
        print(f"      Avg confidence: {stats['avg_confidence']:.3f}")
        print(f"      Disagreement rate: {stats['disagreement_rate']:.2%}")
        print(f"      Review rate: {stats['review_rate']:.2%}")
        
        # Get active snapshot
        active_snapshot = get_active_snapshot(session, "corpus")
        if active_snapshot:
            print(f"\n   Active corpus snapshot: {active_snapshot.version_tag}")
            print(f"      Analytes: {active_snapshot.analyte_count}")
            print(f"      Synonyms: {active_snapshot.synonym_count}")
    
    # 8. Summary
    print("\n" + "=" * 70)
    print("✅ Example completed successfully!")
    print("=" * 70)
    print(f"\nDatabase created at: {Path('examples/example.db').absolute()}")
    print("\nYou can inspect the database with:")
    print("  sqlite3 examples/example.db")
    print("\nOr continue using the Python API for further operations.")
    
    db.close()


if __name__ == "__main__":
    main()
