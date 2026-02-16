"""Find analytes with no or few synonyms."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func, and_
from src.database import Analyte, Synonym
from src.database.connection import DatabaseManager


def main():
    db = DatabaseManager("data/reg153_matcher.db")
    session = db.SessionLocal()
    
    # Get analytes with synonym counts
    query = select(
        Analyte.analyte_id,
        Analyte.preferred_name,
        Analyte.chemical_group,
        Analyte.cas_number,
        Analyte.analyte_type,
        func.count(Synonym.id).label('synonym_count')
    ).outerjoin(
        Synonym, Analyte.analyte_id == Synonym.analyte_id
    ).group_by(
        Analyte.analyte_id
    ).order_by(
        func.count(Synonym.id).asc(),
        Analyte.chemical_group,
        Analyte.preferred_name
    )
    
    results = session.execute(query).all()
    
    # Separate by synonym count
    no_synonyms = []
    few_synonyms = []
    
    for row in results:
        if row.synonym_count == 0:
            no_synonyms.append(row)
        elif row.synonym_count <= 5:
            few_synonyms.append(row)
    
    print("=" * 100)
    print("ANALYTES WITH NO SYNONYMS")
    print("=" * 100)
    print()
    
    if no_synonyms:
        print(f"Found {len(no_synonyms)} analytes with NO synonyms:")
        print()
        for row in no_synonyms:
            cas_display = row.cas_number if row.cas_number else "No CAS"
            print(f"{row.analyte_id:30s} | {row.chemical_group:10s} | {cas_display:15s} | {row.preferred_name}")
    else:
        print("✓ All analytes have at least one synonym!")
    
    print()
    print("=" * 100)
    print("ANALYTES WITH FEW SYNONYMS (1-5)")
    print("=" * 100)
    print()
    
    if few_synonyms:
        print(f"Found {len(few_synonyms)} analytes with 1-5 synonyms:")
        print()
        for row in few_synonyms:
            cas_display = row.cas_number if row.cas_number else "No CAS"
            print(f"{row.synonym_count} | {row.analyte_id:30s} | {row.chemical_group:10s} | {cas_display:15s} | {row.preferred_name}")
    else:
        print("✓ All analytes have at least 6 synonyms!")
    
    print()
    print("=" * 100)
    print(f"Summary: {len(no_synonyms)} with 0 synonyms, {len(few_synonyms)} with 1-5 synonyms")
    print("=" * 100)
    
    session.close()


if __name__ == "__main__":
    main()
