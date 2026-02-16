"""
Analyze chemicals that were not found or have validation issues.
"""
import sys
import csv
from pathlib import Path
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from sqlalchemy import select, text


def analyze_invalid_synonyms():
    """Analyze invalid synonyms from validation."""
    invalid_file = Path("data/validation/invalid_synonyms.csv")
    
    if not invalid_file.exists():
        print("No invalid synonyms file found.")
        return
    
    print("\n" + "=" * 80)
    print("INVALID SYNONYMS ANALYSIS")
    print("=" * 80)
    
    # Count by issue type
    issue_types = Counter()
    by_analyte = defaultdict(list)
    cas_mismatches = []
    
    with open(invalid_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            notes = row['notes']
            issue_types[notes] += 1
            by_analyte[row['analyte_name']].append(row)
            
            if 'CAS mismatch' in notes:
                cas_mismatches.append(row)
    
    print(f"\nTotal invalid synonyms: {sum(issue_types.values()):,}")
    print("\nIssue breakdown:")
    for issue, count in issue_types.most_common():
        print(f"  {count:5,} - {issue}")
    
    print(f"\n\nAnalytes with most invalid synonyms (Top 20):")
    print("-" * 80)
    sorted_analytes = sorted(by_analyte.items(), key=lambda x: len(x[1]), reverse=True)
    for name, issues in sorted_analytes[:20]:
        print(f"  {len(issues):4d} invalid: {name}")
    
    print(f"\n\nCAS Mismatches (synonyms mapping to wrong chemical):")
    print("-" * 80)
    for row in cas_mismatches[:30]:
        print(f"\nSynonym: {row['synonym']}")
        print(f"  Expected: {row['analyte_name']} (CAS: {row['analyte_cas']})")
        print(f"  PubChem:  CAS {row['pubchem_cas']}")


def analyze_low_coverage():
    """Find chemicals with poor synonym coverage."""
    db = DatabaseManager()
    
    print("\n" + "=" * 80)
    print("LOW SYNONYM COVERAGE ANALYSIS")
    print("=" * 80)
    
    with db.session_scope() as session:
        # Get synonym counts per analyte
        query = text("""
            SELECT 
                a.analyte_id,
                a.preferred_name,
                a.cas_number,
                a.analyte_type,
                COUNT(s.id) as synonym_count,
                SUM(CASE WHEN s.harvest_source = 'bootstrap' THEN 1 ELSE 0 END) as bootstrap_only
            FROM analytes a
            LEFT JOIN synonyms s ON a.analyte_id = s.analyte_id
            GROUP BY a.analyte_id
            ORDER BY synonym_count ASC
        """)
        
        result = session.execute(query)
        rows = result.fetchall()
        
        print(f"\n\nChemicals with <10 synonyms:")
        print("-" * 80)
        low_coverage = [r for r in rows if r[4] < 10]
        for row in low_coverage:
            analyte_id, name, cas, atype, syn_count, bootstrap = row
            status = "NO CAS" if not cas else f"CAS: {cas}"
            print(f"  {syn_count:3d} synonyms: {name:50s} ({status})")
        
        print(f"\n\nTotal chemicals analyzed: {len(rows)}")
        print(f"Chemicals with <10 synonyms: {len(low_coverage)}")
        print(f"Chemicals with NO CAS number: {sum(1 for r in rows if not r[2])}")


def analyze_no_cas():
    """Analyze chemicals without CAS numbers."""
    db = DatabaseManager()
    
    print("\n" + "=" * 80)
    print("CHEMICALS WITHOUT CAS NUMBERS")
    print("=" * 80)
    
    with db.session_scope() as session:
        query = text("""
            SELECT 
                a.analyte_id,
                a.preferred_name,
                a.analyte_type,
                COUNT(s.id) as synonym_count
            FROM analytes a
            LEFT JOIN synonyms s ON a.analyte_id = s.analyte_id
            WHERE a.cas_number IS NULL
            GROUP BY a.analyte_id
            ORDER BY a.preferred_name
        """)
        
        result = session.execute(query)
        rows = result.fetchall()
        
        print(f"\nTotal chemicals without CAS: {len(rows)}")
        print("\nList:")
        print("-" * 80)
        for row in rows:
            analyte_id, name, atype, syn_count = row
            print(f"  {name:55s} Type: {atype:30s} Synonyms: {syn_count}")
        
        print("\n\nThese chemicals cannot be validated against PubChem because they are:")
        print("  - Petroleum fractions (F1, F2, F3, F4)")
        print("  - Chemical mixtures (xylene mixture, PCBs)")
        print("  - Fractions/groups without unique CAS numbers")


if __name__ == '__main__':
    analyze_invalid_synonyms()
    analyze_low_coverage()
    analyze_no_cas()
