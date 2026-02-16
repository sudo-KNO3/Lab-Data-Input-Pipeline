"""
Create missing analytes found during PubChem verification and fix wrong matches.
- 1,3,5-Trimethylbenzene (Mesitylene) - CAS 108-67-8
- Chloroethane (Ethyl Chloride) - CAS 75-00-3
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, AnalyteType, SynonymType
from src.normalization.text_normalizer import TextNormalizer

normalizer = TextNormalizer()
db = DatabaseManager()

print("=" * 70)
print("CREATING MISSING ANALYTES & FIXING MATCHES")
print("=" * 70)

new_analytes = [
    {
        'analyte_id': 'REG153_VOCS_043',
        'preferred_name': 'Trimethylbenzene 1,3,5-',
        'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
        'cas_number': '108-67-8',
        'group_code': 'VOCS',
        'table_number': 4,
        'chemical_group': 'Volatile Organic Compounds',
        'molecular_formula': 'C9H12',
        'synonyms': [
            '1,3,5-Trimethylbenzene',
            'Mesitylene',
            '1,3,5-trimethylbenzene',
            'Trimethylbenzene, 1,3,5-',
        ]
    },
    {
        'analyte_id': 'REG153_VOCS_044',
        'preferred_name': 'Chloroethane',
        'analyte_type': AnalyteType.SINGLE_SUBSTANCE,
        'cas_number': '75-00-3',
        'group_code': 'VOCS',
        'table_number': 4,
        'chemical_group': 'Volatile Organic Compounds',
        'molecular_formula': 'C2H5Cl',
        'synonyms': [
            'Chloroethane',
            'Ethyl chloride',
            'Ethyl Chloride',
            'Monochloroethane',
        ]
    },
]

with db.session_scope() as session:
    for a in new_analytes:
        # Check if already exists
        existing = session.query(Analyte).filter(Analyte.analyte_id == a['analyte_id']).first()
        if existing:
            print(f"  Already exists: {a['analyte_id']} ({existing.preferred_name})")
            continue
        
        # Also check by CAS
        by_cas = session.query(Analyte).filter(Analyte.cas_number == a['cas_number']).first()
        if by_cas:
            print(f"  CAS {a['cas_number']} already assigned to {by_cas.analyte_id} ({by_cas.preferred_name})")
            a['analyte_id'] = by_cas.analyte_id
            continue
        
        new_analyte = Analyte(
            analyte_id=a['analyte_id'],
            preferred_name=a['preferred_name'],
            analyte_type=a['analyte_type'],
            cas_number=a['cas_number'],
            group_code=a['group_code'],
            table_number=a['table_number'],
            chemical_group=a['chemical_group'],
            molecular_formula=a['molecular_formula'],
        )
        session.add(new_analyte)
        print(f"  Created: {a['analyte_id']} - {a['preferred_name']} (CAS {a['cas_number']})")
        
        # Add synonyms
        for syn_text in a['synonyms']:
            norm = normalizer.normalize(syn_text)
            new_syn = Synonym(
                analyte_id=a['analyte_id'],
                synonym_raw=syn_text,
                synonym_norm=norm,
                synonym_type=SynonymType.COMMON,
                harvest_source='pubchem_verified',
                confidence=1.0,
            )
            session.add(new_syn)
            print(f"    + synonym: {syn_text}")

print("\nAnalytes created.\n")

# Fix the lab_results matches
print("=" * 70)
print("FIXING WRONG MATCHES IN LAB_RESULTS")
print("=" * 70)

conn = sqlite3.connect('data/lab_results.db')

# Fix 1,3,5-trimethylbenzene
c = conn.execute("""
    UPDATE lab_results 
    SET analyte_id = 'REG153_VOCS_043', match_confidence = 1.0, 
        match_method = 'pubchem_verified',
        validation_status = 'accepted',
        validation_notes = 'PubChem verified: CAS 108-67-8 = 1,3,5-Trimethylbenzene (Mesitylene)'
    WHERE chemical_raw = '1,3,5-trimethylbenzene' AND validation_status = 'needs_review'
""")
print(f"  Fixed {c.rowcount} rows: 1,3,5-trimethylbenzene -> REG153_VOCS_043")

# Fix Chloroethane
c = conn.execute("""
    UPDATE lab_results 
    SET analyte_id = 'REG153_VOCS_044', match_confidence = 1.0,
        match_method = 'pubchem_verified',
        validation_status = 'accepted',
        validation_notes = 'PubChem verified: CAS 75-00-3 = Chloroethane (Ethyl Chloride)'
    WHERE chemical_raw = 'Chloroethane' AND validation_status = 'needs_review'
""")
print(f"  Fixed {c.rowcount} rows: Chloroethane -> REG153_VOCS_044")

conn.commit()

# Final check
total = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
accepted = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'accepted'").fetchone()[0]
not_chem = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'not_chemical'").fetchone()[0]
review = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'needs_review'").fetchone()[0]
pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'pending'").fetchone()[0]

real = total - not_chem
print(f"\n=== FINAL ===")
print(f"Total:      {total}")
print(f"Accepted:   {accepted}/{real} ({100*accepted/real:.1f}%)")
print(f"Non-chem:   {not_chem}")
print(f"Review:     {review}")
print(f"Pending:    {pending}")

conn.close()
