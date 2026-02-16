"""Check ground truth for Dichloropropene entries."""
import sqlite3

conn = sqlite3.connect("data/lab_results.db")
conn.row_factory = sqlite3.Row

rows = conn.execute(
    "SELECT submission_id, chemical_raw, analyte_id, correct_analyte_id "
    "FROM lab_results WHERE chemical_raw LIKE '%Dichloropropene%' "
    "AND validation_status IN ('validated','accepted')"
).fetchall()

print("=== Ground truth for Dichloropropene ===")
for r in rows:
    print(dict(r))

# Also check what correct_analyte_id column exists
cols = conn.execute("PRAGMA table_info(lab_results)").fetchall()
print("\n=== lab_results columns ===")
for c in cols:
    print(c["name"], c["type"])

conn.close()

# Check the analyte DB for VOCS_020, 021, 022
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from src.database.models import Analyte, Synonym

engine = create_engine("sqlite:///data/reg153_matcher.db")
with Session(engine) as session:
    for aid in ["REG153_VOCS_020", "REG153_VOCS_021", "REG153_VOCS_022"]:
        a = session.query(Analyte).filter(Analyte.analyte_id == aid).first()
        if a:
            syns = session.query(Synonym).filter(Synonym.analyte_id == aid).all()
            print(f"\n{aid}: {a.preferred_name} (CAS: {a.cas_number})")
            print(f"  Synonyms ({len(syns)}):")
            for s in syns:
                print(f"    - {s.synonym_raw}")
