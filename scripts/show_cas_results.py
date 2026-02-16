"""Show analytes with and without CAS numbers after harvest."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from src.database import Analyte, AnalyteType
from src.database.connection import DatabaseManager

db = DatabaseManager("data/reg153_matcher.db")
session = db.SessionLocal()

# Get analytes WITH CAS
print("=" * 80)
print("ANALYTES WITH CAS NUMBERS (35 total)")
print("=" * 80)
analytes_with_cas = session.execute(
    select(Analyte)
    .where(
        Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE,
        Analyte.cas_number.isnot(None)
    )
    .order_by(Analyte.chemical_group, Analyte.preferred_name)
).scalars().all()

for a in analytes_with_cas:
    print(f"{a.cas_number:15s} | {a.preferred_name:40s} | {a.chemical_group}")

print()
print("=" * 80)
print("ANALYTES WITHOUT CAS NUMBERS (samples)")
print("=" * 80)
analytes_without_cas = session.execute(
    select(Analyte)
    .where(
        Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE,
        Analyte.cas_number.is_(None)
    )
    .limit(20)
).scalars().all()

for a in analytes_without_cas:
    print(f"{a.chemical_group:15s} | {a.preferred_name}")

session.close()
