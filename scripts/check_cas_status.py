"""Quick script to check CAS number status in database."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func
from src.database import Analyte
from src.database.connection import DatabaseManager

db = DatabaseManager("data/reg153_matcher.db")
session = db.SessionLocal()

# Count totals
total = session.execute(select(func.count()).select_from(Analyte)).scalar()
no_cas = session.execute(
    select(func.count()).select_from(Analyte).where(Analyte.cas_number.is_(None))
).scalar()

print(f"Total analytes: {total}")
print(f"Without CAS: {no_cas}")
print(f"With CAS: {total - no_cas}")
print()

# Show samples
print("Sample of analytes without CAS:")
analytes = session.execute(
    select(Analyte).where(Analyte.cas_number.is_(None)).limit(10)
).scalars().all()

for a in analytes:
    print(f"  {a.analyte_id:30s} | {a.preferred_name}")

session.close()
