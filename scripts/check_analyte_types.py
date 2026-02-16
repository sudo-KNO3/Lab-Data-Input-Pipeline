"""Check analyte types in database."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func
from src.database import Analyte
from src.database.connection import DatabaseManager

db = DatabaseManager("data/reg153_matcher.db")
session = db.SessionLocal()

# Count by type
print("Analyte types:")
types = session.execute(
    select(Analyte.analyte_type, func.count())
    .group_by(Analyte.analyte_type)
).all()

for analyte_type, count in types:
    print(f"  {analyte_type}: {count}")

print()
print("Sample analytes:")
analytes = session.execute(select(Analyte).limit(5)).scalars().all()
for a in analytes:
    print(f"  {a.analyte_id:30s} | Type: {a.analyte_type.value:20s} | {a.preferred_name}")

session.close()
