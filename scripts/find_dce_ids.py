"""Find dichloroethylene IDs."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte

db = DatabaseManager()
with db.session_scope() as session:
    analytes = session.query(Analyte).filter(
        Analyte.preferred_name.like('%ichloroethylene%')
    ).all()
    
    print("Dichloroethylene entries:")
    for a in analytes:
        print(f"  {a.analyte_id:40s} {a.preferred_name:50s} CAS: {a.cas_number}")
