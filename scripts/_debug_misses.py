"""Debug script to analyze the 4 Gate A misses."""
import sys, sqlite3
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.matching import build_engine
from src.normalization.text_normalizer import TextNormalizer

# Check ground truth
conn = sqlite3.connect("data/lab_results.db")
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT submission_id, chemical_raw, analyte_id, correct_analyte_id, validation_status
    FROM lab_results 
    WHERE chemical_raw LIKE '%Dichloropropene%' 
    AND validation_status IN ('validated','accepted')
""").fetchall()

print("=== GROUND TRUTH ===")
for r in rows:
    print(dict(r))

# Re-resolve
normalizer = TextNormalizer()
db = DatabaseManager()
with db.get_session() as session:
    engine = build_engine(session)
    result = engine.resolve("Dichloropropene,1,3-", confidence_threshold=0.70)
    print("\n=== RESOLUTION RESULT ===")
    print(f"  Best match: {result.best_match}")
    if result.best_match:
        print(f"  analyte_id: {result.best_match.analyte_id}")
        print(f"  preferred_name: {result.best_match.preferred_name}")
        print(f"  confidence: {result.best_match.confidence}")
        print(f"  method: {result.best_match.method}")
        print(f"  metadata: {result.best_match.metadata}")
    print(f"  All candidates: {len(result.all_candidates)}")
    for i, c in enumerate(result.all_candidates):
        print(f"    [{i}] {c.analyte_id} {c.preferred_name} conf={c.confidence:.4f} method={c.method}")
    print(f"  Margin: {result.margin}")
    print(f"  Band: {result.confidence_band}")
    print(f"  Signals: {result.signals_used}")

# Check normalized form
print(f"\n=== NORMALIZATION ===")
print(f"  Input: 'Dichloropropene,1,3-'")
print(f"  Normalized: '{normalizer.normalize('Dichloropropene,1,3-')}'")

conn.close()
