"""Quick check: what analyte_id does the ground truth have for review-band items?"""
import sqlite3

conn = sqlite3.connect("data/lab_results.db")
conn.row_factory = sqlite3.Row

review_chemicals = [
    "Chlordane, alpha-",
    "Cyanide (CN-)",
    "Petroleum Hydrocarbons F2-Napth",
    "Petroleum Hydrocarbons F3-PAH",
]

for chem in review_chemicals:
    rows = conn.execute(
        "SELECT submission_id, chemical_raw, analyte_id, correct_analyte_id "
        "FROM lab_results WHERE chemical_raw = ? AND validation_status IN ('validated','accepted')",
        (chem,)
    ).fetchall()
    print(f"\n'{chem}':")
    for r in rows:
        print(f"  sub={r['submission_id']}  ground_truth={r['analyte_id']}  override={r['correct_analyte_id']}")

conn.close()
