"""
Add calculated/metadata parameters to database.

These are lab-reported values that aren't real chemicals but should be tracked:
- Anion Sum, Cation Sum, Ion Balance
- Quality control parameters
- Physical measurements
- Calculated totals
"""
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/reg153_matcher.db")

# Calculated parameters commonly found in lab reports
# Format: (analyte_id, name, analyte_type, chemical_group)
CALCULATED_PARAMS = [
    # Ion balance calculations
    ("WQ_CALC_001", "Anion Sum", "CALCULATED", "Ion Balance"),
    ("WQ_CALC_002", "Cation Sum", "CALCULATED", "Ion Balance"),
    ("WQ_CALC_003", "Ion Balance", "CALCULATED", "Ion Balance"),
    ("WQ_CALC_004", "Charge Balance", "CALCULATED", "Ion Balance"),
    
    # Physical/aggregate measurements
    ("WQ_CALC_005", "Total Dissolved Solids", "CALCULATED", "Physical"),
    ("WQ_CALC_006", "Total Suspended Solids", "CALCULATED", "Physical"),
    ("WQ_CALC_007", "Total Solids", "CALCULATED", "Physical"),
    
    # QC parameters
    ("WQ_CALC_008", "Percent Recovery", "CALCULATED", "QC"),
    ("WQ_CALC_009", "Relative Percent Difference", "CALCULATED", "QC"),
]

def add_calculated_parameters():
    """Add calculated parameters to database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"\nADDING CALCULATED PARAMETERS")
    print(f"{'='*80}\n")
    
    added_count = 0
    
    for analyte_id, name, analyte_type, chemical_group in CALCULATED_PARAMS:
        # Check if already exists
        existing = cursor.execute(
            "SELECT analyte_id FROM analytes WHERE analyte_id = ?",
            (analyte_id,)
        ).fetchone()
        
        if existing:
            print(f"  ⊘ {analyte_id:20s} {name:40s} (already exists)")
            continue
        
        # Insert analyte
        cursor.execute("""
            INSERT INTO analytes (
                analyte_id, preferred_name, analyte_type, chemical_group,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            analyte_id,
            name,
            analyte_type,
            chemical_group,
            datetime.now(),
            datetime.now()
        ))
        
        # Add primary synonym
        cursor.execute("""
            INSERT INTO synonyms (
                analyte_id, synonym_raw, synonym_norm, synonym_type, 
                harvest_source, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            analyte_id,
            name,
            name.lower(),
            "EXACT",
            "bootstrap",
            1.0,
            datetime.now()
        ))
        
        print(f"  ✓ {analyte_id:20s} {name}")
        added_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"✓ Added {added_count} calculated parameters")
    print(f"\nThese will now auto-match in lab reports!")
    
    return added_count


if __name__ == "__main__":
    add_calculated_parameters()
