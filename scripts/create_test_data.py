"""
Create test data in lab_results.db to demo validation workflow.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/lab_results.db")

def create_test_submission():
    """Create a test submission with realistic extraction results."""
    
    conn = sqlite3.connect(str(DB_PATH))
    
    # Insert test submission
    conn.execute("""
        INSERT INTO lab_submissions (
            file_path, file_hash, original_filename, lab_vendor,
            received_date, file_size_bytes, sheet_name,
            extraction_timestamp, extraction_version, layout_confidence,
            validation_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "Excel Lab examples/TEST_Eurofins_Demo.xlsx",
        "abc123def456",
        "TEST_Eurofins_Demo.xlsx",
        "Eurofins",
        datetime.now().date(),
        245000,
        "Analytical Results",
        datetime.now(),
        "1.0.0",
        0.92,
        "pending"
    ))
    
    submission_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    
    # Insert test extraction results with various confidence levels
    test_results = [
        # High confidence - should be auto-accepted
        (13, "Benzene", "benzene", "REG153_VOCS_005", "exact", 1.00, "S-001", "5.2", "µg/L", None),
        (14, "Toluene", "toluene", "REG153_VOCS_011", "exact", 0.99, "S-001", "3.1", "µg/L", None),
        (15, "Ethylbenzene", "ethylbenzene", "REG153_VOCS_015", "exact", 0.98, "S-001", "1.8", "µg/L", None),
        (16, "Xylene M&P", "xylene mp", "REG153_VOCS_041", "exact", 0.97, "S-001", "12.5", "µg/L", None),
        (17, "Naphthalene", "naphthalene", "REG153_PAHS_014", "exact", 0.96, "S-001", "<0.5", "µg/L", "<"),
        
        # Medium confidence - needs review
        (18, "F1-BTEX", "f1btex", "REG153_PHCS_001", "fuzzy", 0.89, "S-001", "250", "mg/kg", None),
        (19, "1+2-Methylnaphthalene", "12methylnaphthalene", "REG153_PAHS_016", "fuzzy", 0.85, "S-001", "2.1", "µg/L", None),
        (20, "Petroleum Hydrocarbons F2", "petroleum hydrocarbons f2", "REG153_PHCS_003", "fuzzy", 0.82, "S-001", "180", "mg/kg", None),
        
        # Low confidence - errors
        (21, "Methlynaphthalene", "methlynaphthalene", "REG153_PAHS_015", "fuzzy", 0.45, "S-001", "0.8", "µg/L", None),
        (22, "PCB", "pcb", "REG153_PCBS_001", "fuzzy", 0.38, "S-001", "<0.1", "µg/L", "<"),
        (23, "F1 Less Benzene", "f1 less benzene", "REG153_PHCS_002", "fuzzy", 0.52, "S-001", "95", "mg/kg", None),
    ]
    
    for row_num, chem_raw, chem_norm, analyte_id, method, conf, sample, value, units, qual in test_results:
        conn.execute("""
            INSERT INTO lab_results (
                submission_id, row_number, chemical_raw, chemical_normalized,
                analyte_id, match_method, match_confidence,
                sample_id, result_value, units, qualifier,
                validation_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            submission_id, row_num, chem_raw, chem_norm,
            analyte_id, method, conf,
            sample, value, units, qual,
            "pending"
        ))
    
    conn.commit()
    conn.close()
    
    return submission_id


def main():
    print("CREATING TEST DATA FOR VALIDATION DEMO")
    print("=" * 80)
    
    submission_id = create_test_submission()
    
    print(f"\n✓ Created test submission: {submission_id}")
    print(f"\nTest data includes:")
    print(f"  • 5 high-confidence matches (✓ will auto-accept)")
    print(f"  • 3 medium-confidence matches (⚠ review)")
    print(f"  • 3 low-confidence matches (✗ need correction)")
    print(f"\n  Total: 11 chemicals extracted")
    
    print("\n" + "=" * 80)
    print("NEXT: Generate validation workbook")
    print(f"\n  python scripts/21_generate_validation_workbook.py --submission-id {submission_id}")
    print(f"\n  This will create an Excel file you can open and test!")


if __name__ == "__main__":
    main()
