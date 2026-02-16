"""Auto-validate all 100% CA files and add synonyms for correct fuzzy matches."""
import sqlite3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_manager import DatabaseManager

def auto_validate_perfect_submissions():
    """Mark all 100%-confidence submissions as validated."""
    conn = sqlite3.connect('data/lab_results.db')
    
    # Find submissions where ALL results are >= 0.95 confidence
    rows = conn.execute("""
        SELECT ls.submission_id, ls.original_filename, ls.lab_vendor,
               COUNT(lr.result_id) as total,
               SUM(CASE WHEN lr.match_confidence >= 0.95 THEN 1 ELSE 0 END) as high
        FROM lab_submissions ls
        JOIN lab_results lr ON ls.submission_id = lr.submission_id
        WHERE ls.validation_status != 'validated'
        GROUP BY ls.submission_id
        HAVING total = high AND total > 0
        ORDER BY ls.submission_id
    """).fetchall()
    
    print(f"Found {len(rows)} submissions at 100% confidence to auto-validate:\n")
    
    for r in rows:
        sub_id, fname, vendor, total, high = r
        print(f"  Sub {sub_id:3d}: {total:3d}/{total:3d} (100%)  {vendor:10s}  {fname}")
        
        # Mark all results as validated
        conn.execute("""
            UPDATE lab_results 
            SET validation_status = 'accepted'
            WHERE submission_id = ? AND match_confidence >= 0.95
        """, (sub_id,))
        
        # Mark submission as validated
        conn.execute("""
            UPDATE lab_submissions 
            SET validation_status = 'validated'
            WHERE submission_id = ?
        """, (sub_id,))
    
    conn.commit()
    print(f"\n  Auto-validated {len(rows)} submissions, {sum(r[3] for r in rows)} results")
    conn.close()
    return len(rows)


def add_missing_synonyms():
    """Add known-correct lab names as synonyms to boost future matching."""
    db = DatabaseManager()
    
    # These are verified correct mappings from fuzzy matching
    # Format: (synonym_text, analyte_id, vendor, source)
    correct_mappings = [
        # Caduceon inverted naming
        ("Dichlorobenzene,1,2-", "REG153_VOCS_010", "Caduceon", "lab_confirmed"),
        ("Dichlorobenzene,1,3-", "REG153_VOCS_011", "Caduceon", "lab_confirmed"),
        ("Dichlorobenzene,1,4-", "REG153_VOCS_012", "Caduceon", "lab_confirmed"),
        ("Xylene, m,p-", "REG153_VOCS_041", "Caduceon", "lab_confirmed"),
        ("Chloromethane (Methyl Chloride)", "REG153_VOCS_029", "Caduceon", "lab_confirmed"),
        ("Trichlorofluoromethane (Freon 11)", "REG153_VOCS_038", "Caduceon", "lab_confirmed"),
        
        # Common single-word chemicals (Caduceon and Eurofins)
        ("Mercury", "REG153_METALS_013", None, "lab_confirmed"),
        ("Arsenic", "REG153_METALS_002", None, "lab_confirmed"),
        ("Benzene", "REG153_VOCS_002", None, "lab_confirmed"),
        ("Styrene", "REG153_VOCS_030", None, "lab_confirmed"),
        ("Pyrene", "REG153_PAHS_019", None, "lab_confirmed"),
        ("Fluorene", "REG153_PAHS_012", None, "lab_confirmed"),
        
        # Eurofins naming conventions
        ("Dichlorobenzene, 1,2-", "REG153_VOCS_010", "Eurofins", "lab_confirmed"),
        ("Dichlorobenzene, 1,3-", "REG153_VOCS_011", "Eurofins", "lab_confirmed"),
        ("Dichlorobenzene, 1,4-", "REG153_VOCS_012", "Eurofins", "lab_confirmed"),
        ("Dichloropropene,1,3-", "REG153_VOCS_020", "Eurofins", "lab_confirmed"),
        ("1,3,5-trimethylbenzene", "REG153_VOCS_040", "Eurofins", "lab_confirmed"),
        ("Cyanide (CN-)", "WQ_CHEM_001", "Eurofins", "lab_confirmed"),
        ("Chlordane, alpha-", "REG153_OCS_002", "Eurofins", "lab_confirmed"),
        ("Chlordane, gamma-", "REG153_OCS_002", "Eurofins", "lab_confirmed"),
        ("Chloroethane", "REG153_VOCS_006", None, "lab_confirmed"),  # Chloroethane = Ethyl Chloride
        
        # Eurofins PHC naming
        ("Petroleum Hydrocarbons F2-Napth", "REG153_PHCS_003", "Eurofins", "lab_confirmed"),
        ("Petroleum Hydrocarbons F3-PAH", "REG153_PHCS_005", "Eurofins", "lab_confirmed"),
        
        # Caduceon calculated params (mark as water quality)
        ("Conductivity Calc", "WQ_PHYS_002", "Caduceon", "lab_confirmed"),
        ("Anion Sum", "WQ_CALC_001", "Caduceon", "lab_confirmed"),
    ]
    
    added = 0
    skipped = 0
    
    with db.get_session() as session:
        from src.database.models import Synonym
        for syn_text, analyte_id, vendor, source in correct_mappings:
            # Check if already exists
            existing = session.query(Synonym).filter(
                Synonym.synonym_text == syn_text,
                Synonym.analyte_id == analyte_id
            ).first()
            
            if existing:
                skipped += 1
                continue
            
            new_syn = Synonym(
                synonym_text=syn_text,
                analyte_id=analyte_id,
                synonym_type='lab_name',
                harvest_source=source,
                is_valid=True,
                vendor=vendor,
            )
            session.add(new_syn)
            added += 1
            print(f"  + {syn_text:45s} -> {analyte_id:25s} [{vendor or 'any'}]")
    
    print(f"\n  Added {added} new synonyms, {skipped} already existed")
    return added


def mark_non_chemical_rows():
    """Mark calculated parameters and footer text as 'not_chemical' so they don't count against accuracy."""
    conn = sqlite3.connect('data/lab_results.db')
    
    non_chemicals = [
        '% Difference', 'Ion Ratio', 'Sodium Adsorption Ratio',
        'TDS (Ion Sum Calc)', 'TDS(calc.)/EC(actual)',
        'Conductivity Calc / Conductivity', 'Langelier Index(25°C)',
        'Saturation pH (25°C)', 'pH (Client Data)', 'Temperature (Client Data)',
        'R.L. = Reporting Limit', 'Anion Sum', 'Conductivity Calc',
    ]
    
    # Also catch footer lines
    footer_patterns = [
        'The analytical results reported herein%',
        'prior written consent from Caduceon%',
        'Reproduction of this analytical%',
    ]
    
    total_marked = 0
    
    # Mark specific non-chemical names
    for name in non_chemicals:
        cur = conn.execute("""
            UPDATE lab_results 
            SET validation_status = 'not_chemical', validation_notes = 'Calculated parameter / non-chemical row'
            WHERE chemical_raw = ? AND validation_status = 'pending'
        """, (name,))
        if cur.rowcount > 0:
            print(f"  Marked {cur.rowcount:3d} rows: {name}")
            total_marked += cur.rowcount
    
    # Mark footer lines
    for pattern in footer_patterns:
        cur = conn.execute("""
            UPDATE lab_results 
            SET validation_status = 'not_chemical', validation_notes = 'Footer / disclaimer text'
            WHERE chemical_raw LIKE ? AND validation_status = 'pending'
        """, (pattern,))
        if cur.rowcount > 0:
            print(f"  Marked {cur.rowcount:3d} rows: {pattern[:50]}...")
            total_marked += cur.rowcount
    
    # Mark QC surrogates (Eurofins)
    surrogates = ['4-bromofluorobenzene', 'Decachlorobiphenyl', '1,2-dichloroethane-d4',
                  'Alpha-androstrane', 'Moisture-Humidite']
    for name in surrogates:
        cur = conn.execute("""
            UPDATE lab_results 
            SET validation_status = 'not_chemical', validation_notes = 'QC surrogate / not Reg 153'
            WHERE chemical_raw = ? AND validation_status = 'pending'
        """, (name,))
        if cur.rowcount > 0:
            print(f"  Marked {cur.rowcount:3d} rows: {name} (surrogate)")
            total_marked += cur.rowcount
    
    conn.commit()
    print(f"\n  Total marked as non-chemical: {total_marked}")
    conn.close()
    return total_marked


def fix_wrong_matches():
    """Fix specific known-wrong fuzzy matches."""
    conn = sqlite3.connect('data/lab_results.db')
    
    fixes = [
        # pH - CaCl2 is a soil pH measurement, not a VOC
        ("pH - CaCl2", None, 0.0, "not_chemical", "Soil pH measurement, not a Reg 153 chemical"),
    ]
    
    for raw, correct_id, new_conf, status, notes in fixes:
        cur = conn.execute("""
            UPDATE lab_results 
            SET analyte_id = ?, match_confidence = ?, validation_status = ?, validation_notes = ?
            WHERE chemical_raw = ?
        """, (correct_id, new_conf, status, notes, raw))
        if cur.rowcount > 0:
            print(f"  Fixed {cur.rowcount} rows: {raw} -> {status}")
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    print("=" * 70)
    print("STEP 1: Auto-validate perfect submissions")
    print("=" * 70)
    auto_validate_perfect_submissions()
    
    print("\n" + "=" * 70)
    print("STEP 2: Mark non-chemical rows")
    print("=" * 70)
    mark_non_chemical_rows()
    
    print("\n" + "=" * 70)
    print("STEP 3: Fix wrong matches")
    print("=" * 70)
    fix_wrong_matches()
    
    print("\n" + "=" * 70)
    print("STEP 4: Add missing synonyms for correct fuzzy matches")
    print("=" * 70)
    add_missing_synonyms()
    
    # Final summary
    print("\n" + "=" * 70)
    print("FINAL STATUS")
    print("=" * 70)
    conn = sqlite3.connect('data/lab_results.db')
    total = conn.execute('SELECT COUNT(*) FROM lab_results').fetchone()[0]
    accepted = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'accepted'").fetchone()[0]
    not_chem = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'not_chemical'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'pending'").fetchone()[0]
    
    real_total = total - not_chem
    print(f"  Total results:     {total}")
    print(f"  Accepted:          {accepted}")
    print(f"  Not-chemical:      {not_chem}")
    print(f"  Still pending:     {pending}")
    print(f"  Real accuracy:     {accepted}/{real_total} ({100*accepted/real_total:.1f}%) (excluding non-chemicals)")
    conn.close()
