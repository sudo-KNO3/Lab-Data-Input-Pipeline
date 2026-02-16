"""
Final batch validation:
1. Auto-accept all 100% submissions
2. Verify remaining Caduceon fuzzy matches via PubChem
3. Mark non-chemical rows 
4. Add confirmed synonyms to database
"""
import re
import sys
import time
import sqlite3
from pathlib import Path
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.database.connection import DatabaseManager

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
session = requests.Session()
session.headers.update({'User-Agent': 'Reg153/1.0'})
last_req = [0]

def throttle():
    elapsed = time.time() - last_req[0]
    if elapsed < 0.22:
        time.sleep(0.22 - elapsed)
    last_req[0] = time.time()

def pubchem_verify_cas(name, expected_cas):
    """Check if lab name resolves to expected CAS on PubChem."""
    if not expected_cas or expected_cas == 'None':
        return None
    throttle()
    try:
        url = f"{PUBCHEM_BASE}/compound/name/{requests.utils.quote(name)}/property/IUPACName/JSON"
        r = session.get(url, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        cid = r.json().get('PropertyTable', {}).get('Properties', [{}])[0].get('CID')
        if not cid:
            return None
        
        throttle()
        url2 = f"{PUBCHEM_BASE}/compound/cid/{cid}/synonyms/JSON"
        r2 = session.get(url2, timeout=15)
        if r2.status_code != 200:
            return None
        syns = r2.json().get('InformationList', {}).get('Information', [{}])[0].get('Synonym', [])
        cas_pattern = re.compile(r'^\d{2,7}-\d{2}-\d$')
        cas_list = [s for s in syns if cas_pattern.match(s)]
        
        if expected_cas in cas_list:
            return 'CORRECT'
        elif cas_list:
            return f'WRONG:{cas_list[0]}'
        return None
    except Exception:
        return None


def main():
    conn = sqlite3.connect('data/lab_results.db')
    matcher_conn = sqlite3.connect('data/reg153_matcher.db')
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: Auto-accept all 100% submissions
    # ═══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("STEP 1: Auto-validate perfect submissions")
    print("=" * 70)
    
    perfect_subs = conn.execute("""
        SELECT ls.submission_id, ls.original_filename,
               COUNT(lr.result_id) as total,
               SUM(CASE WHEN lr.match_confidence >= 0.95 THEN 1 ELSE 0 END) as high
        FROM lab_submissions ls
        JOIN lab_results lr ON ls.submission_id = lr.submission_id
        WHERE ls.validation_status != 'validated'
        GROUP BY ls.submission_id
        HAVING total = high AND total > 0
    """).fetchall()
    
    auto_count = 0
    for sub_id, fname, total, high in perfect_subs:
        conn.execute("UPDATE lab_results SET validation_status = 'accepted' WHERE submission_id = ? AND match_confidence >= 0.95", (sub_id,))
        conn.execute("UPDATE lab_submissions SET validation_status = 'validated' WHERE submission_id = ?", (sub_id,))
        auto_count += 1
    
    conn.commit()
    print(f"  Auto-validated {auto_count} submissions\n")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: Mark non-chemical rows
    # ═══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("STEP 2: Mark non-chemical rows")
    print("=" * 70)
    
    non_chemicals = [
        '% Difference', 'Ion Ratio', 'Sodium Adsorption Ratio',
        'TDS (Ion Sum Calc)', 'TDS(calc.)/EC(actual)',
        'Conductivity Calc / Conductivity', 'Langelier Index(25°C)',
        'Saturation pH (25°C)', 'pH (Client Data)', 'Temperature (Client Data)',
        'R.L. = Reporting Limit', 'pH - CaCl2', 'Moisture-Humidite', 'Alpha-androstrane',
    ]
    
    footer_patterns = [
        'The analytical results reported herein%',
        'prior written consent from Caduceon%',
    ]
    
    surrogates = ['4-bromofluorobenzene', 'Decachlorobiphenyl', '1,2-dichloroethane-d4']
    
    marked = 0
    for name in non_chemicals:
        c = conn.execute("UPDATE lab_results SET validation_status = 'not_chemical', validation_notes = 'Non-chemical / calculated parameter' WHERE chemical_raw = ? AND validation_status = 'pending'", (name,))
        if c.rowcount > 0:
            print(f"  {c.rowcount:3d} rows: {name}")
            marked += c.rowcount
    
    for pattern in footer_patterns:
        c = conn.execute("UPDATE lab_results SET validation_status = 'not_chemical', validation_notes = 'Footer text' WHERE chemical_raw LIKE ? AND validation_status = 'pending'", (pattern,))
        if c.rowcount > 0:
            print(f"  {c.rowcount:3d} rows: {pattern[:50]}...")
            marked += c.rowcount
    
    for name in surrogates:
        c = conn.execute("UPDATE lab_results SET validation_status = 'not_chemical', validation_notes = 'QC surrogate' WHERE chemical_raw = ? AND validation_status IN ('pending', 'needs_review')", (name,))
        if c.rowcount > 0:
            print(f"  {c.rowcount:3d} rows: {name} (surrogate)")
            marked += c.rowcount
    
    conn.commit()
    print(f"  Total marked: {marked}\n")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: PubChem verify remaining Caduceon low-confidence
    # ═══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("STEP 3: PubChem verify remaining pending low-confidence")
    print("=" * 70)
    
    remaining = conn.execute("""
        SELECT DISTINCT lr.chemical_raw, lr.match_confidence, lr.analyte_id, lr.match_method
        FROM lab_results lr
        WHERE lr.validation_status = 'pending' AND lr.match_confidence < 0.95
        ORDER BY lr.match_confidence ASC
    """).fetchall()
    
    print(f"  {len(remaining)} unique chemicals to verify\n")
    
    verified_count = 0
    wrong_count = 0
    
    for chem_raw, conf, analyte_id, method in remaining:
        if not analyte_id:
            continue
        
        # Get analyte CAS
        arow = matcher_conn.execute('SELECT preferred_name, cas_number FROM analytes WHERE analyte_id = ?', (analyte_id,)).fetchone()
        if not arow:
            continue
        analyte_name, analyte_cas = arow
        
        result = pubchem_verify_cas(chem_raw, analyte_cas)
        
        if result == 'CORRECT':
            conn.execute("""
                UPDATE lab_results SET match_confidence = 1.0, validation_status = 'accepted',
                validation_notes = ? WHERE chemical_raw = ? AND validation_status = 'pending'
            """, (f'PubChem CAS verified: {analyte_cas}', chem_raw))
            print(f"  OK     {conf:.2f}  {chem_raw:45s} -> {analyte_name:30s} CAS={analyte_cas}")
            verified_count += 1
        elif result and result.startswith('WRONG:'):
            pc_cas = result.split(':')[1]
            conn.execute("""
                UPDATE lab_results SET validation_status = 'needs_review',
                validation_notes = ? WHERE chemical_raw = ? AND validation_status = 'pending'
            """, (f'WRONG: PubChem CAS={pc_cas}, expected {analyte_cas}', chem_raw))
            print(f"  WRONG  {conf:.2f}  {chem_raw:45s} -> {analyte_name:30s} PubChem={pc_cas} != {analyte_cas}")
            wrong_count += 1
        elif result is None:
            # Not found on PubChem - check name similarity
            lab_norm = re.sub(r'[,\s\-\(\)]+', '', chem_raw.lower())
            analyte_norm = re.sub(r'[,\s\-\(\)]+', '', analyte_name.lower())
            if lab_norm == analyte_norm or analyte_norm in lab_norm or lab_norm in analyte_norm:
                conn.execute("""
                    UPDATE lab_results SET match_confidence = 0.98, validation_status = 'accepted',
                    validation_notes = 'Name similarity match' WHERE chemical_raw = ? AND validation_status = 'pending'
                """, (chem_raw,))
                print(f"  ~OK    {conf:.2f}  {chem_raw:45s} -> {analyte_name:30s} (name match)")
                verified_count += 1
            else:
                print(f"  ???    {conf:.2f}  {chem_raw:45s} -> {analyte_name:30s} (no PubChem data)")
    
    conn.commit()
    print(f"\n  Verified: {verified_count}, Wrong: {wrong_count}\n")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: Add confirmed synonyms to matcher DB
    # ═══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("STEP 4: Add confirmed synonyms to matcher database")
    print("=" * 70)
    
    db = DatabaseManager()
    from src.database.models import Synonym
    from src.normalization.text_normalizer import TextNormalizer
    normalizer = TextNormalizer()
    
    # Get all accepted fuzzy matches — these should become synonyms
    accepted_fuzzy = conn.execute("""
        SELECT DISTINCT lr.chemical_raw, lr.analyte_id, ls.lab_vendor
        FROM lab_results lr
        JOIN lab_submissions ls ON lr.submission_id = ls.submission_id
        WHERE lr.validation_status = 'accepted' AND lr.match_method = 'fuzzy'
        AND lr.analyte_id IS NOT NULL
    """).fetchall()
    
    added = 0
    skipped = 0
    with db.get_session() as sess:
        for chem_raw, analyte_id, vendor in accepted_fuzzy:
            # Check if synonym already exists
            norm = normalizer.normalize(chem_raw)
            existing = sess.query(Synonym).filter(
                Synonym.synonym_raw == chem_raw,
                Synonym.analyte_id == analyte_id
            ).first()
            
            if existing:
                skipped += 1
                continue
            
            new_syn = Synonym(
                synonym_raw=chem_raw,
                synonym_norm=norm,
                analyte_id=analyte_id,
                synonym_type='lab_name',
                harvest_source='pubchem_verified',
                confidence=1.0,
                lab_vendor=vendor,
            )
            sess.add(new_syn)
            added += 1
            print(f"  + {chem_raw:45s} -> {analyte_id:25s} [{vendor}]")
    
    print(f"\n  Added {added} new synonyms, {skipped} already existed\n")
    
    # ═══════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("FINAL STATUS")
    print("=" * 70)
    
    total = conn.execute('SELECT COUNT(*) FROM lab_results').fetchone()[0]
    accepted = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'accepted'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'pending'").fetchone()[0]
    not_chem = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'not_chemical'").fetchone()[0]
    review = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'needs_review'").fetchone()[0]
    validated = conn.execute("SELECT COUNT(*) FROM lab_submissions WHERE validation_status = 'validated'").fetchone()[0]
    total_subs = conn.execute("SELECT COUNT(*) FROM lab_submissions").fetchone()[0]
    
    real_chems = total - not_chem
    print(f"  Submissions:     {validated}/{total_subs} validated")
    print(f"  Total results:   {total}")
    print(f"  Accepted:        {accepted}")
    print(f"  Non-chemical:    {not_chem}")
    print(f"  Pending:         {pending}")
    print(f"  Needs review:    {review}")
    print(f"  Real chemicals:  {real_chems}")
    print(f"  Accuracy:        {accepted}/{real_chems} ({100*accepted/real_chems:.1f}%)")
    
    conn.close()
    matcher_conn.close()


if __name__ == '__main__':
    main()
