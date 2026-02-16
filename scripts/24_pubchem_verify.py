"""
Verify low-confidence chemical matches using PubChem REST API.

For each low-confidence match from batch ingestion, this script:
1. Looks up the lab chemical name on PubChem
2. Gets its CAS number and canonical name
3. Compares against the matched analyte's CAS number
4. Reports whether the match is CORRECT, WRONG, or NOT FOUND

PubChem API: https://pubchem.ncbi.nlm.nih.gov/rest/pug/
Rate limit: 5 requests per second
"""
import sys
import time
import json
import sqlite3
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
RATE_DELAY = 0.22  # ~4.5 req/sec to stay under 5/sec limit


class PubChemVerifier:
    """Verify chemical identity via PubChem."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Reg153ChemicalMatcher/1.0 (verification)'
        })
        self.last_request = 0
        self.cache: Dict[str, dict] = {}
    
    def _throttle(self):
        elapsed = time.time() - self.last_request
        if elapsed < RATE_DELAY:
            time.sleep(RATE_DELAY - elapsed)
        self.last_request = time.time()
    
    def lookup_name(self, name: str) -> Optional[dict]:
        """
        Look up a chemical name on PubChem.
        
        Returns dict with: cid, iupac_name, cas_numbers, synonyms, molecular_formula
        or None if not found.
        """
        # Check cache
        key = name.strip().lower()
        if key in self.cache:
            return self.cache[key]
        
        self._throttle()
        
        # Step 1: Get CID from name
        try:
            url = f"{PUBCHEM_BASE}/compound/name/{requests.utils.quote(name)}/property/IUPACName,MolecularFormula,InChIKey/JSON"
            resp = self.session.get(url, timeout=15)
            
            if resp.status_code == 404:
                self.cache[key] = None
                return None
            resp.raise_for_status()
            
            data = resp.json()
            props = data.get('PropertyTable', {}).get('Properties', [{}])[0]
            cid = props.get('CID')
            iupac = props.get('IUPACName', '')
            formula = props.get('MolecularFormula', '')
            inchikey = props.get('InChIKey', '')
            
        except (requests.RequestException, KeyError, IndexError):
            self.cache[key] = None
            return None
        
        # Step 2: Get synonyms (which include CAS numbers)
        self._throttle()
        cas_numbers = []
        top_synonyms = []
        try:
            url2 = f"{PUBCHEM_BASE}/compound/cid/{cid}/synonyms/JSON"
            resp2 = self.session.get(url2, timeout=15)
            if resp2.status_code == 200:
                syns = resp2.json().get('InformationList', {}).get('Information', [{}])[0].get('Synonym', [])
                for s in syns[:5]:
                    top_synonyms.append(s)
                # Extract CAS numbers (pattern: digits-digits-digit)
                import re
                cas_pattern = re.compile(r'^\d{2,7}-\d{2}-\d$')
                for s in syns:
                    if cas_pattern.match(s):
                        cas_numbers.append(s)
        except Exception:
            pass
        
        result = {
            'cid': cid,
            'iupac_name': iupac,
            'formula': formula,
            'inchikey': inchikey,
            'cas_numbers': cas_numbers,
            'top_synonyms': top_synonyms,
        }
        self.cache[key] = result
        return result
    
    def lookup_cas(self, cas: str) -> Optional[dict]:
        """Look up a CAS number on PubChem."""
        key = f"cas:{cas}"
        if key in self.cache:
            return self.cache[key]
        
        self._throttle()
        try:
            url = f"{PUBCHEM_BASE}/compound/name/{cas}/property/IUPACName,MolecularFormula/JSON"
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 404:
                self.cache[key] = None
                return None
            resp.raise_for_status()
            data = resp.json()
            props = data.get('PropertyTable', {}).get('Properties', [{}])[0]
            result = {
                'cid': props.get('CID'),
                'iupac_name': props.get('IUPACName', ''),
                'formula': props.get('MolecularFormula', ''),
            }
            self.cache[key] = result
            return result
        except Exception:
            self.cache[key] = None
            return None


def get_low_confidence_matches() -> List[dict]:
    """Get all unique low-confidence matches from lab_results."""
    conn = sqlite3.connect('data/lab_results.db')
    rows = conn.execute("""
        SELECT DISTINCT lr.chemical_raw, lr.match_confidence, lr.analyte_id, lr.match_method,
               ls.lab_vendor
        FROM lab_results lr
        JOIN lab_submissions ls ON lr.submission_id = ls.submission_id
        WHERE lr.match_confidence < 0.95 AND lr.validation_status = 'pending'
        ORDER BY lr.match_confidence ASC
    """).fetchall()
    conn.close()
    
    results = []
    for r in rows:
        results.append({
            'chemical_raw': r[0],
            'confidence': r[1],
            'analyte_id': r[2],
            'match_method': r[3],
            'vendor': r[4],
        })
    return results


def get_analyte_info(analyte_id: str) -> Optional[dict]:
    """Get analyte details from matcher database."""
    if not analyte_id:
        return None
    conn = sqlite3.connect('data/reg153_matcher.db')
    row = conn.execute(
        'SELECT preferred_name, cas_number FROM analytes WHERE analyte_id = ?',
        (analyte_id,)
    ).fetchone()
    conn.close()
    if row:
        return {'preferred_name': row[0], 'cas_number': row[1]}
    return None


def verify_match(verifier: PubChemVerifier, lab_name: str, analyte_info: Optional[dict]) -> dict:
    """
    Verify whether lab_name correctly maps to the analyte.
    
    Returns dict with: verdict, pubchem_info, reason
    """
    # Look up the lab name on PubChem
    pc = verifier.lookup_name(lab_name)
    
    if pc is None:
        return {
            'verdict': 'NOT_FOUND',
            'reason': f'"{lab_name}" not found in PubChem',
            'pubchem': None,
        }
    
    if analyte_info is None:
        return {
            'verdict': 'NO_MATCH',
            'reason': 'No analyte_id assigned',
            'pubchem': pc,
        }
    
    analyte_cas = analyte_info.get('cas_number')
    
    # If analyte has no CAS, try IUPAC name comparison
    if not analyte_cas or analyte_cas == 'None':
        # Best-effort: compare names
        analyte_name = analyte_info['preferred_name'].lower()
        iupac = (pc.get('iupac_name') or '').lower()
        top_syns_lower = [s.lower() for s in pc.get('top_synonyms', [])]
        
        if analyte_name in top_syns_lower or analyte_name in iupac:
            return {
                'verdict': 'LIKELY_CORRECT',
                'reason': f'Name match (IUPAC: {pc["iupac_name"]})',
                'pubchem': pc,
            }
        return {
            'verdict': 'UNCERTAIN',
            'reason': f'No CAS to compare, IUPAC: {pc["iupac_name"]}',
            'pubchem': pc,
        }
    
    # Compare CAS numbers
    pc_cas_list = pc.get('cas_numbers', [])
    if analyte_cas in pc_cas_list:
        return {
            'verdict': 'CORRECT',
            'reason': f'CAS match: {analyte_cas} (PubChem CID {pc["cid"]})',
            'pubchem': pc,
        }
    
    # CAS doesn't match - check if PubChem identifies it as a different compound
    if pc_cas_list:
        return {
            'verdict': 'WRONG',
            'reason': f'CAS mismatch: analyte={analyte_cas}, PubChem={pc_cas_list[0]} (IUPAC: {pc["iupac_name"]})',
            'pubchem': pc,
        }
    
    # PubChem found compound but no CAS — compare by looking up the analyte CAS
    analyte_pc = verifier.lookup_cas(analyte_cas)
    if analyte_pc and analyte_pc.get('cid') == pc.get('cid'):
        return {
            'verdict': 'CORRECT',
            'reason': f'Same PubChem CID ({pc["cid"]})',
            'pubchem': pc,
        }
    
    return {
        'verdict': 'UNCERTAIN',
        'reason': f'Could not confirm match (IUPAC: {pc["iupac_name"]})',
        'pubchem': pc,
    }


def apply_corrections(results: List[dict]):
    """Apply verified corrections to lab_results database."""
    conn = sqlite3.connect('data/lab_results.db')
    
    corrected = 0
    marked_not_chem = 0
    confirmed = 0
    
    for r in results:
        verdict = r['verdict']
        chem_raw = r['chemical_raw']
        
        if verdict == 'CORRECT':
            # Confirm the match — boost confidence to 1.0
            conn.execute("""
                UPDATE lab_results 
                SET match_confidence = 1.0, 
                    validation_status = 'accepted',
                    validation_notes = ?
                WHERE chemical_raw = ? AND validation_status = 'pending'
            """, (f"PubChem verified: {r['reason']}", chem_raw))
            confirmed += 1
            
        elif verdict == 'LIKELY_CORRECT':
            # Accept with note
            conn.execute("""
                UPDATE lab_results 
                SET match_confidence = 0.98,
                    validation_status = 'accepted',
                    validation_notes = ?
                WHERE chemical_raw = ? AND validation_status = 'pending'
            """, (f"PubChem likely: {r['reason']}", chem_raw))
            confirmed += 1
            
        elif verdict == 'WRONG':
            # Mark for review with the correct info
            pc = r.get('pubchem', {})
            note = f"WRONG MATCH - PubChem says: {r['reason']}"
            conn.execute("""
                UPDATE lab_results 
                SET validation_status = 'needs_review',
                    validation_notes = ?
                WHERE chemical_raw = ? AND validation_status = 'pending'
            """, (note, chem_raw))
            corrected += 1
            
        elif verdict == 'NOT_FOUND':
            # Not in PubChem — likely a calculated parameter, QC surrogate, or non-standard name
            conn.execute("""
                UPDATE lab_results 
                SET validation_status = 'not_chemical',
                    validation_notes = ?
                WHERE chemical_raw = ? AND validation_status = 'pending'
                  AND (analyte_id IS NULL OR match_confidence = 0.0)
            """, (f"Not found in PubChem: likely non-chemical or QC", chem_raw))
            marked_not_chem += 1
    
    conn.commit()
    conn.close()
    return confirmed, corrected, marked_not_chem


def main():
    print("=" * 80)
    print("PUBCHEM VERIFICATION OF LOW-CONFIDENCE MATCHES")
    print("=" * 80)
    
    # Get low-confidence matches
    matches = get_low_confidence_matches()
    print(f"\nFound {len(matches)} unique low-confidence chemicals to verify\n")
    
    if not matches:
        print("Nothing to verify — all matches are high confidence!")
        return
    
    verifier = PubChemVerifier()
    results = []
    
    # Verify each match
    for i, m in enumerate(matches, 1):
        lab_name = m['chemical_raw']
        analyte_id = m['analyte_id']
        confidence = m['confidence']
        vendor = m['vendor']
        
        # Get analyte info
        analyte_info = get_analyte_info(analyte_id) if analyte_id else None
        analyte_name = analyte_info['preferred_name'] if analyte_info else 'UNMATCHED'
        
        # Skip footer text and clear non-chemicals
        skip_patterns = ['analytical results reported', 'prior written consent',
                        'reproduction', 'R.L. =', 'reporting limit',
                        'Conductivity Calc / Conductivity', '% Difference',
                        'TDS(calc.)', 'Langelier', 'Saturation pH',
                        'Client Data', 'Ion Ratio']
        is_non_chem = any(p.lower() in lab_name.lower() for p in skip_patterns)
        
        if is_non_chem:
            print(f"  [{i:2d}/{len(matches)}] {lab_name[:50]:50s} -> SKIP (non-chemical)")
            results.append({
                'chemical_raw': lab_name,
                'verdict': 'NOT_FOUND',
                'reason': 'Known non-chemical pattern',
                'analyte_id': analyte_id,
                'analyte_name': analyte_name,
            })
            continue
        
        # Query PubChem
        verification = verify_match(verifier, lab_name, analyte_info)
        verdict = verification['verdict']
        reason = verification['reason']
        
        # Color-code verdict
        if verdict == 'CORRECT':
            tag = 'OK'
        elif verdict == 'LIKELY_CORRECT':
            tag = '~OK'
        elif verdict == 'WRONG':
            tag = 'WRONG'
        elif verdict == 'NOT_FOUND':
            tag = 'N/A'
        else:
            tag = '???'
        
        pc = verification.get('pubchem')
        pc_cas = pc['cas_numbers'][0] if pc and pc.get('cas_numbers') else '-'
        a_cas = analyte_info['cas_number'] if analyte_info and analyte_info.get('cas_number') else '-'
        
        print(f"  [{i:2d}/{len(matches)}] {tag:5s}  {confidence:.2f}  {lab_name[:42]:42s} -> {analyte_name[:28]:28s}  lab_CAS={pc_cas:12s}  db_CAS={a_cas:12s}  {reason[:60]}")
        
        results.append({
            'chemical_raw': lab_name,
            'verdict': verdict,
            'reason': reason,
            'analyte_id': analyte_id,
            'analyte_name': analyte_name,
            'pubchem': verification.get('pubchem'),
        })
    
    # Summary
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    
    verdicts = {}
    for r in results:
        v = r['verdict']
        verdicts[v] = verdicts.get(v, 0) + 1
    
    for v, count in sorted(verdicts.items()):
        print(f"  {v:20s}: {count}")
    
    # Show wrong matches in detail
    wrong = [r for r in results if r['verdict'] == 'WRONG']
    if wrong:
        print(f"\n{'=' * 80}")
        print("WRONG MATCHES (need correction)")
        print("=" * 80)
        for r in wrong:
            print(f"\n  Lab name:     {r['chemical_raw']}")
            print(f"  Matched to:   {r['analyte_name']} ({r['analyte_id']})")
            print(f"  PubChem says: {r['reason']}")
            pc = r.get('pubchem', {})
            if pc:
                print(f"  PubChem CAS:  {pc.get('cas_numbers', [])}")
                print(f"  PubChem syns: {pc.get('top_synonyms', [])[:3]}")
    
    # Apply corrections
    print(f"\n{'=' * 80}")
    print("APPLYING CORRECTIONS")
    print("=" * 80)
    confirmed, corrected, marked = apply_corrections(results)
    print(f"  Confirmed correct:  {confirmed}")
    print(f"  Marked for review:  {corrected}")  
    print(f"  Marked non-chemical: {marked}")
    
    # Final state
    conn = sqlite3.connect('data/lab_results.db')
    total = conn.execute('SELECT COUNT(*) FROM lab_results').fetchone()[0]
    accepted = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'accepted'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'pending'").fetchone()[0]
    not_chem = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'not_chemical'").fetchone()[0]
    review = conn.execute("SELECT COUNT(*) FROM lab_results WHERE validation_status = 'needs_review'").fetchone()[0]
    conn.close()
    
    print(f"\n  Database state:")
    print(f"    Total:        {total}")
    print(f"    Accepted:     {accepted}")
    print(f"    Pending:      {pending}")
    print(f"    Not-chemical: {not_chem}")
    print(f"    Needs review: {review}")


if __name__ == '__main__':
    main()
