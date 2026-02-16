"""
Script 22: Batch Ingest Lab Files

Processes all new lab files from the Excel Lab examples folder.
Auto-detects vendor format (Caduceon CA / Eurofins) and applies
format-specific extraction with header filtering.

Usage:
    python scripts/22_batch_ingest.py                          # Process all new files
    python scripts/22_batch_ingest.py --dry-run                # Preview without ingesting
    python scripts/22_batch_ingest.py --filter CA              # Only CA-prefixed files
    python scripts/22_batch_ingest.py --filter Eurofins        # Only Eurofins files
    python scripts/22_batch_ingest.py --file "CA40000-3FEB25.XLS"  # Single file
"""
import argparse
import sqlite3
import hashlib
import re
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.matching.resolution_engine import ResolutionEngine
from src.normalization.text_normalizer import TextNormalizer


# ═══════════════════════════════════════════════════════════════════════════════
# Format-specific row filters (reject non-chemical rows)
# ═══════════════════════════════════════════════════════════════════════════════

# Caduceon CA files: header/subheader text that appears in col 0 but isn't a chemical
CA_SKIP_ROWS = {
    'sample id', 'sample date / time', 'sample date & time',
    'analysis', 'units', 'temperature upon receipt',
    'temperature', 'temp upon receipt', 'sample date',
    'date received', 'date reported', 'report no.', 'report no',
    'customer', 'attention', 'reference', 'works#', 'title',
    'method', 'mdl', 'rdl', 'rl', 'notes', 'comments',
}

# Generic footer/disclaimer patterns
FOOTER_PATTERNS = [
    'prior written consent',
    'analytical results reported',
    'reproduction',
    'reporting limit',
    'r.l. =', 'rl =', 'mdl =',
    'laboratory', 'laboratories',
    'copyright', 'confidential',
    'prohibited without',
    'refer to the samples',
    'digitally signed',
    'official results',
    'page ', 'of ',
    'this report',
    'the results',
    'revision',
]


def calculate_file_hash(file_path: Path) -> str:
    """MD5 hash for deduplication."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def detect_format(df: pd.DataFrame, filename: str) -> str:
    """
    Detect lab file format.
    
    Returns:
        'caduceon_ca', 'eurofins', or 'caduceon_xlsx'
    """
    # Eurofins: row 0 col 5 says "Eurofins"
    if df.shape[1] > 5:
        cell05 = str(df.iloc[0, 5]) if pd.notna(df.iloc[0, 5]) else ''
        if 'eurofins' in cell05.lower():
            return 'eurofins'
    
    # Caduceon CA: row 6 has "Report No." in col 0
    if df.shape[0] > 6:
        cell60 = str(df.iloc[6, 0]) if pd.notna(df.iloc[6, 0]) else ''
        if 'report no' in cell60.lower():
            return 'caduceon_ca'
    
    # Caduceon xlsx (original format)
    if filename.lower().endswith('.xlsx') and 'caduceon' in filename.lower():
        return 'caduceon_xlsx'
    
    # Fallback: try to detect from content
    return 'unknown'


def extract_ca_metadata(df: pd.DataFrame) -> Dict:
    """Extract metadata from a Caduceon CA file header."""
    meta = {'report_no': '', 'customer': '', 'attention': '', 'reference': ''}
    for i in range(min(12, len(df))):
        key = str(df.iloc[i, 0]).strip().lower() if pd.notna(df.iloc[i, 0]) else ''
        val = str(df.iloc[i, 1]).strip() if df.shape[1] > 1 and pd.notna(df.iloc[i, 1]) else ''
        if 'report no' in key:
            meta['report_no'] = val
        elif key == 'customer':
            meta['customer'] = val
        elif key == 'attention':
            meta['attention'] = val
        elif key == 'reference':
            meta['reference'] = val
    return meta


def extract_eurofins_metadata(df: pd.DataFrame) -> Dict:
    """Extract metadata from a Eurofins file header."""
    meta = {'workorder': '', 'client': '', 'date_received': '', 'contact': ''}
    for i in range(min(10, len(df))):
        cell = str(df.iloc[i, 0]) if pd.notna(df.iloc[i, 0]) else ''
        if 'workorder' in cell.lower():
            meta['workorder'] = cell.replace('Workorder No.:', '').strip()
        elif 'client' in cell.lower():
            meta['client'] = cell.replace('Client:', '').strip()
        elif 'date received' in cell.lower():
            meta['date_received'] = cell.replace('Date Received:', '').strip()
        elif 'contact' in cell.lower():
            meta['contact'] = cell.replace('Contact:', '').strip()
    return meta


def is_chemical_row(chem_name: str, fmt: str) -> bool:
    """Check if a row contains a valid chemical name (not a header/footer)."""
    if not chem_name or len(chem_name) < 2:
        return False
    
    chem_lower = chem_name.lower().strip()
    
    # Skip pure numbers
    if re.match(r'^[\d\.\-\<\>]+$', chem_name):
        return False
    
    # Skip known non-chemical items
    if chem_lower in {'total', 'sum', 'notes', 'comments', '', 'nan'}:
        return False
    
    # Skip footer text
    if any(p in chem_lower for p in FOOTER_PATTERNS):
        return False
    
    # Skip very long text (disclaimers)
    if len(chem_name) > 100:
        return False
    
    # Format-specific filters
    if fmt == 'caduceon_ca':
        if chem_lower in CA_SKIP_ROWS:
            return False
        # Skip rows that start with common header patterns
        if chem_lower.startswith(('sample ', 'date ', 'time ')):
            return False
    
    if fmt == 'eurofins':
        # Eurofins QC rows to skip
        if chem_lower in {'decachlorobiphenyl', 'alpha-androstrane'}:
            # These are surrogate/QC markers, not chemicals of interest
            # Actually let's keep them - they might be in the DB
            pass
    
    return True


def extract_ca_chemicals(df: pd.DataFrame) -> List[Tuple[int, str, str, str, str, str]]:
    """
    Extract chemicals from a Caduceon CA format file.
    
    Layout:
        Row 13: Sample ID headers
        Row 15: Analysis, Units
        Row 17: Temperature Upon Receipt (skip)
        Row 18+: Chemical data (col 0=name, col 1=units, cols 2-5=dates, 
                 col 6=MAC, col 7=AO/OG, col 8+=sample results)
    
    Returns:
        List of (row_num, chem_name, units, mac_limit, result_value, sample_id)
    """
    chemicals = []
    
    # Find sample IDs from the header (row 13, col 8+)
    sample_ids = []
    if df.shape[0] > 13:
        for c in range(8, df.shape[1]):
            val = str(df.iloc[13, c]).strip() if pd.notna(df.iloc[13, c]) else ''
            if val and val != 'nan':
                sample_ids.append((c, val))
    
    # Find where data starts (after header rows)
    data_start = 18  # Standard CA layout
    # Verify by checking if row 18 has a chemical name
    if df.shape[0] > 18:
        test = str(df.iloc[18, 0]).strip() if pd.notna(df.iloc[18, 0]) else ''
        if not test or not is_chemical_row(test, 'caduceon_ca'):
            # Try to find the actual start
            for r in range(15, min(25, len(df))):
                t = str(df.iloc[r, 0]).strip() if pd.notna(df.iloc[r, 0]) else ''
                if t and is_chemical_row(t, 'caduceon_ca'):
                    data_start = r
                    break
    
    for row_idx in range(data_start, len(df)):
        cell = df.iloc[row_idx, 0]
        if pd.isna(cell):
            continue
        chem_name = str(cell).strip()
        
        if not is_chemical_row(chem_name, 'caduceon_ca'):
            continue
        
        # Extract units
        units = str(df.iloc[row_idx, 1]).strip() if df.shape[1] > 1 and pd.notna(df.iloc[row_idx, 1]) else ''
        
        # MAC + AO/OG limits
        mac = str(df.iloc[row_idx, 6]).strip() if df.shape[1] > 6 and pd.notna(df.iloc[row_idx, 6]) else ''
        
        # Get result from first sample column
        result_value = ''
        sample_id = ''
        if sample_ids:
            col_idx = sample_ids[0][0]
            sample_id = sample_ids[0][1]
            if col_idx < df.shape[1] and pd.notna(df.iloc[row_idx, col_idx]):
                result_value = str(df.iloc[row_idx, col_idx]).strip()
        
        chemicals.append((row_idx, chem_name, units, mac, result_value, sample_id))
    
    return chemicals


def extract_eurofins_chemicals(df: pd.DataFrame) -> List[Tuple[int, str, str, str, str, str]]:
    """
    Extract chemicals from a Eurofins format file.
    
    Layout:
        Row 12: Group, Analyte, Units, Analytical Method, MRL, ...sample results
        Row 13+: Data rows (col 0=group, col 1=analyte name, col 2=units,
                 col 3=method, col 4=MRL, cols 5+=sample results)
    
    Returns:
        List of (row_num, chem_name, units, method, result_value, sample_id)
    """
    chemicals = []
    
    # Find header row (should have "Analyte" in col 1)
    header_row = 12
    for r in range(8, min(20, len(df))):
        cell = str(df.iloc[r, 1]) if df.shape[1] > 1 and pd.notna(df.iloc[r, 1]) else ''
        if 'analyte' in cell.lower():
            header_row = r
            break
    
    # Find sample IDs from header (row with Sample ID)
    sample_ids = []
    sample_id_row = header_row - 2  # Usually 2 rows above
    if df.shape[0] > sample_id_row and sample_id_row >= 0:
        for r in range(max(0, header_row - 4), header_row):
            cell = str(df.iloc[r, 4]) if df.shape[1] > 4 and pd.notna(df.iloc[r, 4]) else ''
            if 'sample id' in cell.lower():
                sample_id_row = r
                break
        
        for c in range(5, df.shape[1]):
            val = str(df.iloc[sample_id_row, c]).strip() if pd.notna(df.iloc[sample_id_row, c]) else ''
            if val and val != 'nan':
                sample_ids.append((c, val))
    
    data_start = header_row + 1
    
    for row_idx in range(data_start, len(df)):
        cell = df.iloc[row_idx, 1]  # Analyte in col 1
        if pd.isna(cell):
            continue
        chem_name = str(cell).strip()
        
        if not is_chemical_row(chem_name, 'eurofins'):
            continue
        
        # Units
        units = str(df.iloc[row_idx, 2]).strip() if df.shape[1] > 2 and pd.notna(df.iloc[row_idx, 2]) else ''
        
        # Method
        method = str(df.iloc[row_idx, 3]).strip() if df.shape[1] > 3 and pd.notna(df.iloc[row_idx, 3]) else ''
        
        # First sample result
        result_value = ''
        sample_id = ''
        if sample_ids:
            col_idx = sample_ids[0][0]
            sample_id = sample_ids[0][1]
            if col_idx < df.shape[1] and pd.notna(df.iloc[row_idx, col_idx]):
                result_value = str(df.iloc[row_idx, col_idx]).strip()
        
        chemicals.append((row_idx, chem_name, units, method, result_value, sample_id))
    
    return chemicals


def ingest_file(
    file_path: Path,
    lab_db_path: str,
    db_manager: DatabaseManager,
    normalizer: TextNormalizer,
    vendor_override: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, int, int, float]:
    """
    Ingest a single lab file.
    
    Returns:
        (submission_id, total_chemicals, high_confidence_count, accuracy_estimate)
    """
    file_hash = calculate_file_hash(file_path)
    
    lab_conn = sqlite3.connect(lab_db_path)
    
    # Check duplicate
    existing = lab_conn.execute(
        "SELECT submission_id FROM lab_submissions WHERE file_hash = ?",
        (file_hash,)
    ).fetchone()
    
    if existing:
        lab_conn.close()
        return (existing[0], -1, -1, -1.0)  # Already processed
    
    if dry_run:
        lab_conn.close()
        # Just detect format and count chemicals
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        fmt = detect_format(df, file_path.name)
        if fmt == 'caduceon_ca':
            chemicals = extract_ca_chemicals(df)
        elif fmt == 'eurofins':
            chemicals = extract_eurofins_chemicals(df)
        else:
            chemicals = []
        return (-1, len(chemicals), 0, 0.0)
    
    # Archive
    archive_dir = Path("data/raw/lab_archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"{timestamp}_{file_hash[:8]}_{file_path.name}"
    archive_path = archive_dir / archive_name
    shutil.copy2(file_path, archive_path)
    
    # Read file
    df = pd.read_excel(file_path, sheet_name=0, header=None)
    
    # Detect format
    fmt = detect_format(df, file_path.name)
    
    # Auto-detect vendor
    if vendor_override:
        vendor = vendor_override
    elif fmt == 'eurofins':
        vendor = 'Eurofins'
    elif fmt == 'caduceon_ca':
        vendor = 'Caduceon'
    else:
        vendor = 'Unknown'
    
    # Extract chemicals based on format
    if fmt == 'caduceon_ca':
        raw_chemicals = extract_ca_chemicals(df)
        layout_confidence = 0.95  # Well-understood format
    elif fmt == 'eurofins':
        raw_chemicals = extract_eurofins_chemicals(df)
        layout_confidence = 0.90
    else:
        # Fallback to generic extraction
        raw_chemicals = []
        layout_confidence = 0.50
    
    if not raw_chemicals:
        lab_conn.close()
        return (-1, 0, 0, 0.0)
    
    # Create submission
    lab_conn.execute("""
        INSERT INTO lab_submissions (
            file_path, file_hash, original_filename, lab_vendor,
            file_size_bytes, sheet_name,
            extraction_timestamp, extraction_version, layout_confidence,
            validation_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(archive_path), file_hash, file_path.name, vendor,
        file_path.stat().st_size, '0',
        datetime.now().isoformat(), "2.0.0", layout_confidence, "pending"
    ))
    
    submission_id = lab_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    
    # Match chemicals
    match_stats = {"high": 0, "medium": 0, "low": 0}
    
    with db_manager.get_session() as session:
        resolver = ResolutionEngine(session, normalizer)
        
        for row_num, chem_raw, units, method_or_mac, result_value, sample_id in raw_chemicals:
            chem_norm = normalizer.normalize(chem_raw)
            
            result = resolver.resolve(chem_norm, confidence_threshold=0.70,
                                      vendor=vendor)
            
            if result.best_match and result.best_match.confidence >= 0.70:
                analyte_id = result.best_match.analyte_id
                match_method = result.best_match.method
                match_conf = result.best_match.confidence
                
                if match_conf >= 0.95:
                    match_stats["high"] += 1
                else:
                    match_stats["medium"] += 1
            else:
                analyte_id = None
                match_method = "none"
                match_conf = 0.0
                match_stats["low"] += 1
            
            # Extract qualifier from result value
            qualifier = None
            if result_value:
                q_match = re.match(r'^([<>])', result_value)
                if q_match:
                    qualifier = q_match.group(1)
            
            lab_conn.execute("""
                INSERT INTO lab_results (
                    submission_id, row_number, chemical_raw, chemical_normalized,
                    analyte_id, match_method, match_confidence,
                    sample_id, result_value, units, qualifier,
                    lab_method, validation_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                submission_id, row_num, chem_raw, chem_norm,
                analyte_id, match_method, match_conf,
                sample_id, result_value, units, qualifier,
                method_or_mac if fmt == 'eurofins' else None,
                "pending"
            ))
    
    lab_conn.commit()
    lab_conn.close()
    
    total = len(raw_chemicals)
    accuracy = match_stats['high'] / total * 100 if total > 0 else 0
    
    return (submission_id, total, match_stats['high'], accuracy)


def main():
    parser = argparse.ArgumentParser(description="Batch ingest lab files")
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--filter', choices=['CA', 'Eurofins', 'Caduceon'], 
                        help='Only process files of this type')
    parser.add_argument('--file', help='Process a single file')
    parser.add_argument('--folder', default='Excel Lab examples',
                        help='Folder containing lab files')
    parser.add_argument('--vendor', help='Override vendor detection')
    
    args = parser.parse_args()
    
    folder = Path(args.folder)
    if not folder.exists():
        print(f"ERROR: Folder not found: {folder}")
        sys.exit(1)
    
    lab_db_path = "data/lab_results.db"
    
    # Collect files (deduplicate since Windows glob is case-insensitive)
    if args.file:
        files = [folder / args.file]
    else:
        seen = set()
        files = []
        for pattern in ["*.XLS", "*.xls", "*.xlsx"]:
            for f in sorted(folder.glob(pattern)):
                key = f.name.lower()
                if key not in seen:
                    seen.add(key)
                    files.append(f)
    
    # Filter out already-processed Caduceon xlsx files
    already_processed = set()
    if Path(lab_db_path).exists():
        conn = sqlite3.connect(lab_db_path)
        rows = conn.execute("SELECT file_hash FROM lab_submissions").fetchall()
        already_processed = {r[0] for r in rows}
        conn.close()
    
    # Apply type filter
    if args.filter == 'CA':
        files = [f for f in files if f.name.startswith('CA')]
    elif args.filter == 'Eurofins':
        files = [f for f in files if f.name[0].isdigit() and not f.name.lower().endswith('.xlsx')]
    elif args.filter == 'Caduceon':
        files = [f for f in files if 'caduceon' in f.name.lower() or f.name.startswith('CA')]
    
    print("=" * 80)
    print("BATCH LAB FILE INGESTION")
    print("=" * 80)
    print(f"\nFolder: {folder}")
    print(f"Files found: {len(files)}")
    if args.dry_run:
        print("MODE: DRY RUN (preview only)")
    
    # Initialize components
    db_manager = DatabaseManager()
    normalizer = TextNormalizer()
    
    # Process each file
    results = []
    skipped = 0
    errors = 0
    
    for i, file_path in enumerate(files, 1):
        # Check hash for duplicates
        file_hash = calculate_file_hash(file_path)
        if file_hash in already_processed:
            skipped += 1
            continue
        
        # Quick format detection
        try:
            df_preview = pd.read_excel(file_path, sheet_name=0, header=None, nrows=15)
            fmt = detect_format(df_preview, file_path.name)
        except Exception as e:
            print(f"  [{i}/{len(files)}] ERROR reading {file_path.name}: {e}")
            errors += 1
            continue
        
        vendor = args.vendor or ('Eurofins' if fmt == 'eurofins' else 'Caduceon')
        
        print(f"\n  [{i}/{len(files)}] {file_path.name}")
        print(f"         Format: {fmt}  Vendor: {vendor}")
        
        try:
            sub_id, total, high, accuracy = ingest_file(
                file_path, lab_db_path, db_manager, normalizer,
                vendor_override=args.vendor, dry_run=args.dry_run
            )
            
            if total == -1:
                print(f"         SKIPPED (already processed, submission {sub_id})")
                skipped += 1
                already_processed.add(file_hash)
                continue
            
            if sub_id == -1 and args.dry_run:
                print(f"         DRY RUN: {total} chemicals detected")
                results.append((file_path.name, fmt, vendor, total, 0, 0))
                continue
            
            if total == 0:
                print(f"         WARNING: No chemicals extracted")
                errors += 1
                continue
            
            print(f"         Submission {sub_id}: {total} chemicals, "
                  f"{high} high-conf ({accuracy:.0f}%)")
            results.append((file_path.name, fmt, vendor, total, high, accuracy))
            already_processed.add(file_hash)
            
        except Exception as e:
            print(f"         ERROR: {e}")
            errors += 1
            import traceback
            traceback.print_exc()
    
    # Summary
    print(f"\n{'=' * 80}")
    print("BATCH INGESTION SUMMARY")
    print(f"{'=' * 80}")
    
    total_files = len(results)
    total_chemicals = sum(r[3] for r in results)
    total_high = sum(r[4] for r in results)
    
    print(f"\n  Files processed:  {total_files}")
    print(f"  Files skipped:    {skipped} (already in database)")
    print(f"  Files errored:    {errors}")
    print(f"  Total chemicals:  {total_chemicals}")
    if total_chemicals > 0:
        print(f"  High confidence:  {total_high} ({total_high/total_chemicals*100:.1f}%)")
    
    # Per-vendor breakdown
    vendors = {}
    for name, fmt, vendor, total, high, acc in results:
        if vendor not in vendors:
            vendors[vendor] = {'files': 0, 'chemicals': 0, 'high': 0}
        vendors[vendor]['files'] += 1
        vendors[vendor]['chemicals'] += total
        vendors[vendor]['high'] += high
    
    if vendors:
        print(f"\n  Per-vendor breakdown:")
        for v, stats in sorted(vendors.items()):
            vacc = stats['high'] / stats['chemicals'] * 100 if stats['chemicals'] > 0 else 0
            print(f"    {v:15s}: {stats['files']} files, "
                  f"{stats['chemicals']} chemicals, {vacc:.0f}% high-conf")
    
    # Files needing validation
    needs_val = [(n, t, h, a) for n, _, _, t, h, a in results if h < t]
    if needs_val:
        print(f"\n  Files needing validation ({len(needs_val)}):")
        for name, total, high, acc in needs_val[:10]:
            print(f"    {name:45s} {high}/{total} ({acc:.0f}%)")
        if len(needs_val) > 10:
            print(f"    ... and {len(needs_val) - 10} more")
    
    if not args.dry_run and total_files > 0:
        print(f"\n  Next steps:")
        print(f"    1. Review with: python scripts/21_validate_interactive.py --submission-id <ID>")
        print(f"    2. Gate check:  python scripts/gate_b_baseline.py")


if __name__ == "__main__":
    main()
