"""
Backfill medium column for existing lab_results.

Strategy (in order of reliability):
1. Caduceon XLSX files → re-read Row 15 Col 1 "SAMPLE MATRIX:" from source file
2. Caduceon filename hints → GW = Groundwater, SW = Surface Water
3. SGS files → infer from units column already in lab_results (mg/L = Water)
4. Eurofins files → infer from units column (ug/g = Soil, ug/L = Water)
"""

import sqlite3
import os
import re
from pathlib import Path

import pandas as pd


def infer_medium_from_units(units: str) -> str:
    """Infer sample medium from units string."""
    u = units.lower().strip()
    if not u:
        return ''
    if any(kw in u for kw in ['ug/g', 'mg/kg', 'ug/kg', 'ng/g', 'ppm']):
        return 'Soil'
    if any(kw in u for kw in ['mg/l', 'ug/l', 'ng/l', 'cfu/100ml',
                               'mpn/100ml', 'us/cm', 'ntu']):
        return 'Water'
    return ''


def infer_medium_from_filename(filename: str) -> str:
    """Infer medium from filename patterns (GW/SW)."""
    name = filename.upper()
    if ' GW ' in name or '_GW_' in name or name.startswith('GW'):
        return 'Groundwater'
    if ' SW ' in name or '_SW_' in name or name.startswith('SW'):
        return 'Surface Water'
    return ''


def read_caduceon_xlsx_matrix(file_path: str) -> str:
    """Read SAMPLE MATRIX from Caduceon XLSX Row 15, Col 1."""
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        if df.shape[0] > 15 and df.shape[1] > 1:
            v = df.iloc[15, 1]
            if pd.notna(v):
                return str(v).strip()
    except Exception as e:
        print(f"  Warning reading {file_path}: {e}")
    return ''


def main():
    db_path = 'data/lab_results.db'
    conn = sqlite3.connect(db_path)

    # Check current state
    total = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
    filled = conn.execute("SELECT COUNT(*) FROM lab_results WHERE medium IS NOT NULL AND medium != ''").fetchone()[0]
    print(f"Total results: {total}")
    print(f"Already have medium: {filled}")
    print(f"Need backfill: {total - filled}")
    print()

    # Get all submissions
    submissions = conn.execute("""
        SELECT submission_id, lab_vendor, file_path, original_filename
        FROM lab_submissions
    """).fetchall()

    stats = {'caduceon_xlsx': 0, 'filename': 0, 'units': 0, 'skipped': 0}

    for sub_id, vendor, file_path, orig_name in submissions:
        medium = ''

        # Strategy 1: Caduceon XLSX → read SAMPLE MATRIX from source file
        if vendor == 'Caduceon' and file_path and os.path.exists(file_path):
            medium = read_caduceon_xlsx_matrix(file_path)
            if medium:
                updated = conn.execute(
                    "UPDATE lab_results SET medium = ? WHERE submission_id = ? AND (medium IS NULL OR medium = '')",
                    (medium, sub_id)
                ).rowcount
                stats['caduceon_xlsx'] += updated
                print(f"  Sub {sub_id} ({orig_name}): SAMPLE MATRIX = '{medium}' → {updated} rows")
                continue

        # Strategy 2: Filename hints (GW/SW)
        filename_medium = infer_medium_from_filename(orig_name or '')
        if filename_medium:
            updated = conn.execute(
                "UPDATE lab_results SET medium = ? WHERE submission_id = ? AND (medium IS NULL OR medium = '')",
                (filename_medium, sub_id)
            ).rowcount
            stats['filename'] += updated
            print(f"  Sub {sub_id} ({orig_name}): Filename → '{filename_medium}' → {updated} rows")
            continue

        # Strategy 3: Infer from units column per-result
        results = conn.execute(
            "SELECT result_id, units FROM lab_results WHERE submission_id = ? AND (medium IS NULL OR medium = '')",
            (sub_id,)
        ).fetchall()

        if results:
            batch_updates = []
            for result_id, units in results:
                m = infer_medium_from_units(units or '')
                if m:
                    batch_updates.append((m, result_id))

            if batch_updates:
                conn.executemany(
                    "UPDATE lab_results SET medium = ? WHERE result_id = ?",
                    batch_updates
                )
                stats['units'] += len(batch_updates)
                print(f"  Sub {sub_id} ({orig_name}): Units inference → {len(batch_updates)} rows")
            else:
                stats['skipped'] += len(results)
                print(f"  Sub {sub_id} ({orig_name}): Could not determine medium for {len(results)} rows")
        else:
            print(f"  Sub {sub_id} ({orig_name}): All rows already filled")

    conn.commit()

    # Final report
    filled_after = conn.execute("SELECT COUNT(*) FROM lab_results WHERE medium IS NOT NULL AND medium != ''").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Caduceon XLSX (explicit): {stats['caduceon_xlsx']}")
    print(f"  Filename inference:       {stats['filename']}")
    print(f"  Units inference:          {stats['units']}")
    print(f"  Could not determine:      {stats['skipped']}")
    print(f"  Total with medium now:    {filled_after}/{total}")
    print()

    # Show distribution
    print("Medium distribution:")
    for row in conn.execute("SELECT medium, COUNT(*) FROM lab_results GROUP BY medium ORDER BY COUNT(*) DESC"):
        print(f"  {row[0] or '(empty)'}: {row[1]}")

    conn.close()


if __name__ == '__main__':
    main()
