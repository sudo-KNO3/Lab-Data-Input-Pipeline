"""
Caduceon CA format extraction.

Handles the standard Caduceon certificate-of-analysis XLS layout
(used by both SGS and older Caduceon labs):
    Row  6: Report No.
    Row 13: Sample-ID headers (cols 8+)
    Row 14: Sample Date/Time  (cols 8+)
    Row 15: Analysis / Units sub-headers
    Row 18+: Chemical data
             col 0 = chemical name
             col 1 = units
             col 6 = MAC limit
             col 8+ = sample results
"""

from typing import Dict, List, Tuple

import pandas as pd

from .filters import is_chemical_row


def extract_metadata(df: pd.DataFrame) -> Dict[str, str]:
    """
    Extract header metadata from a Caduceon CA file.

    Returns:
        Dict with keys: report_no, customer, attention, reference.
    """
    meta: Dict[str, str] = {
        'report_no': '', 'customer': '', 'attention': '', 'reference': '',
    }
    for i in range(min(12, len(df))):
        key = str(df.iloc[i, 0]).strip().lower() if pd.notna(df.iloc[i, 0]) else ''
        val = (
            str(df.iloc[i, 1]).strip()
            if df.shape[1] > 1 and pd.notna(df.iloc[i, 1])
            else ''
        )
        if 'report no' in key:
            meta['report_no'] = val
        elif key == 'customer':
            meta['customer'] = val
        elif key == 'attention':
            meta['attention'] = val
        elif key == 'reference':
            meta['reference'] = val
    return meta


def extract_chemicals(
    df: pd.DataFrame,
) -> List[Dict]:
    """
    Extract chemical rows from a Caduceon CA file (all samples).

    Returns:
        List of dicts with keys:
            row_num, chemical, units, detection_limit, result_value,
            sample_id, client_id, sample_date, lab_method, chemical_group
    """
    chemicals: List[Dict] = []

    # Sample IDs from header row (row 13, cols 8+)
    sample_info: List[Dict] = []
    if df.shape[0] > 13:
        for c in range(8, df.shape[1]):
            val = str(df.iloc[13, c]).strip() if pd.notna(df.iloc[13, c]) else ''
            if val and val != 'nan':
                # Sample date from row 14
                sample_date = ''
                if df.shape[0] > 14 and pd.notna(df.iloc[14, c]):
                    sample_date = str(df.iloc[14, c]).strip()

                sample_info.append({
                    'col': c,
                    'sample_id': val,
                    'client_id': val,
                    'sample_date': sample_date,
                })

    # Find where chemical data starts (normally row 18)
    data_start = 18
    if df.shape[0] > 18:
        test = str(df.iloc[18, 0]).strip() if pd.notna(df.iloc[18, 0]) else ''
        if not test or not is_chemical_row(test, 'caduceon_ca'):
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

        units = (
            str(df.iloc[row_idx, 1]).strip()
            if df.shape[1] > 1 and pd.notna(df.iloc[row_idx, 1])
            else ''
        )
        mac = (
            str(df.iloc[row_idx, 6]).strip()
            if df.shape[1] > 6 and pd.notna(df.iloc[row_idx, 6])
            else ''
        )

        # Emit one row per sample
        for si in sample_info:
            result_value = ''
            col_idx = si['col']
            if col_idx < df.shape[1] and pd.notna(df.iloc[row_idx, col_idx]):
                result_value = str(df.iloc[row_idx, col_idx]).strip()

            chemicals.append({
                'row_num': row_idx,
                'chemical': chem_name,
                'units': units,
                'detection_limit': mac,
                'result_value': result_value,
                'sample_id': si['sample_id'],
                'client_id': si['client_id'],
                'sample_date': si['sample_date'],
                'lab_method': '',
                'chemical_group': '',
            })

        # Fallback: no sample columns found
        if not sample_info:
            chemicals.append({
                'row_num': row_idx,
                'chemical': chem_name,
                'units': units,
                'detection_limit': mac,
                'result_value': '',
                'sample_id': '',
                'client_id': '',
                'sample_date': '',
                'lab_method': '',
                'chemical_group': '',
            })

    return chemicals
