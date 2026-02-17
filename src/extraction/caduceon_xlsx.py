"""
Caduceon XLSX format extraction.

Handles the standard Caduceon certificate-of-analysis XLSX layout:
    Row  1 col 5/7: Report No.
    Row  7 col 5:   "CADUCEON Environmental Laboratories"
    Row 11 col 1:   Attention
    Row 13 col 1:   Date submitted
    Row 15 col 1:   Sample matrix
    Row 17:         Client IDs    (cols 4+)
    Row 18:         Sample IDs    (cols 4+)
    Row 19:         Date collected (cols 4+)
    Row 20:         Header row ("Parameter", "Units", "R.L.")
    Row 21+:        Chemical data
                    col 0 = chemical name
                    col 1 = units
                    col 2 = R.L. (reporting limit / detection limit)
                    col 4+ = sample results
"""

from typing import Dict, List

import pandas as pd

from .filters import is_chemical_row


def extract_metadata(df: pd.DataFrame) -> Dict[str, str]:
    """
    Extract header metadata from a Caduceon XLSX file.

    Returns:
        Dict with keys: report_no, customer, attention, reference, sample_matrix.
    """
    meta: Dict[str, str] = {
        'report_no': '', 'customer': '', 'attention': '',
        'reference': '', 'sample_matrix': '',
    }

    # Row 1, col 7: report number
    if df.shape[0] > 1 and df.shape[1] > 7:
        v = df.iloc[1, 7]
        if pd.notna(v):
            meta['report_no'] = str(v).strip()

    # Row 7, col 0: "Report To:" line — the next cell is the client name
    if df.shape[0] > 8:
        v = df.iloc[8, 0]
        if pd.notna(v):
            meta['customer'] = str(v).strip()

    # Row 11, col 1: attention
    if df.shape[0] > 11 and df.shape[1] > 1:
        v = df.iloc[11, 1]
        if pd.notna(v):
            meta['attention'] = str(v).strip()

    # Row 13, col 7: customer project
    if df.shape[0] > 13 and df.shape[1] > 7:
        v = df.iloc[13, 7]
        if pd.notna(v):
            meta['reference'] = str(v).strip()

    # Row 15, col 1: sample matrix
    if df.shape[0] > 15 and df.shape[1] > 1:
        v = df.iloc[15, 1]
        if pd.notna(v):
            meta['sample_matrix'] = str(v).strip()

    return meta


def extract_chemicals(
    df: pd.DataFrame,
) -> List[Dict]:
    """
    Extract chemical rows from a Caduceon XLSX file (all samples).

    Returns:
        List of dicts with keys:
            row_num, chemical, units, detection_limit, result_value,
            sample_id, client_id, sample_date, lab_method, chemical_group,
            medium
    """
    chemicals: List[Dict] = []

    # Extract sample matrix from metadata (Row 15, Col 1)
    medium = ''
    if df.shape[0] > 15 and df.shape[1] > 1:
        v = df.iloc[15, 1]
        if pd.notna(v):
            medium = str(v).strip()

    # Find header row containing "Parameter" in col 0
    header_row = 20  # default
    for r in range(15, min(25, len(df))):
        cell = str(df.iloc[r, 0]).strip().lower() if pd.notna(df.iloc[r, 0]) else ''
        if cell == 'parameter':
            header_row = r
            break

    # Build sample info from rows near the header
    sample_info: List[Dict] = []
    sample_id_row = None
    client_id_row = None
    date_row = None

    for r in range(max(0, header_row - 5), header_row):
        label = (
            str(df.iloc[r, 1]).strip().lower()
            if df.shape[1] > 1 and pd.notna(df.iloc[r, 1])
            else ''
        )
        if 'sample id' in label:
            sample_id_row = r
        elif 'client id' in label:
            client_id_row = r
        elif 'date collected' in label or 'date' in label:
            date_row = r

    # Gather sample columns (use sample_id_row or client_id_row)
    ref_row = sample_id_row or client_id_row
    if ref_row is not None:
        for c in range(4, df.shape[1]):
            val = str(df.iloc[ref_row, c]).strip() if pd.notna(df.iloc[ref_row, c]) else ''
            if not val or val == 'nan':
                continue

            sid = val
            cid = ''
            sdate = ''

            if client_id_row is not None and pd.notna(df.iloc[client_id_row, c]):
                cid = str(df.iloc[client_id_row, c]).strip()
            if date_row is not None and pd.notna(df.iloc[date_row, c]):
                sdate = str(df.iloc[date_row, c]).strip()

            # If we found sample_id_row, use that as sample_id;
            # if we also found client_id_row, use that as client_id.
            # If only one row found, it's both.
            if sample_id_row is not None and client_id_row is not None:
                sample_info.append({'col': c, 'sample_id': sid, 'client_id': cid, 'sample_date': sdate})
            else:
                sample_info.append({'col': c, 'sample_id': sid, 'client_id': cid or sid, 'sample_date': sdate})

    data_start = header_row + 1

    for row_idx in range(data_start, len(df)):
        cell = df.iloc[row_idx, 0]
        if pd.isna(cell):
            continue
        chem_name = str(cell).strip()
        if not chem_name:
            continue
        if not is_chemical_row(chem_name, 'caduceon_ca'):
            continue

        units = (
            str(df.iloc[row_idx, 1]).strip()
            if df.shape[1] > 1 and pd.notna(df.iloc[row_idx, 1])
            else ''
        )
        detection_limit = (
            str(df.iloc[row_idx, 2]).strip()
            if df.shape[1] > 2 and pd.notna(df.iloc[row_idx, 2])
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
                'detection_limit': detection_limit,
                'result_value': result_value,
                'sample_id': si['sample_id'],
                'client_id': si['client_id'],
                'sample_date': si['sample_date'],
                'lab_method': '',
                'chemical_group': '',
                'medium': medium,
            })

        # Fallback: no sample columns found
        if not sample_info:
            chemicals.append({
                'row_num': row_idx,
                'chemical': chem_name,
                'units': units,
                'detection_limit': detection_limit,
                'result_value': '',
                'sample_id': '',
                'client_id': '',
                'sample_date': '',
                'lab_method': '',
                'chemical_group': '',
                'medium': medium,
            })

    return chemicals
