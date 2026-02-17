"""
Eurofins format extraction.

Handles the standard Eurofins XLS/XLSX layout:
    Row  0 col 5: "Eurofins" identifier
    Row ~10: Sample-ID row
    Row ~12: Column headers (Group, Analyte, Units, Analytical Method, MRL, …)
    Row 13+: Chemical data
             col 0 = group
             col 1 = analyte name
             col 2 = units
             col 3 = analytical method
             col 4 = MRL
             cols 5+ = sample results
"""

from typing import Dict, List

import pandas as pd

from .filters import is_chemical_row


def _infer_medium_from_units(units: str) -> str:
    """Infer sample medium from units string.

    Returns 'Soil' for mass-per-mass units (ug/g, mg/kg, etc.),
    'Water' for mass-per-volume units (mg/L, ug/L, etc.),
    or '' if undetermined.
    """
    u = units.lower().strip()
    if not u:
        return ''
    if any(kw in u for kw in ['ug/g', 'mg/kg', 'ug/kg', 'ng/g', 'ppm']):
        return 'Soil'
    if any(kw in u for kw in ['mg/l', 'ug/l', 'ng/l', 'cfu/100ml',
                               'mpn/100ml', 'us/cm', 'ntu']):
        return 'Water'
    return ''


def extract_metadata(df: pd.DataFrame) -> Dict[str, str]:
    """
    Extract header metadata from a Eurofins file.

    Returns:
        Dict with keys: workorder, client, date_received, contact.
    """
    meta: Dict[str, str] = {
        'workorder': '', 'client': '', 'date_received': '', 'contact': '',
    }
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


def extract_chemicals(
    df: pd.DataFrame,
) -> List[Dict]:
    """
    Extract chemical rows from a Eurofins file (all samples).

    Returns:
        List of dicts with keys:
            row_num, chemical, units, detection_limit, result_value,
            sample_id, client_id, sample_date, lab_method, chemical_group,
            medium
    """
    chemicals: List[Dict] = []

    # Locate header row (should contain "Analyte" in col 1)
    header_row = 12
    for r in range(8, min(20, len(df))):
        cell = (
            str(df.iloc[r, 1])
            if df.shape[1] > 1 and pd.notna(df.iloc[r, 1])
            else ''
        )
        if 'analyte' in cell.lower():
            header_row = r
            break

    # Build sample info from rows above the header
    sample_info: List[Dict] = []
    sample_id_row = None
    sample_date_row = None

    for r in range(max(0, header_row - 5), header_row):
        cell4 = (
            str(df.iloc[r, 4]).strip().lower()
            if df.shape[1] > 4 and pd.notna(df.iloc[r, 4])
            else ''
        )
        if 'sample id' in cell4:
            sample_id_row = r
        elif 'sample date' in cell4:
            sample_date_row = r

    if sample_id_row is not None:
        for c in range(5, df.shape[1]):
            val = str(df.iloc[sample_id_row, c]).strip() if pd.notna(df.iloc[sample_id_row, c]) else ''
            if val and val != 'nan':
                sdate = ''
                if sample_date_row is not None and pd.notna(df.iloc[sample_date_row, c]):
                    sdate = str(df.iloc[sample_date_row, c]).strip()

                sample_info.append({
                    'col': c,
                    'sample_id': val,
                    'client_id': val,
                    'sample_date': sdate,
                })

    data_start = header_row + 1
    current_group = ''

    for row_idx in range(data_start, len(df)):
        # Track chemical group (col 0)
        group_cell = df.iloc[row_idx, 0]
        if pd.notna(group_cell) and str(group_cell).strip():
            current_group = str(group_cell).strip()

        cell = df.iloc[row_idx, 1]  # Analyte in col 1
        if pd.isna(cell):
            continue
        chem_name = str(cell).strip()
        if not is_chemical_row(chem_name, 'eurofins'):
            continue

        units = (
            str(df.iloc[row_idx, 2]).strip()
            if df.shape[1] > 2 and pd.notna(df.iloc[row_idx, 2])
            else ''
        )
        method = (
            str(df.iloc[row_idx, 3]).strip()
            if df.shape[1] > 3 and pd.notna(df.iloc[row_idx, 3])
            else ''
        )
        mrl = (
            str(df.iloc[row_idx, 4]).strip()
            if df.shape[1] > 4 and pd.notna(df.iloc[row_idx, 4])
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
                'detection_limit': mrl,
                'result_value': result_value,
                'sample_id': si['sample_id'],
                'client_id': si['client_id'],
                'sample_date': si['sample_date'],
                'lab_method': method,
                'chemical_group': current_group,
                'medium': _infer_medium_from_units(units),
            })

        # Fallback: no sample columns found
        if not sample_info:
            chemicals.append({
                'row_num': row_idx,
                'chemical': chem_name,
                'units': units,
                'detection_limit': mrl,
                'result_value': '',
                'sample_id': '',
                'client_id': '',
                'sample_date': '',
                'lab_method': method,
                'chemical_group': current_group,
                'medium': _infer_medium_from_units(units),
            })

    return chemicals
