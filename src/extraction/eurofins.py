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

from typing import Dict, List, Tuple

import pandas as pd

from .filters import is_chemical_row


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
) -> List[Tuple[int, str, str, str, str, str]]:
    """
    Extract chemical rows from a Eurofins file.

    Returns:
        List of (row_num, chem_name, units, method, result_value, sample_id).
    """
    chemicals: List[Tuple[int, str, str, str, str, str]] = []

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

    # Sample IDs (typically a few rows above the header)
    sample_ids: List[Tuple[int, str]] = []
    for r in range(max(0, header_row - 4), header_row):
        cell = (
            str(df.iloc[r, 4])
            if df.shape[1] > 4 and pd.notna(df.iloc[r, 4])
            else ''
        )
        if 'sample id' in cell.lower():
            for c in range(5, df.shape[1]):
                val = str(df.iloc[r, c]).strip() if pd.notna(df.iloc[r, c]) else ''
                if val and val != 'nan':
                    sample_ids.append((c, val))
            break

    data_start = header_row + 1

    for row_idx in range(data_start, len(df)):
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

        result_value = ''
        sample_id = ''
        if sample_ids:
            col_idx = sample_ids[0][0]
            sample_id = sample_ids[0][1]
            if col_idx < df.shape[1] and pd.notna(df.iloc[row_idx, col_idx]):
                result_value = str(df.iloc[row_idx, col_idx]).strip()

        chemicals.append((row_idx, chem_name, units, method, result_value, sample_id))

    return chemicals
