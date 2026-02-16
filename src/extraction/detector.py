"""
Lab file format detection.

Auto-detects whether an Excel file is Caduceon CA, Eurofins,
or another supported format by inspecting cell content.
"""

import pandas as pd


def detect_format(df: pd.DataFrame, filename: str) -> str:
    """
    Detect lab file format from content and filename.

    Args:
        df: Raw DataFrame read without headers (header=None).
        filename: Original filename (used as a fallback signal).

    Returns:
        One of 'caduceon_ca', 'eurofins', 'caduceon_xlsx', or 'unknown'.
    """
    # Eurofins: row 0 col 5 typically says "Eurofins"
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

    return 'unknown'
