"""
Lab file format detection.

Auto-detects whether an Excel file is Caduceon CA, Eurofins,
or another supported format by inspecting cell content.

Format vs Vendor distinction:
  - **Format** describes the spreadsheet *layout* (which extractor to use).
  - **Vendor** is the actual lab name (who produced the data).

  Format 'caduceon_ca' is used by both SGS and Caduceon (same layout).
  The vendor is determined separately via OCR on the embedded logo image.
"""

from pathlib import Path
from typing import Optional

import pandas as pd


def detect_format(df: pd.DataFrame, filename: str) -> str:
    """
    Detect lab file format from content and filename.

    This identifies the *spreadsheet layout* to determine which
    extraction logic to apply.  It does NOT identify the lab vendor —
    use :func:`detect_vendor` for that.

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

    # CA-layout: row 6 has "Report No." in col 0
    # Used by both SGS and older Caduceon — vendor detected via OCR
    if df.shape[0] > 6:
        cell60 = str(df.iloc[6, 0]) if pd.notna(df.iloc[6, 0]) else ''
        if 'report no' in cell60.lower():
            return 'caduceon_ca'

    # Caduceon xlsx: identified by content (row 7 col 5 contains lab name,
    # or row 20 col 0 = "Parameter" with col 1 = "Units")
    # Works even when filename is misspelled (e.g. "Caducoen")
    if filename.lower().endswith('.xlsx'):
        # Content-based: check for Caduceon lab header
        if df.shape[0] > 7 and df.shape[1] > 5:
            cell75 = str(df.iloc[7, 5]) if pd.notna(df.iloc[7, 5]) else ''
            if 'caduceon' in cell75.lower():
                return 'caduceon_xlsx'
        # Content-based: check for "Parameter" / "Units" header row
        if df.shape[0] > 20 and df.shape[1] > 1:
            cell200 = str(df.iloc[20, 0]).strip().lower() if pd.notna(df.iloc[20, 0]) else ''
            cell201 = str(df.iloc[20, 1]).strip().lower() if pd.notna(df.iloc[20, 1]) else ''
            if cell200 == 'parameter' and cell201 == 'units':
                return 'caduceon_xlsx'
        # Filename fallback (handles common misspellings)
        fname_lower = filename.lower()
        if 'caduceon' in fname_lower or 'caducoen' in fname_lower:
            return 'caduceon_xlsx'

    return 'unknown'


def detect_vendor(
    filepath: Path,
    df: Optional[pd.DataFrame] = None,
) -> str:
    """
    Detect lab vendor by OCR-ing the embedded logo image, with
    cell-text fallback.

    This is a convenience wrapper around
    :func:`src.extraction.ocr_vendor.detect_vendor`.

    Args:
        filepath: Path to the Excel file.
        df: Optional pre-loaded DataFrame (header=None) for text fallback.

    Returns:
        Canonical vendor name (e.g. "SGS", "Caduceon", "Eurofins").
    """
    from .ocr_vendor import detect_vendor as _ocr_detect
    return _ocr_detect(filepath, df=df)
