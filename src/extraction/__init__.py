"""
Lab file extraction module.

Provides format detection and vendor-specific extraction for
Ontario environmental lab Excel files (Caduceon, Eurofins).

Usage:
    from src.extraction import detect_format, extract_chemicals

    fmt = detect_format(df, filename)
    rows = extract_chemicals(df, fmt)
"""

from typing import Dict, List, Tuple

import pandas as pd

from .detector import detect_format
from .filters import is_chemical_row, CA_SKIP_ROWS, FOOTER_PATTERNS
from . import caduceon
from . import eurofins


def extract_chemicals(
    df: pd.DataFrame, fmt: str
) -> List[Tuple[int, str, str, str, str, str]]:
    """
    Dispatch to the correct vendor extractor.

    Args:
        df: Raw DataFrame (header=None).
        fmt: Format string from ``detect_format()``.

    Returns:
        List of (row_num, chem_name, units, method_or_mac, result_value, sample_id).
    """
    if fmt == 'caduceon_ca':
        return caduceon.extract_chemicals(df)
    elif fmt == 'eurofins':
        return eurofins.extract_chemicals(df)
    else:
        return []


def extract_metadata(
    df: pd.DataFrame, fmt: str
) -> Dict[str, str]:
    """
    Dispatch to the correct vendor metadata extractor.

    Args:
        df: Raw DataFrame (header=None).
        fmt: Format string from ``detect_format()``.

    Returns:
        Dict of vendor-specific metadata fields.
    """
    if fmt == 'caduceon_ca':
        return caduceon.extract_metadata(df)
    elif fmt == 'eurofins':
        return eurofins.extract_metadata(df)
    else:
        return {}


__all__ = [
    'detect_format',
    'extract_chemicals',
    'extract_metadata',
    'is_chemical_row',
    'caduceon',
    'eurofins',
    'CA_SKIP_ROWS',
    'FOOTER_PATTERNS',
]
