"""
Row-level filters for lab file extraction.

Separates chemical data rows from headers, footers,
disclaimers, and other non-chemical content.
"""

import re
from typing import Set

# ─────────────────────────────────────────────────────────────────────────────
# Caduceon CA: header/subheader text appearing in col 0 that isn't a chemical
# ─────────────────────────────────────────────────────────────────────────────
CA_SKIP_ROWS: Set[str] = {
    'sample id', 'sample date / time', 'sample date & time',
    'analysis', 'units', 'temperature upon receipt',
    'temperature', 'temp upon receipt', 'sample date',
    'date received', 'date reported', 'report no.', 'report no',
    'customer', 'attention', 'reference', 'works#', 'title',
    'method', 'mdl', 'rdl', 'rl', 'notes', 'comments',
}

# ─────────────────────────────────────────────────────────────────────────────
# Generic footer / disclaimer patterns
# ─────────────────────────────────────────────────────────────────────────────
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


def is_chemical_row(chem_name: str, fmt: str) -> bool:
    """
    Determine whether a row contains a valid chemical name.

    Applies generic and format-specific heuristics to reject
    headers, footers, disclaimers, and other non-chemical text.

    Args:
        chem_name: Raw cell text from the chemical-name column.
        fmt: Format identifier ('caduceon_ca', 'eurofins', etc.).

    Returns:
        True if the row is likely a chemical data row.
    """
    if not chem_name or len(chem_name) < 2:
        return False

    chem_lower = chem_name.lower().strip()

    # Pure numbers
    if re.match(r'^[\d\.\-\<\>]+$', chem_name):
        return False

    # Known non-chemical tokens
    if chem_lower in {'total', 'sum', 'notes', 'comments', '', 'nan'}:
        return False

    # Footer text
    if any(p in chem_lower for p in FOOTER_PATTERNS):
        return False

    # Very long text → disclaimer
    if len(chem_name) > 100:
        return False

    # ── Format-specific ──────────────────────────────────────────────────
    if fmt == 'caduceon_ca':
        if chem_lower in CA_SKIP_ROWS:
            return False
        if chem_lower.startswith(('sample ', 'date ', 'time ')):
            return False

    return True
