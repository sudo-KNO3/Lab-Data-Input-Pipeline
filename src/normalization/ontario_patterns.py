"""
Ontario lab-specific pattern handling.

Handles common truncations, notation variants, and spacing issues specific
to Ontario environmental laboratories.
"""

import re
from typing import Dict, List


# Common truncations in Ontario lab reports
_TRUNCATION_MAP: Dict[str, str] = {
    # Dioxanes
    r'\b1,4\s*diox\b': '1,4-dioxane',
    r'\bdiox\b': 'dioxane',
    
    # Chlorinated solvents
    r'\b1,1,1-tca\b': '1,1,1-trichloroethane',
    r'\btca\b': 'trichloroethane',
    r'\btce\b': 'trichloroethylene',
    r'\bpce\b': 'tetrachloroethylene',
    r'\bdce\b': 'dichloroethylene',
    r'\bdca\b': 'dichloroethane',
    
    # Petroleum
    r'\bphc\s+f([1-4])\b': r'petroleum hydrocarbons f\1',
    
    # Metals
    r'\bhexavalent\s+cr\b': 'chromium, hexavalent',
    r'\bcr\s*\(vi\)': 'chromium, hexavalent',
    r'\bcr6\+': 'chromium, hexavalent',
    
    # PAHs
    r'\bpah\b': 'polyaromatic hydrocarbon',
    r'\bnaph\b': 'naphthalene',
    
    # BTEX
    r'\bbtex\b': 'benzene, toluene, ethylbenzene, xylene',
    
    # Phosphorus
    r'\btotal\s+p\b': 'phosphorus, total',
    r'\bt?p\s+\(total\)': 'phosphorus, total',
    
    # Nitrogen
    r'\btotal\s+n\b': 'nitrogen, total',
    r'\btn\b': 'nitrogen, total',
    r'\btkn\b': 'nitrogen, total kjeldahl',
}

# Spacing variants that need normalization
_SPACING_PATTERNS: List[tuple[re.Pattern, str]] = [
    # Number sequences: "1, 2, 3-TCP" -> "1,2,3-TCP"
    (re.compile(r'(\d)\s*,\s*(\d)'), r'\1,\2'),
    
    # Number-letter: "2, 4-D" -> "2,4-D"
    (re.compile(r'(\d)\s*,\s*(\d)\s*-\s*([a-zA-Z])'), r'\1,\2-\3'),
    
    # Di/tri/tetra patterns: "1, 1, 1-" -> "1,1,1-"
    (re.compile(r'(\d)\s*,\s*(\d)\s*,\s*(\d)\s*-'), r'\1,\2,\3-'),
]

# Common Ontario lab notation variants
_NOTATION_VARIANTS: Dict[str, str] = {
    # F-series for petroleum
    r'\bf-?([1-4])\b': r'f\1',
    
    # Para/ortho/meta abbreviations
    r'\bp-': 'para-',
    r'\bo-': 'ortho-',
    r'\bm-': 'meta-',
    
    # Dissolved vs total
    r'\bdiss\b': 'dissolved',
    r'\btot\b': 'total',
    r'\brec\b': 'recoverable',
}

# Compile all patterns for performance
_COMPILED_TRUNCATIONS = {
    re.compile(pattern, re.IGNORECASE): replacement
    for pattern, replacement in _TRUNCATION_MAP.items()
}

_COMPILED_NOTATION = {
    re.compile(pattern, re.IGNORECASE): replacement
    for pattern, replacement in _NOTATION_VARIANTS.items()
}


def apply_ontario_patterns(text: str) -> str:
    """
    Apply Ontario lab-specific pattern normalization.
    
    Handles truncations, spacing variants, and common notation issues
    from Ontario environmental laboratories.
    
    Args:
        text: Raw chemical name from Ontario lab
        
    Returns:
        Normalized chemical name
        
    Examples:
        >>> apply_ontario_patterns("1,4 Diox")
        '1,4-dioxane'
        
        >>> apply_ontario_patterns("1, 1, 1-TCA")
        '1,1,1-trichloroethane'
        
        >>> apply_ontario_patterns("PHC F2")
        'petroleum hydrocarbons f2'
        
        >>> apply_ontario_patterns("Total P")
        'phosphorus, total'
        
        >>> apply_ontario_patterns("2, 4-D")
        '2,4-d'
    """
    if not text or not isinstance(text, str):
        return ""
    
    from .text_normalizer import normalize_text
    
    # First normalize basic text
    text = normalize_text(text)
    
    # Apply truncation expansions
    text = _expand_truncations(text)
    
    # Normalize spacing
    text = _normalize_spacing(text)
    
    # Apply notation variants
    text = _apply_notation_variants(text)
    
    # Final cleanup
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def _expand_truncations(text: str) -> str:
    """
    Expand common truncations.
    
    Args:
        text: Text with potential truncations
        
    Returns:
        Text with truncations expanded
    """
    for pattern, replacement in _COMPILED_TRUNCATIONS.items():
        text = pattern.sub(replacement, text)
    
    return text


def _normalize_spacing(text: str) -> str:
    """
    Normalize spacing patterns.
    
    Args:
        text: Text with spacing issues
        
    Returns:
        Text with normalized spacing
    """
    for pattern, replacement in _SPACING_PATTERNS:
        text = pattern.sub(replacement, text)
    
    return text


def _apply_notation_variants(text: str) -> str:
    """
    Apply notation variant normalization.
    
    Args:
        text: Text with notation variants
        
    Returns:
        Text with standardized notation
    """
    for pattern, replacement in _COMPILED_NOTATION.items():
        text = pattern.sub(replacement, text)
    
    return text


def detect_truncated_name(text: str) -> bool:
    """
    Detect if text appears to be truncated.
    
    Args:
        text: Chemical name
        
    Returns:
        True if text appears truncated
        
    Examples:
        >>> detect_truncated_name("1,4 Diox")
        True
        
        >>> detect_truncated_name("Benzene")
        False
    """
    if not text or not isinstance(text, str):
        return False
    
    text_lower = text.lower()
    
    # Check against known truncation patterns
    for pattern in _COMPILED_TRUNCATIONS.keys():
        if pattern.search(text_lower):
            return True
    
    return False


def expand_abbreviation(text: str) -> str:
    """
    Expand laboratory abbreviations to full names.
    
    Args:
        text: Text with potential abbreviations
        
    Returns:
        Text with abbreviations expanded
        
    Examples:
        >>> expand_abbreviation("TCA")
        'trichloroethane'
        
        >>> expand_abbreviation("TCE")
        'trichloroethylene'
    """
    if not text or not isinstance(text, str):
        return ""
    
    from .text_normalizer import normalize_text
    text = normalize_text(text)
    
    return _expand_truncations(text)


def normalize_metal_notation(text: str) -> str:
    """
    Normalize metal-specific notation (e.g., valence states).
    
    Args:
        text: Metal name with potential notation
        
    Returns:
        Normalized metal name
        
    Examples:
        >>> normalize_metal_notation("Cr(VI)")
        'chromium, hexavalent'
        
        >>> normalize_metal_notation("Hexavalent Cr")
        'chromium, hexavalent'
    """
    if not text or not isinstance(text, str):
        return ""
    
    from .text_normalizer import normalize_text
    text = normalize_text(text)
    
    # Apply metal-specific patterns
    text = re.sub(r'\bcr\s*\(vi\)', 'chromium, hexavalent', text, flags=re.IGNORECASE)
    text = re.sub(r'\bhexavalent\s+cr\b', 'chromium, hexavalent', text, flags=re.IGNORECASE)
    text = re.sub(r'\bcr6\+', 'chromium, hexavalent', text, flags=re.IGNORECASE)
    
    return text


def normalize_number_spacing(text: str) -> str:
    """
    Normalize spacing around numbers in chemical names.
    
    Particularly important for Ontario lab data with inconsistent spacing.
    
    Args:
        text: Text with potential spacing issues
        
    Returns:
        Text with normalized number spacing
        
    Examples:
        >>> normalize_number_spacing("1, 2, 3-Trichloropropane")
        '1,2,3-trichloropropane'
        
        >>> normalize_number_spacing("2, 4-D")
        '2,4-d'
    """
    if not text or not isinstance(text, str):
        return ""
    
    from .text_normalizer import normalize_text
    text = normalize_text(text)
    
    return _normalize_spacing(text)
