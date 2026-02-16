"""
Quality filters for harvested synonyms.

Implements validation and filtering logic to remove low-quality synonyms
from API harvest results before database insertion.
"""
import re
from typing import List

from loguru import logger


# Blacklist patterns
MIXTURE_TERMS = [
    "mixture",
    "formulation",
    "solution",
    "preparation",
    "blend",
    "suspension",
    "emulsion",
    "concentrate",
]

GENERIC_TERMS = [
    "standard",
    "total",
    "sample",
    "control",
    "blank",
    "reference",
    "reagent",
    "analytical",
    "certified",
    "pure",
    "technical",
    "grade",
    "unspecified",
    "unknown",
    "other",
    "various",
]

TRADE_NAME_MARKERS = ["®", "™", "©"]

# Regex patterns
CAS_PATTERN = re.compile(r"\b\d{1,7}-\d{2}-\d\b")
BRACKETED_CAS_PATTERN = re.compile(r"\s*\[\s*\d{1,7}-\d{2}-\d\s*\]\s*$")
PARENTHETICAL_INFO_PATTERN = re.compile(r"\s*\([^)]*\)\s*$")


def is_valid_ascii(text: str) -> bool:
    """
    Check if text contains only ASCII characters.
    
    Args:
        text: Input text
        
    Returns:
        True if text is valid ASCII
    """
    try:
        text.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def contains_blacklisted_term(text: str, blacklist: List[str]) -> bool:
    """
    Check if text contains any blacklisted terms.
    
    Args:
        text: Input text
        blacklist: List of terms to check
        
    Returns:
        True if any blacklisted term is found
    """
    text_lower = text.lower()
    return any(term in text_lower for term in blacklist)


def is_valid_abbreviation(text: str) -> bool:
    """
    Validate abbreviations (≤10 chars).
    
    Rules:
    - Alphanumeric + limited punctuation (hyphen, period, single quote)
    - No spaces allowed
    - At least 2 characters
    
    Args:
        text: Input text
        
    Returns:
        True if valid abbreviation
    """
    if len(text) > 10:
        return False

    if len(text) < 2:
        return False

    # Check for spaces
    if " " in text:
        return False

    # Allow alphanumeric, hyphen, period, apostrophe
    allowed_pattern = re.compile(r"^[a-zA-Z0-9\-\.']+$")
    return bool(allowed_pattern.match(text))


def clean_synonym_text(text: str) -> str:
    """
    Clean and normalize synonym text.
    
    - Remove bracketed CAS numbers
    - Remove parenthetical information
    - Strip whitespace
    - Normalize spaces
    
    Args:
        text: Input text
        
    Returns:
        Cleaned text
    """
    # Remove bracketed CAS numbers
    text = BRACKETED_CAS_PATTERN.sub("", text)

    # Remove parenthetical information (often purity or context)
    text = PARENTHETICAL_INFO_PATTERN.sub("", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def filter_synonyms(
    synonyms: List[str],
    analyte_type: str,
    max_length: int = 120,
    require_ascii: bool = True,
) -> List[str]:
    """
    Apply quality filters to synonym list.
    
    Filters applied:
    1. Length filter (drop >max_length chars)
    2. Content filter (drop mixture/formulation terms)
    3. Language filter (drop non-ASCII if required)
    4. Trade name heuristics (drop if contains ®, ™)
    5. Generic term filter (drop "standard", "total", etc.)
    6. Abbreviation validation (for short synonyms)
    7. Deduplication (case-insensitive)
    
    Args:
        synonyms: List of raw synonym strings
        analyte_type: Type of analyte ('single_substance', 'mixture', etc.)
        max_length: Maximum allowed synonym length
        require_ascii: Whether to require ASCII-only text
        
    Returns:
        Filtered list of synonyms
    """
    if not synonyms:
        return []

    filtered = []
    stats = {
        "initial_count": len(synonyms),
        "too_long": 0,
        "non_ascii": 0,
        "mixture_terms": 0,
        "generic_terms": 0,
        "trade_names": 0,
        "invalid_abbreviation": 0,
        "empty_after_clean": 0,
        "duplicates": 0,
    }

    seen_normalized = set()

    for synonym in synonyms:
        # Skip empty
        if not synonym or not synonym.strip():
            continue

        # Clean text
        cleaned = clean_synonym_text(synonym)

        if not cleaned:
            stats["empty_after_clean"] += 1
            continue

        # Length filter
        if len(cleaned) > max_length:
            stats["too_long"] += 1
            logger.debug(f"Dropping (too long): {cleaned[:50]}...")
            continue

        # ASCII filter
        if require_ascii and not is_valid_ascii(cleaned):
            stats["non_ascii"] += 1
            logger.debug(f"Dropping (non-ASCII): {cleaned[:50]}")
            continue

        # Trade name filter
        if any(marker in cleaned for marker in TRADE_NAME_MARKERS):
            stats["trade_names"] += 1
            logger.debug(f"Dropping (trade name): {cleaned[:50]}")
            continue

        # Mixture term filter (only for single_substance types)
        if analyte_type == "single_substance" and contains_blacklisted_term(
            cleaned, MIXTURE_TERMS
        ):
            stats["mixture_terms"] += 1
            logger.debug(f"Dropping (mixture term): {cleaned[:50]}")
            continue

        # Generic term filter
        if contains_blacklisted_term(cleaned, GENERIC_TERMS):
            stats["generic_terms"] += 1
            logger.debug(f"Dropping (generic term): {cleaned[:50]}")
            continue

        # Abbreviation validation (for short synonyms)
        if len(cleaned) <= 10 and not is_valid_abbreviation(cleaned):
            stats["invalid_abbreviation"] += 1
            logger.debug(f"Dropping (invalid abbreviation): {cleaned}")
            continue

        # Deduplication (case-insensitive)
        normalized_lower = cleaned.lower()
        if normalized_lower in seen_normalized:
            stats["duplicates"] += 1
            continue

        seen_normalized.add(normalized_lower)
        filtered.append(cleaned)

    # Log statistics
    stats["final_count"] = len(filtered)
    stats["filtered_count"] = stats["initial_count"] - stats["final_count"]

    if stats["initial_count"] > 0:
        retention_rate = (stats["final_count"] / stats["initial_count"]) * 100
        logger.info(
            f"Synonym filtering: {stats['initial_count']} → {stats['final_count']} "
            f"({retention_rate:.1f}% retention)"
        )
        logger.debug(f"Filter stats: {stats}")

    return filtered


def validate_cas_format(cas_number: str) -> bool:
    """
    Validate CAS number format.
    
    Args:
        cas_number: CAS registry number
        
    Returns:
        True if valid CAS format
    """
    if not cas_number:
        return False

    # Basic format check
    if not CAS_PATTERN.fullmatch(cas_number):
        return False

    # Check digit validation
    parts = cas_number.split("-")
    if len(parts) != 3:
        return False

    check_digit = int(parts[2])
    digits = parts[0] + parts[1]

    # Calculate checksum
    checksum = 0
    for i, digit in enumerate(reversed(digits)):
        checksum += int(digit) * (i + 1)

    return (checksum % 10) == check_digit


def extract_cas_from_text(text: str) -> List[str]:
    """
    Extract CAS numbers from text.
    
    Args:
        text: Input text
        
    Returns:
        List of CAS numbers found
    """
    matches = CAS_PATTERN.findall(text)
    return [cas for cas in matches if validate_cas_format(cas)]
