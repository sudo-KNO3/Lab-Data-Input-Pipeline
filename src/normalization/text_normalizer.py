"""
Text normalization module for chemical names.

Provides comprehensive text preprocessing and standardization capabilities
for chemical nomenclature, handling variants in notation, punctuation,
stereochemistry, and common abbreviations.
"""

import re
import unicodedata
from typing import Optional

# Versioned normalization — increment when rules change, migrate existing rows
NORMALIZATION_VERSION = 1


class TextNormalizer:
    """
    Normalizes chemical names to a standard form for matching.
    
    Handles:
    - Unicode normalization    - Case folding
    - Whitespace and punctuation standardization
    - Chemical abbreviations (tert-, sec-, iso-, ortho-, meta-, para-)
    - Greek letters (alpha, beta, gamma, etc.)
    - Stereochemistry notation ((+), (-), (R), (S), (E), (Z))
    - Numeric prefixes (di-, tri-, tetra-, etc.)
    
    The normalization preserves chemical structure information while
    standardizing notation variants commonly seen in laboratory data.
    """
    
    # Chemical abbreviations mapping
    ABBREVIATIONS = {
        r'\btert\b': 'tertiary',
        r'\bt-\b': 'tertiary',
        r'\bsec\b': 'secondary',
        r'\bs-\b': 'secondary',
        r'\biso\b': 'iso',
        r'\bi-\b': 'iso',
        r'\bn-\b': 'normal',
        r'\bortho\b': 'ortho',
        r'\bo-\b': 'ortho',
        r'\bmeta\b': 'meta',
        r'\bm-\b': 'meta',
        r'\bpara\b': 'para',
        r'\bp-\b': 'para',
    }
    
    # Greek letter mappings
    GREEK_LETTERS = {
        r'\balpha\b': 'α',
        r'\bbeta\b': 'β',
        r'\bgamma\b': 'γ',
        r'\bdelta\b': 'δ',
        r'\bepsilon\b': 'ε',
        r'\bzeta\b': 'ζ',
        r'\beta\b': 'η',
        r'\btheta\b': 'θ',
        r'\biota\b': 'ι',
        r'\bkappa\b': 'κ',
        r'\blambda\b': 'λ',
        r'\bmu\b': 'μ',
        r'\bnu\b': 'ν',
        r'\bxi\b': 'ξ',
        r'\bomicron\b': 'ο',
        r'\bpi\b': 'π',
        r'\brho\b': 'ρ',
        r'\bsigma\b': 'σ',
        r'\btau\b': 'τ',
        r'\bupsilon\b': 'υ',
        r'\bphi\b': 'φ',
        r'\bchi\b': 'χ',
        r'\bpsi\b': 'ψ',
        r'\bomega\b': 'ω',
    }
    
    # Numeric prefixes for chemical nomenclature
    NUMERIC_PREFIXES = {
        r'\bdi-': 'di',
        r'\btri-': 'tri',
        r'\btetra-': 'tetra',
        r'\bpenta-': 'penta',
        r'\bhexa-': 'hexa',
        r'\bhepta-': 'hepta',
        r'\bocta-': 'octa',
        r'\bnona-': 'nona',
        r'\bdeca-': 'deca',
        r'\bmono-': 'mono',
        r'\bpoly-': 'poly',
    }
    
    def __init__(self):
        """Initialize the text normalizer."""
        pass
    
    def normalize(self, text: str) -> str:
        """
        Apply the complete normalization pipeline to chemical name text.
        
        Pipeline order:
        1. Unicode normalization (NFKC)
        2. Whitespace collapse
        3. Punctuation standardization
        4. Abbreviation expansion
        5. Greek letter normalization
        6. Stereochemistry normalization
        7. Numeric prefix normalization
        8. Case folding (lowercase)
        9. Final whitespace trim
        
        Args:
            text: Raw chemical name text
            
        Returns:
            Normalized chemical name
            
        Examples:
            >>> normalizer = TextNormalizer()
            >>> normalizer.normalize("Benzo(a)pyrene")
            'benzo a pyrene'
            >>> normalizer.normalize("1,4-Dioxane")
            '1 4 dioxane'
            >>> normalizer.normalize("tert-Butanol")
            'tertiary butanol'
        """
        if not text or not isinstance(text, str):
            return ''
        
        # Step 1: Unicode normalization
        text = self._unicode_normalize(text)
        
        # Step 2: Collapse whitespace early
        text = self._collapse_whitespace(text)
        
        # Step 3: Standardize punctuation
        text = self._standardize_punctuation(text)
        
        # Step 4: Expand abbreviations
        text = self._expand_abbreviations(text)
        
        # Step 5: Normalize Greek letters
        text = self._normalize_greek_letters(text)
        
        # Step 6: Normalize stereochemistry
        text = self._normalize_stereochemistry(text)
        
        # Step 7: Normalize numeric prefixes
        text = self._normalize_numeric_prefixes(text)
        
        # Step 8: Remove trailing periods (not part of chemical identity)
        text = text.rstrip('.')
        
        # Step 9: Case folding (lowercase)
        text = self._case_fold(text)
        
        # Step 10: Final whitespace cleanup
        text = self._collapse_whitespace(text)
        
        return text.strip()
    
    def _unicode_normalize(self, text: str) -> str:
        """
        Apply Unicode NFKC normalization.
        
        NFKC (Normalization Form KC) performs compatibility decomposition
        followed by canonical composition. This handles various Unicode
        representations of the same character.
        
        Args:
            text: Input text
            
        Returns:
            Unicode-normalized text
        """
        return unicodedata.normalize('NFKC', text)
    
    def _case_fold(self, text: str) -> str:
        """
        Convert text to lowercase using case folding.
        
        Case folding is more aggressive than simple lowercasing and is
        designed for caseless matching.
        
        Args:
            text: Input text
            
        Returns:
            Lowercase text
        """
        return text.casefold()
    
    def _collapse_whitespace(self, text: str) -> str:
        """
        Collapse multiple consecutive whitespace characters to a single space.
        
        Handles spaces, tabs, newlines, and other whitespace characters.
        
        Args:
            text: Input text
            
        Returns:
            Text with collapsed whitespace
        """
        return re.sub(r'\s+', ' ', text)
    
    def _standardize_punctuation(self, text: str) -> str:
        """
        Standardize punctuation marks in chemical names.
        
        Transformations:
        - Parentheses, brackets, braces → spaces
        - Commas → spaces
        - Hyphens, en-dashes, em-dashes → spaces
        - Multiple punctuation → single space
        - Preserve periods in numeric contexts (e.g., 2.4-)
        
        Args:
            text: Input text
            
        Returns:
            Text with standardized punctuation
            
        Examples:
            >>> normalizer._standardize_punctuation("Benzo(a)pyrene")
            'Benzo a pyrene'
            >>> normalizer._standardize_punctuation("1,2,3-Trichlorobenzene")
            '1 2 3 Trichlorobenzene'
        """
        # Replace various types of brackets with spaces
        text = re.sub(r'[(){}\[\]]', ' ', text)
        
        # Replace commas with spaces
        text = re.sub(r',', ' ', text)
        
        # Replace various dash types with spaces
        # Unicode: hyphen, en-dash, em-dash, minus sign
        text = re.sub(r'[-\u2010-\u2015\u2212]', ' ', text)
        
        # Replace apostrophes and quotes
        text = re.sub(r"['\"]", ' ', text)
        
        # Replace semicolons and colons
        text = re.sub(r'[;:]', ' ', text)
        
        # Normalize multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def _expand_abbreviations(self, text: str) -> str:
        """
        Expand common chemical abbreviations.
        
        Handles:
        - Positional isomer prefixes: ortho, meta, para
        - Structural prefixes: tert, sec, iso, n
        
        Args:
            text: Input text
            
        Returns:
            Text with expanded abbreviations
            
        Examples:
            >>> normalizer._expand_abbreviations("o-Xylene")
            'ortho Xylene'
            >>> normalizer._expand_abbreviations("tert-Butanol")
            'tertiary Butanol'
        """
        result = text
        for pattern, replacement in self.ABBREVIATIONS.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        return result
    
    def _normalize_greek_letters(self, text: str) -> str:
        """
        Normalize Greek letter names to their Unicode symbols.
        
        Converts spelled-out Greek letters (alpha, beta, etc.) to
        their corresponding Unicode symbols (α, β, etc.).
        
        Args:
            text: Input text
            
        Returns:
            Text with normalized Greek letters
            
        Examples:
            >>> normalizer._normalize_greek_letters("alpha-Hexachlorocyclohexane")
            'α-Hexachlorocyclohexane'
        """
        result = text
        for pattern, replacement in self.GREEK_LETTERS.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        return result
    
    def _normalize_stereochemistry(self, text: str) -> str:
        """
        Normalize stereochemistry notation.
        
        Handles:
        - Optical rotation: (+), (-), (±)
        - Absolute configuration: (R), (S)
        - E/Z isomerism: (E), (Z)
        
        Preserves the stereochemical information while standardizing format.
        
        Args:
            text: Input text
            
        Returns:
            Text with normalized stereochemistry
            
        Examples:
            >>> normalizer._normalize_stereochemistry("(+)-Camphor")
            '+ Camphor'
            >>> normalizer._normalize_stereochemistry("(R)-2-Butanol")
            'R 2 Butanol'
        """
        # Remove parentheses around stereochemistry descriptors
        # Pattern matches (+), (-), (±), (R), (S), (E), (Z)
        text = re.sub(r'\(([+\-±RSEZrsez])\)', r'\1', text)
        
        # Ensure space after stereochemistry descriptor
        text = re.sub(r'([+\-±RSEZrsez])([a-zA-Z])', r'\1 \2', text)
        
        return text
    
    def _normalize_numeric_prefixes(self, text: str) -> str:
        """
        Normalize numeric prefixes in chemical nomenclature.
        
        Removes trailing hyphens from numeric prefixes (di-, tri-, tetra-, etc.)
        to create a consistent format.
        
        Args:
            text: Input text
            
        Returns:
            Text with normalized numeric prefixes
            
        Examples:
            >>> normalizer._normalize_numeric_prefixes("Tri-chloroethylene")
            'Tri chloroethylene'
        """
        result = text
        for pattern, replacement in self.NUMERIC_PREFIXES.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        return result


# Module-level singleton for convenience function
_normalizer_instance = None


def _get_normalizer() -> TextNormalizer:
    """Get or create the module-level TextNormalizer singleton."""
    global _normalizer_instance
    if _normalizer_instance is None:
        _normalizer_instance = TextNormalizer()
    return _normalizer_instance


def normalize_text(text: str) -> str:
    """
    Convenience function for text normalization.
    
    Uses a module-level TextNormalizer singleton to avoid
    repeated construction. This function is the public API
    for modules that import normalize_text directly.
    
    Args:
        text: Chemical name text to normalize
        
    Returns:
        Normalized text string
    """
    return _get_normalizer().normalize(text)
