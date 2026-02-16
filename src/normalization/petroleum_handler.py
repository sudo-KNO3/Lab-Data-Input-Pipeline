"""
Petroleum hydrocarbon fraction handling module.

Specialized handler for Ontario Regulation 153 petroleum hydrocarbon
fractions (F1, F2, F3, F4) with various notation formats.
"""

import re
from typing import Optional


class PetroleumHandler:
    """
    Handles petroleum hydrocarbon (PHC) fraction detection and normalization.
    
    Ontario Regulation 153 defines four PHC fractions:
    - F1 (C6-C10): Light aliphatic hydrocarbons
    - F2 (C10-C16): Medium aliphatic hydrocarbons
    - F3 (C16-C34): Heavy aliphatic hydrocarbons
    - F4 (>C34): Very heavy aliphatic hydrocarbons
    
    This class handles various notation formats seen in laboratory data.
    """
    
    # Carbon range definitions for PHC fractions
    FRACTION_RANGES = {
        'F1': ('C6', 'C10'),
        'F2': ('C10', 'C16'),
        'F3': ('C16', 'C34'),
        'F4': ('C34', None),  # >C34
    }
    
    # Common PHC notation patterns
    PHC_PATTERNS = [
        # "PHC F2", "PHC F2 - (C10-C16)", "PHC Fraction 2"
        re.compile(
            r'(?:phc|petroleum\s+hydrocarbons?)\s*'
            r'(?:fraction\s*)?'
            r'[fF]?([1-4])',
            re.IGNORECASE
        ),
        # "F2 (C10-C16)", "Fraction 2"
        re.compile(
            r'\b[fF](?:raction\s*)?([1-4])\b',
            re.IGNORECASE
        ),
        # Extract from carbon range: "C10-C16", "C10 to C16"
        re.compile(
            r'\b[cC](\d+)\s*(?:-|to)\s*[cC](\d+)\b',
            re.IGNORECASE
        ),
        # Greater than notation: ">C34", "> C34"
        re.compile(
            r'>\s*[cC](\d+)',
            re.IGNORECASE
        ),
    ]
    
    # Aliases and common variations
    PHC_ALIASES = {
        'f1': ['f1', 'fraction 1', 'fraction 1', 'c6-c10', 'c6 to c10'],
        'f2': ['f2', 'fraction 2', 'fraction 2', 'c10-c16', 'c10 to c16'],
        'f3': ['f3', 'fraction 3', 'fraction 3', 'c16-c34', 'c16 to c34'],
        'f4': ['f4', 'fraction 4', 'fraction 4', '>c34', 'greater than c34'],
    }
    
    def __init__(self):
        """Initialize the petroleum handler."""
        pass
    
    def detect_phc_fraction(self, text: str) -> Optional[str]:
        """
        Detect PHC fraction from text.
        
        Recognizes various notation formats and returns standardized
        fraction identifier (F1, F2, F3, or F4).
        
        Args:
            text: Input text potentially containing PHC fraction reference
            
        Returns:
            Fraction identifier ('F1', 'F2', 'F3', 'F4') or None if not detected
            
        Examples:
            >>> handler = PetroleumHandler()
            >>> handler.detect_phc_fraction("PHC F2 (C10-C16)")
            'F2'
            >>> handler.detect_phc_fraction("Petroleum Hydrocarbons Fraction 3")
            'F3'
            >>> handler.detect_phc_fraction("F1")
            'F1'
            >>> handler.detect_phc_fraction(">C34")
            'F4'
        """
        if not text or not isinstance(text, str):
            return None
        
        text_lower = text.lower()
        
        # Try explicit fraction patterns first
        for pattern in self.PHC_PATTERNS[:2]:
            match = pattern.search(text)
            if match:
                fraction_num = match.group(1)
                return f'F{fraction_num}'
        
        # Try carbon range pattern
        carbon_range_pattern = self.PHC_PATTERNS[2]
        match = carbon_range_pattern.search(text)
        if match:
            start_carbon = int(match.group(1))
            end_carbon = int(match.group(2))
            
            # Match to known fraction ranges
            fraction = self._carbon_range_to_fraction(start_carbon, end_carbon)
            if fraction:
                return fraction
        
        # Try greater-than pattern (for F4)
        gt_pattern = self.PHC_PATTERNS[3]
        match = gt_pattern.search(text)
        if match:
            carbon_num = int(match.group(1))
            if carbon_num >= 34:
                return 'F4'
        
        # Check aliases
        for fraction, aliases in self.PHC_ALIASES.items():
            for alias in aliases:
                if alias in text_lower:
                    return fraction.upper()
        
        return None
    
    def _carbon_range_to_fraction(
        self,
        start_carbon: int,
        end_carbon: int
    ) -> Optional[str]:
        """
        Map carbon range to PHC fraction.
        
        Args:
            start_carbon: Starting carbon number
            end_carbon: Ending carbon number
            
        Returns:
            Fraction identifier or None if range doesn't match
        """
        # Check each fraction's range
        if start_carbon == 6 and end_carbon == 10:
            return 'F1'
        elif start_carbon == 10 and end_carbon == 16:
            return 'F2'
        elif start_carbon == 16 and end_carbon == 34:
            return 'F3'
        elif start_carbon >= 34:
            return 'F4'
        
        # Fuzzy matching - allow some variation
        # F1: C6-C10
        if 5 <= start_carbon <= 7 and 9 <= end_carbon <= 11:
            return 'F1'
        # F2: C10-C16
        elif 9 <= start_carbon <= 11 and 15 <= end_carbon <= 17:
            return 'F2'
        # F3: C16-C34
        elif 15 <= start_carbon <= 17 and 32 <= end_carbon <= 35:
            return 'F3'
        
        return None
    
    def normalize_phc_notation(self, text: str) -> str:
        """
        Normalize PHC notation to standard format.
        
        Converts various PHC notations to standardized format for
        consistent matching and storage.
        
        Args:
            text: Input text with PHC notation
            
        Returns:
            Normalized text with standardized PHC notation
            
        Examples:
            >>> handler = PetroleumHandler()
            >>> handler.normalize_phc_notation("PHC F2 (C10-C16)")
            'phc f2'
            >>> handler.normalize_phc_notation("Petroleum Hydrocarbons Fraction 3")
            'phc f3'
        """
        if not text or not isinstance(text, str):
            return ''
        
        # Detect fraction
        fraction = self.detect_phc_fraction(text)
        if not fraction:
            # No PHC detected, return as-is
            return text
        
        # Normalize to "PHC FX" format
        normalized = f'phc {fraction.lower()}'
        
        return normalized
    
    def is_phc(self, text: str) -> bool:
        """
        Check if text refers to petroleum hydrocarbons.
        
        Args:
            text: Input text
            
        Returns:
            True if text contains PHC reference, False otherwise
        """
        if not text or not isinstance(text, str):
            return False
        
        text_lower = text.lower()
        
        # Check for PHC keywords
        phc_keywords = [
            'phc',
            'petroleum hydrocarbons',
            'petroleum hydrocarbon',
        ]
        
        for keyword in phc_keywords:
            if keyword in text_lower:
                return True
        
        # Check if fraction is detected
        if self.detect_phc_fraction(text):
            return True
        
        return False
    
    def get_fraction_carbon_range(self, fraction: str) -> Optional[tuple]:
        """
        Get carbon range for a given PHC fraction.
        
        Args:
            fraction: Fraction identifier ('F1', 'F2', 'F3', 'F4')
            
        Returns:
            Tuple of (start_carbon, end_carbon) or None if invalid fraction
            
        Examples:
            >>> handler = PetroleumHandler()
            >>> handler.get_fraction_carbon_range('F2')
            ('C10', 'C16')
        """
        fraction_upper = fraction.upper()
        return self.FRACTION_RANGES.get(fraction_upper)
    
    def get_fraction_description(self, fraction: str) -> Optional[str]:
        """
        Get human-readable description of PHC fraction.
        
        Args:
            fraction: Fraction identifier ('F1', 'F2', 'F3', 'F4')
            
        Returns:
            Description string or None if invalid fraction
        """
        descriptions = {
            'F1': 'Light aliphatic hydrocarbons (C6-C10)',
            'F2': 'Medium aliphatic hydrocarbons (C10-C16)',
            'F3': 'Heavy aliphatic hydrocarbons (C16-C34)',
            'F4': 'Very heavy aliphatic hydrocarbons (>C34)',
        }
        
        fraction_upper = fraction.upper()
        return descriptions.get(fraction_upper)
