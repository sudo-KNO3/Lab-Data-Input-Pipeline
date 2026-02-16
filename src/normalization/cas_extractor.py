"""
CAS number extraction and validation module.

Handles CAS (Chemical Abstracts Service) number extraction from text,
validation, and database lookup functionality.
"""

import re
from typing import Optional, Any


class CASExtractor:
    """
    Extracts and validates CAS Registry Numbers from chemical data.
    
    CAS numbers are unique identifiers for chemical substances.
    Format: 2-7 digits, hyphen, 2 digits, hyphen, 1 check digit
    Example: 71-43-2 (Benzene)
    
    The check digit is calculated using a specific algorithm to validate
    the CAS number's integrity.
    """
    
    # CAS number regex pattern
    # Format: \d{2,7}-\d{2}-\d
    CAS_PATTERN = re.compile(
        r'\b(\d{2,7}-\d{2}-\d)\b'
    )
    
    def __init__(self):
        """Initialize the CAS extractor."""
        pass
    
    def extract_cas(self, text: str) -> Optional[str]:
        """
        Extract CAS number from text.
        
        Searches for patterns matching CAS number format and returns
        the first valid CAS number found.
        
        Args:
            text: Input text potentially containing a CAS number
            
        Returns:
            CAS number if found and valid, None otherwise
            
        Examples:
            >>> extractor = CASExtractor()
            >>> extractor.extract_cas("Benzene (CAS: 71-43-2)")
            '71-43-2'
            >>> extractor.extract_cas("Toluene 108-88-3")
            '108-88-3'
            >>> extractor.extract_cas("No CAS here")
            None
        """
        if not text or not isinstance(text, str):
            return None
        
        matches = self.CAS_PATTERN.findall(text)
        
        for cas in matches:
            if self.validate_cas(cas):
                return cas
        
        return None
    
    def extract_all_cas(self, text: str) -> list[str]:
        """
        Extract all valid CAS numbers from text.
        
        Args:
            text: Input text
            
        Returns:
            List of all valid CAS numbers found
        """
        if not text or not isinstance(text, str):
            return []
        
        matches = self.CAS_PATTERN.findall(text)
        return [cas for cas in matches if self.validate_cas(cas)]
    
    def validate_cas(self, cas: str) -> bool:
        """
        Validate CAS number using check digit algorithm.
        
        The check digit is calculated by:
        1. Remove hyphens from CAS number
        2. Take all digits except the last (check digit)
        3. Starting from the right, multiply each digit by its position (1, 2, 3, ...)
        4. Sum all products
        5. Take sum modulo 10
        6. Compare with check digit
        
        Args:
            cas: CAS number to validate
            
        Returns:
            True if CAS number is valid, False otherwise
            
        Examples:
            >>> extractor = CASExtractor()
            >>> extractor.validate_cas("71-43-2")
            True
            >>> extractor.validate_cas("71-43-3")
            False
        """
        if not cas or not isinstance(cas, str):
            return False
        
        # Check format
        if not self.CAS_PATTERN.match(cas):
            return False
        
        # Remove hyphens
        digits_only = cas.replace('-', '')
        
        # Must have at least 5 digits (minimum: 2-digit + 2-digit + check digit)
        if len(digits_only) < 5:
            return False
        
        # Extract check digit (last digit)
        try:
            check_digit = int(digits_only[-1])
        except ValueError:
            return False
        
        # Calculate expected check digit
        # Process all digits except the last one, from right to left
        number_part = digits_only[:-1]
        total = 0
        
        for i, digit in enumerate(reversed(number_part), start=1):
            try:
                total += int(digit) * i
            except ValueError:
                return False
        
        expected_check_digit = total % 10
        
        return check_digit == expected_check_digit
    
    def format_cas(self, cas: str) -> Optional[str]:
        """
        Format a CAS number to standard format with hyphens.
        
        Accepts CAS numbers with or without hyphens and returns
        standardized format.
        
        Args:
            cas: CAS number (with or without hyphens)
            
        Returns:
            Formatted CAS number or None if invalid
            
        Examples:
            >>> extractor = CASExtractor()
            >>> extractor.format_cas("71432")
            '71-43-2'
            >>> extractor.format_cas("71-43-2")
            '71-43-2'
        """
        if not cas or not isinstance(cas, str):
            return None
        
        # Remove any existing hyphens
        digits_only = cas.replace('-', '')
        
        # Must be at least 5 digits
        if len(digits_only) < 5:
            return None
        
        # Format as XXX...X-XX-X
        check_digit = digits_only[-1]
        second_part = digits_only[-3:-1]
        first_part = digits_only[:-3]
        
        formatted = f"{first_part}-{second_part}-{check_digit}"
        
        # Validate the formatted CAS
        if self.validate_cas(formatted):
            return formatted
        
        return None
    
    def lookup_by_cas(self, cas: str, db_session: Any) -> Optional[Any]:
        """
        Look up an analyte in the database by CAS number.
        
        Args:
            cas: CAS number to look up
            db_session: SQLAlchemy database session
            
        Returns:
            Analyte database record if found, None otherwise
            
        Note:
            This method requires the database models to be imported.
            Returns None if database is not available or CAS not found.
        """
        if not self.validate_cas(cas):
            return None
        
        if db_session is None:
            return None
        
        try:
            # Import database models
            from src.database.models import Analyte
            
            # Query database for CAS number
            result = db_session.query(Analyte).filter(
                Analyte.cas_number == cas
            ).first()
            
            return result
            
        except ImportError:
            # Database models not available
            return None
        except Exception:
            # Database error
            return None
    
    def is_cas_format(self, text: str) -> bool:
        """
        Check if text matches CAS number format (without validation).
        
        Useful for quickly checking if a string looks like a CAS number
        before performing full validation.
        
        Args:
            text: Text to check
            
        Returns:
            True if text matches CAS format, False otherwise
        """
        if not text or not isinstance(text, str):
            return False
        
        return self.CAS_PATTERN.match(text.strip()) is not None
