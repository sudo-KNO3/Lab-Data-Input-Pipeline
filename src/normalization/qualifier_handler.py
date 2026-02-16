"""
Qualifier handling module for chemical names.

Manages the extraction and preservation of chemical qualifiers like
"total", "dissolved", "hexavalent", etc., with intelligent decision-making
based on the canonical analyte database.
"""

import re
from typing import List, Tuple, Optional, Dict, Any


class QualifierHandler:
    """
    Handles qualifier extraction and preservation for chemical names.
    
    Qualifiers are descriptive terms that modify chemical names, such as
    "total", "dissolved", "hexavalent", etc. This class determines whether
    qualifiers should be preserved based on whether the canonical database
    differentiates between variants (e.g., "Chromium" vs "Chromium VI").
    """
    
    # Common qualifiers found in laboratory data
    COMMON_QUALIFIERS = [
        'total',
        'dissolved',
        'recoverable',
        'extractable',
        'hexavalent',
        'trivalent',
        'total recoverable',
        'acid extractable',
        'weak acid dissociable',
        'reactive',
        'available',
        'soluble',
        'inorganic',
        'organic',
        'elemental',
        'ionic',
        'free',
        'combined',
        'as n',  # as nitrogen
        'as p',  # as phosphorus
        'as cn',  # as cyanide
    ]
    
    # Qualifiers that should almost always be preserved
    PRESERVE_ALWAYS = [
        'hexavalent',
        'trivalent',
        'as n',
        'as p',
        'as cn',
        'elemental',
        'ionic',
    ]
    
    def __init__(self):
        """Initialize the qualifier handler."""
        # Compile regex patterns for efficiency
        self._qualifier_patterns = {
            q: re.compile(
                r'\b' + re.escape(q) + r'\b',
                flags=re.IGNORECASE
            )
            for q in self.COMMON_QUALIFIERS
        }
    
    def should_preserve_qualifier(
        self,
        analyte_name: str,
        qualifier: str,
        analytes_db: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Determine if a qualifier should be preserved for a given analyte.
        
        Decision logic:
        1. If qualifier is in PRESERVE_ALWAYS list  preserve
        2. If analytes_db provided, check if database has variants with/without qualifier
        3. If database differentiates variants  preserve
        4. Otherwise  safe to strip
        
        Args:
            analyte_name: Base chemical name (without qualifier)
            qualifier: The qualifier in question (e.g., "total", "dissolved")
            analytes_db: Optional dictionary mapping analyte names to database records
            
        Returns:
            True if qualifier should be preserved, False if it can be stripped
            
        Examples:
            >>> handler = QualifierHandler()
            >>> handler.should_preserve_qualifier("Chromium", "hexavalent")
            True
            >>> handler.should_preserve_qualifier("Iron", "total")
            False  # Depends on database
        """
        # Always preserve certain qualifiers
        if qualifier.lower() in [q.lower() for q in self.PRESERVE_ALWAYS]:
            return True
        
        # If no database provided, err on the side of preservation
        if analytes_db is None:
            return True
        
        # Check if database has variants with this qualifier
        analyte_lower = analyte_name.lower()
        qualifier_lower = qualifier.lower()
        
        # Look for entries that match base name with and without qualifier
        has_with_qualifier = False
        has_without_qualifier = False
        
        for db_name in analytes_db.keys():
            db_name_lower = db_name.lower()
            
            # Check if this is the base analyte without qualifier
            if db_name_lower == analyte_lower:
                has_without_qualifier = True
            
            # Check if this is the analyte with this specific qualifier
            if (analyte_lower in db_name_lower and 
                qualifier_lower in db_name_lower):
                has_with_qualifier = True
        
        # If database differentiates variants, preserve the qualifier
        if has_with_qualifier and has_without_qualifier:
            return True
        
        # If only one variant exists, qualifier is not differentiating
        return False
    
    def strip_qualifiers(
        self,
        text: str,
        preserve_list: Optional[List[str]] = None
    ) -> Tuple[str, List[str]]:
        """
        Remove qualifiers from text and return both cleaned text and extracted qualifiers.
        
        Args:
            text: Input text potentially containing qualifiers
            preserve_list: Optional list of qualifiers that should NOT be stripped
            
        Returns:
            Tuple of (cleaned_text, extracted_qualifiers)
            - cleaned_text: Text with qualifiers removed
            - extracted_qualifiers: List of qualifiers that were found and removed
            
        Examples:
            >>> handler = QualifierHandler()
            >>> handler.strip_qualifiers("Iron (Total Recoverable)")
            ('Iron', ['total recoverable'])
            >>> handler.strip_qualifiers("Chromium, Hexavalent", preserve_list=['hexavalent'])
            ('Chromium, Hexavalent', [])
        """
        if not text or not isinstance(text, str):
            return '', []
        
        preserve_list = preserve_list or []
        preserve_lower = [q.lower() for q in preserve_list]
        
        extracted_qualifiers = []
        cleaned_text = text
        
        # Sort qualifiers by length (longest first) to handle multi-word qualifiers
        sorted_qualifiers = sorted(
            self.COMMON_QUALIFIERS,
            key=len,
            reverse=True
        )
        
        for qualifier in sorted_qualifiers:
            # Skip if this qualifier should be preserved
            if qualifier.lower() in preserve_lower:
                continue
            
            # Check if qualifier is present
            pattern = self._qualifier_patterns[qualifier]
            match = pattern.search(cleaned_text)
            
            if match:
                extracted_qualifiers.append(qualifier)
                # Remove the qualifier and surrounding punctuation
                cleaned_text = pattern.sub('', cleaned_text)
        
        # Clean up punctuation and whitespace
        cleaned_text = self._cleanup_after_removal(cleaned_text)
        
        return cleaned_text, extracted_qualifiers
    
    def _cleanup_after_removal(self, text: str) -> str:
        """
        Clean up text after qualifier removal.
        
        Removes:
        - Trailing/leading commas, parentheses
        - Extra whitespace
        - Empty parentheses
        
        Args:
            text: Text to clean up
            
        Returns:
            Cleaned text
        """
        # Remove empty parentheses
        text = re.sub(r'\(\s*\)', '', text)
        
        # Remove trailing/leading commas and whitespace
        text = re.sub(r'\s*,\s*$', '', text)
        text = re.sub(r'^\s*,\s*', '', text)
        
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Trim
        return text.strip()
    
    def extract_all_qualifiers(self, text: str) -> List[str]:
        """
        Extract all qualifiers present in text without removing them.
        
        Useful for analysis and logging purposes.
        
        Args:
            text: Input text
            
        Returns:
            List of all qualifiers found in text
        """
        found_qualifiers = []
        
        for qualifier in self.COMMON_QUALIFIERS:
            pattern = self._qualifier_patterns[qualifier]
            if pattern.search(text):
                found_qualifiers.append(qualifier)
        
        return found_qualifiers
    
    def has_qualifier(self, text: str, qualifier: str) -> bool:
        """
        Check if text contains a specific qualifier.
        
        Args:
            text: Input text
            qualifier: Qualifier to check for
            
        Returns:
            True if qualifier is present, False otherwise
        """
        if qualifier not in self._qualifier_patterns:
            # Create pattern on the fly for non-standard qualifiers
            pattern = re.compile(
                r'\b' + re.escape(qualifier) + r'\b',
                flags=re.IGNORECASE
            )
        else:
            pattern = self._qualifier_patterns[qualifier]
        
        return pattern.search(text) is not None
