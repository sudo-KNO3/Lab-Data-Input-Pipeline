"""
Tests for text normalization module.

Tests:
- Unicode normalization
- Punctuation standardization
- Chemical abbreviation expansion
- Qualifier handling
- CAS extraction
- PHC fraction detection
"""

import pytest
from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.normalization.qualifier_handler import QualifierHandler
from src.normalization.petroleum_handler import PetroleumHandler
from tests.fixtures.test_data import (
    NORMALIZATION_TEST_CASES,
    CAS_EXTRACTION_TEST_CASES,
    PHC_FRACTION_TEST_CASES,
    QUALIFIER_TEST_CASES,
)


# ============================================================================
# TEXT NORMALIZER TESTS
# ============================================================================

class TestTextNormalizer:
    """Tests for TextNormalizer class."""
    
    def test_normalize_basic(self, text_normalizer):
        """Test basic normalization."""
        result = text_normalizer.normalize("Benzene")
        assert result == "benzene", "Should normalize to lowercase"
    
    def test_normalize_unicode(self, text_normalizer):
        """Test Unicode character normalization."""
        test_cases = [
            ("Benzène", "benzene"),
            ("toluène", "toluene"),
            ("Café", "cafe"),
            ("naïve", "naive"),
        ]
        
        for input_text, expected in test_cases:
            result = text_normalizer.normalize(input_text)
            assert expected in result, f"Failed to normalize '{input_text}' properly"
    
    def test_normalize_whitespace(self, text_normalizer):
        """Test whitespace collapse."""
        test_cases = [
            ("  Benzene  ", "benzene"),
            ("Benzene\t\n", "benzene"),
            ("Benzene   Multiple   Spaces", "benzene multiple spaces"),
        ]
        
        for input_text, expected in test_cases:
            result = text_normalizer.normalize(input_text)
            assert result == expected, f"Whitespace not normalized correctly for '{input_text}'"
    
    def test_normalize_case_folding(self, text_normalizer):
        """Test case folding."""
        test_cases = [
            "BENZENE",
            "Benzene",
            "benzene",
            "BeNzEnE",
        ]
        
        results = [text_normalizer.normalize(text) for text in test_cases]
        assert len(set(results)) == 1, "Case variations should normalize to same result"
        assert results[0] == "benzene"
    
    def test_normalize_punctuation(self, text_normalizer):
        """Test punctuation standardization."""
        result = text_normalizer.normalize("1,2-Dichloroethane")
        # Should handle hyphens and commas
        assert "dichloroethane" in result
        
        result = text_normalizer.normalize("Benzene (total)")
        assert "benzene" in result
        assert "total" in result
    
    def test_normalize_chemical_abbreviations(self, text_normalizer):
        """Test chemical abbreviation expansion."""
        test_cases = [
            ("tert-Butanol", "tertiary"),
            ("sec-Butanol", "secondary"),
            ("iso-Propanol", "iso"),
            ("n-Hexane", "normal"),
        ]
        
        for input_text, expected_word in test_cases:
            result = text_normalizer.normalize(input_text)
            assert expected_word in result, f"Failed to expand abbreviation in '{input_text}'"
    
    def test_normalize_positional_isomers(self, text_normalizer):
        """Test ortho/meta/para notation."""
        test_cases = [
            "ortho-Xylene",
            "o-Xylene",
            "meta-Xylene",
            "m-Xylene",
            "para-Xylene",
            "p-Xylene",
        ]
        
        for text in test_cases:
            result = text_normalizer.normalize(text)
            assert "xylene" in result, f"Failed to normalize '{text}'"
    
    def test_normalize_numeric_prefixes(self, text_normalizer):
        """Test numeric prefix handling."""
        result = text_normalizer.normalize("1,2-Dichloroethane")
        assert "dichloroethane" in result
        
        result = text_normalizer.normalize("1,1,1-Trichloroethane")
        assert "trichloroethane" in result
    
    def test_normalize_empty_input(self, text_normalizer):
        """Test handling of empty input."""
        assert text_normalizer.normalize("") == ""
        assert text_normalizer.normalize(None) == ""
        assert text_normalizer.normalize("   ") == ""
    
    def test_normalize_preserves_structure(self, text_normalizer):
        """Test that normalization preserves essential chemical structure."""
        # Numbers should be preserved
        result = text_normalizer.normalize("1,2,3-Trimethylbenzene")
        assert any(char.isdigit() for char in result), "Should preserve numbers"
        
        # Should keep meaningful content
        result = text_normalizer.normalize("Benzo(a)pyrene")
        assert "benzo" in result
        assert "pyrene" in result
    
    def test_normalize_batch(self, text_normalizer):
        """Test batch normalization from test data."""
        for input_text, expected in NORMALIZATION_TEST_CASES:
            result = text_normalizer.normalize(input_text)
            assert expected in result or result in expected, \
                f"Normalization failed for '{input_text}': got '{result}', expected '{expected}'"
        assert "iso" in normalizer.normalize("iso-Propanol")
    
    def test_greek_letter_normalization(self, normalizer):
        """Test Greek letter normalization."""
        result = normalizer.normalize("alpha-Hexachlorocyclohexane")
        assert "α" in result or "alpha" in result.lower()
        
        result = normalizer.normalize("beta-Hexachlorocyclohexane")
        assert "β" in result or "beta" in result.lower()
        
        result = normalizer.normalize("gamma-BHC")
        assert "γ" in result or "gamma" in result.lower()
    
    def test_stereochemistry_normalization(self, normalizer):
        """Test stereochemistry notation."""
        # Optical rotation
        result = normalizer.normalize("(+)-Camphor")
        assert "camphor" in result
        
        # Absolute configuration
        result = normalizer.normalize("(R)-2-Butanol")
        assert "butanol" in result
        
        result = normalizer.normalize("(S)-Ibuprofen")
        assert "ibuprofen" in result
    
    def test_numeric_prefix_normalization(self, normalizer):
        """Test numeric prefix handling."""
        assert "di" in normalizer.normalize("Di-chloromethane")
        assert "tri" in normalizer.normalize("Tri-chloroethylene")
        assert "tetra" in normalizer.normalize("Tetra-chloroethylene")
        assert "penta" in normalizer.normalize("Penta-chlorophenol")
    
    def test_complex_names(self, normalizer):
        """Test complex chemical names."""
        # Complex PAH
        result = normalizer.normalize("Benzo(a)pyrene")
        assert "benzo" in result
        assert "pyrene" in result
        
        # Complex organochlorine
        result = normalizer.normalize("1,2,3,4-Tetrachlorobenzene")
        assert "1" in result
        assert "tetrachlorobenzene" in result
        
        # Complex phthalate
        result = normalizer.normalize("Bis(2-ethylhexyl)phthalate")
        assert "bis" in result
        assert "ethylhexyl" in result
        assert "phthalate" in result
    
    def test_unicode_normalization(self, normalizer):
        """Test Unicode normalization."""
        # Test NFKC normalization
        result = normalizer._unicode_normalize("Café")
        assert "Café" in result or "Cafe" in result
    
    def test_empty_input(self, normalizer):
        """Test handling of empty/invalid input."""
        assert normalizer.normalize("") == ""
        assert normalizer.normalize(None) == ""
        assert normalizer.normalize("   ") == ""


class TestQualifierHandler:
    """Test suite for QualifierHandler class."""
    
    @pytest.fixture
    def handler(self):
        """Create QualifierHandler instance."""
        return QualifierHandler()
    
    def test_strip_simple_qualifiers(self, handler):
        """Test stripping simple qualifiers."""
        text, qualifiers = handler.strip_qualifiers("Iron (Total)")
        assert "Iron" in text
        assert "total" in [q.lower() for q in qualifiers]
        
        text, qualifiers = handler.strip_qualifiers("Copper, Dissolved")
        assert "Copper" in text
        assert "dissolved" in [q.lower() for q in qualifiers]
    
    def test_strip_complex_qualifiers(self, handler):
        """Test stripping multi-word qualifiers."""
        text, qualifiers = handler.strip_qualifiers("Iron (Total Recoverable)")
        assert "Iron" in text
        assert "total recoverable" in [q.lower() for q in qualifiers]
    
    def test_preserve_list(self, handler):
        """Test qualifier preservation."""
        text, qualifiers = handler.strip_qualifiers(
            "Chromium, Hexavalent",
            preserve_list=['hexavalent']
        )
        assert "Hexavalent" in text
        assert len(qualifiers) == 0
    
    def test_extract_all_qualifiers(self, handler):
        """Test extracting all qualifiers."""
        qualifiers = handler.extract_all_qualifiers(
            "Iron (Total Recoverable)"
        )
        assert len(qualifiers) > 0
        
        qualifiers = handler.extract_all_qualifiers("Benzene")
        assert len(qualifiers) == 0
    
    def test_has_qualifier(self, handler):
        """Test qualifier detection."""
        assert handler.has_qualifier("Iron (Total)", "total")
        assert handler.has_qualifier("Copper, Dissolved", "dissolved")
        assert not handler.has_qualifier("Benzene", "total")
    
    def test_should_preserve_always(self, handler):
        """Test qualifiers that should always be preserved."""
        # Hexavalent should always be preserved
        assert handler.should_preserve_qualifier(
            "Chromium",
            "hexavalent",
            None
        )
        
        # As N should always be preserved
        assert handler.should_preserve_qualifier(
            "Nitrogen",
            "as n",
            None
        )


class TestCASExtractor:
    """Test suite for CASExtractor class."""
    
    @pytest.fixture
    def extractor(self):
        """Create CASExtractor instance."""
        return CASExtractor()
    
    def test_extract_cas_basic(self, extractor):
        """Test basic CAS extraction."""
        assert extractor.extract_cas("Benzene (71-43-2)") == "71-43-2"
        assert extractor.extract_cas("Toluene 108-88-3") == "108-88-3"
        assert extractor.extract_cas("CAS: 67-64-1") == "67-64-1"
    
    def test_extract_cas_none(self, extractor):
        """Test CAS extraction with no CAS present."""
        assert extractor.extract_cas("No CAS number here") is None
        assert extractor.extract_cas("") is None
        assert extractor.extract_cas(None) is None
    
    def test_validate_cas_valid(self, extractor):
        """Test CAS validation with valid numbers."""
        # Benzene
        assert extractor.validate_cas("71-43-2")
        # Toluene
        assert extractor.validate_cas("108-88-3")
        # Acetone
        assert extractor.validate_cas("67-64-1")
        # Water
        assert extractor.validate_cas("7732-18-5")
    
    def test_validate_cas_invalid(self, extractor):
        """Test CAS validation with invalid numbers."""
        # Wrong check digit
        assert not extractor.validate_cas("71-43-3")
        # Wrong format
        assert not extractor.validate_cas("12345")
        assert not extractor.validate_cas("1-2-3")
        # Empty
        assert not extractor.validate_cas("")
        assert not extractor.validate_cas(None)
    
    def test_format_cas(self, extractor):
        """Test CAS formatting."""
        assert extractor.format_cas("71432") == "71-43-2"
        assert extractor.format_cas("71-43-2") == "71-43-2"
        assert extractor.format_cas("1088883") == "108-88-3"
    
    def test_is_cas_format(self, extractor):
        """Test CAS format detection."""
        assert extractor.is_cas_format("71-43-2")
        assert extractor.is_cas_format("108-88-3")
        assert not extractor.is_cas_format("benzene")
        assert not extractor.is_cas_format("12-34")
    
    def test_extract_all_cas(self, extractor):
        """Test extraction of multiple CAS numbers."""
        text = "Benzene (71-43-2) and Toluene (108-88-3)"
        cas_numbers = extractor.extract_all_cas(text)
        assert len(cas_numbers) == 2
        assert "71-43-2" in cas_numbers
        assert "108-88-3" in cas_numbers


class TestPetroleumHandler:
    """Test suite for PetroleumHandler class."""
    
    @pytest.fixture
    def handler(self):
        """Create PetroleumHandler instance."""
        return PetroleumHandler()
    
    def test_detect_phc_explicit(self, handler):
        """Test PHC detection with explicit notation."""
        assert handler.detect_phc_fraction("PHC F1") == "F1"
        assert handler.detect_phc_fraction("PHC F2") == "F2"
        assert handler.detect_phc_fraction("PHC F3") == "F3"
        assert handler.detect_phc_fraction("PHC F4") == "F4"
    
    def test_detect_phc_with_range(self, handler):
        """Test PHC detection with carbon range."""
        assert handler.detect_phc_fraction("PHC F2 (C10-C16)") == "F2"
        assert handler.detect_phc_fraction("F3 (C16-C34)") == "F3"
        assert handler.detect_phc_fraction("Fraction 1 (C6-C10)") == "F1"
    
    def test_detect_phc_from_range(self, handler):
        """Test PHC detection from carbon range alone."""
        assert handler.detect_phc_fraction("C10-C16") == "F2"
        assert handler.detect_phc_fraction("C16-C34") == "F3"
        assert handler.detect_phc_fraction("C6-C10") == "F1"
    
    def test_detect_phc_greater_than(self, handler):
        """Test PHC F4 detection with greater-than notation."""
        assert handler.detect_phc_fraction(">C34") == "F4"
        assert handler.detect_phc_fraction("> C34") == "F4"
    
    def test_detect_phc_verbose(self, handler):
        """Test PHC detection with verbose notation."""
        assert handler.detect_phc_fraction("Petroleum Hydrocarbons F2") == "F2"
        assert handler.detect_phc_fraction("Petroleum Hydrocarbon Fraction 3") == "F3"
    
    def test_detect_phc_none(self, handler):
        """Test PHC detection with non-PHC text."""
        assert handler.detect_phc_fraction("Benzene") is None
        assert handler.detect_phc_fraction("Iron") is None
        assert handler.detect_phc_fraction("") is None
    
    def test_normalize_phc_notation(self, handler):
        """Test PHC notation normalization."""
        assert "phc f2" in handler.normalize_phc_notation("PHC F2 (C10-C16)")
        assert "phc f3" in handler.normalize_phc_notation("Petroleum Hydrocarbons Fraction 3")
    
    def test_is_phc(self, handler):
        """Test PHC detection."""
        assert handler.is_phc("PHC F2")
        assert handler.is_phc("Petroleum Hydrocarbons F3")
        assert handler.is_phc("F1 (C6-C10)")
        assert not handler.is_phc("Benzene")
        assert not handler.is_phc("Iron")
    
    def test_get_fraction_carbon_range(self, handler):
        """Test getting carbon range for fraction."""
        assert handler.get_fraction_carbon_range("F1") == ("C6", "C10")
        assert handler.get_fraction_carbon_range("F2") == ("C10", "C16")
        assert handler.get_fraction_carbon_range("F3") == ("C16", "C34")
        assert handler.get_fraction_carbon_range("F4") == ("C34", None)
    
    def test_get_fraction_description(self, handler):
        """Test getting fraction description."""
        desc = handler.get_fraction_description("F2")
        assert "C10-C16" in desc
        assert "Medium" in desc or "medium" in desc


class TestIntegration:
    """Integration tests combining multiple normalizers."""
    
    def test_full_pipeline(self):
        """Test complete normalization pipeline."""
        normalizer = TextNormalizer()
        cas_extractor = CASExtractor()
        qualifier_handler = QualifierHandler()
        phc_handler = PetroleumHandler()
        
        # Test 1: Complex chemical with qualifiers
        text = "Chromium, Hexavalent (Total Recoverable)"
        clean_text, qualifiers = qualifier_handler.strip_qualifiers(text)
        normalized = normalizer.normalize(clean_text)
        assert "chromium" in normalized
        
        # Test 2: Chemical with CAS
        text = "Benzene (CAS: 71-43-2)"
        cas = cas_extractor.extract_cas(text)
        assert cas == "71-43-2"
        assert cas_extractor.validate_cas(cas)
        
        # Test 3: PHC with complex notation
        text = "Petroleum Hydrocarbons F2 (C10-C16) - Total Extractable"
        fraction = phc_handler.detect_phc_fraction(text)
        assert fraction == "F2"
        
        clean_text, qualifiers = qualifier_handler.strip_qualifiers(text)
        normalized_phc = phc_handler.normalize_phc_notation(clean_text)
        assert "phc f2" in normalized_phc
    
    def test_ontario_variants(self):
        """Test handling of Ontario-specific variants."""
        normalizer = TextNormalizer()
        
        # Test common Ontario lab notation variants
        test_cases = [
            ("1,4-Dioxane", "1 4 dioxane"),
            ("Benzo(a)pyrene", "benzo a pyrene"),
            ("Bis(2-ethylhexyl)phthalate", "bis 2 ethylhexyl phthalate"),
            ("o-Xylene", "ortho xylene"),
            ("m-Xylene", "meta xylene"),
            ("p-Xylene", "para xylene"),
        ]
        
        for input_text, expected_substring in test_cases:
            result = normalizer.normalize(input_text)
            assert expected_substring in result.lower(), \
                f"Expected ''{expected_substring}'' in normalized ''{result}''"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
