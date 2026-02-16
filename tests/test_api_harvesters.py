"""
Tests for API harvesters and quality filters.
"""
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from src.bootstrap import (
    APIError,
    ChemicalResolverHarvester,
    PubChemHarvester,
    clean_synonym_text,
    extract_cas_from_text,
    filter_synonyms,
    validate_cas_format,
)


# ============================================================================
# Quality Filter Tests
# ============================================================================


class TestQualityFilters:
    """Test suite for synonym quality filters."""

    def test_filter_synonyms_basic(self):
        """Test basic synonym filtering."""
        synonyms = [
            "Benzene",
            "benzene",  # Duplicate
            "Cyclohexatriene",
            "Benzenol",
        ]

        filtered = filter_synonyms(synonyms, "single_substance")

        assert len(filtered) == 3  # One duplicate removed
        assert "Benzene" in filtered
        assert "Cyclohexatriene" in filtered

    def test_filter_synonyms_length(self):
        """Test length filtering."""
        synonyms = [
            "Short name",
            "A" * 150,  # Too long
            "Normal length chemical name",
        ]

        filtered = filter_synonyms(synonyms, "single_substance", max_length=120)

        assert len(filtered) == 2
        assert all(len(s) <= 120 for s in filtered)

    def test_filter_synonyms_mixture_terms(self):
        """Test mixture term filtering for single substances."""
        synonyms = [
            "Benzene",
            "Benzene solution",  # Should be filtered
            "Benzene mixture",  # Should be filtered
            "Benzene formulation",  # Should be filtered
        ]

        filtered = filter_synonyms(synonyms, "single_substance")

        assert len(filtered) == 1
        assert filtered[0] == "Benzene"

    def test_filter_synonyms_generic_terms(self):
        """Test generic term filtering."""
        synonyms = [
            "Benzene",
            "Benzene standard",  # Should be filtered
            "Total benzene",  # Should be filtered
            "Benzene sample",  # Should be filtered
        ]

        filtered = filter_synonyms(synonyms, "single_substance")

        assert len(filtered) == 1
        assert filtered[0] == "Benzene"

    def test_filter_synonyms_trade_names(self):
        """Test trade name filtering."""
        synonyms = [
            "Benzene",
            "BenzPro®",  # Should be filtered
            "ChemX™",  # Should be filtered
        ]

        filtered = filter_synonyms(synonyms, "single_substance")

        assert len(filtered) == 1
        assert filtered[0] == "Benzene"

    def test_filter_synonyms_abbreviations(self):
        """Test abbreviation validation."""
        synonyms = [
            "PCB",  # Valid
            "TCE",  # Valid
            "A",  # Too short
            "ABC 123",  # Has space - invalid
            "ABCDEFGHIJK",  # Too long for abbreviation
        ]

        filtered = filter_synonyms(synonyms, "single_substance")

        assert "PCB" in filtered
        assert "TCE" in filtered
        assert "A" not in filtered
        assert "ABC 123" not in filtered

    def test_filter_synonyms_non_ascii(self):
        """Test non-ASCII filtering."""
        synonyms = [
            "Benzene",
            "Benzène",  # Non-ASCII
            "苯",  # Chinese
        ]

        filtered = filter_synonyms(synonyms, "single_substance", require_ascii=True)

        assert len(filtered) == 1
        assert filtered[0] == "Benzene"

    def test_clean_synonym_text(self):
        """Test synonym text cleaning."""
        assert clean_synonym_text("Benzene [71-43-2]") == "Benzene"
        assert clean_synonym_text("Benzene (99%)") == "Benzene"
        assert clean_synonym_text("  Benzene  ") == "Benzene"
        assert clean_synonym_text("Benzene\n\t(pure)") == "Benzene"

    def test_validate_cas_format(self):
        """Test CAS number validation."""
        # Valid CAS numbers
        assert validate_cas_format("71-43-2")
        assert validate_cas_format("50-00-0")
        assert validate_cas_format("7732-18-5")

        # Invalid CAS numbers
        assert not validate_cas_format("71-43-3")  # Wrong check digit
        assert not validate_cas_format("71-43")  # Missing part
        assert not validate_cas_format("abc-de-f")  # Non-numeric
        assert not validate_cas_format("")  # Empty

    def test_extract_cas_from_text(self):
        """Test CAS number extraction."""
        text = "Benzene (CAS: 71-43-2) and Toluene (CAS: 108-88-3)"
        cas_numbers = extract_cas_from_text(text)

        assert len(cas_numbers) == 2
        assert "71-43-2" in cas_numbers
        assert "108-88-3" in cas_numbers


# ============================================================================
# PubChem Harvester Tests
# ============================================================================


class TestPubChemHarvester:
    """Test suite for PubChem harvester."""

    @pytest.fixture
    def harvester(self, tmp_path):
        """Create harvester instance with temporary cache."""
        return PubChemHarvester(cache_dir=tmp_path)

    @pytest.fixture
    def mock_pubchem_response(self):
        """Mock successful PubChem response."""
        return {
            "InformationList": {
                "Information": [
                    {
                        "Synonym": [
                            "Benzene",
                            "Benzol",
                            "Cyclohexatriene",
                            "Phenyl hydride",
                            "71-43-2",
                        ]
                    }
                ]
            }
        }

    def test_harvest_synonyms_success(self, harvester, mock_pubchem_response):
        """Test successful synonym harvest."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_pubchem_response
            mock_request.return_value = mock_response

            synonyms = harvester.harvest_synonyms("71-43-2", "Benzene")

            assert len(synonyms) == 5
            assert "Benzene" in synonyms
            assert "Benzol" in synonyms

    def test_harvest_synonyms_not_found(self, harvester):
        """Test harvest when compound not found."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_request.return_value = mock_response

            synonyms = harvester.harvest_synonyms("99999-99-9", "Unknown")

            assert len(synonyms) == 0

    def test_harvest_synonyms_fallback_to_name(self, harvester, mock_pubchem_response):
        """Test fallback to name search when CAS fails."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            # First call (CAS) returns 404, second (name) succeeds
            mock_404 = Mock()
            mock_404.status_code = 404

            mock_200 = Mock()
            mock_200.status_code = 200
            mock_200.json.return_value = mock_pubchem_response

            mock_request.side_effect = [mock_404, mock_200]

            synonyms = harvester.harvest_synonyms(None, "Benzene")

            assert len(synonyms) == 5
            assert mock_request.call_count == 1  # Only name search called

    def test_rate_limiting(self, harvester):
        """Test rate limiting enforcement."""
        calls, period = harvester.get_rate_limit()
        assert calls == 5
        assert period == 1

    def test_get_properties(self, harvester):
        """Test property retrieval."""
        mock_properties = {
            "PropertyTable": {
                "Properties": [
                    {
                        "IUPACName": "benzene",
                        "MolecularFormula": "C6H6",
                        "MolecularWeight": 78.11,
                        "InChI": "InChI=1S/C6H6/c1-2-4-6-5-3-1/h1-6H",
                        "InChIKey": "UHOVQNZJYSORNB-UHFFFAOYSA-N",
                    }
                ]
            }
        }

        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_properties
            mock_request.return_value = mock_response

            properties = harvester.get_properties("71-43-2")

            assert properties is not None
            assert properties["MolecularFormula"] == "C6H6"
            assert properties["IUPACName"] == "benzene"


# ============================================================================
# Chemical Resolver Harvester Tests
# ============================================================================


class TestChemicalResolverHarvester:
    """Test suite for NCI Chemical Resolver harvester."""

    @pytest.fixture
    def harvester(self, tmp_path):
        """Create harvester instance with temporary cache."""
        return ChemicalResolverHarvester(cache_dir=tmp_path)

    def test_harvest_synonyms_success(self, harvester):
        """Test successful synonym harvest."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "Benzene\nBenzol\nCyclohexatriene\nPhenyl hydride"
            mock_request.return_value = mock_response

            synonyms = harvester.harvest_synonyms("71-43-2", "Benzene")

            assert len(synonyms) == 4
            assert "Benzene" in synonyms
            assert "Benzol" in synonyms

    def test_harvest_synonyms_not_found(self, harvester):
        """Test harvest when compound not found."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_request.return_value = mock_response

            synonyms = harvester.harvest_synonyms("99999-99-9", "Unknown")

            assert len(synonyms) == 0

    def test_get_smiles(self, harvester):
        """Test SMILES retrieval."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "c1ccccc1"
            mock_request.return_value = mock_response

            smiles = harvester.get_smiles("71-43-2")

            assert smiles == "c1ccccc1"

    def test_get_inchi(self, harvester):
        """Test InChI retrieval."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "InChI=1S/C6H6/c1-2-4-6-5-3-1/h1-6H"
            mock_request.return_value = mock_response

            inchi = harvester.get_inchi("71-43-2")

            assert inchi.startswith("InChI=")

    def test_get_inchi_key(self, harvester):
        """Test InChIKey retrieval."""
        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "UHOVQNZJYSORNB-UHFFFAOYSA-N"
            mock_request.return_value = mock_response

            inchi_key = harvester.get_inchi_key("71-43-2")

            assert inchi_key == "UHOVQNZJYSORNB-UHFFFAOYSA-N"

    def test_rate_limiting(self, harvester):
        """Test rate limiting enforcement."""
        calls, period = harvester.get_rate_limit()
        assert calls == 2
        assert period == 1


# ============================================================================
# Base API Tests
# ============================================================================


class TestBaseAPIHarvester:
    """Test suite for base API functionality."""

    @pytest.fixture
    def harvester(self, tmp_path):
        """Create harvester instance."""
        return PubChemHarvester(cache_dir=tmp_path)

    def test_caching(self, harvester):
        """Test response caching."""
        with patch.object(harvester.session, "get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"test": "data"}
            mock_get.return_value = mock_response

            # First request
            response1 = harvester._make_request("https://example.com")

            # Second request should potentially use cache
            # (behavior depends on requests-cache configuration)
            response2 = harvester._make_request("https://example.com")

            assert response1.status_code == 200
            assert response2.status_code == 200

    def test_error_handling_timeout(self, harvester):
        """Test timeout error handling."""
        with patch.object(harvester.session, "get") as mock_get:
            mock_get.side_effect = requests.Timeout("Connection timeout")

            with pytest.raises(APIError) as exc_info:
                harvester._make_request("https://example.com")

            assert "timeout" in str(exc_info.value).lower()

    def test_error_handling_http_error(self, harvester):
        """Test HTTP error handling."""
        with patch.object(harvester.session, "get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.raise_for_status.side_effect = requests.HTTPError("Server error")
            mock_get.return_value = mock_response

            with pytest.raises(APIError) as exc_info:
                harvester._make_request("https://example.com")

            assert "500" in str(exc_info.value) or "error" in str(exc_info.value).lower()

    def test_exponential_backoff(self, harvester):
        """Test exponential backoff retry logic."""
        with patch.object(harvester.session, "get") as mock_get:
            # Fail twice, then succeed
            mock_response_fail = Mock()
            mock_response_fail.status_code = 500
            mock_response_fail.raise_for_status.side_effect = requests.HTTPError()

            mock_response_success = Mock()
            mock_response_success.status_code = 200
            mock_response_success.json.return_value = {}

            mock_get.side_effect = [
                requests.RequestException("Error 1"),
                requests.RequestException("Error 2"),
                mock_response_success,
            ]

            # Should succeed after retries
            response = harvester._make_request("https://example.com")
            assert response.status_code == 200
            assert mock_get.call_count == 3

    def test_context_manager(self, harvester):
        """Test context manager usage."""
        with harvester as h:
            assert h.session is not None

        # Session should be closed after context exit
        # (Can't easily test this without internal access)

    def test_cache_info(self, harvester):
        """Test cache info retrieval."""
        info = harvester.get_cache_info()
        assert isinstance(info, dict)
        assert "backend" in info or "responses_cached" in info


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests with controlled scenarios."""

    @pytest.fixture
    def harvester(self, tmp_path):
        """Create harvester with temporary cache."""
        return PubChemHarvester(cache_dir=tmp_path, cache_expire_after=1)

    def test_full_harvest_and_filter_pipeline(self, harvester):
        """Test complete harvest and filter pipeline."""
        mock_response = {
            "InformationList": {
                "Information": [
                    {
                        "Synonym": [
                            "Benzene",
                            "Benzol",
                            "Benzene solution",  # Will be filtered
                            "Benzene standard",  # Will be filtered
                            "B",  # Too short
                            "Cyclohexatriene",
                            "A" * 150,  # Too long
                        ]
                    }
                ]
            }
        }

        with patch.object(harvester, "_rate_limited_request") as mock_request:
            mock_resp = Mock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = mock_response
            mock_request.return_value = mock_resp

            # Harvest
            raw_synonyms = harvester.harvest_synonyms("71-43-2", "Benzene")

            # Filter
            filtered = filter_synonyms(raw_synonyms, "single_substance")

            # Should have filtered out: solution, standard, B (too short), long string
            assert len(filtered) < len(raw_synonyms)
            assert "Benzene" in filtered
            assert "Cyclohexatriene" in filtered
            assert "Benzene solution" not in filtered
            assert "Benzene standard" not in filtered


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
