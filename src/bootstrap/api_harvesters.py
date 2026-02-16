"""
API harvesters for chemical synonym collection.

Implements harvesters for:
- PubChem (NCBI)
- Chemical Identifier Resolver (NCI/CACTUS)
- NPRI (Canada)
"""
import time
from typing import Any, Dict, List, Optional

from loguru import logger
from ratelimit import limits, sleep_and_retry

from .base_api import APIError, BaseAPIHarvester


class PubChemHarvester(BaseAPIHarvester):
    """
    Harvester for PubChem database.
    
    PubChem provides:
    - Synonyms
    - IUPAC names
    - Molecular formula
    - Molecular weight
    - InChI/InChIKey
    
    Rate limit: 5 requests/second (no auth)
    """

    BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_calls = 0
        self.last_api_call = 0  # Track last actual API call time

    def get_rate_limit(self) -> tuple[int, int]:
        """Rate limit: 5 requests per second."""
        return (5, 1)

    def _rate_limited_request(self, url: str, **kwargs):
        """Make rate-limited request (only limits actual API calls, not cache hits)."""
        import time
        
        # Make the request
        response = self._make_request(url, **kwargs)
        
        # Check if it was cached
        is_cached = getattr(response, "from_cache", False)
        
        if not is_cached:
            # This was an actual API call - apply rate limiting
            self.api_calls += 1
            current_time = time.time()
            time_since_last = current_time - self.last_api_call
            
            # Ensure at least 0.2 seconds between API calls (5 per second)
            if time_since_last < 0.2 and self.last_api_call > 0:
                time.sleep(0.2 - time_since_last)
            
            self.last_api_call = time.time()
        
        return response

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from PubChem.
        
        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name (fallback if CAS fails)
            
        Returns:
            List of synonyms
        """
        synonyms = []

        # Try CAS number first
        if cas_number:
            try:
                cas_synonyms = self._get_synonyms_by_cas(cas_number)
                if cas_synonyms:
                    synonyms.extend(cas_synonyms)
                    logger.debug(f"PubChem: Found {len(cas_synonyms)} synonyms for CAS {cas_number}")
                    return synonyms
            except APIError as e:
                logger.warning(f"PubChem CAS lookup failed for {cas_number}: {e}")

        # Fallback to name search
        if chemical_name:
            try:
                name_synonyms = self._get_synonyms_by_name(chemical_name)
                if name_synonyms:
                    synonyms.extend(name_synonyms)
                    logger.debug(f"PubChem: Found {len(name_synonyms)} synonyms for '{chemical_name}'")
            except APIError as e:
                logger.warning(f"PubChem name lookup failed for '{chemical_name}': {e}")

        return synonyms

    def _get_synonyms_by_cas(self, cas_number: str) -> List[str]:
        """Get synonyms by CAS number."""
        url = f"{self.BASE_URL}/compound/name/{cas_number}/synonyms/JSON"
        response = self._rate_limited_request(url)

        if response.status_code == 404:
            logger.debug(f"PubChem: No data for CAS {cas_number}")
            return []

        data = self._parse_json_response(response)
        if not data:
            return []

        try:
            synonyms = data["InformationList"]["Information"][0]["Synonym"]
            return synonyms if isinstance(synonyms, list) else []
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Failed to parse PubChem synonyms: {e}")
            return []

    def _get_synonyms_by_name(self, name: str) -> List[str]:
        """Get synonyms by chemical name."""
        url = f"{self.BASE_URL}/compound/name/{name}/synonyms/JSON"
        response = self._rate_limited_request(url)

        if response.status_code == 404:
            return []

        data = self._parse_json_response(response)
        if not data:
            return []

        try:
            synonyms = data["InformationList"]["Information"][0]["Synonym"]
            return synonyms if isinstance(synonyms, list) else []
        except (KeyError, IndexError, TypeError):
            return []

    def get_preferred_name(self, identifier: str, use_cas: bool = True) -> Optional[str]:
        """
        Get PubChem's preferred/default name for a chemical.
        
        Returns IUPAC name if available, otherwise first synonym (most common name).
        
        Args:
            identifier: CAS number or chemical name
            use_cas: If True, treats identifier as CAS number
            
        Returns:
            Preferred chemical name or None
        """
        if not identifier:
            return None
            
        try:
            # Get properties including IUPAC name
            if use_cas:
                url = f"{self.BASE_URL}/compound/name/{identifier}/property/IUPACName/JSON"
            else:
                url = f"{self.BASE_URL}/compound/name/{identifier}/property/IUPACName/JSON"
                
            response = self._rate_limited_request(url)
            
            if response.status_code == 200:
                data = self._parse_json_response(response)
                if data and "PropertyTable" in data:
                    iupac_name = data["PropertyTable"]["Properties"][0].get("IUPACName")
                    if iupac_name:
                        logger.debug(f"PubChem: Found IUPAC name '{iupac_name}' for '{identifier}'")
                        return iupac_name
            
            # Fallback: Get first synonym (usually the most common name)
            synonyms = self._get_synonyms_by_cas(identifier) if use_cas else self._get_synonyms_by_name(identifier)
            if synonyms and len(synonyms) > 0:
                # First synonym is typically the most common/preferred name
                preferred = synonyms[0]
                logger.debug(f"PubChem: Using first synonym '{preferred}' for '{identifier}'")
                return preferred
                
            return None
            
        except (APIError, KeyError, IndexError, TypeError) as e:
            logger.warning(f"Failed to get preferred name from PubChem for '{identifier}': {e}")
            return None
    
    def get_cas_number(self, chemical_name: str) -> Optional[str]:
        """
        Get CAS number for a chemical by name from PubChem.
        
        Searches PubChem by name and extracts the CAS number from synonyms.
        
        Args:
            chemical_name: Chemical name to search
            
        Returns:
            CAS number string or None
        """
        if not chemical_name:
            return None
            
        try:
            # Get CID first
            cid_url = f"{self.BASE_URL}/compound/name/{chemical_name}/cids/JSON"
            response = self._rate_limited_request(cid_url)
            
            if response.status_code == 404:
                logger.debug(f"PubChem: No CID found for '{chemical_name}'")
                return None
                
            data = self._parse_json_response(response)
            if not data or "IdentifierList" not in data:
                return None
                
            cids = data["IdentifierList"].get("CID", [])
            if not cids:
                return None
                
            # Use first CID to get synonyms
            cid = cids[0]
            syn_url = f"{self.BASE_URL}/compound/cid/{cid}/synonyms/JSON"
            response = self._rate_limited_request(syn_url)
            
            if response.status_code != 200:
                return None
                
            data = self._parse_json_response(response)
            if not data:
                return None
                
            # Look for CAS number in synonyms
            synonyms = data["InformationList"]["Information"][0].get("Synonym", [])
            
            # CAS numbers have format: 123-45-6 or 12345-67-8
            import re
            cas_pattern = r'\b\d{2,7}-\d{2}-\d\b'
            
            for synonym in synonyms:
                if isinstance(synonym, str) and re.match(cas_pattern, synonym):
                    logger.debug(f"PubChem: Found CAS {synonym} for '{chemical_name}'")
                    return synonym
                    
            return None
            
        except (APIError, KeyError, IndexError, TypeError) as e:
            logger.warning(f"Failed to get CAS number from PubChem for '{chemical_name}': {e}")
            return None

    def get_properties(self, cas_number: str) -> Optional[Dict[str, Any]]:
        """
        Get chemical properties from PubChem.
        
        Args:
            cas_number: CAS registry number
            
        Returns:
            Dictionary with properties or None
        """
        properties = [
            "IUPACName",
            "MolecularFormula",
            "MolecularWeight",
            "InChI",
            "InChIKey",
        ]
        property_string = ",".join(properties)

        url = f"{self.BASE_URL}/compound/name/{cas_number}/property/{property_string}/JSON"

        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                return None

            data = self._parse_json_response(response)
            if not data:
                return None

            return data["PropertyTable"]["Properties"][0]
        except (APIError, KeyError, IndexError, TypeError) as e:
            logger.warning(f"Failed to get PubChem properties for {cas_number}: {e}")
            return None


class ChemicalResolverHarvester(BaseAPIHarvester):
    """
    Harvester for NCI Chemical Identifier Resolver (CACTUS).
    
    Provides:
    - SMILES
    - InChI/InChIKey
    - Alternative names
    - Structure conversions
    
    Rate limit: 2 requests/second (recommended for courtesy)
    """

    BASE_URL = "https://cactus.nci.nih.gov/chemical/structure"

    def __init__(self, **kwargs):
        """Initialize harvester with rate limiting tracking."""
        super().__init__(**kwargs)
        self.last_api_call = 0

    def get_rate_limit(self) -> tuple[int, int]:
        """Rate limit: 2 requests per second."""
        return (2, 1)

    def _rate_limited_request(self, url: str, **kwargs):
        """Make rate-limited request (only limits actual API calls, not cache hits)."""
        import time
        
        # Make the request
        response = self._make_request(url, **kwargs)
        
        # Check if it was cached
        is_cached = getattr(response, "from_cache", False)
        
        if not is_cached:
            # This was an actual API call - apply rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_api_call
            
            # Ensure at least 0.5 seconds between API calls (2 per second)
            if time_since_last < 0.5 and self.last_api_call > 0:
                time.sleep(0.5 - time_since_last)
            
            self.last_api_call = time.time()
        
        return response

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from Chemical Identifier Resolver.
        
        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name (fallback)
            
        Returns:
            List of synonyms
        """
        # Try CAS number first
        identifier = cas_number if cas_number else chemical_name
        if not identifier:
            return []

        try:
            synonyms = self._get_names(identifier)
            if synonyms:
                logger.debug(f"NCI: Found {len(synonyms)} names for {identifier}")
                return synonyms
        except APIError as e:
            logger.warning(f"NCI lookup failed for {identifier}: {e}")

        return []

    def _get_names(self, identifier: str) -> List[str]:
        """Get all names for a chemical."""
        url = f"{self.BASE_URL}/{identifier}/names"

        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                return []

            # Response is plain text with one name per line
            text = response.text.strip()
            if not text:
                return []

            names = [line.strip() for line in text.split("\n") if line.strip()]
            return names
        except APIError as e:
            logger.debug(f"NCI names lookup failed: {e}")
            return []

    def get_smiles(self, identifier: str) -> Optional[str]:
        """
        Convert identifier to SMILES.
        
        Args:
            identifier: CAS number or chemical name
            
        Returns:
            SMILES string or None
        """
        url = f"{self.BASE_URL}/{identifier}/smiles"

        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                return None

            smiles = response.text.strip()
            return smiles if smiles else None
        except APIError:
            return None

    def get_inchi(self, identifier: str) -> Optional[str]:
        """
        Convert identifier to InChI.
        
        Args:
            identifier: CAS number or chemical name
            
        Returns:
            InChI string or None
        """
        url = f"{self.BASE_URL}/{identifier}/stdinchi"

        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                return None

            inchi = response.text.strip()
            return inchi if inchi else None
        except APIError:
            return None

    def get_inchi_key(self, identifier: str) -> Optional[str]:
        """
        Convert identifier to InChIKey.
        
        Args:
            identifier: CAS number or chemical name
            
        Returns:
            InChIKey string or None
        """
        url = f"{self.BASE_URL}/{identifier}/stdinchikey"

        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                return None

            inchi_key = response.text.strip()
            return inchi_key if inchi_key else None
        except APIError:
            return None


class NPRIHarvester(BaseAPIHarvester):
    """
    Harvester for Canada's National Pollutant Release Inventory.
    
    NPRI provides:
    - Substance verification (Canadian regulatory list)
    - Alternative names
    - CAS validation
    
    Note: NPRI primarily provides bulk CSV downloads rather than REST API.
    This harvester checks if substance exists in Canadian inventory.
    
    Rate limit: 2 requests/second (courtesy)
    """

    # NPRI provides CSVs rather than REST API
    # Using ECCC's Open Data portal
    BASE_URL = "https://open.canada.ca/data/en/dataset/40e01423-7728-429c-ac9d-2954385ccdfb"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._substance_cache = None
        self._load_substance_list()

    def get_rate_limit(self) -> tuple[int, int]:
        """Rate limit: 2 requests per second."""
        return (2, 1)

    def _load_substance_list(self):
        """
        Load NPRI substance list.
        
        Note: In production, this would download and cache the substance CSV.
        For now, we'll implement a basic stub.
        """
        # TODO: Download and parse NPRI substance list CSV
        # https://open.canada.ca/data/en/dataset/40e01423-7728-429c-ac9d-2954385ccdfb
        logger.warning("NPRI substance list not yet implemented - using stub")
        self._substance_cache = {}

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from NPRI.
        
        Currently returns empty list as NPRI requires CSV parsing.
        
        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name
            
        Returns:
            List of synonyms (currently empty - stub)
        """
        # Stub implementation - NPRI requires CSV download
        logger.debug("NPRI harvester is a stub - requires CSV implementation")
        return []

    def verify_substance(self, cas_number: str) -> bool:
        """
        Check if substance exists in NPRI inventory.
        
        Args:
            cas_number: CAS registry number
            
        Returns:
            True if substance is in NPRI
        """
        if not self._substance_cache:
            return False

        return cas_number in self._substance_cache


def create_harvesters(cache_dir: Optional[str] = None) -> Dict[str, BaseAPIHarvester]:
    """
    Create all API harvesters.
    
    Args:
        cache_dir: Optional cache directory path
        
    Returns:
        Dictionary of harvester instances
    """
    harvesters = {
        "pubchem": PubChemHarvester(cache_dir=cache_dir),
        "nci": ChemicalResolverHarvester(cache_dir=cache_dir),
        "npri": NPRIHarvester(cache_dir=cache_dir),
    }

    logger.info(f"Initialized {len(harvesters)} API harvesters")
    return harvesters
