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
        Download and parse the NPRI substance list CSV from ECCC's open data portal.

        Builds a CAS-indexed dict mapping CAS numbers to English/French name lists.
        The CSV is cached by requests_cache so subsequent runs avoid re-downloading.
        Falls back to an empty cache on any error so the harvester degrades gracefully.
        """
        import csv
        import io

        # ECCC open data download URL for the NPRI substance list
        csv_url = (
            "https://data-donnees.az.ec.gc.ca/api/file?path=/substances/"
            "organize-organiser/canada-s-national-pollutant-release-inventory"
            "--linventaire-national-des-rejets-de-polluants-du-canada/"
            "NPRI-INRP_SubstList_ListeSubst.csv"
        )

        try:
            response = self._make_request(csv_url)
            if response.status_code != 200:
                logger.warning(
                    f"NPRI: Could not download substance list (HTTP {response.status_code})"
                )
                self._substance_cache = {}
                return

            reader = csv.DictReader(io.StringIO(response.text))
            fieldnames = [f.strip() for f in (reader.fieldnames or [])]

            # Flexible column detection — column headers vary across annual releases
            cas_col = next((f for f in fieldnames if "CAS" in f.upper()), None)
            en_col = next(
                (f for f in fieldnames if "ENGLISH" in f.upper() or
                 ("NAME" in f.upper() and "FRENCH" not in f.upper() and "FRAN" not in f.upper())),
                None,
            )
            fr_col = next(
                (f for f in fieldnames if "FRENCH" in f.upper() or "FRAN" in f.upper()),
                None,
            )

            if not cas_col:
                logger.warning(
                    f"NPRI: Cannot identify CAS column. Available: {fieldnames[:8]}"
                )
                self._substance_cache = {}
                return

            self._substance_cache = {}
            for row in reader:
                cas = row.get(cas_col, "").strip()
                if not cas or cas.upper() in ("N/A", "NA", ""):
                    continue
                names: List[str] = []
                if en_col:
                    n = row.get(en_col, "").strip()
                    if n:
                        names.append(n)
                if fr_col:
                    n = row.get(fr_col, "").strip()
                    if n and n not in names:
                        names.append(n)
                if names:
                    self._substance_cache[cas] = names

            logger.info(f"NPRI: Loaded {len(self._substance_cache)} substances from CSV")

        except Exception as exc:
            logger.warning(f"NPRI substance list load failed ({exc}); harvester disabled")
            self._substance_cache = {}

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from NPRI substance list.

        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name (unused — NPRI is CAS-indexed)

        Returns:
            List of synonyms found in the NPRI substance list
        """
        if not cas_number or not self._substance_cache:
            return []
        return list(self._substance_cache.get(cas_number, []))

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


class CASCommonChemistryHarvester(BaseAPIHarvester):
    """
    Harvester for CAS Common Chemistry.

    Free API from the American Chemical Society providing authoritative
    chemical names and synonyms for ~500,000 common substances. CAS is the
    primary source for definitive CAS numbers and names, so its synonyms
    typically have high quality and authority.

    API docs: https://commonchemistry.cas.org/api
    Rate limit: 2 requests/second (conservative; no published limit)
    """

    BASE_URL = "https://commonchemistry.cas.org/api"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.last_api_call = 0.0

    def get_rate_limit(self) -> tuple[int, int]:
        """Rate limit: 2 requests per second."""
        return (2, 1)

    def _rate_limited_request(self, url: str, **kwargs):
        """Make rate-limited request, skipping sleep for cached responses."""
        response = self._make_request(url, **kwargs)
        is_cached = getattr(response, "from_cache", False)
        if not is_cached:
            current = time.time()
            elapsed = current - self.last_api_call
            if elapsed < 0.5 and self.last_api_call > 0:
                time.sleep(0.5 - elapsed)
            self.last_api_call = time.time()
        return response

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from CAS Common Chemistry.

        CAS Common Chemistry only supports CAS-number lookups, so this
        harvester returns an empty list when no CAS number is provided.

        Args:
            cas_number: CAS registry number
            chemical_name: Unused (CAS required)

        Returns:
            List of synonyms including the preferred name
        """
        if not cas_number:
            return []

        url = f"{self.BASE_URL}/detail?cas_rn={cas_number}"
        try:
            response = self._rate_limited_request(url)
            if response.status_code == 404:
                logger.debug(f"CAS Common Chemistry: No record for {cas_number}")
                return []

            data = self._parse_json_response(response)
            if not data:
                return []

            synonyms: List[str] = list(data.get("synonyms", []))

            # Prepend the primary/preferred name so it gets highest weight
            name = data.get("name", "")
            if name and name not in synonyms:
                synonyms.insert(0, name)

            logger.debug(
                f"CAS Common Chemistry: {len(synonyms)} synonyms for {cas_number}"
            )
            return synonyms

        except APIError as exc:
            logger.warning(f"CAS Common Chemistry lookup failed for {cas_number}: {exc}")
            return []


class ChEBIHarvester(BaseAPIHarvester):
    """
    Harvester for ChEBI (Chemical Entities of Biological Interest).

    ChEBI is a freely available database of molecular entities focused on
    'small' chemical compounds, maintained by the European Bioinformatics
    Institute. Particularly useful for biochemically relevant compounds
    (metabolites, cofactors, lipids, vitamins).

    Uses the ChEBI REST/XML web service.
    API docs: https://www.ebi.ac.uk/webservices/chebi/
    Rate limit: 3 requests/second (2 calls per lookup: search + get entity)
    """

    BASE_URL = "https://www.ebi.ac.uk/webservices/chebi/2.0/test"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.last_api_call = 0.0

    def get_rate_limit(self) -> tuple[int, int]:
        """Rate limit: 3 requests per second."""
        return (3, 1)

    def _rate_limited_request(self, url: str, **kwargs):
        """Make rate-limited request, skipping sleep for cached responses."""
        response = self._make_request(url, **kwargs)
        is_cached = getattr(response, "from_cache", False)
        if not is_cached:
            current = time.time()
            elapsed = current - self.last_api_call
            if elapsed < 0.34 and self.last_api_call > 0:
                time.sleep(0.34 - elapsed)
            self.last_api_call = time.time()
        return response

    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms from ChEBI.

        Searches by CAS number first, falling back to chemical name. Returns
        synonyms from the top ChEBI match only to avoid cross-compound pollution.

        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name (fallback)

        Returns:
            List of synonyms from ChEBI
        """
        identifier = cas_number or chemical_name
        if not identifier:
            return []

        try:
            chebi_ids = self._search_chebi(identifier)
            if not chebi_ids:
                logger.debug(f"ChEBI: No match for {identifier}")
                return []

            synonyms = self._get_synonyms(chebi_ids[0])
            logger.debug(
                f"ChEBI: {len(synonyms)} synonyms for {identifier} ({chebi_ids[0]})"
            )
            return synonyms

        except Exception as exc:
            logger.warning(f"ChEBI lookup failed for {identifier}: {exc}")
            return []

    def _search_chebi(self, query: str) -> List[str]:
        """Search ChEBI and return a list of ChEBI IDs for the top matches."""
        import xml.etree.ElementTree as ET

        response = self._rate_limited_request(
            f"{self.BASE_URL}/getLiteEntity",
            params={
                "search": query,
                "searchCategory": "ALL",
                "maximumResults": "5",
                "stars": "ALL",
            },
        )
        if response.status_code != 200:
            return []

        try:
            root = ET.fromstring(response.text)
            # Use local-name matching to be namespace-agnostic
            ids = [
                elem.text
                for elem in root.iter()
                if (elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag) == "chebiId"
                and elem.text
            ]
            return ids[:3]
        except ET.ParseError as exc:
            logger.debug(f"ChEBI XML parse error during search: {exc}")
            return []

    def _get_synonyms(self, chebi_id: str) -> List[str]:
        """Retrieve all synonyms for a single ChEBI entity."""
        import xml.etree.ElementTree as ET

        response = self._rate_limited_request(
            f"{self.BASE_URL}/getCompleteEntity",
            params={"chebiId": chebi_id},
        )
        if response.status_code != 200:
            return []

        try:
            root = ET.fromstring(response.text)
            synonyms: List[str] = []

            for elem in root.iter():
                local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                # Primary ASCII name
                if local == "chebiAsciiName" and elem.text:
                    synonyms.append(elem.text)

                # <synonyms> elements contain <data> children with synonym text
                if local == "synonyms":
                    for child in elem:
                        child_local = (
                            child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        )
                        if child_local == "data" and child.text:
                            synonyms.append(child.text)

            # Deduplicate while preserving order
            seen: set = set()
            unique: List[str] = []
            for s in synonyms:
                if s.lower() not in seen:
                    seen.add(s.lower())
                    unique.append(s)
            return unique

        except ET.ParseError as exc:
            logger.debug(f"ChEBI XML parse error for {chebi_id}: {exc}")
            return []


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
        "cas_common_chemistry": CASCommonChemistryHarvester(cache_dir=cache_dir),
        "chebi": ChEBIHarvester(cache_dir=cache_dir),
    }

    logger.info(f"Initialized {len(harvesters)} API harvesters")
    return harvesters
