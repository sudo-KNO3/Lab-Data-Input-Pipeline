"""
PubChem fallback matcher for chemical name resolution.

When the local synonym database doesn't contain a match, this module
queries PubChem's PUG REST API to:
1. Look up the chemical name → get CID, CAS number(s), and synonyms
2. Cross-reference the CAS against existing analytes in the local DB
3. If a CAS match is found → auto-add the lab name as a new synonym
4. If no CAS match → report the PubChem data for human review
5. Cache all lookups (hit or miss) to avoid re-querying

Rate-limited to respect PubChem's 5 req/sec guideline.
"""

import re
import time
import json
import logging
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Tuple

from sqlalchemy.orm import Session

from src.database.models import Synonym, Analyte
from src.normalization.text_normalizer import TextNormalizer
from src.matching.match_result import MatchResult

logger = logging.getLogger(__name__)

# PubChem PUG REST base URL
PUBCHEM_BASE = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug'

# CAS number regex pattern
CAS_PATTERN = re.compile(r'^\d{2,7}-\d{2}-\d$')

# Default local cache file
DEFAULT_CACHE_PATH = Path(__file__).parent.parent.parent / 'data' / 'pubchem_cache.json'

# Minimum seconds between PubChem requests
MIN_REQUEST_INTERVAL = 0.25  # 4 req/sec


class PubChemFallback:
    """
    PubChem-backed fallback matcher for unknown chemical names.
    
    Integrates into the resolution cascade as a final step before
    marking a chemical as unmatched. When triggered:
    
    1. Checks local disk cache first (avoids redundant API calls)
    2. Queries PubChem by name → gets CID and synonyms
    3. Extracts CAS numbers from PubChem synonyms
    4. Cross-references CAS against existing analytes in our DB
    5. If match found: inserts the original lab name as a new synonym → exact match
    6. If no match: returns PubChem metadata for human review
    
    All lookups are cached locally so repeated ingestions don't re-query.
    """
    
    def __init__(self,
                 db_session: Session,
                 normalizer: Optional[TextNormalizer] = None,
                 cache_path: Optional[Path] = None,
                 max_synonyms_to_harvest: int = 25,
                 auto_add_synonyms: bool = True):
        """
        Args:
            db_session: SQLAlchemy session for analyte/synonym lookups
            normalizer: TextNormalizer instance (creates new if None)
            cache_path: Path to JSON cache file (default: data/pubchem_cache.json)
            max_synonyms_to_harvest: Max PubChem synonyms to store per compound
            auto_add_synonyms: If True, automatically add lab name as synonym
                               when CAS cross-reference finds an existing analyte
        """
        self.db_session = db_session
        self.normalizer = normalizer or TextNormalizer()
        self.cache_path = cache_path or DEFAULT_CACHE_PATH
        self.max_synonyms_to_harvest = max_synonyms_to_harvest
        self.auto_add_synonyms = auto_add_synonyms
        self._last_request_time = 0.0
        self._cache = self._load_cache()
    
    # ── Cache management ──────────────────────────────────────────────
    
    def _load_cache(self) -> Dict:
        """Load the local PubChem lookup cache from disk."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load PubChem cache: {e}")
        return {}
    
    def _save_cache(self):
        """Persist the cache to disk."""
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.warning(f"Failed to save PubChem cache: {e}")
    
    def _cache_key(self, text: str) -> str:
        """Generate a cache key from input text."""
        return text.strip().lower()
    
    # ── PubChem API calls ─────────────────────────────────────────────
    
    def _rate_limit(self):
        """Enforce minimum interval between PubChem requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()
    
    def _pubchem_get(self, url: str) -> Optional[Dict]:
        """
        Make a GET request to PubChem with rate limiting.
        
        Returns parsed JSON or None on error/not-found.
        """
        self._rate_limit()
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Reg153-ChemMatcher/2.0 (automated-ingestion)'
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug(f"PubChem 404: {url}")
                return None
            logger.warning(f"PubChem HTTP error {e.code}: {url}")
            return None
        except Exception as e:
            logger.warning(f"PubChem request failed: {e}")
            return None
    
    def _search_pubchem(self, name: str) -> Optional[Dict]:
        """
        Search PubChem for a chemical name.
        
        Returns dict with CID, IUPAC name, CAS numbers, and synonyms,
        or None if not found.
        """
        # Step 1: Get CID and basic properties
        encoded = urllib.parse.quote(name)
        prop_url = (
            f'{PUBCHEM_BASE}/compound/name/{encoded}'
            f'/property/IUPACName,MolecularFormula,MolecularWeight/JSON'
        )
        prop_data = self._pubchem_get(prop_url)
        if not prop_data:
            return None
        
        try:
            props = prop_data['PropertyTable']['Properties'][0]
            cid = props['CID']
        except (KeyError, IndexError):
            return None
        
        # Step 2: Get synonyms (includes CAS numbers)
        syn_url = f'{PUBCHEM_BASE}/compound/cid/{cid}/synonyms/JSON'
        syn_data = self._pubchem_get(syn_url)
        
        synonyms = []
        cas_numbers = []
        if syn_data:
            try:
                all_syns = syn_data['InformationList']['Information'][0]['Synonym']
                
                # Extract CAS numbers
                cas_numbers = [s for s in all_syns if CAS_PATTERN.match(s)]
                
                # Filter useful synonyms (skip internal registry IDs)
                for s in all_syns:
                    if len(s) > 120:
                        continue
                    if any(s.startswith(prefix) for prefix in
                           ('DTXSID', 'DTXCID', 'SCHEMBL', 'RefChem:', 'AKOS',
                            'MFCD', 'NSC-', 'EINECS', 'EC ')):
                        continue
                    synonyms.append(s)
                    if len(synonyms) >= self.max_synonyms_to_harvest:
                        break
            except (KeyError, IndexError):
                pass
        
        return {
            'cid': cid,
            'iupac_name': props.get('IUPACName', ''),
            'formula': props.get('MolecularFormula', ''),
            'molecular_weight': props.get('MolecularWeight', 0),
            'cas_numbers': cas_numbers,
            'synonyms': synonyms,
            'queried_at': datetime.now().isoformat(),
        }
    
    # ── CAS cross-reference ───────────────────────────────────────────
    
    def _find_analyte_by_cas(self, cas_numbers: List[str]) -> Optional[Analyte]:
        """
        Check if any of the CAS numbers match an existing analyte.
        
        Checks both the analytes.cas_number column and CAS entries
        in the synonyms table.
        """
        for cas in cas_numbers:
            # Check analytes table
            analyte = self.db_session.query(Analyte).filter(
                Analyte.cas_number == cas
            ).first()
            if analyte:
                logger.info(f"PubChem CAS cross-ref: {cas} → {analyte.preferred_name}")
                return analyte
            
            # Check synonyms table for CAS stored as synonym
            syn = self.db_session.query(Synonym).filter(
                Synonym.synonym_raw == cas
            ).first()
            if syn:
                analyte = self.db_session.query(Analyte).filter(
                    Analyte.analyte_id == syn.analyte_id
                ).first()
                if analyte:
                    logger.info(f"PubChem CAS synonym cross-ref: {cas} → {analyte.preferred_name}")
                    return analyte
        
        return None
    
    # ── Synonym insertion ─────────────────────────────────────────────
    
    def _add_synonym(self, analyte_id: str, raw_text: str, source: str = 'pubchem_auto') -> bool:
        """
        Add a new synonym to the database if it doesn't already exist.
        
        Returns True if a new synonym was inserted.
        """
        normalized = self.normalizer.normalize(raw_text)
        if not normalized:
            return False
        
        # Check if this normalized form already exists for this analyte
        existing = self.db_session.query(Synonym).filter(
            Synonym.analyte_id == analyte_id,
            Synonym.synonym_norm == normalized
        ).first()
        
        if existing:
            return False
        
        new_syn = Synonym(
            analyte_id=analyte_id,
            synonym_raw=raw_text,
            synonym_norm=normalized,
            synonym_type='common_name',
            harvest_source=source,
            confidence=1.0,
            created_at=datetime.now(),
        )
        self.db_session.add(new_syn)
        
        try:
            self.db_session.flush()
            logger.info(f"Added synonym '{raw_text}' → {analyte_id} (source={source})")
            return True
        except Exception as e:
            logger.warning(f"Failed to add synonym '{raw_text}': {e}")
            self.db_session.rollback()
            return False
    
    def _harvest_pubchem_synonyms(self, analyte_id: str, pubchem_data: Dict) -> int:
        """
        Add useful PubChem synonyms to the database for an analyte.
        
        Returns count of newly added synonyms.
        """
        added = 0
        for syn in pubchem_data.get('synonyms', []):
            if self._add_synonym(analyte_id, syn, source='pubchem_auto'):
                added += 1
        return added
    
    # ── Main resolve method ───────────────────────────────────────────
    
    def resolve(self, input_text: str) -> Tuple[Optional[MatchResult], Dict]:
        """
        Attempt to resolve a chemical name via PubChem lookup.
        
        Workflow:
        1. Check local cache → return cached result if available
        2. Query PubChem API by name
        3. Extract CAS numbers from PubChem synonyms
        4. Cross-reference CAS against local analyte database
        5. If match found + auto_add_synonyms:
           - Add the original lab name as a synonym
           - Optionally harvest additional PubChem synonyms
           - Return exact match (confidence 1.0)
        6. If no CAS match but PubChem found the compound:
           - Cache the PubChem data for review
           - Return None (requires human decision on which analyte to map to)
        
        Args:
            input_text: Chemical name as reported by the lab
            
        Returns:
            Tuple of (MatchResult or None, metadata dict)
            metadata always contains 'pubchem_status' key:
              - 'cache_hit_matched': cached, previously matched
              - 'cache_hit_unmatched': cached, previously looked up but no match
              - 'api_matched': fresh API call, CAS matched existing analyte
              - 'api_found_no_cas_match': PubChem found it but no CAS in our DB
              - 'api_not_found': PubChem doesn't know this name
              - 'skipped': name too short or clearly not a chemical
        """
        metadata = {}
        
        # Quick sanity check — skip very short or clearly non-chemical strings
        stripped = input_text.strip()
        if len(stripped) < 2:
            metadata['pubchem_status'] = 'skipped'
            return None, metadata
        
        cache_key = self._cache_key(input_text)
        
        # ── Check cache ────────────────────────────────────────────
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            metadata['pubchem_status'] = 'cache_hit'
            metadata['pubchem_data'] = cached
            
            # If previously matched, try to return the match
            if cached.get('matched_analyte_id'):
                analyte = self.db_session.query(Analyte).filter(
                    Analyte.analyte_id == cached['matched_analyte_id']
                ).first()
                if analyte:
                    metadata['pubchem_status'] = 'cache_hit_matched'
                    return MatchResult(
                        analyte_id=analyte.analyte_id,
                        preferred_name=analyte.preferred_name,
                        confidence=1.0,
                        method='exact',
                        score=1.0,
                        metadata={
                            'source': 'pubchem_cache',
                            'cas_number': cached.get('cas_numbers', [None])[0],
                        }
                    ), metadata
            
            metadata['pubchem_status'] = 'cache_hit_unmatched'
            return None, metadata
        
        # ── Query PubChem API ──────────────────────────────────────
        logger.info(f"PubChem lookup: '{input_text}'")
        pubchem_data = self._search_pubchem(input_text)
        
        if pubchem_data is None:
            # Not found on PubChem — cache the miss
            self._cache[cache_key] = {
                'query': input_text,
                'found': False,
                'queried_at': datetime.now().isoformat(),
            }
            self._save_cache()
            metadata['pubchem_status'] = 'api_not_found'
            return None, metadata
        
        # ── Cross-reference CAS ────────────────────────────────────
        cas_numbers = pubchem_data.get('cas_numbers', [])
        analyte = self._find_analyte_by_cas(cas_numbers) if cas_numbers else None
        
        if analyte:
            # CAS matched an existing analyte!
            metadata['pubchem_status'] = 'api_matched'
            metadata['matched_analyte'] = analyte.preferred_name
            metadata['cas_matched'] = analyte.cas_number
            metadata['pubchem_data'] = pubchem_data
            
            # Cache the successful match
            pubchem_data['matched_analyte_id'] = analyte.analyte_id
            pubchem_data['found'] = True
            self._cache[cache_key] = pubchem_data
            self._save_cache()
            
            if self.auto_add_synonyms:
                # Add the original lab name as a synonym
                self._add_synonym(analyte.analyte_id, input_text, source='pubchem_auto')
                
                # Harvest additional PubChem synonyms
                n_added = self._harvest_pubchem_synonyms(analyte.analyte_id, pubchem_data)
                metadata['synonyms_added'] = n_added + 1
                
                self.db_session.commit()
                logger.info(
                    f"PubChem resolved '{input_text}' → {analyte.preferred_name} "
                    f"via CAS {analyte.cas_number}, added {n_added + 1} synonyms"
                )
            
            return MatchResult(
                analyte_id=analyte.analyte_id,
                preferred_name=analyte.preferred_name,
                confidence=1.0,
                method='exact',
                score=1.0,
                metadata={
                    'source': 'pubchem_api',
                    'cas_number': analyte.cas_number,
                    'pubchem_cid': pubchem_data.get('cid'),
                    'synonyms_added': metadata.get('synonyms_added', 0),
                }
            ), metadata
        
        # ── PubChem found it but no CAS match in our DB ───────────
        pubchem_data['found'] = True
        pubchem_data['matched_analyte_id'] = None
        self._cache[cache_key] = pubchem_data
        self._save_cache()
        
        metadata['pubchem_status'] = 'api_found_no_cas_match'
        metadata['pubchem_data'] = pubchem_data
        logger.info(
            f"PubChem found '{input_text}' (CID={pubchem_data.get('cid')}, "
            f"CAS={cas_numbers}) but no matching analyte in local DB"
        )
        
        return None, metadata
