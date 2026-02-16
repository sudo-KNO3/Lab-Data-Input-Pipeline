"""
Harvest chemical synonyms from PubChem REST API.

This script queries PubChem for synonyms of all analytes in the database.
For analytes with CAS numbers, it queries by CAS. For single substances
without CAS numbers, it queries by chemical name.

PubChem API: https://pubchem.ncbi.nlm.nih.gov/rest/pug/
Rate limit: 5 requests per second (0.2s delay)

Usage:
    python scripts/harvest_pubchem_synonyms.py
"""
import sys
import time
from pathlib import Path
from typing import List, Set, Dict, Optional
import requests
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, SynonymType
from src.normalization.text_normalizer import TextNormalizer
from sqlalchemy import select


class PubChemHarvester:
    """Harvest synonyms from PubChem REST API."""
    
    BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    RATE_LIMIT_DELAY = 0.2  # 5 requests per second
    MAX_SYNONYMS = 50  # Limit to top 50 synonyms per compound
    MAX_SYNONYM_LENGTH = 120  # Filter out very long synonyms
    
    def __init__(self):
        """Initialize the harvester."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Reg153ChemicalMatcher/1.0 (research project)'
        })
        self.normalizer = TextNormalizer()
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _clean_synonym(self, synonym: str) -> Optional[str]:
        """
        Clean and validate a synonym.
        
        Args:
            synonym: Raw synonym text
            
        Returns:
            Cleaned synonym or None if invalid
        """
        if not synonym or not isinstance(synonym, str):
            return None
        
        # Strip whitespace
        synonym = synonym.strip()
        
        # Filter out empty or too long
        if not synonym or len(synonym) > self.MAX_SYNONYM_LENGTH:
            return None
        
        # Filter out synonyms that are just CAS numbers
        if synonym.replace('-', '').replace(' ', '').isdigit():
            return None
        
        # Filter out synonyms with mostly special characters
        alpha_count = sum(c.isalpha() for c in synonym)
        if alpha_count < len(synonym) * 0.3:  # Must be at least 30% letters
            return None
        
        return synonym
    
    def fetch_synonyms(self, identifier: str, identifier_type: str = "name") -> tuple[List[str], bool, Optional[str]]:
        """
        Fetch synonyms for a chemical from PubChem.
        
        Args:
            identifier: CAS number or chemical name
            identifier_type: Either "cas" or "name"
            
        Returns:
            Tuple of (synonyms_list, success, error_message)
        """
        # Rate limit
        self._rate_limit()
        
        # Build URL - use name endpoint
        url = f"{self.BASE_URL}/compound/name/{identifier}/synonyms/JSON"
        
        try:
            response = self.session.get(url, timeout=10)
            
            # Handle 404 (compound not found)
            if response.status_code == 404:
                return [], True, "Not found in PubChem"
            
            # Raise for other errors
            response.raise_for_status()
            
            # Parse JSON
            data = response.json()
            
            # Extract synonyms from response
            if 'InformationList' in data and 'Information' in data['InformationList']:
                info = data['InformationList']['Information']
                if info and len(info) > 0 and 'Synonym' in info[0]:
                    raw_synonyms = info[0]['Synonym']
                    
                    # Clean and filter synonyms
                    clean_synonyms = []
                    for syn in raw_synonyms[:self.MAX_SYNONYMS]:
                        cleaned = self._clean_synonym(syn)
                        if cleaned:
                            clean_synonyms.append(cleaned)
                    
                    return clean_synonyms, True, None
            
            return [], True, "No synonyms in response"
            
        except requests.Timeout:
            return [], False, "Request timeout"
        except requests.RequestException as e:
            return [], False, f"Request failed: {str(e)}"
        except Exception as e:
            return [], False, f"Unexpected error: {str(e)}"
    
    def fetch_with_retry(self, identifier: str, identifier_type: str = "name", max_retries: int = 1) -> tuple[List[str], bool, Optional[str]]:
        """
        Fetch synonyms with retry logic.
        
        Args:
            identifier: CAS number or chemical name
            identifier_type: Either "cas" or "name"
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple of (synonyms_list, success, error_message)
        """
        for attempt in range(max_retries + 1):
            synonyms, success, error = self.fetch_synonyms(identifier, identifier_type)
            
            if success or attempt == max_retries:
                return synonyms, success, error
            
            # Exponential backoff
            time.sleep(1 * (2 ** attempt))
        
        return [], False, "Max retries exceeded"


def get_existing_synonyms(session, analyte_id: str, source: str) -> Set[str]:
    """
    Get existing normalized synonyms for an analyte from a specific source.
    
    Args:
        session: Database session
        analyte_id: Analyte ID
        source: Harvest source name
        
    Returns:
        Set of normalized synonym texts
    """
    query = select(Synonym.synonym_norm).where(
        Synonym.analyte_id == analyte_id,
        Synonym.harvest_source == source,
    )
    results = session.execute(query).scalars().all()
    return set(results)


def process_analytes(db_path: str = "data/reg153_matcher.db"):
    """
    Main processing function.
    
    Args:
        db_path: Path to SQLite database
    """
    print("=" * 70)
    print("PubChem Synonym Harvester")
    print("=" * 70)
    print()
    
    # Initialize database manager
    db = DatabaseManager(db_path)
    print(f"✓ Connected to database: {db_path}")
    
    # Initialize harvester
    harvester = PubChemHarvester()
    normalizer = TextNormalizer()
    print(f"✓ Initialized PubChem harvester")
    print(f"  - Rate limit: 5 requests/second")
    print(f"  - Max synonyms per compound: {harvester.MAX_SYNONYMS}")
    print(f"  - Max synonym length: {harvester.MAX_SYNONYM_LENGTH} chars")
    print()
    
    # Load analytes - prefer those with CAS, but also include those without
    with db.session_scope() as session:
        # First try analytes with CAS numbers
        query_with_cas = select(Analyte).where(Analyte.cas_number.isnot(None))
        analytes_with_cas = session.execute(query_with_cas).scalars().all()
        
        # Also get single substances without CAS (we can query by name)
        from src.database.models import AnalyteType
        query_by_name = select(Analyte).where(
            Analyte.cas_number.is_(None),
            Analyte.analyte_type == AnalyteType.SINGLE_SUBSTANCE
        )
        analytes_by_name = session.execute(query_by_name).scalars().all()
        
        analytes = list(analytes_with_cas) + list(analytes_by_name)
        
        cas_count = len(analytes_with_cas)
        name_count = len(analytes_by_name)
        
        print(f"✓ Found {len(analytes)} analytes to process:")
        print(f"  - {cas_count} with CAS numbers (will query by CAS)")
        print(f"  - {name_count} single substances without CAS (will query by name)")
        print()
    
    if not analytes:
        print("No analytes to process. Exiting.")
        return
    
    # Statistics
    stats = {
        'total_analytes': len(analytes),
        'processed': 0,
        'success': 0,
        'not_found': 0,
        'errors': 0,
        'total_synonyms_fetched': 0,
        'total_synonyms_inserted': 0,
        'total_duplicates': 0,
    }
    
    # Process each analyte
    print("Processing analytes...")
    print()
    
    for analyte in tqdm(analytes, desc="Harvesting", unit="analyte"):
        stats['processed'] += 1
        
        with db.session_scope() as session:
            # Get existing synonyms from PubChem
            existing = get_existing_synonyms(session, analyte.analyte_id, 'pubchem')
            
            # Determine identifier to use (CAS or name)
            if analyte.cas_number:
                identifier = analyte.cas_number
                identifier_type = "cas"
                identifier_display = analyte.cas_number
            else:
                identifier = analyte.preferred_name
                identifier_type = "name"
                identifier_display = analyte.preferred_name[:30]
            
            # Fetch synonyms from PubChem
            synonyms, success, error = harvester.fetch_with_retry(identifier, identifier_type)
            
            if not success:
                stats['errors'] += 1
                tqdm.write(f"✗ {identifier_display:30} ({analyte.preferred_name[:40]:40}): {error}")
                continue
            
            if not synonyms:
                stats['not_found'] += 1
                continue
            
            stats['success'] += 1
            stats['total_synonyms_fetched'] += len(synonyms)
            
            # Process and insert synonyms
            new_count = 0
            duplicate_count = 0
            
            for synonym_raw in synonyms:
                # Normalize
                synonym_norm = normalizer.normalize(synonym_raw)
                
                # Skip if empty after normalization
                if not synonym_norm:
                    continue
                
                # Skip if already exists
                if synonym_norm in existing:
                    duplicate_count += 1
                    continue
                
                # Insert new synonym
                synonym = Synonym(
                    analyte_id=analyte.analyte_id,
                    synonym_raw=synonym_raw,
                    synonym_norm=synonym_norm,
                    synonym_type=SynonymType.COMMON,
                    harvest_source='pubchem',
                    confidence=0.90,
                )
                session.add(synonym)
                existing.add(synonym_norm)
                new_count += 1
            
            session.commit()
            
            stats['total_synonyms_inserted'] += new_count
            stats['total_duplicates'] += duplicate_count
            
            if new_count > 0:
                tqdm.write(
                    f"✓ {identifier_display:30} ({analyte.preferred_name[:40]:40}): "
                    f"{len(synonyms)} fetched, {new_count} new, {duplicate_count} duplicates"
                )
    
    # Print final statistics
    print()
    print("=" * 70)
    print("HARVEST COMPLETE")
    print("=" * 70)
    print()
    print(f"Total analytes:              {stats['total_analytes']:6,}")
    print(f"Processed:                   {stats['processed']:6,}")
    print(f"Successful:                  {stats['success']:6,}")
    print(f"Not found in PubChem:        {stats['not_found']:6,}")
    print(f"Errors:                      {stats['errors']:6,}")
    print()
    print(f"Total synonyms fetched:      {stats['total_synonyms_fetched']:6,}")
    print(f"New synonyms inserted:       {stats['total_synonyms_inserted']:6,}")
    print(f"Duplicates skipped:          {stats['total_duplicates']:6,}")
    print()
    
    if stats['success'] > 0:
        avg_synonyms = stats['total_synonyms_inserted'] / stats['success']
        print(f"Average new synonyms/analyte: {avg_synonyms:.1f}")
    
    print()
    print("✓ Synonym database expanded successfully!")
    print()


if __name__ == "__main__":
    try:
        process_analytes()
    except KeyboardInterrupt:
        print("\n\nHarvesting interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
