"""
Parallel PubChem query execution system for exhaustive synonym harvesting.

This module provides concurrent querying capabilities to efficiently harvest
synonyms for chemical name variants from PubChem, respecting API rate limits.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set, Dict, Tuple, Optional
from loguru import logger
import time

from src.bootstrap.api_harvesters import PubChemHarvester
from src.database.models import Analyte


def query_pubchem_variant(
    harvester: PubChemHarvester, 
    variant: str, 
    cas_number: Optional[str] = None
) -> Tuple[str, List[str]]:
    """
    Query PubChem for synonyms of a single chemical name variant.
    
    Tries name lookup first, then falls back to CAS lookup if available.
    
    Args:
        harvester: PubChemHarvester instance for API queries
        variant: Chemical name variant to query
        cas_number: Optional CAS number for fallback lookup
        
    Returns:
        Tuple of (variant, synonyms_list). Returns empty list on failure.
    """
    try:
        # Try name lookup first
        logger.debug(f"Querying PubChem for variant: {variant}")
        synonyms = harvester._get_synonyms_by_name(variant)
        
        if synonyms:
            logger.debug(f"Found {len(synonyms)} synonyms for variant '{variant}'")
            return (variant, synonyms)
        
        # If no results and CAS exists, try CAS lookup
        if not synonyms and cas_number:
            logger.debug(f"No results for name, trying CAS lookup: {cas_number}")
            synonyms = harvester._get_synonyms_by_cas(cas_number)
            if synonyms:
                logger.debug(f"Found {len(synonyms)} synonyms via CAS for variant '{variant}'")
                return (variant, synonyms)
        
        logger.debug(f"No synonyms found for variant: {variant}")
        return (variant, [])
        
    except Exception as e:
        logger.debug(f"Error querying variant '{variant}': {e}")
        return (variant, [])


def harvest_synonyms_parallel(
    analyte: Analyte,
    variants: Set[str],
    harvester: PubChemHarvester,
    max_workers: int = 3
) -> Dict[str, List[str]]:
    """
    Query PubChem for synonyms of multiple variants in parallel.
    
    Uses ThreadPoolExecutor to query multiple variants concurrently while
    respecting PubChem's rate limits (5 req/sec).
    
    Args:
        analyte: Analyte record containing CAS number
        variants: Set of chemical name variants to query
        harvester: PubChemHarvester instance for API queries
        max_workers: Number of concurrent worker threads (default: 3)
        
    Returns:
        Dictionary mapping successful variants to their synonym lists.
        Failed queries are excluded from results.
    """
    results = {}
    cas_number = analyte.cas_number
    
    logger.info(f"Starting parallel harvest for {len(variants)} variants using {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all variant queries as futures
        future_to_variant = {
            executor.submit(query_pubchem_variant, harvester, variant, cas_number): variant
            for variant in variants
        }
        
        # Process results as they complete
        for future in as_completed(future_to_variant):
            variant, synonyms = future.result()
            
            if synonyms:
                results[variant] = synonyms
                logger.debug(f"Successfully harvested {len(synonyms)} synonyms for '{variant}'")
            
            # Rate limiting: sleep between processing results
            # At 0.2s per request with 3 workers = ~3 req/sec sustained
            time.sleep(0.2)
    
    logger.info(f"Parallel harvest complete: {len(results)}/{len(variants)} variants successful")
    return results


def deduplicate_synonyms(results: Dict[str, List[str]]) -> Set[str]:
    """
    Deduplicate synonyms from multiple variant query results.
    
    Collects all synonyms from all successful variant queries and returns
    a unique set.
    
    Args:
        results: Dictionary mapping variants to their synonym lists
        
    Returns:
        Set of unique synonyms across all variants
    """
    all_synonyms = set()
    
    for variant, synonyms in results.items():
        all_synonyms.update(synonyms)
    
    logger.debug(f"Deduplicated {len(all_synonyms)} unique synonyms from {len(results)} variant results")
    return all_synonyms
