"""
Synonym Validation Script

Validates harvested synonyms by querying PubChem to verify they actually
map to the correct chemical identity (CAS number match).

This catches false positives from exhaustive variant generation where
unrelated compounds might have been captured.
"""

import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, and_
from tqdm import tqdm

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym
from src.bootstrap.api_harvesters import PubChemHarvester


@dataclass
class ValidationResult:
    """Result of synonym validation."""
    synonym_id: int
    synonym_raw: str
    analyte_id: str
    analyte_name: str
    analyte_cas: Optional[str]
    pubchem_cas: Optional[str]
    is_valid: bool
    confidence: float
    notes: str


class SynonymValidator:
    """
    Validates synonyms against PubChem.
    
    For each synonym:
    1. Query PubChem for the synonym name
    2. Get CAS number returned by PubChem
    3. Compare to analyte's CAS number
    4. Flag mismatches for review
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize validator.
        
        Args:
            db_manager: Database manager
        """
        self.db_manager = db_manager
        self.harvester = PubChemHarvester()
        self.validation_stats = Counter()
    
    def validate_synonym(self, synonym: Synonym, analyte: Analyte) -> ValidationResult:
        """
        Validate single synonym against PubChem.
        
        Args:
            synonym: Synonym to validate
            analyte: Associated analyte
            
        Returns:
            ValidationResult
        """
        result = ValidationResult(
            synonym_id=synonym.id,
            synonym_raw=synonym.synonym_raw,
            analyte_id=analyte.analyte_id,
            analyte_name=analyte.preferred_name,
            analyte_cas=analyte.cas_number,
            pubchem_cas=None,
            is_valid=False,
            confidence=0.0,
            notes=""
        )
        
        # Skip if analyte has no CAS (can't validate)
        if not analyte.cas_number:
            result.is_valid = True  # Assume valid
            result.confidence = 0.5
            result.notes = "No analyte CAS - cannot validate"
            self.validation_stats['no_cas'] += 1
            return result
        
        try:
            # Query PubChem for this synonym (rate limiting handled by harvester)
            pubchem_cas = self.harvester.get_cas_number(synonym.synonym_raw)
            result.pubchem_cas = pubchem_cas
            
            # No result from PubChem
            if not pubchem_cas:
                result.is_valid = False
                result.confidence = 0.3
                result.notes = "PubChem returned no CAS"
                self.validation_stats['no_pubchem_result'] += 1
                return result
            
            # Compare CAS numbers
            if self._cas_match(analyte.cas_number, pubchem_cas):
                result.is_valid = True
                result.confidence = 1.0
                result.notes = "CAS match confirmed"
                self.validation_stats['valid'] += 1
            else:
                result.is_valid = False
                result.confidence = 0.0
                result.notes = f"CAS mismatch: {analyte.cas_number} != {pubchem_cas}"
                self.validation_stats['cas_mismatch'] += 1
        
        except Exception as e:
            result.is_valid = False
            result.confidence = 0.0
            result.notes = f"Error: {str(e)[:100]}"
            self.validation_stats['error'] += 1
        
        return result
    
    def _cas_match(self, cas1: str, cas2: str) -> bool:
        """Check if two CAS numbers match (normalize formatting)."""
        if not cas1 or not cas2:
            return False
        
        # Normalize: remove spaces, hyphens, leading zeros
        def normalize(cas):
            return cas.replace('-', '').replace(' ', '').lstrip('0')
        
        return normalize(cas1) == normalize(cas2)
    
    def validate_batch(self, 
                       harvest_source: str = 'pubchem_exhaustive',
                       limit: int = None,
                       analyte_id: str = None) -> List[ValidationResult]:
        """
        Validate batch of synonyms.
        
        Args:
            harvest_source: Only validate synonyms from this source
            limit: Maximum number to validate
            analyte_id: Only validate synonyms for specific analyte
            
        Returns:
            List of validation results
        """
        print(f"\nLoading synonyms from '{harvest_source}' into array...")
        print("=" * 80)
        
        with self.db_manager.session_scope() as session:
            # Build query
            query = (
                session.query(Synonym, Analyte)
                .join(Analyte, Synonym.analyte_id == Analyte.analyte_id)
                .filter(Synonym.harvest_source == harvest_source)
            )
            
            if analyte_id:
                query = query.filter(Synonym.analyte_id == analyte_id)
            
            if limit:
                query = query.limit(limit)
            
            # Load all synonyms into array
            synonym_array = query.all()
            
        print(f"[OK] Loaded {len(synonym_array):,} synonyms into array")
        print(f"  Memory usage: ~{len(synonym_array) * 0.5:.1f} KB")
        print(f"\nStarting validation of {len(synonym_array):,} synonyms...")
        print("=" * 80)
        
        # Process entire array
        results = []
        checkpoint_interval = 1000
        
        for idx, (synonym, analyte) in enumerate(tqdm(synonym_array, desc="Validating"), 1):
            result = self.validate_synonym(synonym, analyte)
            results.append(result)
            
            # Checkpoint progress every N items
            if idx % checkpoint_interval == 0:
                valid_count = sum(1 for r in results if r.is_valid)
                print(f"\nCheckpoint: {idx:,}/{len(synonym_array):,} completed - "
                      f"Valid: {valid_count:,} ({100*valid_count/idx:.1f}%)")
        
        print(f"\n[OK] Completed validation of {len(results):,} synonyms from array")
        return results
    
    def print_validation_report(self, results: List[ValidationResult]):
        """Print validation report."""
        print("\n" + "=" * 80)
        print("SYNONYM VALIDATION REPORT")
        print("=" * 80)
        
        # Summary statistics
        total = len(results)
        valid = sum(1 for r in results if r.is_valid)
        invalid = total - valid
        
        print(f"\nTotal Validated: {total:,}")
        print(f"Valid:           {valid:,} ({100*valid/total:.1f}%)")
        print(f"Invalid:         {invalid:,} ({100*invalid/total:.1f}%)")
        
        # Detailed breakdown
        print("\nValidation Details:")
        for key, count in self.validation_stats.most_common():
            print(f"  {key:25s}: {count:6,} ({100*count/total:5.1f}%)")
        
        # Show invalid synonyms
        invalid_results = [r for r in results if not r.is_valid]
        if invalid_results:
            print(f"\nInvalid Synonyms (showing first 20):")
            print("-" * 80)
            for result in invalid_results[:20]:
                print(f"\nSynonym: {result.synonym_raw}")
                print(f"  Analyte: {result.analyte_name} (CAS: {result.analyte_cas})")
                print(f"  PubChem CAS: {result.pubchem_cas}")
                print(f"  Issue: {result.notes}")
        
        # Save invalid to CSV for review
        if invalid_results:
            output_file = Path("data/validation/invalid_synonyms.csv")
            output_file.parent.mkdir(exist_ok=True)
            
            with output_file.open('w', encoding='utf-8') as f:
                f.write("synonym_id,synonym,analyte_id,analyte_name,analyte_cas,pubchem_cas,notes\n")
                for r in invalid_results:
                    f.write(f'{r.synonym_id},"{r.synonym_raw}",{r.analyte_id},"{r.analyte_name}",'
                           f'{r.analyte_cas},{r.pubchem_cas},"{r.notes}"\n')
            
            print(f"\n[OK] Invalid synonyms saved to: {output_file}")
    
    def mark_invalid_synonyms(self, results: List[ValidationResult], confidence_threshold: float = 0.5):
        """
        Mark invalid synonyms in database.
        
        Args:
            results: Validation results
            confidence_threshold: Mark synonyms below this confidence as invalid
        """
        with self.db_manager.session_scope() as session:
            marked = 0
            for result in results:
                if result.confidence < confidence_threshold:
                    synonym = session.query(Synonym).get(result.synonym_id)
                    if synonym:
                        # Lower confidence score
                        synonym.confidence = result.confidence
                        marked += 1
            
            session.commit()
            print(f"\n[OK] Marked {marked} synonyms with low confidence")


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description='Validate harvested synonyms against PubChem'
    )
    parser.add_argument(
        '--database',
        type=str,
        default='data/reg153_matcher.db',
        help='Path to database'
    )
    parser.add_argument(
        '--source',
        type=str,
        default='pubchem_exhaustive',
        help='Harvest source to validate'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of synonyms to validate (for testing)'
    )
    parser.add_argument(
        '--analyte-id',
        type=str,
        help='Validate synonyms for specific analyte only'
    )
    parser.add_argument(
        '--mark-invalid',
        action='store_true',
        help='Mark invalid synonyms in database with low confidence'
    )
    
    args = parser.parse_args()
    
    # Initialize
    db_manager = DatabaseManager(args.database)
    validator = SynonymValidator(db_manager)
    
    # Run validation
    results = validator.validate_batch(
        harvest_source=args.source,
        limit=args.limit,
        analyte_id=args.analyte_id
    )
    
    # Report
    validator.print_validation_report(results)
    
    # Mark invalid synonyms if requested
    if args.mark_invalid:
        print("\nMarking invalid synonyms in database...")
        validator.mark_invalid_synonyms(results)


if __name__ == '__main__':
    main()
