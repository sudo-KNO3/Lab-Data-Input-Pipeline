"""
Harvest synonyms for specific analytes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, SynonymType
from src.bootstrap.api_harvesters import PubChemHarvester


def harvest_for_analyte(analyte_id, harvester, db_manager):
    """Harvest synonyms for a specific analyte."""
    with db_manager.session_scope() as session:
        analyte = session.query(Analyte).filter_by(analyte_id=analyte_id).first()
        
        if not analyte:
            print(f"[SKIP] Analyte not found: {analyte_id}")
            return 0
        
        if not analyte.cas_number:
            print(f"[SKIP] {analyte.preferred_name} - No CAS number")
            return 0
        
        print(f"\n[HARVEST] {analyte.preferred_name}")
        print(f"  CAS: {analyte.cas_number}")
        
        # Get synonyms from PubChem
        synonyms = harvester.harvest_synonyms(
            cas_number=analyte.cas_number,
            chemical_name=analyte.preferred_name
        )
        
        if not synonyms:
            print(f"  No synonyms found")
            return 0
        
        print(f"  Found {len(synonyms)} synonyms")
        
        # Add to database
        added = 0
        for syn_text in synonyms:
            # Check if already exists
            existing = session.query(Synonym).filter_by(
                analyte_id=analyte_id,
                synonym_raw=syn_text,
                harvest_source='pubchem'
            ).first()
            
            if not existing:
                synonym = Synonym(
                    analyte_id=analyte_id,
                    synonym_raw=syn_text,
                    synonym_norm=syn_text.lower(),
                    synonym_type=SynonymType.COMMON,
                    harvest_source='pubchem',
                    confidence=1.0
                )
                session.add(synonym)
                added += 1
        
        session.commit()
        print(f"  Added {added} new synonyms")
        return added


if __name__ == '__main__':
    print("\n" + "="*80)
    print("TARGETED SYNONYM HARVESTING")
    print("="*80)
    
    analyte_ids = [
        'REG153_VOC_109_M',   # m-xylene
        'REG153_VOC_109_P',   # p-xylene  
        'REG153_VOC_047',     # cis-1,2-dichloroethylene
        'REG153_VOC_048',     # trans-1,2-dichloroethylene
    ]
    
    db = DatabaseManager()
    harvester = PubChemHarvester()
    
    total_added = 0
    for analyte_id in analyte_ids:
        added = harvest_for_analyte(analyte_id, harvester, db)
        total_added += added
    
    print("\n" + "="*80)
    print(f"COMPLETE - Added {total_added} total synonyms")
    print("="*80)
