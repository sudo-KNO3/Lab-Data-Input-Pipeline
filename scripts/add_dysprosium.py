"""
Add Dysprosium element with correct CAS number.
"""
import sys
sys.path.insert(0, '.')

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym
from src.bootstrap.api_harvesters import PubChemHarvester
from src.normalization.text_normalizer import TextNormalizer
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def main():
    db = DatabaseManager('data/reg153_matcher.db')
    harvester = PubChemHarvester()
    normalizer = TextNormalizer()
    
    print('ADDING DYSPROSIUM (MISSING ELEMENT)')
    print('=' * 80)
    
    symbol = 'Dy'
    name = 'Dysprosium'
    cas = '7429-91-6'  # Correct CAS number
    atomic_num = 66
    
    with db.get_session() as session:
        # Check if it exists
        existing = session.query(Analyte).filter(
            Analyte.cas_number == cas
        ).first()
        
        if existing:
            print(f'Dysprosium already exists: {existing.analyte_id}')
            return
        
        analyte_id = f'ELEMENT_{atomic_num:03d}'
        
        # Add analyte
        analyte = Analyte(
            analyte_id=analyte_id,
            preferred_name=name,
            cas_number=cas,
            analyte_type='SINGLE_SUBSTANCE',
            molecular_formula=symbol,
            created_at=datetime.now()
        )
        session.add(analyte)
        
        print(f'\n{symbol} {name} ({cas})')
        
        # Add bootstrap synonyms
        bootstrap_syns = [
            name,
            symbol,
            f'{name} (Total)',
            f'{name} Total',
        ]
        
        for syn in bootstrap_syns:
            synonym = Synonym(
                analyte_id=analyte_id,
                synonym_raw=syn,
                synonym_norm=normalizer.normalize(syn),
                synonym_type='COMMON',
                harvest_source='bootstrap',
                confidence=1.0,
                created_at=datetime.now()
            )
            session.add(synonym)
        
        print(f'  Bootstrap: {len(bootstrap_syns)} synonyms', end=' ... ')
        
        # Get PubChem synonyms
        try:
            pubchem_syns = harvester.harvest_synonyms(cas, name)
            
            if pubchem_syns:
                added = 0
                bootstrap_norms = [normalizer.normalize(b) for b in bootstrap_syns]
                
                for syn in pubchem_syns:
                    norm = normalizer.normalize(syn)
                    if norm not in bootstrap_norms:
                        synonym = Synonym(
                            analyte_id=analyte_id,
                            synonym_raw=syn,
                            synonym_norm=norm,
                            synonym_type='COMMON',
                            harvest_source='pubchem',
                            confidence=0.9,
                            created_at=datetime.now()
                        )
                        session.add(synonym)
                        added += 1
                
                total_syns = len(bootstrap_syns) + added
                print(f'{total_syns} total ({added} from PubChem)')
            else:
                print(f'{len(bootstrap_syns)} total (PubChem: none found)')
        except Exception as e:
            print(f'PubChem error: {str(e)[:50]}')
        
        session.commit()
        print('\n' + '=' * 80)
        print('✓ SUCCESS: Dysprosium added to database')

if __name__ == '__main__':
    main()
