"""
Add Tin and Titanium to the database with PubChem synonym harvest.
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
    
    print('ADDING TIN AND TITANIUM WITH PUBCHEM HARVEST')
    print('=' * 80)
    
    # Define the metals
    metals = [
        {
            'analyte_id': 'WQ_IONS_012',
            'preferred_name': 'Tin',
            'cas_number': '7440-31-5',
            'analyte_type': 'SINGLE_SUBSTANCE',
            'molecular_formula': 'Sn'
        },
        {
            'analyte_id': 'WQ_IONS_013',
            'preferred_name': 'Titanium',
            'cas_number': '7440-32-6',
            'analyte_type': 'SINGLE_SUBSTANCE',
            'molecular_formula': 'Ti'
        }
    ]
    
    total_added = 0
    
    with db.get_session() as session:
        for metal in metals:
            # Add analyte
            analyte = Analyte(
                analyte_id=metal['analyte_id'],
                preferred_name=metal['preferred_name'],
                cas_number=metal['cas_number'],
                analyte_type=metal['analyte_type'],
                molecular_formula=metal['molecular_formula'],
                created_at=datetime.now()
            )
            session.add(analyte)
            
            name = metal['preferred_name']
            cas = metal['cas_number']
            print(f'\n{name} ({cas}):')
            
            # Add bootstrap synonyms
            bootstrap_syns = [
                name,
                f'{name} (Total)',
                f'{name} Total'
            ]
            
            for syn in bootstrap_syns:
                synonym = Synonym(
                    analyte_id=metal['analyte_id'],
                    synonym_raw=syn,
                    synonym_norm=normalizer.normalize(syn),
                    synonym_type='COMMON',
                    harvest_source='bootstrap',
                    confidence=1.0,
                    created_at=datetime.now()
                )
                session.add(synonym)
            
            print(f'  Bootstrap: {len(bootstrap_syns)} synonyms')
            
            # Get PubChem synonyms
            pubchem_syns = harvester.harvest_synonyms(cas, name)
            
            if pubchem_syns:
                added = 0
                bootstrap_norms = [normalizer.normalize(b) for b in bootstrap_syns]
                
                for syn in pubchem_syns:
                    norm = normalizer.normalize(syn)
                    if norm not in bootstrap_norms:
                        synonym = Synonym(
                            analyte_id=metal['analyte_id'],
                            synonym_raw=syn,
                            synonym_norm=norm,
                            synonym_type='COMMON',
                            harvest_source='pubchem',
                            confidence=0.9,
                            created_at=datetime.now()
                        )
                        session.add(synonym)
                        added += 1
                
                print(f'  PubChem: {added} new synonyms (from {len(pubchem_syns)} total)')
                total_added += added + len(bootstrap_syns)
            else:
                print(f'  PubChem: No synonyms found')
                total_added += len(bootstrap_syns)
        
        session.commit()
        print('\n' + '=' * 80)
        print(f'SUCCESS: Added 2 analytes with {total_added} total synonyms')

if __name__ == '__main__':
    main()
