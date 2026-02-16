"""
Fix incorrect CAS numbers for Boron (total) and Chromium (total).

These analytes have wrong CAS numbers which caused false positive validation failures.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym

# Correct CAS numbers
CAS_CORRECTIONS = {
    'Boron (total)': '7440-42-8',      # Was incorrectly 7429-90-5
    'Chromium (total)': '7440-47-3',   # Was incorrectly 7429-90-5  
}

def main():
    db = DatabaseManager('data/reg153_matcher.db')
    
    with db.session_scope() as session:
        print('='*80)
        print('CORRECTING CAS NUMBERS')
        print('='*80)
        print()
        
        for analyte_name, correct_cas in CAS_CORRECTIONS.items():
            analyte = session.query(Analyte).filter(
                Analyte.preferred_name == analyte_name
            ).first()
            
            if analyte:
                old_cas = analyte.cas_number
                analyte.cas_number = correct_cas
                
                print(f'{analyte_name}:')
                print(f'  Old CAS: {old_cas}')
                print(f'  New CAS: {correct_cas}')
                print(f'  ✓ Updated')
                print()
        
        session.commit()
        
        print('='*80)
        print('CAS corrections saved to database')
        print()
        print('NEXT STEP:')
        print('  Re-analyze validation results with corrected CAS numbers.')
        print('  Many "CAS mismatch" synonyms will now be valid.')
        print('='*80)

if __name__ == '__main__':
    main()
