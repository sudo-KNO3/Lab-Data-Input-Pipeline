"""
Verify problematic CAS numbers by querying PubChem.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bootstrap.api_harvesters import PubChemHarvester


def check_cas_number(cas, description):
    """Query PubChem for a CAS number and report what it finds."""
    print(f"\n{'='*80}")
    print(f"Testing: {description}")
    print(f"CAS Number: {cas}")
    print("="*80)
    
    harvester = PubChemHarvester()
    
    try:
        # Get what PubChem thinks this CAS is
        result_cas = harvester.get_cas_number(cas)
        print(f"PubChem returns CAS: {result_cas}")
        
        # Try to get synonyms
        synonyms = harvester.harvest_synonyms(cas_number=cas, chemical_name=None)
        
        if synonyms:
            print(f"Found {len(synonyms)} synonyms:")
            for i, syn in enumerate(synonyms[:15], 1):
                print(f"  {i:2d}. {syn}")
            if len(synonyms) > 15:
                print(f"  ... and {len(synonyms)-15} more")
        else:
            print("No synonyms found")
            
    except Exception as e:
        print(f"ERROR: {e}")


def check_chemical_name(name, description):
    """Query PubChem for a chemical name."""
    print(f"\n{'='*80}")
    print(f"Testing: {description}")
    print(f"Chemical Name: {name}")
    print("="*80)
    
    harvester = PubChemHarvester()
    
    try:
        # Get CAS from name
        result_cas = harvester.get_cas_number(name)
        print(f"PubChem returns CAS: {result_cas}")
        
        # Get synonyms
        synonyms = harvester.harvest_synonyms(cas_number=None, chemical_name=name)
        
        if synonyms:
            print(f"Found {len(synonyms)} synonyms:")
            for i, syn in enumerate(synonyms[:15], 1):
                print(f"  {i:2d}. {syn}")
            if len(synonyms) > 15:
                print(f"  ... and {len(synonyms)-15} more")
        else:
            print("No synonyms found")
            
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == '__main__':
    print("\n" + "="*80)
    print("PUBCHEM CAS NUMBER VERIFICATION")
    print("="*80)
    
    # Check the problematic CAS numbers
    check_cas_number("7429-90-5", "Mysterious CAS (appears for Boron AND Chromium in database)")
    check_cas_number("7440-42-8", "Boron (correct)")
    check_cas_number("7440-47-3", "Chromium (correct)")
    check_cas_number("26264-54-0", "Hexachlorocyclohexane gamma-")
    
    # Check xylene isomers
    check_chemical_name("o-xylene", "Ortho-xylene (1,2-dimethylbenzene)")
    check_chemical_name("m-xylene", "Meta-xylene (1,3-dimethylbenzene)")
    check_chemical_name("p-xylene", "Para-xylene (1,4-dimethylbenzene)")
    check_chemical_name("xylene", "Xylene (unspecified)")
    
    # Check dichloroethylene isomers
    check_chemical_name("1,1-dichloroethylene", "1,1-Dichloroethylene")
    check_chemical_name("cis-1,2-dichloroethylene", "cis-1,2-Dichloroethylene")
    check_chemical_name("trans-1,2-dichloroethylene", "trans-1,2-Dichloroethylene")
    check_cas_number("156-59-2", "cis-1,2-Dichloroethylene CAS")
    check_cas_number("156-60-5", "trans-1,2-Dichloroethylene CAS")
    
    print("\n" + "="*80)
    print("VERIFICATION COMPLETE")
    print("="*80)
