"""
Interactive script to verify bootstrap names against PubChem.

Verifies that each bootstrap name (your internal display name) correctly maps
to the right PubChem compound by checking CAS numbers. Once verified, you can
optionally harvest all PubChem synonyms for that compound.

Workflow:
1. Shows your bootstrap name (preferred_name)
2. Queries PubChem to find the compound
3. Displays PubChem's CAS and compound name
4. You confirm if it's the correct match
5. If confirmed, CAS is verified/updated and you can harvest synonyms

Usage:
    python scripts/confirm_bootstrap_names.py
    python scripts/confirm_bootstrap_names.py --db path/to/database.db
    python scripts/confirm_bootstrap_names.py --analyte-type VOCS
    python scripts/confirm_bootstrap_names.py --start-from REG153_VOCS_010
    python scripts/confirm_bootstrap_names.py --auto-harvest
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import and_
from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym
from src.bootstrap.api_harvesters import PubChemHarvester
from src.normalization.text_normalizer import TextNormalizer


def verify_analyte_with_pubchem(analyte: Analyte, harvester: PubChemHarvester) -> Tuple[Optional[str], Optional[str]]:
    """
    Verify an analyte's bootstrap name against PubChem.
    
    Args:
        analyte: Analyte object
        harvester: PubChem harvester instance
        
    Returns:
        (pubchem_cas, pubchem_name) tuple or (None, None) if not found
    """
    bootstrap_name = analyte.preferred_name
    
    # Query PubChem for this name
    pubchem_cas = harvester.get_cas_number(bootstrap_name)
    if not pubchem_cas:
        return None, None
    
    # Get PubChem's preferred name for this compound
    pubchem_name = harvester.get_preferred_name(pubchem_cas, use_cas=True)
    
    return pubchem_cas, pubchem_name


def confirm_names_interactive(db_path: str, analyte_type: str = None, start_from: str = None, auto_harvest: bool = False) -> dict:
    """
    Interactive verification of bootstrap names against PubChem.
    
    For each analyte:
    1. Show bootstrap name (preferred_name)
    2. Query PubChem for this name
    3. Display PubChem's CAS and compound name
    4. Prompt user to confirm if it's the correct match
    5. If confirmed, verify/update CAS and optionally harvest synonyms
    
    Args:
        db_path: Path to database
        analyte_type: Filter by analyte type (e.g., 'VOCS', 'METALS')
        start_from: Start from specific analyte_id (resume feature)
        auto_harvest: Automatically harvest PubChem synonyms after confirmation
        
    Returns:
        Statistics dictionary
    """
    db_manager = DatabaseManager(db_path=db_path)
    harvester = PubChemHarvester()
    normalizer = TextNormalizer()
    
    stats = {
        'total_analytes': 0,
        'reviewed': 0,
        'confirmed': 0,
        'cas_updated': 0,
        'cas_conflicts': 0,
        'skipped': 0,
        'not_found': 0,
        'synonyms_harvested': 0
    }
    
    print(f"\n{'='*80}")
    print(f"Bootstrap Name Verification")
    print(f"{'='*80}")
    print(f"Database: {db_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print("Instructions:")
    print("  For each analyte, verify it maps to the correct PubChem compound:")
    print("  [y] - Confirm this is the correct compound")
    print("  [n] - Not the correct compound (skip for now)")
    print("  [m] - Manually enter correct PubChem name or CAS number")
    print("  [s] - Skip without checking")
    print("  [q] - Quit and save progress")
    print(f"\n{'='*80}\n")
    
    try:
        with db_manager.get_session() as session:
            # Build query
            query = session.query(Analyte)
            
            if analyte_type:
                query = query.filter(Analyte.analyte_id.like(f'REG153_{analyte_type}%'))
            
            if start_from:
                query = query.filter(Analyte.analyte_id >= start_from)
            
            query = query.order_by(Analyte.analyte_id)
            
            analytes = query.all()
            stats['total_analytes'] = len(analytes)
            
            print(f"Found {stats['total_analytes']} analytes to verify\n")
            
            for i, analyte in enumerate(analytes, 1):
                stats['reviewed'] += 1
                
                bootstrap_name = analyte.preferred_name
                current_cas = analyte.cas_number
                
                # Display analyte info
                print(f"[{i}/{stats['total_analytes']}] {analyte.analyte_id}")
                print(f"  Bootstrap Name: {bootstrap_name}")
                if current_cas:
                    print(f"  Current CAS:    {current_cas}")
                print()
                
                # Query PubChem
                print(f"  Querying PubChem for '{bootstrap_name}'...")
                pubchem_cas, pubchem_name = verify_analyte_with_pubchem(analyte, harvester)
                
                if not pubchem_cas:
                    stats['not_found'] += 1
                    print(f"  [!] PubChem: No compound found")
                    print(f"  -> Consider manual entry\n")
                    
                    choice = input(f"  [m]anual entry / [s]kip / [q]uit? ").strip().lower()
                    if choice == 'q':
                        print("\n[OK] Quitting and saving progress...")
                        session.commit()
                        return stats
                    elif choice == 'm':
                        entry_type = input(f"  Enter [n]ame or [c]as? ").strip().lower()
                        if entry_type == 'n':
                            manual_name = input(f"  Enter PubChem compound name: ").strip()
                            if manual_name:
                                print(f"  -> Querying PubChem for '{manual_name}'...")
                                manual_cas = harvester.get_cas_number(manual_name)
                                if manual_cas:
                                    manual_pubchem_name = harvester.get_preferred_name(manual_cas, use_cas=True)
                                    print(f"  [OK] Found:")
                                    print(f"      CAS:  {manual_cas}")
                                    print(f"      Name: {manual_pubchem_name}")
                                    confirm = input(f"  Use this compound? [y/n]: ").strip().lower()
                                    if confirm == 'y':
                                        analyte.cas_number = manual_cas
                                        stats['cas_updated'] += 1
                                        stats['confirmed'] += 1
                                        print(f"  [OK] CAS updated to: {manual_cas}\n")
                                        
                                        # Optionally harvest
                                        if auto_harvest:
                                            print(f"  -> Harvesting synonyms...")
                                            synonyms = harvester.harvest_synonyms(manual_cas, bootstrap_name)
                                            added = 0
                                            for syn_raw in synonyms:
                                                syn_norm = normalizer.normalize(syn_raw)
                                                existing = session.query(Synonym).filter(
                                                    and_(
                                                        Synonym.analyte_id == analyte.analyte_id,
                                                        Synonym.synonym_norm == syn_norm
                                                    )
                                                ).first()
                                                if not existing:
                                                    synonym = Synonym(
                                                        analyte_id=analyte.analyte_id,
                                                        synonym_raw=syn_raw,
                                                        synonym_norm=syn_norm,
                                                        synonym_type='COMMON',
                                                        harvest_source='pubchem',
                                                        confidence=0.9
                                                    )
                                                    session.add(synonym)
                                                    added += 1
                                            print(f"  [OK] Added {added} synonyms\n")
                                            stats['synonyms_harvested'] += added
                                    else:
                                        print(f"  -> Skipped\n")
                                else:
                                    print(f"  [!] PubChem lookup failed for that name\n")
                        elif entry_type == 'c':
                            manual_cas = input(f"  Enter CAS number: ").strip()
                            if manual_cas:
                                analyte.cas_number = manual_cas
                                stats['cas_updated'] += 1
                                print(f"  [OK] CAS updated to: {manual_cas}\n")
                        continue
                    else:
                        stats['skipped'] += 1
                        print()
                        continue
                
                # Show PubChem result
                print(f"  [OK] PubChem Found:")
                print(f"      CAS:  {pubchem_cas}")
                print(f"      Name: {pubchem_name}")
                
                # Check for CAS conflict
                if current_cas and current_cas != pubchem_cas:
                    stats['cas_conflicts'] += 1
                    print(f"  [!] CAS MISMATCH: Database has {current_cas}, PubChem has {pubchem_cas}")
                
                print()
                
                # Get confirmation
                while True:
                    choice = input(f"  Confirm match? [y/n/m/s/q]: ").strip().lower()
                    
                    if choice == 'q':
                        print("\n[OK] Quitting and saving progress...")
                        session.commit()
                        return stats
                    
                    if choice == 's':
                        stats['skipped'] += 1
                        print()
                        break
                    
                    if choice == 'n':
                        print(f"  -> Skipped (not the right compound)\n")
                        stats['skipped'] += 1
                        break
                    
                    if choice == 'm':
                        entry_type = input(f"  Enter [n]ame or [c]as? ").strip().lower()
                        if entry_type == 'n':
                            manual_name = input(f"  Enter PubChem compound name: ").strip()
                            if manual_name:
                                print(f"  -> Querying PubChem for '{manual_name}'...")
                                manual_cas = harvester.get_cas_number(manual_name)
                                if manual_cas:
                                    manual_pubchem_name = harvester.get_preferred_name(manual_cas, use_cas=True)
                                    print(f"  [OK] Found:")
                                    print(f"      CAS:  {manual_cas}")
                                    print(f"      Name: {manual_pubchem_name}")
                                    confirm = input(f"  Use this compound? [y/n]: ").strip().lower()
                                    if confirm == 'y':
                                        analyte.cas_number = manual_cas
                                        stats['cas_updated'] += 1
                                        stats['confirmed'] += 1
                                        print(f"  [OK] CAS updated to: {manual_cas}")
                                        
                                        # Optionally harvest
                                        if auto_harvest:
                                            print(f"  -> Harvesting synonyms...")
                                            synonyms = harvester.harvest_synonyms(manual_cas, bootstrap_name)
                                            added = 0
                                            for syn_raw in synonyms:
                                                syn_norm = normalizer.normalize(syn_raw)
                                                existing = session.query(Synonym).filter(
                                                    and_(
                                                        Synonym.analyte_id == analyte.analyte_id,
                                                        Synonym.synonym_norm == syn_norm
                                                    )
                                                ).first()
                                                if not existing:
                                                    synonym = Synonym(
                                                        analyte_id=analyte.analyte_id,
                                                        synonym_raw=syn_raw,
                                                        synonym_norm=syn_norm,
                                                        synonym_type='COMMON',
                                                        harvest_source='pubchem',
                                                        confidence=0.9
                                                    )
                                                    session.add(synonym)
                                                    added += 1
                                            print(f"  [OK] Added {added} synonyms")
                                            stats['synonyms_harvested'] += added
                                        print()
                                    else:
                                        print(f"  -> Skipped")
                                else:
                                    print(f"  [!] PubChem lookup failed for that name")
                        elif entry_type == 'c':
                            manual_cas = input(f"  Enter correct CAS number: ").strip()
                            if manual_cas:
                                analyte.cas_number = manual_cas
                                stats['cas_updated'] += 1
                                stats['confirmed'] += 1
                                print(f"  [OK] CAS updated to: {manual_cas}\n")
                        break
                    
                    if choice == 'y':
                        # Update CAS if needed
                        if not current_cas or current_cas != pubchem_cas:
                            analyte.cas_number = pubchem_cas
                            stats['cas_updated'] += 1
                            print(f"  [OK] CAS updated to: {pubchem_cas}")
                        
                        stats['confirmed'] += 1
                        
                        # Optionally harvest synonyms
                        if auto_harvest:
                            print(f"  -> Harvesting synonyms from PubChem...")
                            synonyms = harvester.harvest_synonyms(pubchem_cas, bootstrap_name)
                            
                            # Add to database
                            added = 0
                            for syn_raw in synonyms:
                                syn_norm = normalizer.normalize(syn_raw)
                                
                                # Check if exists
                                existing = session.query(Synonym).filter(
                                    and_(
                                        Synonym.analyte_id == analyte.analyte_id,
                                        Synonym.synonym_norm == syn_norm
                                    )
                                ).first()
                                
                                if not existing:
                                    synonym = Synonym(
                                        analyte_id=analyte.analyte_id,
                                        synonym_raw=syn_raw,
                                        synonym_norm=syn_norm,
                                        synonym_type='COMMON',
                                        harvest_source='pubchem',
                                        confidence=0.9
                                    )
                                    session.add(synonym)
                                    added += 1
                            
                            print(f"  [OK] Added {added} synonyms")
                            stats['synonyms_harvested'] += added
                        
                        print()
                        break
                    
                    print(f"  Invalid choice. Please enter y, n, m, s, or q.")
                
                # Commit periodically
                if i % 10 == 0:
                    session.commit()
                    print(f"--- Progress saved (reviewed {i}/{stats['total_analytes']}) ---\n")
            
            # Final commit
            session.commit()
            
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user. Progress has been saved.")
        return stats
    
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return stats
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Verification Complete")
    print(f"{'='*80}")
    print(f"Total analytes:       {stats['total_analytes']}")
    print(f"Reviewed:             {stats['reviewed']}")
    print(f"Confirmed:            {stats['confirmed']}")
    print(f"CAS updated:          {stats['cas_updated']}")
    print(f"CAS conflicts:        {stats['cas_conflicts']}")
    print(f"Skipped:              {stats['skipped']}")
    print(f"Not found in PubChem: {stats['not_found']}")
    if auto_harvest:
        print(f"Synonyms harvested:   {stats['synonyms_harvested']}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Verify bootstrap names against PubChem compounds'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='data/reg153_matcher.db',
        help='Path to the database file (default: data/reg153_matcher.db)'
    )
    parser.add_argument(
        '--analyte-type',
        type=str,
        help='Filter by analyte type (e.g., VOCS, METALS, PHCS)'
    )
    parser.add_argument(
        '--start-from',
        type=str,
        help='Start from specific analyte_id (for resuming)'
    )
    parser.add_argument(
        '--auto-harvest',
        action='store_true',
        help='Automatically harvest PubChem synonyms after confirmation'
    )
    
    args = parser.parse_args()
    
    # Verify database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: Database file not found: {args.db}")
        print("Please run setup_database.py first to create the database.")
        sys.exit(1)
    
    # Run verification
    stats = confirm_names_interactive(
        args.db,
        analyte_type=args.analyte_type,
        start_from=args.start_from,
        auto_harvest=args.auto_harvest
    )
    
    # Exit
    print("[OK] Verification complete!")
    sys.exit(0)


if __name__ == '__main__':
    main()
