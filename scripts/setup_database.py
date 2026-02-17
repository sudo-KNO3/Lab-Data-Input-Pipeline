"""
Database setup and initialization script for Reg 153 Chemical Matcher.

This script:
1. Initializes the SQLite database
2. Creates all tables and indexes
3. Loads reg153_master.csv into the analytes table
4. Provides verification statistics

Usage:
    python scripts/setup_database.py
"""

import sys
import csv
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseManager
from src.database.crud_new import (
    insert_analyte,
    insert_snapshot_registry,
    get_database_statistics,
)
from src.database.models import Base, AnalyteType
from src.database.models import Base, AnalyteType


# Configuration
DEFAULT_DB_PATH = "data/reg153_matcher.db"
DEFAULT_CSV_PATH = "data/processed/canonical/reg153_master.csv"


def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()


def parse_analyte_type(type_str: str) -> AnalyteType:
    """
    Parse analyte_type string from CSV to enum.
    
    Args:
        type_str: String like 'single_substance', 'fraction_or_group', etc.
    
    Returns:
        AnalyteType enum
    """
    type_map = {
        'single_substance': AnalyteType.SINGLE_SUBSTANCE,
        'fraction_or_group': AnalyteType.FRACTION_OR_GROUP,
        'suite': AnalyteType.SUITE,
        'parameter': AnalyteType.PARAMETER,
    }
    
    return type_map.get(type_str.lower(), AnalyteType.SINGLE_SUBSTANCE)


def load_reg153_master(db_manager: DatabaseManager, csv_path: str) -> int:
    """
    Load reg153_master.csv into the analytes table.
    
    Expected CSV columns:
    - Analyte_ID (REG153_XXX format)
    - Preferred_Name
    - Analyte_Type (single_substance, fraction_or_group, suite, parameter)
    - CAS_Number (optional)
    - Group_Code (optional, e.g., PHC_F1, BTEX)
    - Table_Number (optional, 1-9)
    - Chemical_Group (optional, e.g., Metals, VOCs)
    - SMILES (optional)
    - InChI_Key (optional)
    - Molecular_Formula (optional)
    
    Args:
        db_manager: Database manager instance
        csv_path: Path to reg153_master.csv
    
    Returns:
        Number of analytes loaded
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"\n{'='*70}")
    print(f"Loading analytes from: {csv_path}")
    print(f"{'='*70}\n")
    
    analytes_loaded = 0
    errors = []
    
    with db_manager.session_scope() as session:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (accounting for header)
                try:
                    # Required fields
                    analyte_id = row.get('Analyte_ID', '').strip()
                    preferred_name = row.get('Preferred_Name', '').strip()
                    analyte_type_str = row.get('Analyte_Type', 'single_substance').strip()
                    
                    if not analyte_id or not preferred_name:
                        errors.append(f"Row {row_num}: Missing Analyte_ID or Preferred_Name")
                        continue
                    
                    # Optional fields
                    cas_number = row.get('CAS_Number', '').strip() or None
                    group_code = row.get('Group_Code', '').strip() or None
                    chemical_group = row.get('Chemical_Group', '').strip() or None
                    smiles = row.get('SMILES', '').strip() or None
                    inchi_key = row.get('InChI_Key', '').strip() or None
                    molecular_formula = row.get('Molecular_Formula', '').strip() or None
                    parent_analyte_id = row.get('Parent_Analyte_ID', '').strip() or None
                    
                    # Parse table number
                    table_number = None
                    table_num_str = row.get('Table_Number', '').strip()
                    if table_num_str:
                        try:
                            table_number = int(table_num_str)
                        except ValueError:
                            pass
                    
                    # Parse analyte type
                    analyte_type = parse_analyte_type(analyte_type_str)
                    
                    # Insert analyte
                    insert_analyte(
                        session=session,
                        analyte_id=analyte_id,
                        preferred_name=preferred_name,
                        analyte_type=analyte_type,
                        cas_number=cas_number,
                        group_code=group_code,
                        table_number=table_number,
                        chemical_group=chemical_group,
                        smiles=smiles,
                        inchi_key=inchi_key,
                        molecular_formula=molecular_formula,
                        parent_analyte_id=parent_analyte_id,
                    )
                    
                    analytes_loaded += 1
                    
                    if analytes_loaded % 20 == 0:
                        print(f"  Loaded {analytes_loaded} analytes...", end='\r')
                
                except Exception as e:
                    errors.append(f"Row {row_num}: {str(e)}")
                    continue
    
    print(f"\n  ✓ Successfully loaded {analytes_loaded} analytes")
    
    if errors:
        print(f"\n  ⚠ {len(errors)} errors encountered:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"    - {error}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more errors")
    
    return analytes_loaded


def create_snapshot_entry(db_manager: DatabaseManager, csv_path: str, db_path: str) -> None:
    """
    Create initial snapshot registry entry.
    
    Args:
        db_manager: Database manager instance
        csv_path: Path to source CSV file
        db_path: Path to database file
    """
    corpus_hash = compute_file_hash(csv_path)
    
    with db_manager.session_scope() as session:
        insert_snapshot_registry(
            session=session,
            version="v1.0_bootstrap",
            release_date=date.today(),
            corpus_hash=corpus_hash,
            db_file_path=db_path,
            notes="Initial bootstrap from reg153_master.csv",
        )
    
    print(f"\n  ✓ Created snapshot registry entry (corpus hash: {corpus_hash[:16]}...)")


def print_statistics(db_manager: DatabaseManager) -> None:
    """
    Print database statistics after loading.
    
    Args:
        db_manager: Database manager instance
    """
    with db_manager.session_scope() as session:
        stats = get_database_statistics(session)
    
    print(f"\n{'='*70}")
    print(f"DATABASE STATISTICS")
    print(f"{'='*70}\n")
    print(f"  Analytes:          {stats['analytes']:>6}")
    print(f"  Synonyms:          {stats['synonyms']:>6}")
    print(f"  Lab Variants:      {stats['lab_variants']:>6}")
    print(f"  Match Decisions:   {stats['match_decisions']:>6}")
    print(f"  Validated:         {stats['validated_decisions']:>6}")
    print(f"  Ingested:          {stats['ingested_decisions']:>6}")
    print()


def verify_analyte_breakdown(db_manager: DatabaseManager) -> None:
    """
    Verify analyte breakdown by chemical group.
    
    Args:
        db_manager: Database manager instance
    """
    from sqlalchemy import select, func
    from src.database.models import Analyte
    
    print(f"{'='*70}")
    print(f"ANALYTE BREAKDOWN BY CHEMICAL GROUP")
    print(f"{'='*70}\n")
    
    with db_manager.session_scope() as session:
        # Group by chemical_group
        results = session.execute(
            select(
                Analyte.chemical_group,
                func.count(Analyte.analyte_id).label('count')
            )
            .group_by(Analyte.chemical_group)
            .order_by(func.count(Analyte.analyte_id).desc())
        ).all()
        
        for group, count in results:
            group_name = group or '(unspecified)'
            print(f"  {group_name:<30} {count:>4}")
    
    print()


def main():
    """Main setup routine."""
    parser = argparse.ArgumentParser(
        description="Initialize Reg 153 Chemical Matcher database"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=DEFAULT_CSV_PATH,
        help=f"Path to reg153_master.csv (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing database if it exists",
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print(" ONTARIO REG 153 CHEMICAL MATCHER - DATABASE SETUP")
    print("="*70)
    
    # Check if CSV exists
    csv_file = Path(args.csv_path)
    if not csv_file.exists():
        print(f"\n❌ ERROR: CSV file not found: {args.csv_path}")
        print("   Please ensure reg153_master.csv exists before running setup.")
        sys.exit(1)
    
    # Check if database already exists
    db_file = Path(args.db_path)
    if db_file.exists() and not args.drop_existing:
        response = input(f"\n⚠  Database already exists at: {args.db_path}\n   Overwrite? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("   Setup cancelled.")
            sys.exit(0)
    
    if db_file.exists():
        db_file.unlink()
        print(f"   Existing database deleted: {args.db_path}")
    
    try:
        # Initialize database
        print(f"\nInitializing database at: {args.db_path}")
        db_manager = DatabaseManager(db_path=args.db_path, echo=False)
        
        # Create all tables
        print("Creating database schema...")
        db_manager.create_all_tables()
        print("  ✓ All tables and indexes created")
        
        # Load analytes from CSV
        analytes_loaded = load_reg153_master(db_manager, args.csv_path)
        
        # Create snapshot entry
        create_snapshot_entry(db_manager, args.csv_path, args.db_path)
        
        # Print statistics
        print_statistics(db_manager)
        
        # Print breakdown
        verify_analyte_breakdown(db_manager)
        
        # Success message
        print("="*70)
        print(" ✓ DATABASE SETUP COMPLETE")
        print("="*70)
        print(f"\n Database: {args.db_path}")
        print(f" Analytes: {analytes_loaded}")
        print(f"\n Next steps:")
        print(f"   1. Run 04_harvest_api_synonyms.py to populate synonyms")
        print(f"   2. Run 09_generate_embeddings.py to create embeddings")
        print(f"   3. Use the matching engine with the populated database")
        print()
        
        # Close database
        db_manager.close()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
