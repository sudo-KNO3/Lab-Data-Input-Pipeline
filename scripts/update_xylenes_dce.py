"""
Update xylene entries and dichloroethylene CAS numbers.
Add proper handling for m&p-xylene as reported in lab data.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, AnalyteType, SynonymType


def update_xylenes():
    """Update xylene entries for proper lab data handling."""
    db = DatabaseManager()
    
    print("\n" + "="*80)
    print("UPDATING XYLENE ENTRIES")
    print("="*80)
    
    with db.session_scope() as session:
        # Update existing xylene entries with proper CAS
        xylene_updates = [
            ('Xylene O', '95-47-6'),
            ('Xylene M&P', None),  # Meta + para xylene mixture as reported in lab data
            ('Xylene mixture', None)  # Total xylene mixture (o+m+p) - lab specific
        ]
        
        for name_pattern, cas in xylene_updates:
            analyte = session.query(Analyte).filter(
                Analyte.preferred_name.like(f'%{name_pattern}%')
            ).first()
            
            if analyte:
                old_cas = analyte.cas_number
                analyte.cas_number = cas
                print(f"\n[UPDATE] {analyte.preferred_name}")
                print(f"  Old CAS: {old_cas}")
                print(f"  New CAS: {cas or 'None (mixture)'}")
        
        # Check if we need to add individual m-xylene and p-xylene for synonym harvesting
        m_xylene = session.query(Analyte).filter(
            Analyte.preferred_name == 'Xylene M (m-xylene)'
        ).first()
        
        p_xylene = session.query(Analyte).filter(
            Analyte.preferred_name == 'Xylene P (p-xylene)'
        ).first()
        
        # Add m-xylene if not exists (for synonym harvesting only)
        if not m_xylene:
            print(f"\n[ADD] Xylene M (m-xylene) - for synonym harvesting")
            m_xylene = Analyte(
                analyte_id='REG153_VOC_109_M',
                preferred_name='Xylene M (m-xylene)',
                cas_number='108-38-3',
                analyte_type=AnalyteType.SINGLE_SUBSTANCE
            )
            session.add(m_xylene)
            
            # Add bootstrap synonym
            synonym = Synonym(
                analyte_id='REG153_VOC_109_M',
                synonym_raw='m-xylene',
                synonym_norm='m-xylene',
                synonym_type=SynonymType.COMMON,
                harvest_source='bootstrap',
                confidence=1.0
            )
            session.add(synonym)
        
        # Add p-xylene if not exists (for synonym harvesting only)
        if not p_xylene:
            print(f"\n[ADD] Xylene P (p-xylene) - for synonym harvesting")
            p_xylene = Analyte(
                analyte_id='REG153_VOC_109_P',
                preferred_name='Xylene P (p-xylene)',
                cas_number='106-42-3',
                analyte_type=AnalyteType.SINGLE_SUBSTANCE
            )
            session.add(p_xylene)
            
            # Add bootstrap synonym
            synonym = Synonym(
                analyte_id='REG153_VOC_109_P',
                synonym_raw='p-xylene',
                synonym_norm='p-xylene',
                synonym_type=SynonymType.COMMON,
                harvest_source='bootstrap',
                confidence=1.0
            )
            session.add(synonym)
        
        session.commit()
        print("\n[OK] Xylene entries updated")


def update_dichloroethylene():
    """Add CAS numbers for dichloroethylene isomers."""
    db = DatabaseManager()
    
    print("\n" + "="*80)
    print("UPDATING DICHLOROETHYLENE ISOMERS")
    print("="*80)
    
    with db.session_scope() as session:
        dce_updates = [
            ('Dichloroethylene 1,2-cis-', '156-59-2'),
            ('Dichloroethylene 1,2-trans-', '156-60-5'),
        ]
        
        for name, cas in dce_updates:
            analyte = session.query(Analyte).filter(
                Analyte.preferred_name == name
            ).first()
            
            if analyte:
                old_cas = analyte.cas_number
                analyte.cas_number = cas
                print(f"\n[UPDATE] {name}")
                print(f"  Old CAS: {old_cas}")
                print(f"  New CAS: {cas}")
        
        session.commit()
        print("\n[OK] Dichloroethylene CAS numbers updated")


def verify_all_changes():
    """Verify all database changes."""
    db = DatabaseManager()
    
    print("\n" + "="*80)
    print("VERIFICATION REPORT")
    print("="*80)
    
    with db.session_scope() as session:
        print("\nAll Xylene entries:")
        xylenes = session.query(Analyte).filter(
            Analyte.preferred_name.like('%xylene%')
        ).order_by(Analyte.preferred_name).all()
        
        for x in xylenes:
            syn_count = session.query(Synonym).filter_by(analyte_id=x.analyte_id).count()
            print(f"  {x.preferred_name:50s} CAS: {x.cas_number or 'None':15s} Syns: {syn_count:4d}")
        
        print("\nDichloroethylene isomers:")
        dces = session.query(Analyte).filter(
            Analyte.preferred_name.like('%ichloroethylene%')
        ).order_by(Analyte.preferred_name).all()
        
        for d in dces:
            syn_count = session.query(Synonym).filter_by(analyte_id=d.analyte_id).count()
            print(f"  {d.preferred_name:50s} CAS: {d.cas_number or 'None':15s} Syns: {syn_count:4d}")


if __name__ == '__main__':
    print("\n" + "="*80)
    print("DATABASE UPDATE SCRIPT - Xylenes & Dichloroethylene")
    print("="*80)
    
    update_xylenes()
    update_dichloroethylene()
    verify_all_changes()
    
    print("\n" + "="*80)
    print("COMPLETE - Ready to harvest synonyms")
    print("="*80)
    print("\nNext steps:")
    print("1. Harvest synonyms for new entries:")
    print("   python scripts/04_harvest_api_synonyms.py --source pubchem --analyte-ids REG153_VOC_109_M,REG153_VOC_109_P,REG153_VOC_047,REG153_VOC_048")
    print("2. Re-validate all synonyms:")
    print("   python scripts/validate_synonyms.py --source pubchem --mark-invalid")
