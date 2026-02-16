"""
Add Water Quality Parameters to Database

Expands reg153_matcher.db to include water quality parameters commonly found
in municipal wastewater and environmental lab reports.

Categories:
- NUTRIENTS: Nitrogen and phosphorus species
- IONS: Major anions and cations
- PHYSICAL: pH, conductivity, turbidity, TSS, TDS
- ORGANIC: BOD, COD, organic carbon
- MICRO: Microbiology (E. coli)
- CHEM: Other chemicals (cyanide, phenolics)
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, Synonym, AnalyteType, SynonymType
from src.normalization.text_normalizer import TextNormalizer
from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stdout, level="INFO")


def add_water_quality_parameters():
    """Add water quality parameters with manually created synonyms"""
    
    db = DatabaseManager('data/reg153_matcher.db')
    normalizer = TextNormalizer()
    
    with db.session_scope() as session:
        
        # Define parameters to add
        # Format: (analyte_id, preferred_name, group_code, chemical_group, cas_number, synonyms_list)
        parameters = [
            # NUTRIENTS
            ('WQ_NUTR_001', 'Total Kjeldahl Nitrogen', 'NUTRIENTS', 'Nitrogen', None, [
                'Total Kjeldahl Nitrogen', 'TKN', 'Kjeldahl Nitrogen', 'Kjeldahl N',
                'Total Kjeldahl Nitrogen (TKN)', 'Nitrogen Kjeldahl Total'
            ]),
            
            ('WQ_NUTR_002', 'Ammonia', 'NUTRIENTS', 'Nitrogen', '7664-41-7', [
                'Ammonia', 'Ammonia (N)', 'Ammonia+Ammonium (N)', 'Ammonia (N)-Total (NH3+NH4)',
                'Ammonia-N', 'Total Ammonia', 'NH3', 'Ammonia Nitrogen', 'Ammonium'
            ]),
            
            ('WQ_NUTR_003', 'Ammonia Unionized', 'NUTRIENTS', 'Nitrogen', None, [
                'Ammonia (N)-unionized', 'Unionized Ammonia', 'Un-ionized Ammonia',
                'Ammonia Unionized', 'NH3 Unionized'
            ]),
            
            ('WQ_NUTR_004', 'Nitrate', 'NUTRIENTS', 'Nitrogen', '14797-55-8', [
                'Nitrate', 'Nitrate (N)', 'Nitrate (as N)', 'Nitrate-N',
                'Nitrate Nitrogen', 'NO3', 'NO3-N'
            ]),
            
            ('WQ_NUTR_005', 'Nitrite', 'NUTRIENTS', 'Nitrogen', '14797-65-0', [
                'Nitrite', 'Nitrite (N)', 'Nitrite (as N)', 'Nitrite-N',
                'Nitrite Nitrogen', 'NO2', 'NO2-N'
            ]),
            
            ('WQ_NUTR_006', 'Nitrate + Nitrite', 'NUTRIENTS', 'Nitrogen', None, [
                'Nitrate + Nitrite (as N)', 'Nitrate+Nitrite', 'NO3+NO2',
                'Nitrate and Nitrite', 'Nitrate/Nitrite', 'NOx'
            ]),
            
            ('WQ_NUTR_007', 'Phosphorus Total', 'NUTRIENTS', 'Phosphorus', None, [
                'Phosphorus', 'Phosphorus (Total)', 'Phosphorus (total)',
                'Total Phosphorus', 'Phosphorus-Total', 'TP', 'P-Total'
            ]),
            
            # IONS/MINERALS
            ('WQ_IONS_001', 'Chloride', 'IONS', 'Anions', '16887-00-6', [
                'Chloride', 'Cl', 'Chloride Ion', 'Cl-'
            ]),
            
            ('WQ_IONS_002', 'Fluoride', 'IONS', 'Anions', '16984-48-8', [
                'Fluoride', 'F', 'Fluoride Ion', 'F-'
            ]),
            
            ('WQ_IONS_003', 'Sulphate', 'IONS', 'Anions', '14808-79-8', [
                'Sulphate', 'Sulfate', 'SO4', 'Sulphate Ion', 'Sulfate Ion', 'SO4-2'
            ]),
            
            ('WQ_IONS_004', 'Calcium', 'IONS', 'Cations', '7440-70-2', [
                'Calcium', 'Calcium (Total)', 'Ca', 'Calcium Ion', 'Ca+2'
            ]),
            
            ('WQ_IONS_005', 'Magnesium', 'IONS', 'Cations', '7439-95-4', [
                'Magnesium', 'Mg', 'Magnesium Ion', 'Mg+2'
            ]),
            
            ('WQ_IONS_006', 'Sodium', 'IONS', 'Cations', '7440-23-5', [
                'Sodium', 'Na', 'Sodium Ion', 'Na+'
            ]),
            
            ('WQ_IONS_007', 'Potassium', 'IONS', 'Cations', '7440-09-7', [
                'Potassium', 'Potassium (total)', 'K', 'Potassium Ion', 'K+'
            ]),
            
            ('WQ_IONS_008', 'Iron', 'IONS', 'Metals', '7439-89-6', [
                'Iron', 'Iron (Total)', 'Fe', 'Iron Total'
            ]),
            
            ('WQ_IONS_009', 'Manganese', 'IONS', 'Metals', '7439-96-5', [
                'Manganese', 'Mn', 'Manganese Total'
            ]),
            
            ('WQ_IONS_010', 'Aluminum', 'IONS', 'Metals', '7429-90-5', [
                'Aluminum', 'Aluminum (Total)', 'Aluminium', 'Aluminium (Total)',
                'Al', 'Aluminum Total'
            ]),
            
            ('WQ_IONS_011', 'Strontium', 'IONS', 'Metals', '7440-24-6', [
                'Strontium', 'Sr', 'Strontium Total'
            ]),
            
            # PHYSICAL PARAMETERS
            ('WQ_PHYS_001', 'pH', 'PHYSICAL', 'Physical', None, [
                'pH', 'pH @25°C', 'pH @ 25C', 'pH (25C)', 'pH at 25C',
                'pH @ 25 deg C', 'pH Value'
            ]),
            
            ('WQ_PHYS_002', 'Conductivity', 'PHYSICAL', 'Physical', None, [
                'Conductivity', 'Conductivity @25°C', 'Conductivity @ 25C',
                'Specific Conductance', 'Conductivity (25C)', 'EC',
                'Electrical Conductivity'
            ]),
            
            ('WQ_PHYS_003', 'Turbidity', 'PHYSICAL', 'Physical', None, [
                'Turbidity', 'Turbidity (NTU)', 'NTU'
            ]),
            
            ('WQ_PHYS_004', 'Colour', 'PHYSICAL', 'Physical', None, [
                'Colour', 'Color', 'True Colour', 'True Color', 'TCU', 'Colour (TCU)'
            ]),
            
            ('WQ_PHYS_005', 'Total Suspended Solids', 'PHYSICAL', 'Solids', None, [
                'Total Suspended Solids', 'TSS', 'Suspended Solids',
                'Total Suspended Solids (TSS)', 'TSS (mg/L)'
            ]),
            
            ('WQ_PHYS_006', 'Total Dissolved Solids', 'PHYSICAL', 'Solids', None, [
                'Total Dissolved Solids', 'TDS', 'Dissolved Solids',
                'TDS (Calc. from Cond.)', 'TDS Calculated', 'TDS (Calculated)'
            ]),
            
            ('WQ_PHYS_007', 'Total Solids', 'PHYSICAL', 'Solids', None, [
                'Total Solids', 'TS', 'Total Solids (TS)'
            ]),
            
            ('WQ_PHYS_008', 'Volatile Solids', 'PHYSICAL', 'Solids', None, [
                'Volatile Solids', 'VS', 'Volatile Suspended Solids', 'VSS'
            ]),
            
            ('WQ_PHYS_009', 'Hardness', 'PHYSICAL', 'Physical', None, [
                'Hardness', 'Hardness (as CaCO3)', 'Total Hardness',
                'Hardness as CaCO3', 'Hardness (CaCO3)'
            ]),
            
            ('WQ_PHYS_010', 'Alkalinity', 'PHYSICAL', 'Physical', None, [
                'Alkalinity', 'Alkalinity (as CaCO3)', 'Total Alkalinity',
                'Alkalinity(CaCO3) to pH4.5', 'Alkalinity to pH 4.5',
                'Alkalinity as CaCO3'
            ]),
            
            # ORGANIC PARAMETERS
            ('WQ_ORG_001', 'BOD5', 'ORGANIC', 'Oxygen Demand', None, [
                'BOD5', 'BOD', 'Biochemical Oxygen Demand (BOD5)',
                'Biochemical Oxygen Demand', '5-Day BOD', 'BOD (5 day)',
                'Biological Oxygen Demand'
            ]),
            
            ('WQ_ORG_002', 'COD', 'ORGANIC', 'Oxygen Demand', None, [
                'COD', 'Chemical Oxygen Demand', 'COD (Chemical Oxygen Demand)'
            ]),
            
            ('WQ_ORG_003', 'Total Organic Carbon', 'ORGANIC', 'Carbon', None, [
                'Total Organic Carbon', 'TOC', 'Organic Carbon Total'
            ]),
            
            ('WQ_ORG_004', 'Dissolved Organic Carbon', 'ORGANIC', 'Carbon', None, [
                'Dissolved Organic Carbon', 'DOC', 'Organic Carbon Dissolved'
            ]),
            
            # MICROBIOLOGY
            ('WQ_MICRO_001', 'E. coli', 'MICRO', 'Bacteria', None, [
                'E. Coli', 'Ecoli', 'E.coli', 'E coli', 'Escherichia coli',
                'E. coli (MPN)', 'E. coli (CFU)'
            ]),
            
            # OTHER CHEMICALS
            ('WQ_CHEM_001', 'Cyanide Total', 'CHEM', 'Inorganic', '57-12-5', [
                'Cyanide (Total)', 'Cyanide Total', 'Total Cyanide', 'CN Total',
                'Cyanide-Total'
            ]),
            
            ('WQ_CHEM_002', 'Cyanide WAD', 'CHEM', 'Inorganic', None, [
                'Cyanide (WAD)', 'Cyanide WAD', 'WAD Cyanide',
                'Weak Acid Dissociable Cyanide', 'CN-WAD'
            ]),
            
            ('WQ_CHEM_003', 'Phenolics', 'CHEM', 'Organic', None, [
                'Phenolics', 'Phenols', 'Total Phenolics', 'Phenol Total'
            ]),
        ]
        
        logger.info("="*80)
        logger.info("ADDING WATER QUALITY PARAMETERS")
        logger.info("="*80)
        
        added_count = 0
        synonym_count = 0
        
        for analyte_id, preferred_name, group_code, chemical_group, cas_number, synonyms_list in parameters:
            
            # Check if analyte already exists
            existing = session.query(Analyte).filter(
                Analyte.analyte_id == analyte_id
            ).first()
            
            if existing:
                logger.info(f"Skipping {analyte_id} - already exists")
                continue
            
            # Create analyte
            new_analyte = Analyte(
                analyte_id=analyte_id,
                preferred_name=preferred_name,
                analyte_type=AnalyteType.SINGLE_SUBSTANCE,
                cas_number=cas_number,
                group_code=group_code,
                chemical_group=chemical_group
            )
            session.add(new_analyte)
            session.flush()
            
            logger.info(f"\nAdded: {analyte_id} - {preferred_name}")
            if cas_number:
                logger.info(f"  CAS: {cas_number}")
            
            # Add synonyms
            for syn_raw in synonyms_list:
                syn_norm = normalizer.normalize(syn_raw)
                
                # Check if synonym already exists
                existing_syn = session.query(Synonym).filter(
                    Synonym.analyte_id == analyte_id,
                    Synonym.synonym_norm == syn_norm
                ).first()
                
                if not existing_syn:
                    synonym = Synonym(
                        analyte_id=analyte_id,
                        synonym_raw=syn_raw,
                        synonym_norm=syn_norm,
                        synonym_type=SynonymType.COMMON,
                        harvest_source='bootstrap',
                        confidence=1.0
                    )
                    session.add(synonym)
                    synonym_count += 1
            
            added_count += 1
            logger.info(f"  Added {len(synonyms_list)} synonyms")
        
        session.commit()
        
        logger.info("\n" + "="*80)
        logger.info(f"[OK] Added {added_count} analytes with {synonym_count} total synonyms")
        logger.info("="*80)


if __name__ == "__main__":
    add_water_quality_parameters()
