"""
Add all periodic table elements to the database with PubChem synonym harvest.
Checks which elements already exist and only adds missing ones.
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

# All 118 elements with CAS numbers
PERIODIC_TABLE = [
    # Format: (symbol, name, cas_number, atomic_number)
    ('H', 'Hydrogen', '1333-74-0', 1),
    ('He', 'Helium', '7440-59-7', 2),
    ('Li', 'Lithium', '7439-93-2', 3),
    ('Be', 'Beryllium', '7440-41-7', 4),
    ('B', 'Boron', '7440-42-8', 5),
    ('C', 'Carbon', '7440-44-0', 6),
    ('N', 'Nitrogen', '7727-37-9', 7),
    ('O', 'Oxygen', '7782-44-7', 8),
    ('F', 'Fluorine', '7782-41-4', 9),
    ('Ne', 'Neon', '7440-01-9', 10),
    ('Na', 'Sodium', '7440-23-5', 11),
    ('Mg', 'Magnesium', '7439-95-4', 12),
    ('Al', 'Aluminum', '7429-90-5', 13),
    ('Si', 'Silicon', '7440-21-3', 14),
    ('P', 'Phosphorus', '7723-14-0', 15),
    ('S', 'Sulfur', '7704-34-9', 16),
    ('Cl', 'Chlorine', '7782-50-5', 17),
    ('Ar', 'Argon', '7440-37-1', 18),
    ('K', 'Potassium', '7440-09-7', 19),
    ('Ca', 'Calcium', '7440-70-2', 20),
    ('Sc', 'Scandium', '7440-20-2', 21),
    ('Ti', 'Titanium', '7440-32-6', 22),
    ('V', 'Vanadium', '7440-62-2', 23),
    ('Cr', 'Chromium', '7440-47-3', 24),
    ('Mn', 'Manganese', '7439-96-5', 25),
    ('Fe', 'Iron', '7439-89-6', 26),
    ('Co', 'Cobalt', '7440-48-4', 27),
    ('Ni', 'Nickel', '7440-02-0', 28),
    ('Cu', 'Copper', '7440-50-8', 29),
    ('Zn', 'Zinc', '7440-66-6', 30),
    ('Ga', 'Gallium', '7440-55-3', 31),
    ('Ge', 'Germanium', '7440-56-4', 32),
    ('As', 'Arsenic', '7440-38-2', 33),
    ('Se', 'Selenium', '7782-49-2', 34),
    ('Br', 'Bromine', '7726-95-6', 35),
    ('Kr', 'Krypton', '7439-90-9', 36),
    ('Rb', 'Rubidium', '7440-17-7', 37),
    ('Sr', 'Strontium', '7440-24-6', 38),
    ('Y', 'Yttrium', '7440-65-5', 39),
    ('Zr', 'Zirconium', '7440-67-7', 40),
    ('Nb', 'Niobium', '7440-03-1', 41),
    ('Mo', 'Molybdenum', '7439-98-7', 42),
    ('Tc', 'Technetium', '7440-26-8', 43),
    ('Ru', 'Ruthenium', '7440-18-8', 44),
    ('Rh', 'Rhodium', '7440-16-6', 45),
    ('Pd', 'Palladium', '7440-05-3', 46),
    ('Ag', 'Silver', '7440-22-4', 47),
    ('Cd', 'Cadmium', '7440-43-9', 48),
    ('In', 'Indium', '7440-74-6', 49),
    ('Sn', 'Tin', '7440-31-5', 50),
    ('Sb', 'Antimony', '7440-36-0', 51),
    ('Te', 'Tellurium', '13494-80-9', 52),
    ('I', 'Iodine', '7553-56-2', 53),
    ('Xe', 'Xenon', '7440-63-3', 54),
    ('Cs', 'Cesium', '7440-46-2', 55),
    ('Ba', 'Barium', '7440-39-3', 56),
    ('La', 'Lanthanum', '7439-91-0', 57),
    ('Ce', 'Cerium', '7440-45-1', 58),
    ('Pr', 'Praseodymium', '7440-10-0', 59),
    ('Nd', 'Neodymium', '7440-00-8', 60),
    ('Pm', 'Promethium', '7440-12-2', 61),
    ('Sm', 'Samarium', '7440-19-9', 62),
    ('Eu', 'Europium', '7440-53-1', 63),
    ('Gd', 'Gadolinium', '7440-54-2', 64),
    ('Tb', 'Terbium', '7440-27-9', 65),
    ('Dy', 'Dysprosium', '7440-61-1', 66),
    ('Ho', 'Holmium', '7440-60-0', 67),
    ('Er', 'Erbium', '7439-90-9', 68),
    ('Tm', 'Thulium', '7440-30-4', 69),
    ('Yb', 'Ytterbium', '7440-64-4', 70),
    ('Lu', 'Lutetium', '7439-94-3', 71),
    ('Hf', 'Hafnium', '7440-58-6', 72),
    ('Ta', 'Tantalum', '7440-25-7', 73),
    ('W', 'Tungsten', '7440-33-7', 74),
    ('Re', 'Rhenium', '7440-15-5', 75),
    ('Os', 'Osmium', '7440-04-2', 76),
    ('Ir', 'Iridium', '7439-88-5', 77),
    ('Pt', 'Platinum', '7440-06-4', 78),
    ('Au', 'Gold', '7440-57-5', 79),
    ('Hg', 'Mercury', '7439-97-6', 80),
    ('Tl', 'Thallium', '7440-28-0', 81),
    ('Pb', 'Lead', '7439-92-1', 82),
    ('Bi', 'Bismuth', '7440-69-9', 83),
    ('Po', 'Polonium', '7440-08-6', 84),
    ('At', 'Astatine', '7440-68-8', 85),
    ('Rn', 'Radon', '10043-92-2', 86),
    ('Fr', 'Francium', '7440-73-5', 87),
    ('Ra', 'Radium', '7440-14-4', 88),
    ('Ac', 'Actinium', '7440-34-8', 89),
    ('Th', 'Thorium', '7440-29-1', 90),
    ('Pa', 'Protactinium', '7440-13-3', 91),
    ('U', 'Uranium', '7440-61-1', 92),
    ('Np', 'Neptunium', '7439-99-8', 93),
    ('Pu', 'Plutonium', '7440-07-5', 94),
    ('Am', 'Americium', '7440-35-9', 95),
    ('Cm', 'Curium', '7440-51-9', 96),
    ('Bk', 'Berkelium', '7440-40-6', 97),
    ('Cf', 'Californium', '7440-71-3', 98),
    ('Es', 'Einsteinium', '7429-92-7', 99),
    ('Fm', 'Fermium', '7440-72-4', 100),
    ('Md', 'Mendelevium', '7440-11-1', 101),
    ('No', 'Nobelium', '10028-14-5', 102),
    ('Lr', 'Lawrencium', '22537-19-5', 103),
    ('Rf', 'Rutherfordium', '53850-36-5', 104),
    ('Db', 'Dubnium', '53850-35-4', 105),
    ('Sg', 'Seaborgium', '54038-81-2', 106),
    ('Bh', 'Bohrium', '54037-14-8', 107),
    ('Hs', 'Hassium', '54037-57-9', 108),
    ('Mt', 'Meitnerium', '54038-01-6', 109),
    ('Ds', 'Darmstadtium', '54083-77-1', 110),
    ('Rg', 'Roentgenium', '54386-24-2', 111),
    ('Cn', 'Copernicium', '54084-26-3', 112),
    ('Nh', 'Nihonium', '54084-70-7', 113),
    ('Fl', 'Flerovium', '54085-16-4', 114),
    ('Mc', 'Moscovium', '54085-64-2', 115),
    ('Lv', 'Livermorium', '54100-71-9', 116),
    ('Ts', 'Tennessine', '54101-14-3', 117),
    ('Og', 'Oganesson', '54144-19-3', 118),
]

def main():
    db = DatabaseManager('data/reg153_matcher.db')
    harvester = PubChemHarvester()
    normalizer = TextNormalizer()
    
    print('ADDING ALL PERIODIC TABLE ELEMENTS')
    print('=' * 80)
    
    with db.get_session() as session:
        # Check which elements already exist
        existing_cas = set()
        existing_query = session.query(Analyte.cas_number).filter(
            Analyte.cas_number.isnot(None)
        ).all()
        existing_cas = {cas for (cas,) in existing_query}
        
        print(f'\nExisting elements in database: {len(existing_cas)}')
        
        # Filter to elements not yet in database
        elements_to_add = [
            elem for elem in PERIODIC_TABLE 
            if elem[2] not in existing_cas
        ]
        
        print(f'Elements to add: {len(elements_to_add)}')
        print()
        
        if not elements_to_add:
            print('All elements already in database!')
            return
        
        total_synonyms = 0
        element_counter = 1
        
        for symbol, name, cas, atomic_num in elements_to_add:
            # Generate analyte_id
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
            
            print(f'{element_counter:3d}/{len(elements_to_add):3d} {symbol:2s} {name:15s} ({cas})', end=' ... ')
            element_counter += 1
            
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
                    print(f'{total_syns:4d} synonyms ({len(bootstrap_syns)} bootstrap + {added} PubChem)')
                    total_synonyms += total_syns
                else:
                    print(f'{len(bootstrap_syns):4d} synonyms (bootstrap only)')
                    total_synonyms += len(bootstrap_syns)
            except Exception as e:
                print(f'{len(bootstrap_syns):4d} synonyms (PubChem error: {str(e)[:30]})')
                total_synonyms += len(bootstrap_syns)
        
        session.commit()
        print('\n' + '=' * 80)
        print(f'SUCCESS: Added {len(elements_to_add)} elements with {total_synonyms:,} total synonyms')

if __name__ == '__main__':
    main()
