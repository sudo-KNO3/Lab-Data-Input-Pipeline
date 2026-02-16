"""Generate ontario_variants_known.csv with known lab naming patterns."""

import pandas as pd
from pathlib import Path

# Data for Ontario lab naming variants
variants_data = [
    # Benzene
    ("Benzene", "Benzene", "ALS", 850, "standard", "Standard naming"),
    ("Benezene", "Benzene", "ALS", 12, "typo", "Common typo"),
    ("Benzene ", "Benzene", "ALS", 45, "spacing", "Trailing space"),
    ("BENZENE", "Benzene", "SGS", 320, "case", "All caps style"),
    
    # Toluene
    ("Toluene", "Toluene", "ALS", 920, "standard", "Standard naming"),
    ("Toluenne", "Toluene", "ALS", 8, "typo", "Double 'n' typo"),
    ("Toluene ", "Toluene", "ALS", 38, "spacing", "Trailing space"),
    ("TOLUENE", "Toluene", "SGS", 415, "case", "All caps style"),
    
    # Trichloroethylene
    ("Trichloroethylene", "Trichloroethylene", "ALS", 650, "standard", "Full name"),
    ("TCE", "Trichloroethylene", "ALS", 1200, "abbreviation", "Common abbreviation"),
    ("Trichloroethene", "Trichloroethylene", "ALS", 85, "variant", "Alternative spelling"),
    ("Trichloroethylene (TCE)", "Trichloroethylene", "BureauVeritas", 450, "combined", "Name with abbreviation"),
    
    # 1,1,1-Trichloroethane
    ("1,1,1-Trichloroethane", "1,1,1-Trichloroethane", "ALS", 580, "standard", "Standard with commas"),
    ("1,1,1-TCA", "1,1,1-Trichloroethane", "ALS", 920, "abbreviation", "Common abbreviation"),
    ("1, 1, 1-TCA", "1,1,1-Trichloroethane", "ALS", 65, "spacing", "Spaces after commas"),
    ("1,1,1 TCA", "1,1,1-Trichloroethane", "SGS", 110, "spacing", "Space before TCA"),
    ("1,1,1-Trichloroethane", "1,1,1-Trichloroethane", "BureauVeritas", 380, "standard", "Standard naming"),
    
    # 1,4-Dioxane
    ("1,4-Dioxane", "1,4-Dioxane", "ALS", 720, "standard", "Standard naming"),
    ("1,4 Diox", "1,4-Dioxane", "ALS", 180, "truncation", "Common lab truncation"),
    ("1,4-Dioxane ", "1,4-Dioxane", "ALS", 42, "spacing", "Trailing space"),
    ("Dioxane, 1,4-", "1,4-Dioxane", "SGS", 290, "inverted", "Inverted order"),
    
    # Benzo(a)pyrene
    ("Benzo(a)pyrene", "Benzo(a)pyrene", "ALS", 450, "standard", "Standard parenthesis"),
    ("B(a)P", "Benzo(a)pyrene", "ALS", 1850, "abbreviation", "Very common abbreviation"),
    ("BaP", "Benzo(a)pyrene", "SGS", 920, "abbreviation", "Short abbreviation"),
    ("Benzo[a]pyrene", "Benzo(a)pyrene", "ALS", 95, "bracket_variant", "Square brackets instead"),
    ("Benzo(a)pyrene", "Benzo(a)pyrene", "BureauVeritas", 550, "standard", "Standard naming"),
    
    #Naphthalene
    ("Naphthalene", "Naphthalene", "ALS", 890, "standard", "Standard naming"),
    ("Naphthalene ", "Naphthalene", "ALS", 68, "spacing", "Trailing space"),
    ("NAPHTHALENE", "Naphthalene", "SGS", 410, "case", "All caps style"),
    
    # PHC F2
    ("PHC F2", "Petroleum Hydrocarbons F2", "ALS", 1500, "abbreviation", "Common PHC abbreviation"),
    ("PHC F2 (C10-C16)", "Petroleum Hydrocarbons F2", "ALS", 680, "detailed", "With carbon range"),
    ("Petroleum Hydrocarbons F2", "Petroleum Hydrocarbons F2", "ALS", 420, "standard", "Full name"),
    ("F2 Petroleum Hydrocarbons", "Petroleum Hydrocarbons F2", "SGS", 310, "inverted", "Inverted order"),
    ("PHC F2 (C10-C16)", "Petroleum Hydrocarbons F2", "BureauVeritas", 590, "detailed", "With carbon range"),
    
    # PHC F3
    ("PHC F3", "Petroleum Hydrocarbons F3", "ALS", 1450, "abbreviation", "Common PHC abbreviation"),
    ("PHC F3 (C16-C34)", "Petroleum Hydrocarbons F3", "ALS", 720, "detailed", "With carbon range"),
    ("Petroleum Hydrocarbons F3", "Petroleum Hydrocarbons F3", "ALS", 390, "standard", "Full name"),
    ("F3 Petroleum Hydrocarbons", "Petroleum Hydrocarbons F3", "SGS", 285, "inverted", "Inverted order"),
    ("PHC F3 (C16-C34)", "Petroleum Hydrocarbons F3", "BureauVeritas", 610, "detailed", "With carbon range"),
    
    # Chromium
    ("Chromium", "Chromium", "ALS", 680, "standard", "Standard naming"),
    ("Cr", "Chromium", "ALS", 420, "abbreviation", "Element symbol"),
    ("Chromium, Total", "Chromium", "SGS", 1650, "qualifier", "With Total qualifier"),
    ("Chromium (Total)", "Chromium", "SGS", 890, "qualifier", "Parenthetical Total"),
    ("Chromium, Total", "Chromium", "BureauVeritas", 520, "qualifier", "Total qualifier"),
    
    # Lead
    ("Lead", "Lead", "ALS", 720, "standard", "Standard naming"),
    ("Pb", "Lead", "ALS", 580, "abbreviation", "Element symbol"),
    ("Lead, Total", "Lead", "SGS", 1720, "qualifier", "With Total qualifier"),
    ("Lead (Total)", "Lead", "SGS", 950, "qualifier", "Parenthetical Total"),
    ("Lead, Total", "Lead", "BureauVeritas", 610, "qualifier", "Total qualifier"),
    
    # Arsenic
    ("Arsenic", "Arsenic", "ALS", 690, "standard", "Standard naming"),
    ("As", "Arsanic", "ALS", 510, "abbreviation", "Element symbol"),
    ("Arsenic, Total", "Arsenic", "SGS", 1580, "qualifier", "With Total qualifier"),
    ("Arsenic (Total)", "Arsenic", "SGS", 880, "qualifier", "Parenthetical Total"),
    ("Arsenic, Total", "Arsenic", "BureauVeritas", 580, "qualifier", "Total qualifier"),
    
    # Xylene
    ("Xylene", "Xylene", "ALS", 450, "standard", "Mixed xylenes"),
    ("Xylenes", "Xylene", "ALS", 920, "plural", "Plural form"),
    ("Xylenes (total)", "Xylene", "SGS", 710, "qualifier", "Total xylenes"),
    ("m&p-Xylene", "Xylene", "ALS", 180, "isomer", "Meta and para"),
    ("o-Xylene", "o-Xylene", "ALS", 420, "isomer", "Ortho isomer"),
    
    # Ethylbenzene
    ("Ethylbenzene", "Ethylbenzene", "ALS", 820, "standard", "Standard naming"),
    ("Ethyl Benzene", "Ethylbenzene", "ALS", 35, "spacing", "Space variant"),
    ("ETHYLBENZENE", "Ethylbenzene", "SGS", 310, "case", "All caps"),
    
    # Vinyl Chloride
    ("Vinyl Chloride", "Vinyl Chloride", "ALS", 520, "standard", "Standard naming"),
    ("Vinly Chloride", "Vinyl Chloride", "ALS", 8, "typo", "Missing 'i'"),
    ("Chloroethene", "Vinyl Chloride", "ALS", 45, "iupac", "IUPAC name"),
    
    # MEK
    ("Methylethylketone", "Methyl Ethyl Ketone", "ALS", 280, "nospace", "No spaces"),
    ("Methyl Ethyl Ketone", "Methyl Ethyl Ketone", "ALS", 450, "standard", "Standard spacing"),
    ("MEK", "Methyl Ethyl Ketone", "ALS", 1100, "abbreviation", "Common abbreviation"),
    ("2-Butanone", "Methyl Ethyl Ketone", "SGS", 380, "iupac", "IUPAC name"),
    
    # Acetone
    ("Acetone", "Acetone", "ALS", 950, "standard", "Standard naming"),
    ("2-Propanone", "Acetone", "SGS", 120, "iupac", "IUPAC name"),
    ("Dimethyl Ketone", "Acetone", "ALS", 15, "variant", "Alternative name"),
    
    # Chloroform
    ("Chloroform", "Chloroform", "ALS", 720, "standard", "Standard naming"),
    ("Trichloromethane", "Chloroform", "SGS", 85, "iupac", "IUPAC name"),
    
    # Carbon Tetrachloride
    ("Carbon Tetrachloride", "Carbon Tetrachloride", "ALS", 580, "standard", "Standard naming"),
    ("Tetrachloromethane", "Carbon Tetrachloride", "SGS", 95, "iupac", "IUPAC name"),
    
    # Methylene Chloride
    ("Methylene Chloride", "Methylene Chloride", "ALS", 620, "standard", "Standard naming"),
    ("Dichloromethane", "Methylene Chloride", "SGS", 450, "iupac", "IUPAC name"),
    ("DCM", "Methylene Chloride", "ALS", 380, "abbreviation", "Common abbreviation"),
    
    # Dichloroethanes
    ("1,1-Dichloroethane", "1,1-Dichloroethane", "ALS", 410, "standard", "Standard naming"),
    ("1,1-DCA", "1,1-Dichloroethane", "ALS", 520, "abbreviation", "Common abbreviation"),
    ("1,2-Dichloroethane", "1,2-Dichloroethane", "ALS", 480, "standard", "Standard naming"),
    ("1,2-DCA", "1,2-Dichloroethane", "ALS", 590, "abbreviation", "Common abbreviation"),
    ("EDC", "1,2-Dichloroethane", "ALS", 120, "abbreviation", "Ethylene dichloride"),
    
    # Dichloroethenes
    ("1,1-Dichloroethene", "1,1-Dichloroethene", "ALS", 390, "standard", "Standard naming"),
    ("1,1-DCE", "1,1-Dichloroethene", "ALS", 680, "abbreviation", "Common abbreviation"),
    ("cis-1,2-Dichloroethene", "cis-1,2-Dichloroethene", "ALS", 420, "standard", "Cis isomer"),
    ("cis-1,2-DCE", "cis-1,2-Dichloroethene", "ALS", 510, "abbreviation", "Cis abbreviation"),
    ("trans-1,2-Dichloroethene", "trans-1,2-Dichloroethene", "ALS", 380, "standard", "Trans isomer"),
    ("trans-1,2-DCE", "trans-1,2-Dichloroethene", "ALS", 460, "abbreviation", "Trans abbreviation"),
    
    # Tetrachloroethene
    ("Tetrachloroethene", "Tetrachloroethene", "ALS", 520, "standard", "Standard naming"),
    ("Tetrachloroethylene", "Tetrachloroethene", "ALS", 380, "variant", "Alternative spelling"),
    ("PCE", "Tetrachloroethene", "ALS", 1450, "abbreviation", "Very common abbreviation"),
    ("PERC", "Tetrachloroethene", "ALS", 280, "abbreviation", "Perchloroethylene"),
    
    # Other chlorinated compounds
    ("1,1,2-Trichloroethane", "1,1,2-Trichloroethane", "ALS", 310, "standard", "Standard naming"),
    ("1,1,2-TCA", "1,1,2-Trichloroethane", "ALS", 420, "abbreviation", "Common abbreviation"),
    ("1,1,2,2-Tetrachloroethane", "1,1,2,2-Tetrachloroethane", "ALS", 180, "standard", "Standard naming"),
    
    # Metals
    ("Mercury", "Mercury", "ALS", 620, "standard", "Standard naming"),
    ("Hg", "Mercury", "ALS", 480, "abbreviation", "Element symbol"),
    ("Mercury, Total", "Mercury", "SGS", 890, "qualifier", "With Total qualifier"),
    
    ("Cadmium", "Cadmium", "ALS", 580, "standard", "Standard naming"),
    ("Cd", "Cadmium", "ALS", 390, "abbreviation", "Element symbol"),
    ("Cadmium, Total", "Cadmium", "SGS", 720, "qualifier", "With Total qualifier"),
    
    ("Copper", "Copper", "ALS", 710, "standard", "Standard naming"),
    ("Cu", "Copper", "ALS", 620, "abbreviation", "Element symbol"),
    ("Copper, Total", "Copper", "SGS", 1120, "qualifier", "With Total qualifier"),
    
    ("Nickel", "Nickel", "ALS", 650, "standard", "Standard naming"),
    ("Ni", "Nickel", "ALS", 480, "abbreviation", "Element symbol"),
    ("Nickel, Total", "Nickel", "SGS", 980, "qualifier", "With Total qualifier"),
    
    ("Zinc", "Zinc", "ALS", 690, "standard", "Standard naming"),
    ("Zn", "Zinc", "ALS", 520, "abbreviation", "Element symbol"),
    ("Zinc, Total", "Zinc", "SGS", 1050, "qualifier", "With Total qualifier"),
    
    # PAHs
    ("Anthracene", "Anthracene", "ALS", 420, "standard", "Standard PAH"),
    ("Fluoranthene", "Fluoranthene", "ALS", 480, "standard", "Standard PAH"),
    ("Pyrene", "Pyrene", "ALS", 510, "standard", "Standard PAH"),
    ("Benzo(b)fluoranthene", "Benzo(b)fluoranthene", "ALS", 380, "standard", "Standard PAH"),
    ("B(b)F", "Benzo(b)fluoranthene", "ALS", 620, "abbreviation", "Common abbreviation"),
    ("Benzo(k)fluoranthene", "Benzo(k)fluoranthene", "ALS", 350, "standard", "Standard PAH"),
    ("B(k)F", "Benzo(k)fluoranthene", "ALS", 580, "abbreviation", "Common abbreviation"),
    ("Dibenz(a,h)anthracene", "Dibenz(a,h)anthracene", "ALS", 320, "standard", "Standard PAH"),
    ("Indeno(1,2,3-cd)pyrene", "Indeno(1,2,3-cd)pyrene", "ALS", 310, "standard", "Standard PAH"),
    ("Benzo(a)anthracene", "Benzo(a)anthracene", "ALS", 330, "standard", "Standard PAH"),
    ("B(a)A", "Benzo(a)anthracene", "ALS", 510, "abbreviation", "Common abbreviation"),
    ("Benzo(g,h,i)perylene", "Benzo(g,h,i)perylene", "ALS", 280, "standard", "Standard PAH"),
    
    # Phenols
    ("Phenol", "Phenol", "ALS", 520, "standard", "Standard naming"),
    ("2-Methylphenol", "2-Methylphenol", "ALS", 280, "standard", "Standard naming"),
    ("o-Cresol", "2-Methylphenol", "ALS", 420, "common", "Common name"),
    ("4-Methylphenol", "4-Methylphenol", "ALS", 260, "standard", "Standard naming"),
    ("p-Cresol", "4-Methylphenol", "ALS", 390, "common", "Common name"),
    ("Pentachlorophenol", "Pentachlorophenol", "ALS", 310, "standard", "Standard naming"),
    ("PCP", "Pentachlorophenol", "ALS", 580, "abbreviation", "Common abbreviation"),
    
    # Other aromatics
    ("Styrene", "Styrene", "ALS", 480, "standard", "Standard naming"),
    ("Vinylbenzene", "Styrene", "SGS", 45, "iupac", "IUPAC variant"),
    ("Isopropylbenzene", "Isopropylbenzene", "ALS", 220, "standard", "Standard naming"),
    ("Cumene", "Isopropylbenzene", "ALS", 380, "common", "Common name"),
    ("n-Butylbenzene", "n-Butylbenzene", "ALS", 180, "standard", "Standard naming"),
    ("Nitrobenzene", "Nitrobenzene", "ALS", 280, "standard", "Standard naming"),
    ("Hexachlorobenzene", "Hexachlorobenzene", "ALS", 240, "standard", "Standard naming"),
    ("HCB", "Hexachlorobenzene", "ALS", 420, "abbreviation", "Common abbreviation"),
    
    # Chlorobenzenes
    ("1,2,4-Trichlorobenzene", "1,2,4-Trichlorobenzene", "ALS", 280, "standard", "Standard naming"),
    ("1,2-Dichlorobenzene", "1,2-Dichlorobenzene", "ALS", 310, "standard", "Standard naming"),
    ("o-Dichlorobenzene", "1,2-Dichlorobenzene", "ALS", 280, "common", "Common name"),
    ("1,4-Dichlorobenzene", "1,4-Dichlorobenzene", "ALS", 340, "standard", "Standard naming"),
    ("p-Dichlorobenzene", "1,4-Dichlorobenzene", "ALS", 420, "common", "Common name"),
    
    # Additional PAHs
    ("Acenaphthene", "Acenaphthene", "ALS", 290, "standard", "Standard PAH"),
    ("Acenaphthylene", "Acenaphthylene", "ALS", 270, "standard", "Standard PAH"),
    ("Fluorene", "Fluorene", "ALS", 310, "standard", "Standard PAH"),
    ("Phenanthrene", "Phenanthrene", "ALS", 380, "standard", "Standard PAH"),
    ("Chrysene", "Chrysene", "ALS", 350, "standard", "Standard PAH"),
    
    # TPH
    ("TPH (C10-C25)", "Total Petroleum Hydrocarbons", "ALS", 680, "range", "Carbon range"),
    ("Total Petroleum Hydrocarbons", "Total Petroleum Hydrocarbons", "ALS", 420, "standard", "Full name"),
    ("TPH", "Total Petroleum Hydrocarbons", "ALS", 1250, "abbreviation", "Common abbreviation"),
]

# Create DataFrame
df = pd.DataFrame(variants_data, columns=[
    'observed_text', 'canonical_analyte_id', 'lab_vendor', 
    'frequency', 'variant_type', 'notes'
])

# Save to CSV
output_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher") / "data" / "training" / "ontario_variants_known.csv"
df.to_csv(output_path, index=False)

print(f"Created ontario_variants_known.csv with {len(df)} entries")
print(f"Saved to: {output_path}")
print(f"\nUnique canonical analytes: {df['canonical_analyte_id'].nunique()}")
print(f"Lab vendors: {df['lab_vendor'].unique().tolist()}")
print(f"\nVariant types:")
for vtype, count in df['variant_type'].value_counts().items():
    print(f"  {vtype}: {count}")
