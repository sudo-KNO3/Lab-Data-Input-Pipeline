"""Generate ontario_variants_known.csv with known lab naming patterns."""

import csv
from pathlib import Path

# Data for Ontario lab naming variants
variants_data = [
    ("observed_text", "canonical_analyte_id", "lab_vendor", "frequency", "variant_type", "notes"),
    # Benzene
    ("Benzene", "Benzene", "ALS", "850", "standard", "Standard naming"),
    ("Benezene", "Benzene", "ALS", "12", "typo", "Common typo"),
    ("Benzene ", "Benzene", "ALS", "45", "spacing", "Trailing space"),
    ("BENZENE", "Benzene", "SGS", "320", "case", "All caps style"),
    
    # Toluene
    ("Toluene", "Toluene", "ALS", "920", "standard", "Standard naming"),
    ("Toluenne", "Toluene", "ALS", "8", "typo", "Double 'n' typo"),
    ("Toluene ", "Toluene", "ALS", "38", "spacing", "Trailing space"),
    ("TOLUENE", "Toluene", "SGS", "415", "case", "All caps style"),
    
    # Trichloroethylene
    ("Trichloroethylene", "Trichloroethylene", "ALS", "650", "standard", "Full name"),
    ("TCE", "Trichloroethylene", "ALS", "1200", "abbreviation", "Common abbreviation"),
    ("Trichloroethene", "Trichloroethylene", "ALS", "85", "variant", "Alternative spelling"),
    ("Trichloroethylene (TCE)", "Trichloroethylene", "BureauVeritas", "450", "combined", "Name with abbreviation"),
    
    # 1,1,1-Trichloroethane
    ("1,1,1-Trichloroethane", "1,1,1-Trichloroethane", "ALS", "580", "standard", "Standard with commas"),
    ("1,1,1-TCA", "1,1,1-Trichloroethane", "ALS", "920", "abbreviation", "Common abbreviation"),
    ("1, 1, 1-TCA", "1,1,1-Trichloroethane", "ALS", "65", "spacing", "Spaces after commas"),
    ("1,1,1 TCA", "1,1,1-Trichloroethane", "SGS", "110", "spacing", "Space before TCA"),
    
    # 1,4-Dioxane
    ("1,4-Dioxane", "1,4-Dioxane", "ALS", "720", "standard", "Standard naming"),
    ("1,4 Diox", "1,4-Dioxane", "ALS", "180", "truncation", "Common lab truncation"),
    ("1,4-Dioxane ", "1,4-Dioxane", "ALS", "42", "spacing", "Trailing space"),
    ("Dioxane, 1,4-", "1,4-Dioxane", "SGS", "290", "inverted", "Inverted order"),
    
    # Benzo(a)pyrene
    ("Benzo(a)pyrene", "Benzo(a)pyrene", "ALS", "450", "standard", "Standard parenthesis"),
    ("B(a)P", "Benzo(a)pyrene", "ALS", "1850", "abbreviation", "Very common abbreviation"),
    ("BaP", "Benzo(a)pyrene", "SGS", "920", "abbreviation", "Short abbreviation"),
    ("Benzo[a]pyrene", "Benzo(a)pyrene", "ALS", "95", "bracket_variant", "Square brackets instead"),
    
    # Naphthalene
    ("Naphthalene", "Naphthalene", "ALS", "890", "standard", "Standard naming"),
    ("Naphthalene ", "Naphthalene", "ALS", "68", "spacing", "Trailing space"),
    ("NAPHTHALENE", "Naphthalene", "SGS", "410", "case", "All caps style"),
    
    # PHC F2
    ("PHC F2", "Petroleum Hydrocarbons F2", "ALS", "1500", "abbreviation", "Common PHC abbreviation"),
    ("PHC F2 (C10-C16)", "Petroleum Hydrocarbons F2", "ALS", "680", "detailed", "With carbon range"),
    ("Petroleum Hydrocarbons F2", "Petroleum Hydrocarbons F2", "ALS", "420", "standard", "Full name"),
    ("F2 Petroleum Hydrocarbons", "Petroleum Hydrocarbons F2", "SGS", "310", "inverted", "Inverted order"),
    
    # PHC F3
    ("PHC F3", "Petroleum Hydrocarbons F3", "ALS", "1450", "abbreviation", "Common PHC abbreviation"),
    ("PHC F3 (C16-C34)", "Petroleum Hydrocarbons F3", "ALS", "720", "detailed", "With carbon range"),
    ("Petroleum Hydrocarbons F3", "Petroleum Hydrocarbons F3", "ALS", "390", "standard", "Full name"),
    ("F3 Petroleum Hydrocarbons", "Petroleum Hydrocarbons F3", "SGS", "285", "inverted", "Inverted order"),
    
    # Chromium
    ("Chromium", "Chromium", "ALS", "680", "standard", "Standard naming"),
    ("Cr", "Chromium", "ALS", "420", "abbreviation", "Element symbol"),
    ("Chromium, Total", "Chromium", "SGS", "1650", "qualifier", "With Total qualifier"),
    ("Chromium (Total)", "Chromium", "SGS", "890", "qualifier", "Parenthetical Total"),
    
    # Lead
    ("Lead", "Lead", "ALS", "720", "standard", "Standard naming"),
    ("Pb", "Lead", "ALS", "580", "abbreviation", "Element symbol"),
    ("Lead, Total", "Lead", "SGS", "1720", "qualifier", "With Total qualifier"),
    ("Lead (Total)", "Lead", "SGS", "950", "qualifier", "Parenthetical Total"),
    
    # Arsenic
    ("Arsenic", "Arsenic", "ALS", "690", "standard", "Standard naming"),
    ("As", "Arsenic", "ALS", "510", "abbreviation", "Element symbol"),
    ("Arsenic, Total", "Arsenic", "SGS", "1580", "qualifier", "With Total qualifier"),
    ("Arsenic (Total)", "Arsenic", "SGS", "880", "qualifier", "Parenthetical Total"),
    
    # Additional common compounds
    ("TCE", "Trichloroethylene", "SGS", "890", "abbreviation", "Common abbreviation"),
    ("PCE", "Tetrachloroethene", "ALS", "1450", "abbreviation", "Very common abbreviation"),
    ("DCE", "Dichloroethene", "ALS", "420", "abbreviation", "Common abbreviation"),
    ("Chloroform", "Chloroform", "ALS", "720", "standard", "Standard naming"),
    ("Methylene Chloride", "Methylene Chloride", "ALS", "620", "standard", "Standard naming"),
    ("DCM", "Methylene Chloride", "ALS", "380", "abbreviation", "Common abbreviation"),
    ("MEK", "Methyl Ethyl Ketone", "ALS", "1100", "abbreviation", "Common abbreviation"),
    ("Acetone", "Acetone", "ALS", "950", "standard", "Standard naming"),
    ("Xylenes", "Xylene", "ALS", "920", "plural", "Plural form"),
    ("Ethylbenzene", "Ethylbenzene", "ALS", "820", "standard", "Standard naming"),
    
    # PAHs
    ("Anthracene", "Anthracene", "ALS", "420", "standard", "Standard PAH"),
    ("Fluoranthene", "Fluoranthene", "ALS", "480", "standard", "Standard PAH"),
    ("Pyrene", "Pyrene", "ALS", "510", "standard", "Standard PAH"),
    ("B(b)F", "Benzo(b)fluoranthene", "ALS", "620", "abbreviation", "Common abbreviation"),
    ("B(k)F", "Benzo(k)fluoranthene", "ALS", "580", "abbreviation", "Common abbreviation"),
    
    # TPH
    ("TPH", "Total Petroleum Hydrocarbons", "ALS", "1250", "abbreviation", "Common abbreviation"),
    ("Total Petroleum Hydrocarbons", "Total Petroleum Hydrocarbons", "ALS", "420", "standard", "Full name"),
]

# Save to CSV
output_path = Path(r"n:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher") / "data" / "training" / "ontario_variants_known.csv"

with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(variants_data)

print(f"Created ontario_variants_known.csv with {len(variants_data)-1} entries")
print(f"Saved to: {output_path}")
