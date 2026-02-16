#!/usr/bin/env python3
"""Check if skipped parameters actually exist in database."""

import sqlite3

conn = sqlite3.connect('data/reg153_matcher.db')

# List of parameters user is skipping
skipped_params = [
    "Phosphorus (Total)",
    "Turbidity",
    "Total Suspended Solids",
    "Total Organic Carbon",
    "Total Kjeldahl Nitrogen",
    "Aluminum",
    "Zinc",
    "Nickel",
    "Manganese",
    "pH",
    "Conductivity",
    "Hardness",
    "BOD5",
    "COD",
    "Chloride",
    "Sulphate",
    "Fluoride",
    "Cation Sum",
    "Sodium",
    "Potassium",
    "Magnesium",
    "Vanadium",
    "Uranium",
    "Titanium",
    "Tin",
    "Thallium",
    "Strontium",
    "Silver",
    "Selenium",
    "Molybdenum",
    "Copper",
    "Cobalt",
    "Chromium (VI)"
]

print("Checking if skipped parameters exist in database:\n")

for param in skipped_params:
    # Check if synonym exists
    result = conn.execute("""
        SELECT s.analyte_id, a.preferred_name, s.synonym_type, s.harvest_source
        FROM synonyms s
        JOIN analytes a ON s.analyte_id = a.analyte_id
        WHERE s.synonym_raw LIKE ?
        LIMIT 1
    """, (f"%{param}%",)).fetchone()
    
    if result:
        print(f"✓ {param:40s} -> {result[0]:20s} {result[1]}")
    else:
        # Try normalized
        result2 = conn.execute("""
            SELECT s.analyte_id, a.preferred_name
            FROM synonyms s
            JOIN analytes a ON s.analyte_id = a.analyte_id
            WHERE s.synonym_norm LIKE ?
            LIMIT 1
        """, (f"%{param.lower()}%",)).fetchone()
        
        if result2:
            print(f"~ {param:40s} -> {result2[0]:20s} {result2[1]} (norm match)")
        else:
            print(f"✗ {param:40s} NOT FOUND")

conn.close()
