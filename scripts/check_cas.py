#!/usr/bin/env python3
"""Check what CAS 71-55-6 is."""

import sqlite3

conn = sqlite3.connect('data/reg153_matcher.db')

# Check by CAS number
result = conn.execute(
    "SELECT analyte_id, preferred_name, cas_number FROM analytes WHERE cas_number = ?",
    ('71-55-6',)
).fetchone()

if result:
    print(f"CAS 71-55-6 in database:")
    print(f"  ID: {result[0]}")
    print(f"  Name: {result[1]}")
    print(f"  CAS: {result[2]}")
else:
    print("CAS 71-55-6 NOT found in database")
    print("\nSearching for Trichloroethane...")
    results = conn.execute(
        "SELECT analyte_id, preferred_name, cas_number FROM analytes"
    ).fetchall()
    tce_results = [r for r in results if 'trichloroethane' in r[1].lower()]
    for r in tce_results:
        print(f"  {r[0]}: {r[1]} (CAS: {r[2]})")

conn.close()
