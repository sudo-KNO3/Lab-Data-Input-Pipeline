#!/usr/bin/env python3
"""Check methylnaphthalene entries."""

import sqlite3

conn = sqlite3.connect('data/reg153_matcher.db')

print('Methylnaphthalene analytes:')
results = conn.execute("""
    SELECT a.analyte_id, a.preferred_name, a.analyte_type 
    FROM analytes a 
    JOIN synonyms s ON a.analyte_id = s.analyte_id 
    WHERE s.synonym_raw LIKE '%methylnaphthalene%' 
    GROUP BY a.analyte_id 
    ORDER BY a.analyte_id
""").fetchall()

for r in results:
    print(f'  {r[0]:20s} {r[1]:45s} ({r[2]})')

print('\nAll synonyms containing "methylnaphthalene":')
syns = conn.execute("""
    SELECT analyte_id, synonym_raw, synonym_type, harvest_source
    FROM synonyms 
    WHERE synonym_raw LIKE '%methylnaphthalene%' 
    ORDER BY analyte_id, synonym_raw
""").fetchall()

for s in syns:
    print(f'  {s[0]:20s} {s[1]:50s} ({s[2]}, {s[3]})')

conn.close()
