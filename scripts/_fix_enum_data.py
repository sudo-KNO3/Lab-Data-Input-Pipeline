"""One-time data fix: correct invalid enum values in the database."""
import sqlite3

conn = sqlite3.connect("data/reg153_matcher.db")

# Check and fix analyte_type = 'chemical'
rows = conn.execute(
    "SELECT analyte_id, preferred_name, analyte_type FROM analytes WHERE analyte_type = 'chemical'"
).fetchall()
print(f"Analytes with 'chemical' type: {len(rows)}")
for r in rows:
    print(f"  {r[0]}: {r[1]}")
conn.execute("UPDATE analytes SET analyte_type = 'SINGLE_SUBSTANCE' WHERE analyte_type = 'chemical'")

# Check and fix synonym_type = 'common_name'
bad_syns = conn.execute(
    "SELECT COUNT(*) FROM synonyms WHERE synonym_type = 'common_name'"
).fetchone()[0]
print(f"Synonyms with 'common_name' type: {bad_syns}")
conn.execute("UPDATE synonyms SET synonym_type = 'COMMON' WHERE synonym_type = 'common_name'")

conn.commit()
print("Fixed.")
conn.close()
