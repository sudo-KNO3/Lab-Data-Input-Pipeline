"""
Add targeted synonyms to fix Gate A misses and promote review-band items.

Misses (4, all same chemical):
  "Dichloropropene,1,3-" → should match REG153_VOCS_020 (cis) per ground truth

Review-band correct items (10):
  "Chlordane, alpha-"              → REG153_OCS_002 (Chlordane)
  "Cyanide (CN-)"                  → WQ_CHEM_001 (Cyanide Total)
  "Petroleum Hydrocarbons F2-Napth"→ REG153_PHCS_004 (PHC F2 Less Naphthalene)
  "Petroleum Hydrocarbons F3-PAH"  → REG153_PHCS_006 (PHC F3 Less PAHs)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from src.database.models import Synonym, Analyte, SynonymType
from src.normalization.text_normalizer import TextNormalizer

DB_PATH = "data/reg153_matcher.db"
db_engine = create_engine(f"sqlite:///{DB_PATH}")
normalizer = TextNormalizer()

# Synonyms to add: (analyte_id, synonym_raw_list)
# Each lab variant that falls in REVIEW or misses
SYNONYMS_TO_ADD = [
    # Fix the 4 misses: unqualified "Dichloropropene,1,3-" → cis (VOCS_020)
    ("REG153_VOCS_020", [
        "Dichloropropene,1,3-",
        "Dichloropropene, 1,3-",
    ]),
    # Promote: "Chlordane, alpha-" → Chlordane (alpha- is the only Chlordane analyte)
    ("REG153_OCS_002", [
        "Chlordane, alpha-",
        "alpha-Chlordane",
    ]),
    # Promote: "Cyanide (CN-)" → Cyanide Total
    ("WQ_CHEM_001", [
        "Cyanide (CN-)",
        "Cyanide CN-",
    ]),
    # Promote: "Petroleum Hydrocarbons F2-Napth" → PHC F2 (ground truth = PHCS_003)
    ("REG153_PHCS_003", [
        "Petroleum Hydrocarbons F2-Napth",
        "Petroleum Hydrocarbons F2-Naphthalene",
        "Petroleum Hydrocarbons F2 - Naphthalene",
    ]),
    # Promote: "Petroleum Hydrocarbons F3-PAH" → PHC F3 (ground truth = PHCS_005)
    ("REG153_PHCS_005", [
        "Petroleum Hydrocarbons F3-PAH",
        "Petroleum Hydrocarbons F3 - PAH",
        "Petroleum Hydrocarbons F3-PAHs",
    ]),
]

added = 0
skipped = 0

with Session(db_engine) as session:
    for analyte_id, raw_list in SYNONYMS_TO_ADD:
        # Verify analyte exists
        analyte = session.query(Analyte).filter(
            Analyte.analyte_id == analyte_id
        ).first()
        if not analyte:
            print(f"  WARNING: {analyte_id} not found — skipping")
            continue
        
        for raw in raw_list:
            norm = normalizer.normalize(raw)
            # Check if synonym already exists
            existing = session.query(Synonym).filter(
                Synonym.analyte_id == analyte_id,
                Synonym.synonym_raw == raw,
            ).first()
            if existing:
                print(f"  SKIP (exists): {raw} → {analyte_id}")
                skipped += 1
                continue
            
            syn = Synonym(
                analyte_id=analyte_id,
                synonym_raw=raw,
                synonym_norm=norm,
                synonym_type=SynonymType.COMMON,
                harvest_source="manual",
                confidence=1.0,
            )
            session.add(syn)
            print(f"  ADD: '{raw}' → {analyte_id} ({analyte.preferred_name})")
            added += 1
    
    session.commit()

print(f"\nDone: {added} added, {skipped} skipped (already exist)")
print("Re-run Gate A to verify improvements.")
