"""
Script 18: Vendor Micro-Controller Database Migration

Applies schema changes for the vendor-conditioned prior layer:
- Adds lab_vendor, normalization_version to synonyms
- Adds last_seen_date, collision_count, last_collision_date, normalization_version to lab_variants
- Adds UNIQUE constraint on lab_variants(lab_vendor, observed_text)
- Creates lab_variant_confirmations table with indexes

Safe to run multiple times (idempotent: checks column/table existence before altering).

Usage:
    python scripts/18_vendor_migration.py
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path(__file__).parent.parent / "data" / "reg153_matcher.db"


def column_exists(cursor, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def table_exists(cursor, table: str) -> bool:
    """Check if a table exists."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cursor.fetchone() is not None


def index_exists(cursor, index_name: str) -> bool:
    """Check if an index exists."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
    return cursor.fetchone() is not None


def migrate(db_path: Path = DB_PATH):
    """Run all vendor micro-controller migrations."""
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        sys.exit(1)
    
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    changes = 0
    
    print(f"Migrating: {db_path}")
    print(f"{'='*60}")
    
    # ── 1. synonyms: add lab_vendor ────────────────────────────────
    if not column_exists(cur, "synonyms", "lab_vendor"):
        cur.execute("ALTER TABLE synonyms ADD COLUMN lab_vendor VARCHAR(100)")
        print("  + Added synonyms.lab_vendor")
        changes += 1
    else:
        print("  . synonyms.lab_vendor already exists")
    
    # ── 2. synonyms: add normalization_version ─────────────────────
    if not column_exists(cur, "synonyms", "normalization_version"):
        cur.execute("ALTER TABLE synonyms ADD COLUMN normalization_version INTEGER DEFAULT 1")
        print("  + Added synonyms.normalization_version")
        changes += 1
    else:
        print("  . synonyms.normalization_version already exists")
    
    # ── 3. synonyms: composite index ───────────────────────────────
    if not index_exists(cur, "ix_synonyms_vendor_norm"):
        cur.execute("CREATE INDEX ix_synonyms_vendor_norm ON synonyms(lab_vendor, synonym_norm)")
        print("  + Created index ix_synonyms_vendor_norm")
        changes += 1
    else:
        print("  . ix_synonyms_vendor_norm already exists")
    
    # ── 4. lab_variants: add last_seen_date ────────────────────────
    if not column_exists(cur, "lab_variants", "last_seen_date"):
        cur.execute("ALTER TABLE lab_variants ADD COLUMN last_seen_date DATE")
        print("  + Added lab_variants.last_seen_date")
        changes += 1
    else:
        print("  . lab_variants.last_seen_date already exists")
    
    # ── 5. lab_variants: add collision_count ───────────────────────
    if not column_exists(cur, "lab_variants", "collision_count"):
        cur.execute("ALTER TABLE lab_variants ADD COLUMN collision_count INTEGER DEFAULT 0 NOT NULL")
        print("  + Added lab_variants.collision_count")
        changes += 1
    else:
        print("  . lab_variants.collision_count already exists")
    
    # ── 6. lab_variants: add last_collision_date ───────────────────
    if not column_exists(cur, "lab_variants", "last_collision_date"):
        cur.execute("ALTER TABLE lab_variants ADD COLUMN last_collision_date DATE")
        print("  + Added lab_variants.last_collision_date")
        changes += 1
    else:
        print("  . lab_variants.last_collision_date already exists")
    
    # ── 7. lab_variants: add normalization_version ─────────────────
    if not column_exists(cur, "lab_variants", "normalization_version"):
        cur.execute("ALTER TABLE lab_variants ADD COLUMN normalization_version INTEGER DEFAULT 1 NOT NULL")
        print("  + Added lab_variants.normalization_version")
        changes += 1
    else:
        print("  . lab_variants.normalization_version already exists")
    
    # ── 8. lab_variants: UNIQUE index on (lab_vendor, observed_text)
    if not index_exists(cur, "uq_lab_variant_vendor_text"):
        cur.execute("CREATE UNIQUE INDEX uq_lab_variant_vendor_text ON lab_variants(lab_vendor, observed_text)")
        print("  + Created UNIQUE index uq_lab_variant_vendor_text")
        changes += 1
    else:
        print("  . uq_lab_variant_vendor_text already exists")
    
    # ── 9. lab_variant_confirmations table ─────────────────────────
    if not table_exists(cur, "lab_variant_confirmations"):
        cur.execute("""
            CREATE TABLE lab_variant_confirmations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                variant_id INTEGER NOT NULL REFERENCES lab_variants(id) ON DELETE CASCADE,
                submission_id VARCHAR(100) NOT NULL,
                confirmed_analyte_id VARCHAR(50) NOT NULL REFERENCES analytes(analyte_id),
                confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                valid_for_consensus BOOLEAN DEFAULT 1,
                UNIQUE(variant_id, submission_id)
            )
        """)
        print("  + Created table lab_variant_confirmations")
        changes += 1
        
        # Indexes
        cur.execute("CREATE INDEX ix_lvc_variant_confirmed_at ON lab_variant_confirmations(variant_id, confirmed_at)")
        cur.execute("CREATE INDEX ix_lvc_confirmed_analyte_id ON lab_variant_confirmations(confirmed_analyte_id)")
        cur.execute("CREATE INDEX ix_lvc_variant_analyte ON lab_variant_confirmations(variant_id, confirmed_analyte_id)")
        print("  + Created 3 indexes on lab_variant_confirmations")
        changes += 3
    else:
        print("  . lab_variant_confirmations table already exists")
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"Migration complete: {changes} changes applied")
    return changes


if __name__ == "__main__":
    migrate()
