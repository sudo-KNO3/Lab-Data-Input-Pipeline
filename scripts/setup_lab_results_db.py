"""
Set up lab_results.db schema for production Excel learning system.

This database tracks:
- Every lab file submitted (with archive)
- All extracted data
- Validation history
- Learned templates
- Extraction errors for continuous improvement
"""
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/lab_results.db")

def create_schema(conn: sqlite3.Connection):
    """Create all tables for lab results tracking."""
    
    # Track every file submitted
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_submissions (
            submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            file_hash TEXT UNIQUE NOT NULL,
            original_filename TEXT NOT NULL,
            lab_vendor TEXT,
            received_date DATE DEFAULT CURRENT_DATE,
            file_size_bytes INTEGER,
            sheet_name TEXT,
            
            -- Extraction metadata
            extraction_timestamp DATETIME,
            extraction_version TEXT DEFAULT '1.0.0',
            layout_confidence FLOAT,
            template_id TEXT,
            
            -- Validation status
            validation_status TEXT DEFAULT 'pending',
            validated_by TEXT,
            validated_at DATETIME,
            extraction_accuracy FLOAT,
            
            -- Learning status
            used_for_training BOOLEAN DEFAULT 0,
            ground_truth_quality FLOAT,
            
            -- Timestamps
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Track every chemical extraction
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER NOT NULL,
            
            -- Extraction location
            row_number INTEGER,
            column_number INTEGER,
            
            -- Chemical identification
            chemical_raw TEXT NOT NULL,
            chemical_normalized TEXT,
            analyte_id TEXT,
            
            -- Matching metadata
            match_method TEXT,
            match_confidence FLOAT,
            match_alternatives TEXT,
            
            -- Sample data
            sample_id TEXT,
            result_value TEXT,
            units TEXT,
            qualifier TEXT,
            detection_limit TEXT,
            lab_method TEXT,
            
            -- Validation tracking
            validation_status TEXT DEFAULT 'pending',
            human_override BOOLEAN DEFAULT 0,
            correct_analyte_id TEXT,
            validation_notes TEXT,
            
            -- Timestamps
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (submission_id) REFERENCES lab_submissions(submission_id)
        )
    """)
    
    # Learned templates from validated files
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learned_templates (
            template_id TEXT PRIMARY KEY,
            vendor TEXT NOT NULL,
            template_name TEXT NOT NULL,
            
            -- Pattern fingerprint (stored as JSON)
            structure_fingerprint TEXT NOT NULL,
            header_row_range TEXT,
            data_start_row_pattern TEXT,
            chemical_column_indicators TEXT,
            sample_column_pattern TEXT,
            metadata_patterns TEXT,
            
            -- Training metadata
            learned_from_files TEXT,
            num_training_examples INTEGER DEFAULT 1,
            confidence_score FLOAT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Performance metrics
            avg_extraction_accuracy FLOAT,
            false_positive_rate FLOAT,
            files_matched INTEGER DEFAULT 0,
            
            -- Additional metadata
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Track extraction errors for learning
    conn.execute("""
        CREATE TABLE IF NOT EXISTS extraction_errors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER NOT NULL,
            error_type TEXT NOT NULL,
            
            -- Error details
            expected_value TEXT,
            extracted_value TEXT,
            row_number INTEGER,
            column_number INTEGER,
            error_metadata TEXT,
            
            -- Resolution tracking
            resolved_at DATETIME,
            resolution_method TEXT,
            resolution_notes TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (submission_id) REFERENCES lab_submissions(submission_id)
        )
    """)
    
    # Create indexes for performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_submissions_hash ON lab_submissions(file_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_submissions_vendor ON lab_submissions(lab_vendor)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_submissions_status ON lab_submissions(validation_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_results_submission ON lab_results(submission_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_results_analyte ON lab_results(analyte_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_results_status ON lab_results(validation_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_errors_submission ON extraction_errors(submission_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_templates_vendor ON learned_templates(vendor)")
    
    conn.commit()


def main():
    """Initialize the lab_results database."""
    print("SETTING UP LAB_RESULTS.DB")
    print("=" * 80)
    
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if database already exists
    db_exists = DB_PATH.exists()
    
    if db_exists:
        print(f"\nDatabase already exists at: {DB_PATH}")
        response = input("Do you want to recreate it? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            return
        DB_PATH.unlink()
        print("Deleted existing database.")
    
    # Create database and schema
    print(f"\nCreating database at: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    
    try:
        create_schema(conn)
        
        # Verify tables created
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        
        print("\nTables created:")
        for (table_name,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  ✓ {table_name} ({count} rows)")
        
        # Check indexes
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        
        print(f"\nIndexes created: {len(indexes)}")
        
        print("\n" + "=" * 80)
        print("✓ SUCCESS: Database schema initialized")
        print(f"\nDatabase location: {DB_PATH.absolute()}")
        print("\nNext steps:")
        print("  1. Run scripts/20_ingest_lab_file.py to archive and extract files")
        print("  2. Run scripts/21_validate_extraction.py to review extractions")
        print("  3. Run scripts/23_retrain_from_validated.py to improve templates")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
