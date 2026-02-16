"""
Import validated corrections from Excel workbook back into system.

This script:
1. Reads the filled-out validation workbook
2. Updates lab_results with human corrections
3. Ingests new synonyms into reg153_matcher.db
4. Tracks extraction errors for template learning
5. Marks submission as validated
6. Triggers retraining if enough validations accumulated
"""
import sys
sys.path.insert(0, '.')

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import argparse
from datetime import datetime
import re

from src.database.connection import DatabaseManager
from src.learning.synonym_ingestion import SynonymIngestor
from src.normalization.text_normalizer import TextNormalizer


class ValidationImporter:
    """Import and process human validations from Excel."""
    
    def __init__(self):
        self.db = DatabaseManager('data/reg153_matcher.db')
        self.lab_db_path = Path('data/lab_results.db')
        self.normalizer = TextNormalizer()
        self.synonym_ingestor = SynonymIngestor()
        
        self.stats = {
            'total_reviewed': 0,
            'corrections_made': 0,
            'synonyms_added': 0,
            'errors_logged': 0,
            'auto_accepted': 0
        }
    
    def import_validations(
        self,
        excel_path: Path,
        validated_by: str = "user"
    ) -> bool:
        """
        Import validations from Excel workbook.
        
        Args:
            excel_path: Path to filled validation workbook
            validated_by: Username of validator
            
        Returns:
            True if successful
        """
        print(f"Importing validations from: {excel_path}")
        
        if not excel_path.exists():
            print(f"✗ File not found: {excel_path}")
            return False
        
        # Read the validation sheet
        try:
            df = pd.read_excel(
                excel_path,
                sheet_name="🔬 Chemical Review",
                header=0
            )
        except Exception as e:
            print(f"✗ Error reading Excel file: {e}")
            return False
        
        # Extract submission ID from filename or read from first row
        submission_id = self._extract_submission_id(excel_path, df)
        
        if not submission_id:
            print("✗ Could not determine submission ID")
            return False
        
        print(f"\nProcessing submission {submission_id}...")
        print(f"Rows in workbook: {len(df)}")
        
        # Process each row
        lab_conn = sqlite3.connect(str(self.lab_db_path))
        
        with self.db.get_session() as session:
            for idx, row in df.iterrows():
                self._process_validation_row(
                    row, submission_id, lab_conn, session
                )
        
        # Mark submission as validated
        self._finalize_submission(
            submission_id, validated_by, lab_conn
        )
        
        lab_conn.commit()
        lab_conn.close()
        
        # Check if retraining needed
        self._check_retraining_trigger()
        
        return True
    
    def _extract_submission_id(self, excel_path: Path, df: pd.DataFrame) -> Optional[int]:
        """Extract submission ID from filename."""
        # Try to parse from filename: validation_123_20260213.xlsx
        match = re.search(r'validation_(\d+)_', excel_path.name)
        if match:
            return int(match.group(1))
        
        # Could also store in a hidden cell in Excel
        return None
    
    def _process_validation_row(
        self,
        row: pd.Series,
        submission_id: int,
        lab_conn: sqlite3.Connection,
        session
    ):
        """Process a single validation row."""
        self.stats['total_reviewed'] += 1
        
        row_num = row.get('Row')
        chem_raw = row.get('Chemical Name\n(From Excel)')
        matched_to = row.get('Matched To')
        corrected_match = row.get('Corrected Match')
        validation_notes = row.get('Validation Notes')
        confidence = row.get('Confidence')
        status = row.get('Status')
        
        # Skip if no data
        if pd.isna(chem_raw):
            return
        
        # Parse confidence
        try:
            if confidence and isinstance(confidence, str) and '%' in confidence:
                conf_val = float(confidence.strip('%')) / 100
            else:
                conf_val = None
        except:
            conf_val = None
        
        # Determine if correction was made
        correction_made = pd.notna(corrected_match) and str(corrected_match).strip()
        
        if correction_made:
            # User provided correction
            self.stats['corrections_made'] += 1
            
            # Parse analyte_id from "Name (ID)" format
            correct_analyte_id = self._parse_analyte_id(corrected_match)
            
            if correct_analyte_id:
                # Update lab_results with correction
                lab_conn.execute("""
                    UPDATE lab_results
                    SET correct_analyte_id = ?,
                        human_override = 1,
                        validation_status = 'validated',
                        validation_notes = ?
                    WHERE submission_id = ? AND row_number = ?
                """, (correct_analyte_id, validation_notes, submission_id, row_num))
                
                # Add synonym to matcher database
                try:
                    added = self.synonym_ingestor.ingest_validated_synonym(
                        raw_text=chem_raw,
                        analyte_id=correct_analyte_id,
                        db_session=session
                    )
                    if added:
                        self.stats['synonyms_added'] += 1
                except Exception as e:
                    print(f"  ⚠ Could not add synonym: {e}")
                
                # Log extraction error
                original_analyte_id = self._parse_analyte_id(matched_to) if pd.notna(matched_to) else None
                if original_analyte_id != correct_analyte_id:
                    lab_conn.execute("""
                        INSERT INTO extraction_errors (
                            submission_id, error_type,
                            expected_value, extracted_value,
                            row_number, error_metadata,
                            resolution_method, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        submission_id, 'chemical_mismatch',
                        correct_analyte_id, original_analyte_id,
                        row_num,
                        f'{{"confidence": {conf_val}, "raw_text": "{chem_raw}"}}',
                        'human_correction', datetime.now()
                    ))
                    self.stats['errors_logged'] += 1
        
        else:
            # No correction = user accepted auto-match
            self.stats['auto_accepted'] += 1
            
            # Mark as validated (correct)
            lab_conn.execute("""
                UPDATE lab_results
                SET validation_status = 'validated',
                    validation_notes = ?
                WHERE submission_id = ? AND row_number = ?
            """, (validation_notes, submission_id, row_num))
    
    def _parse_analyte_id(self, text: str) -> Optional[str]:
        """Parse analyte ID from 'Name (ID)' format.
        
        Handles formats:
        - "Chemical Name (REG153_XXX_001)"
        - "REG153_XXX_001"
        - Malformed or corrupted data
        """
        if not text or pd.isna(text):
            return None
        
        # Convert to string and strip whitespace
        text = str(text).strip()
        
        if not text:
            return None
        
        # Match pattern: "Something (ANALYTE_ID)" - most common case
        match = re.search(r'\(([A-Z0-9_]+)\)\s*$', text)
        if match:
            return match.group(1)
        
        # Check if entire text is just an analyte ID (fallback)
        if re.match(r'^[A-Z0-9_]+$', text):
            return text
        
        return None
    
    def _finalize_submission(
        self,
        submission_id: int,
        validated_by: str,
        lab_conn: sqlite3.Connection
    ):
        """Mark submission as validated and calculate accuracy."""
        
        # Calculate extraction accuracy
        total = lab_conn.execute("""
            SELECT COUNT(*) FROM lab_results
            WHERE submission_id = ?
        """, (submission_id,)).fetchone()[0]
        
        correct = lab_conn.execute("""
            SELECT COUNT(*) FROM lab_results
            WHERE submission_id = ?
              AND (human_override = 0 OR correct_analyte_id IS NOT NULL)
        """, (submission_id,)).fetchone()[0]
        
        accuracy = correct / total if total > 0 else 0.0
        
        # Update submission
        lab_conn.execute("""
            UPDATE lab_submissions
            SET validation_status = 'validated',
                validated_by = ?,
                validated_at = ?,
                extraction_accuracy = ?,
                used_for_training = 1,
                ground_truth_quality = ?,
                updated_at = ?
            WHERE submission_id = ?
        """, (
            validated_by,
            datetime.now(),
            accuracy,
            1.0,  # High quality since human validated
            datetime.now(),
            submission_id
        ))
        
        print(f"\n✓ Submission {submission_id} marked as validated")
        print(f"  Extraction accuracy: {accuracy:.1%}")
    
    def _check_retraining_trigger(self):
        """Check if enough validations accumulated to trigger retraining."""
        lab_conn = sqlite3.connect(str(self.lab_db_path))
        
        # Count validated files not yet used for training update
        pending_training = lab_conn.execute("""
            SELECT COUNT(*) FROM lab_submissions
            WHERE validation_status = 'validated'
              AND used_for_training = 0
        """).fetchone()[0]
        
        lab_conn.close()
        
        print(f"\nValidated files pending training: {pending_training}")
        
        if pending_training >= 10:
            print("\n💡 RECOMMENDATION: Run template retraining")
            print("   python scripts/23_retrain_from_validated.py")
        elif pending_training >= 5:
            print("\n💡 Consider running template retraining soon")
    
    def print_summary(self):
        """Print import summary."""
        print("\n" + "=" * 80)
        print("IMPORT SUMMARY")
        print("=" * 80)
        print(f"\nTotal rows reviewed:     {self.stats['total_reviewed']}")
        print(f"✓ Auto-accepted:         {self.stats['auto_accepted']}")
        print(f"✏ Corrections made:      {self.stats['corrections_made']}")
        print(f"➕ Synonyms added:       {self.stats['synonyms_added']}")
        print(f"📝 Errors logged:        {self.stats['errors_logged']}")
        
        if self.stats['synonyms_added'] > 0:
            print(f"\n🎓 System learned {self.stats['synonyms_added']} new chemical name variants!")


def main():
    parser = argparse.ArgumentParser(
        description="Import validated corrections from Excel workbook"
    )
    parser.add_argument(
        '--file',
        type=Path,
        required=True,
        help='Path to filled validation workbook'
    )
    parser.add_argument(
        '--validated-by',
        type=str,
        default='user',
        help='Username of validator (default: user)'
    )
    
    args = parser.parse_args()
    
    print("IMPORTING VALIDATIONS")
    print("=" * 80)
    
    importer = ValidationImporter()
    
    success = importer.import_validations(
        excel_path=args.file,
        validated_by=args.validated_by
    )
    
    if success:
        importer.print_summary()
        print("\n✓ SUCCESS: Validations imported and system updated")
    else:
        print("\n✗ FAILED to import validations")
        sys.exit(1)


if __name__ == "__main__":
    main()
