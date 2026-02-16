"""
Script 21B: Terminal-based Interactive Validation

Validate chemical extractions directly in PowerShell/terminal.
No Excel needed - fast, keyboard-driven workflow.

Usage:
    python scripts/21_validate_interactive.py --submission-id 2
    python scripts/21_validate_interactive.py --submission-id 2 --auto-accept-confident
"""
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.database.models import Analyte, MatchDecision
from src.database.crud import (
    get_or_create_lab_variant,
    create_lab_variant_confirmation,
    check_variant_collision,
    increment_lab_variant_frequency,
)
from src.normalization.text_normalizer import TextNormalizer
from src.matching.resolution_engine import ResolutionEngine
from src.learning.synonym_ingestion import SynonymIngestor
import hashlib


class InteractiveValidator:
    """Terminal-based validation interface."""
    
    def __init__(self, lab_db_path: str = "data/lab_results.db"):
        self.lab_db_path = lab_db_path
        self.db = DatabaseManager()
        self.normalizer = TextNormalizer()
        self.synonym_ingestor = SynonymIngestor()
        
        self.stats = {
            'total': 0,
            'auto_accepted': 0,
            'reviewed': 0,
            'corrected': 0,
            'skipped': 0,
            'synonyms_added': 0
        }
    
    def get_submission_info(self, submission_id: int) -> Optional[dict]:
        """Get submission details."""
        conn = sqlite3.connect(self.lab_db_path)
        row = conn.execute("""
            SELECT original_filename, lab_vendor, extraction_timestamp, layout_confidence
            FROM lab_submissions
            WHERE submission_id = ?
        """, (submission_id,)).fetchone()
        conn.close()
        
        if not row:
            return None
        
        return {
            'filename': row[0],
            'vendor': row[1],
            'timestamp': row[2],
            'layout_confidence': row[3]
        }
    
    def get_results_to_review(
        self, 
        submission_id: int,
        confidence_threshold: float = 0.95
    ) -> List[Tuple]:
        """Get extraction results that need review."""
        conn = sqlite3.connect(self.lab_db_path)
        results = conn.execute("""
            SELECT 
                result_id, row_number, chemical_raw, chemical_normalized,
                analyte_id, match_method, match_confidence,
                sample_id, result_value, units
            FROM lab_results
            WHERE submission_id = ?
            AND (validation_status = 'pending' OR validation_status IS NULL)
            ORDER BY 
                CASE 
                    WHEN match_confidence < 0.70 THEN 1
                    WHEN match_confidence < ? THEN 2
                    ELSE 3
                END,
                row_number
        """, (submission_id, confidence_threshold)).fetchall()
        conn.close()
        
        return results
    
    def get_top_suggestions(self, chem_normalized: str, top_n: int = 5,
                            vendor: Optional[str] = None) -> Tuple[List[dict], object]:
        """Get top N match suggestions and the full resolution result."""
        with self.db.get_session() as session:
            resolver = ResolutionEngine(session, self.normalizer)
            result = resolver.resolve(chem_normalized, confidence_threshold=0.50,
                                      vendor=vendor)
            
            suggestions = []
            for match in result.all_candidates[:top_n]:
                suggestions.append({
                    'analyte_id': match.analyte_id,
                    'name': match.preferred_name,
                    'confidence': match.confidence,
                    'method': match.method
                })
            
            return suggestions, result
    
    def display_chemical_review(
        self,
        result: Tuple,
        suggestions: List[dict],
        index: int,
        total: int
    ):
        """Display chemical for review with suggestions."""
        (result_id, row_num, chem_raw, chem_norm, analyte_id, 
         match_method, match_conf, sample_id, result_val, units) = result
        
        # Color coding
        if match_conf and match_conf >= 0.95:
            status = "+ CONFIDENT"
            color = "Green"
        elif match_conf and match_conf >= 0.70:
            status = "~ REVIEW"
            color = "Yellow"
        else:
            status = "- LOW CONFIDENCE"
            color = "Red"
        
        print(f"\n{'='*80}")
        print(f"Chemical {index}/{total} (Row {row_num})")
        print(f"{'='*80}")
        print(f"\n  Original: {chem_raw}")
        print(f"  Normalized: {chem_norm}")
        
        if sample_id or result_val:
            print(f"  Context: Sample={sample_id}, Result={result_val} {units or ''}")
        
        print(f"\n  Status: {status} ({match_conf:.1%} confidence)")
        
        if analyte_id:
            with self.db.get_session() as session:
                analyte = session.query(Analyte).filter(Analyte.analyte_id == analyte_id).first()
                if analyte:
                    print(f"  Current Match: {analyte.preferred_name} ({analyte_id})")
        
        if suggestions:
            print(f"\n  Top Suggestions:")
            for i, sug in enumerate(suggestions, 1):
                marker = "→" if sug['analyte_id'] == analyte_id else " "
                print(f"    {marker} {i}. {sug['name']:40s} ({sug['analyte_id']}) - {sug['confidence']:.1%}")
    
    def prompt_user(self, current_analyte_id: Optional[str]) -> Tuple[str, Optional[str]]:
        """Prompt user for action."""
        print(f"\n  Actions:")
        print(f"    1-5            = Select suggestion")
        print(f"    ENTER          = Accept current match")
        print(f"    n              = Add NEW analyte to database")
        print(f"    s              = Skip (don't validate)")
        print(f"    REG153_PAHS_016 = Type analyte ID directly")
        print(f"    q              = Quit and save")
        
        choice = input(f"\n  Your choice: ").strip()
        
        if not choice:
            return 'accept', current_analyte_id
        elif choice.lower() == 's':
            return 'skip', None
        elif choice.lower() == 'q':
            return 'quit', None
        elif choice.lower() == 'n':
            return 'new', None
        elif choice.isdigit() and 1 <= int(choice) <= 5:
            return 'select', int(choice)
        elif choice.upper().startswith('REG153_') or choice.upper().startswith('WQ_') or choice.upper().startswith('ELEMENT_'):
            return 'manual', choice.upper()
        else:
            print(f"  X Invalid choice: {choice}")
            return self.prompt_user(current_analyte_id)
    
    def update_result(
        self,
        result_id: int,
        correct_analyte_id: str,
        chemical_raw: str,
        notes: str = None,
        original_analyte_id: str = None,
        match_confidence: float = None,
        match_method: str = None,
        lab_vendor: str = None,
        resolution_result=None,
        submission_id: Optional[int] = None,
    ):
        """Update lab_results with correction, create MatchDecision, upsert LabVariant."""
        conn = sqlite3.connect(self.lab_db_path)
        conn.execute("""
            UPDATE lab_results
            SET correct_analyte_id = ?,
                human_override = 1,
                validation_status = 'validated',
                validation_notes = ?
            WHERE result_id = ?
        """, (correct_analyte_id, notes, result_id))
        conn.commit()
        conn.close()
        
        # Determine cascade confirmation state for dual gate
        cascade_confirmed = False
        cascade_margin = 0.0
        if resolution_result:
            cascade_margin = resolution_result.margin or 0.0
            # Confirmed if engine auto-accepted AND matched same analyte
            if (resolution_result.confidence_band == "AUTO_ACCEPT"
                    and resolution_result.best_match
                    and resolution_result.best_match.analyte_id == correct_analyte_id):
                cascade_confirmed = True
        
        # Add synonym to knowledge base AND create MatchDecision audit trail
        with self.db.get_session() as session:
            added = self.synonym_ingestor.ingest_validated_synonym(
                raw_text=chemical_raw,
                analyte_id=correct_analyte_id,
                db_session=session,
                lab_vendor=lab_vendor,
                cascade_confirmed=cascade_confirmed,
                cascade_margin=cascade_margin,
            )
            if added:
                self.stats['synonyms_added'] += 1
                print(f"    -> Added synonym to knowledge base")
            
            # ── LabVariant upsert + confirmation tracking ──────────────
            if lab_vendor:
                chem_norm = self.normalizer.normalize(chemical_raw)
                variant, created = get_or_create_lab_variant(
                    session,
                    lab_vendor=lab_vendor,
                    observed_text=chem_norm,
                    validated_match_id=correct_analyte_id,
                    confidence=match_confidence,
                )
                if not created:
                    # Check for collision: variant already points elsewhere?
                    if (variant.validated_match_id is not None
                            and variant.validated_match_id != correct_analyte_id):
                        variant.collision_count = (variant.collision_count or 0) + 1
                        variant.last_collision_date = datetime.now().date()
                        print(f"    ! Collision #{variant.collision_count} for '{chem_norm}' "
                              f"(was {variant.validated_match_id}, now {correct_analyte_id})")
                    # Update to latest validated match
                    variant.validated_match_id = correct_analyte_id
                    variant.confidence = match_confidence
                    increment_lab_variant_frequency(session, variant.id)
                
                # Record confirmation for consensus tracking
                if submission_id is not None:
                    create_lab_variant_confirmation(
                        session,
                        variant_id=variant.id,
                        submission_id=str(submission_id),
                        confirmed_analyte_id=correct_analyte_id,
                    )
            
            # B5: Create MatchDecision record for calibrator visibility
            is_correction = (original_analyte_id is not None 
                           and original_analyte_id != correct_analyte_id)
            
            decision = MatchDecision(
                input_text=chemical_raw,
                matched_analyte_id=correct_analyte_id,
                match_method=match_method or 'unknown',
                confidence_score=match_confidence or 0.0,
                top_k_candidates=[],
                signals_used={},
                corpus_snapshot_hash=hashlib.md5(b'interactive').hexdigest()[:16],
                model_hash=hashlib.md5(b'interactive').hexdigest()[:16],
                human_validated=True,
                validation_notes=notes,
                disagreement_flag=is_correction,
                ingested=added or False,
                margin=resolution_result.margin if resolution_result else None,
                cross_method_conflict=(
                    resolution_result.signals_used.get('cross_method_conflict', False)
                    if resolution_result else False
                ),
                lab_vendor=lab_vendor,
            )
            
            # Populate richer signals if resolution result available
            if resolution_result:
                decision.signals_used = resolution_result.signals_used
                decision.top_k_candidates = [
                    {'analyte_id': c.analyte_id, 'score': c.score, 'method': c.method}
                    for c in resolution_result.all_candidates
                ]
            
            session.add(decision)
            session.commit()
    
    def create_new_analyte(self, chemical_raw: str) -> Optional[str]:
        """Create a new analyte interactively."""
        print(f"\n  {'='*76}")
        print(f"  CREATE NEW ANALYTE")
        print(f"  {'='*76}")
        
        # Suggest name based on chemical_raw
        suggested_name = chemical_raw.title()
        name = input(f"\n  Analyte name [{suggested_name}]: ").strip() or suggested_name
        
        # Determine category
        print(f"\n  Category:")
        print(f"    1 = Calculated/QC parameter")
        print(f"    2 = Physical measurement")
        print(f"    3 = Ion/metal")
        print(f"    4 = Organic compound")
        print(f"    5 = Other")
        
        cat_choice = input(f"  Select (1-5): ").strip()
        
        # Generate ID based on category
        if cat_choice == '1':
            prefix = 'WQ_CALC'
            analyte_type = 'CALCULATED'
            chem_group = 'QC'
        elif cat_choice == '2':
            prefix = 'WQ_PHYS'
            analyte_type = 'PARAMETER'
            chem_group = 'Physical'
        elif cat_choice == '3':
            prefix = 'WQ_IONS'
            analyte_type = 'SINGLE_SUBSTANCE'
            chem_group = 'Ions'
        elif cat_choice == '4':
            prefix = 'REG153_MISC'
            analyte_type = 'SINGLE_SUBSTANCE'
            chem_group = 'Organic'
        else:
            prefix = 'WQ_OTHER'
            analyte_type = 'PARAMETER'
            chem_group = 'Other'
        
        # Find next available ID
        import sqlite3
        conn = sqlite3.connect("data/reg153_matcher.db")
        cursor = conn.cursor()
        
        # Get max ID for this prefix
        max_id = cursor.execute(f"""
            SELECT MAX(CAST(SUBSTR(analyte_id, LENGTH(?) + 2) AS INTEGER))
            FROM analytes
            WHERE analyte_id LIKE ? || '_%'
        """, (prefix, prefix)).fetchone()[0]
        
        next_num = (max_id or 0) + 1
        analyte_id = f"{prefix}_{next_num:03d}"
        
        print(f"\n  Generated ID: {analyte_id}")
        custom_id = input(f"  Use different ID? [ENTER to accept]: ").strip()
        if custom_id:
            analyte_id = custom_id.upper()
        
        # Create analyte
        try:
            cursor.execute("""
                INSERT INTO analytes (
                    analyte_id, preferred_name, analyte_type, chemical_group,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
            """, (analyte_id, name, analyte_type, chem_group))
            
            # Add primary synonym
            cursor.execute("""
                INSERT INTO synonyms (
                    analyte_id, synonym_raw, synonym_norm, synonym_type,
                    harvest_source, confidence, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (analyte_id, name, name.lower(), 'COMMON', 'user_created', 1.0))
            
            conn.commit()
            conn.close()
            
            print(f"\n  + Created: {name} ({analyte_id})")
            return analyte_id
            
        except sqlite3.IntegrityError as e:
            print(f"\n  X Error: {e}")
            print(f"  ID already exists or invalid")
            conn.close()
            return None
    
    def finalize_submission(self, submission_id: int):
        """Mark submission as validated and calculate accuracy."""
        conn = sqlite3.connect(self.lab_db_path)
        
        # Calculate accuracy
        total = conn.execute("""
            SELECT COUNT(*) FROM lab_results
            WHERE submission_id = ?
        """, (submission_id,)).fetchone()[0]
        
        correct = conn.execute("""
            SELECT COUNT(*) FROM lab_results
            WHERE submission_id = ?
            AND (
                (human_override = 1 AND correct_analyte_id IS NOT NULL)
                OR (human_override = 0 AND analyte_id IS NOT NULL)
            )
        """, (submission_id,)).fetchone()[0]
        
        accuracy = correct / total if total > 0 else 0
        
        # Update submission
        conn.execute("""
            UPDATE lab_submissions
            SET validation_status = 'validated',
                validated_at = ?,
                extraction_accuracy = ?
            WHERE submission_id = ?
        """, (datetime.now(), accuracy, submission_id))
        
        conn.commit()
        conn.close()
        
        return accuracy
    
    def validate(
        self,
        submission_id: int,
        auto_accept_confident: bool = False,
        confidence_threshold: float = 0.95
    ):
        """Main validation loop."""
        
        # Get submission info
        info = self.get_submission_info(submission_id)
        if not info:
            print(f"ERROR: Submission {submission_id} not found")
            return
        
        print(f"\n{'='*80}")
        print(f"INTERACTIVE VALIDATION")
        print(f"{'='*80}")
        print(f"\nFile: {info['filename']}")
        print(f"Vendor: {info['vendor']}")
        print(f"Layout Confidence: {info['layout_confidence']:.1%}")
        
        # Get results
        results = self.get_results_to_review(submission_id, confidence_threshold)
        self.stats['total'] = len(results)
        
        print(f"\nTotal chemicals: {len(results)}")
        
        if auto_accept_confident:
            high_conf = sum(1 for r in results if r[6] and r[6] >= confidence_threshold)
            print(f"  Auto-accepting {high_conf} high-confidence matches")
            self.stats['auto_accepted'] = high_conf
        
        # Review each result
        for idx, result in enumerate(results, 1):
            result_id, row_num, chem_raw, chem_norm, analyte_id, match_method, match_conf = result[:7]
            
            # Auto-accept high confidence if enabled
            if auto_accept_confident and match_conf and match_conf >= confidence_threshold:
                continue
            
            # Get suggestions
            vendor = info.get('vendor', None)
            suggestions, resolution_result = self.get_top_suggestions(chem_norm, vendor=vendor)
            
            # Display
            self.display_chemical_review(result, suggestions, idx, len(results))
            
            # Prompt user
            action, value = self.prompt_user(analyte_id)
            
            if action == 'quit':
                print(f"\n  Saving progress...")
                break
            
            elif action == 'skip':
                # Mark as skipped in database so it won't show again
                conn = sqlite3.connect(self.lab_db_path)
                conn.execute("""
                    UPDATE lab_results
                    SET validation_status = 'skipped',
                        validation_notes = 'Skipped by user - not a valid chemical'
                    WHERE result_id = ?
                """, (result_id,))
                conn.commit()
                conn.close()
                print(f"  - Skipped")
                self.stats['skipped'] += 1
            
            elif action == 'accept':
                if analyte_id:
                    self.update_result(
                        result_id, analyte_id, chem_raw, "Accepted by user",
                        original_analyte_id=analyte_id,
                        match_confidence=match_conf,
                        match_method=match_method,
                        lab_vendor=vendor,
                        resolution_result=resolution_result,
                        submission_id=submission_id
                    )
                    print(f"  + Accepted")
                    self.stats['reviewed'] += 1
                else:
                    print(f"  ~ No match to accept, skipping")
                    self.stats['skipped'] += 1
            
            elif action == 'select':
                if value <= len(suggestions):
                    selected = suggestions[value - 1]
                    self.update_result(
                        result_id, 
                        selected['analyte_id'], 
                        chem_raw,
                        f"User selected from suggestions",
                        original_analyte_id=analyte_id,
                        match_confidence=match_conf,
                        match_method=match_method,
                        lab_vendor=vendor,
                        resolution_result=resolution_result,
                        submission_id=submission_id
                    )
                    print(f"  + Updated to: {selected['name']} ({selected['analyte_id']})")
                    self.stats['corrected'] += 1
                else:
                    print(f"  X Invalid selection")
                    self.stats['skipped'] += 1
            
            elif action == 'manual':
                # Verify analyte ID exists
                with self.db.get_session() as session:
                    analyte = session.query(Analyte).filter(Analyte.analyte_id == value).first()
                    if analyte:
                        self.update_result(
                            result_id, value, chem_raw, "Manual entry",
                            original_analyte_id=analyte_id,
                            match_confidence=match_conf,
                            match_method=match_method,
                            lab_vendor=vendor,
                            resolution_result=resolution_result,
                            submission_id=submission_id
                        )
                        print(f"  + Updated to: {analyte.preferred_name} ({value})")
                        self.stats['corrected'] += 1
                    else:
                        print(f"  X Analyte ID not found: {value}")
                        self.stats['skipped'] += 1
            
            elif action == 'new':
                # Create new analyte
                new_analyte_id = self.create_new_analyte(chem_raw)
                if new_analyte_id:
                    self.update_result(
                        result_id, new_analyte_id, chem_raw, "New analyte created",
                        original_analyte_id=analyte_id,
                        match_confidence=match_conf,
                        match_method=match_method,
                        lab_vendor=vendor,
                        resolution_result=resolution_result,
                        submission_id=submission_id
                    )
                    print(f"  + Matched to new analyte")
                    self.stats['corrected'] += 1
                else:
                    print(f"  X Failed to create analyte")
                    self.stats['skipped'] += 1
        
        # Finalize
        accuracy = self.finalize_submission(submission_id)
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"VALIDATION COMPLETE")
        print(f"{'='*80}")
        print(f"\nSubmission ID: {submission_id}")
        print(f"File: {info['filename']}")
        print(f"\nResults:")
        print(f"  Total chemicals:     {self.stats['total']:3d}")
        print(f"  Auto-accepted:       {self.stats['auto_accepted']:3d}")
        print(f"  Reviewed:            {self.stats['reviewed']:3d}")
        print(f"  Corrected:           {self.stats['corrected']:3d}")
        print(f"  Skipped:             {self.stats['skipped']:3d}")
        print(f"  Synonyms added:      {self.stats['synonyms_added']:3d}")
        print(f"\nExtraction Accuracy: {accuracy:.1%}")
        
        # Check retraining trigger
        conn = sqlite3.connect(self.lab_db_path)
        validated_count = conn.execute("""
            SELECT COUNT(*) FROM lab_submissions
            WHERE validation_status = 'validated'
            AND used_for_training = 0
        """).fetchone()[0]
        conn.close()
        
        if validated_count >= 10:
            print(f"\n>> RETRAINING RECOMMENDED")
            print(f"   {validated_count} validated files ready for template learning")
            print(f"   Run: python scripts/23_retrain_from_validated.py")


def main():
    parser = argparse.ArgumentParser(
        description="Interactive terminal-based validation"
    )
    parser.add_argument(
        '--submission-id',
        type=int,
        required=True,
        help='Submission ID to validate'
    )
    parser.add_argument(
        '--auto-accept-confident',
        action='store_true',
        help='Auto-accept high-confidence matches (≥95%)'
    )
    parser.add_argument(
        '--confidence-threshold',
        type=float,
        default=0.95,
        help='Confidence threshold for auto-accept (default: 0.95)'
    )
    
    args = parser.parse_args()
    
    validator = InteractiveValidator()
    validator.validate(
        args.submission_id,
        args.auto_accept_confident,
        args.confidence_threshold
    )


if __name__ == "__main__":
    main()
