"""
Gate A Baseline Measurement Script

Re-resolves all validated lab results through the Phase A updated engine
and compares against validated ground truth.

Measures:
- Per-file accuracy, auto-accept rate, review rate, unknown rate
- Mean confidence, confidence std dev, margin distribution
- Semantic signal availability (if embeddings exist)
- Comparison to pre-Phase-A baseline
"""
import sys
import sqlite3
import statistics
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.matching.resolution_engine import ResolutionEngine
from src.normalization.text_normalizer import TextNormalizer


def run_baseline():
    """Run Gate A baseline measurement."""
    lab_db_path = "data/lab_results.db"
    
    if not Path(lab_db_path).exists():
        print("ERROR: data/lab_results.db not found")
        return
    
    # Connect to lab results (SQLite)
    conn = sqlite3.connect(lab_db_path)
    conn.row_factory = sqlite3.Row
    
    # Get all submissions
    submissions = conn.execute("""
        SELECT submission_id, original_filename, lab_vendor
        FROM lab_submissions 
        ORDER BY submission_id
    """).fetchall()
    
    print("=" * 80)
    print("GATE A — BASELINE MEASUREMENT (Phase A: Stabilize Signal Layer)")
    print("=" * 80)
    print(f"\nSubmissions found: {len(submissions)}")
    
    # Initialize resolution engine (no semantic matcher yet — embeddings may not exist)
    db = DatabaseManager()
    
    overall_stats = {
        'total': 0,
        'correct': 0,
        'auto_accept': 0,
        'review': 0,
        'unknown': 0,
        'confidences': [],
        'margins': [],
        'signals': defaultdict(int),
    }
    
    file_results = []
    
    for sub in submissions:
        sub_id = sub['submission_id']
        filename = sub['original_filename']
        vendor = sub['lab_vendor']
        
        # Get validated results for this submission
        results = conn.execute("""
            SELECT result_id, chemical_raw, analyte_id, correct_analyte_id,
                   validation_status, match_confidence, match_method
            FROM lab_results
            WHERE submission_id = ?
            AND validation_status = 'validated'
        """, (sub_id,)).fetchall()
        
        if not results:
            continue
        
        print(f"\n{'─' * 70}")
        print(f"Submission {sub_id}: {filename} ({vendor})")
        print(f"{'─' * 70}")
        
        file_stats = {
            'total': 0,
            'correct': 0,
            'auto_accept': 0,
            'review': 0, 
            'unknown': 0,
            'confidences': [],
            'margins': [],
            'method_counts': defaultdict(int),
        }
        
        with db.get_session() as session:
            engine = ResolutionEngine(db_session=session)
            
            for row in results:
                chemical_raw = row['chemical_raw']
                # Ground truth: the validated correct analyte
                ground_truth = row['correct_analyte_id'] or row['analyte_id']
                
                if not ground_truth:
                    continue
                
                file_stats['total'] += 1
                
                # Re-resolve through Phase A engine
                result = engine.resolve(chemical_raw, confidence_threshold=0.70)
                
                # Record confidence and margin
                if result.best_match:
                    file_stats['confidences'].append(result.best_match.confidence)
                    file_stats['method_counts'][result.best_match.method] += 1
                else:
                    file_stats['confidences'].append(0.0)
                    
                file_stats['margins'].append(result.margin)
                
                # Check correctness
                predicted_id = result.best_match.analyte_id if result.best_match else None
                is_correct = (predicted_id == ground_truth)
                if is_correct:
                    file_stats['correct'] += 1
                
                # Band classification
                if result.confidence_band == "AUTO_ACCEPT":
                    file_stats['auto_accept'] += 1
                elif result.confidence_band == "REVIEW":
                    file_stats['review'] += 1
                else:
                    file_stats['unknown'] += 1
                
                # Track signals
                for signal, used in result.signals_used.items():
                    if used:
                        overall_stats['signals'][signal] += 1
        
        # File summary
        n = file_stats['total']
        if n == 0:
            continue
            
        accuracy = file_stats['correct'] / n * 100
        auto_rate = file_stats['auto_accept'] / n * 100
        review_rate = file_stats['review'] / n * 100
        unknown_rate = file_stats['unknown'] / n * 100
        mean_conf = statistics.mean(file_stats['confidences']) if file_stats['confidences'] else 0
        std_conf = statistics.stdev(file_stats['confidences']) if len(file_stats['confidences']) > 1 else 0
        mean_margin = statistics.mean(file_stats['margins']) if file_stats['margins'] else 0
        
        print(f"  Items:       {n}")
        print(f"  Accuracy:    {file_stats['correct']}/{n} ({accuracy:.1f}%)")
        print(f"  Auto-Accept: {file_stats['auto_accept']}/{n} ({auto_rate:.1f}%)")
        print(f"  Review:      {file_stats['review']}/{n} ({review_rate:.1f}%)")
        print(f"  Unknown:     {file_stats['unknown']}/{n} ({unknown_rate:.1f}%)")
        print(f"  Confidence:  μ={mean_conf:.4f}  σ={std_conf:.4f}")
        print(f"  Margin:      μ={mean_margin:.4f}")
        print(f"  Methods:     {dict(file_stats['method_counts'])}")
        
        file_results.append({
            'submission_id': sub_id,
            'filename': filename,
            'total': n,
            'accuracy': accuracy,
            'auto_rate': auto_rate,
            'review_rate': review_rate,
            'unknown_rate': unknown_rate,
            'mean_confidence': mean_conf,
            'std_confidence': std_conf,
            'mean_margin': mean_margin,
        })
        
        # Accumulate overall
        overall_stats['total'] += file_stats['total']
        overall_stats['correct'] += file_stats['correct']
        overall_stats['auto_accept'] += file_stats['auto_accept']
        overall_stats['review'] += file_stats['review']
        overall_stats['unknown'] += file_stats['unknown']
        overall_stats['confidences'].extend(file_stats['confidences'])
        overall_stats['margins'].extend(file_stats['margins'])
    
    conn.close()
    
    # Overall summary
    t = overall_stats['total']
    if t == 0:
        print("\nNo validated results found.")
        return
    
    print(f"\n{'=' * 80}")
    print("OVERALL SUMMARY")
    print(f"{'=' * 80}")
    print(f"  Files:       {len(file_results)}")
    print(f"  Total items: {t}")
    print(f"  Accuracy:    {overall_stats['correct']}/{t} ({overall_stats['correct']/t*100:.1f}%)")
    print(f"  Auto-Accept: {overall_stats['auto_accept']}/{t} ({overall_stats['auto_accept']/t*100:.1f}%)")
    print(f"  Review:      {overall_stats['review']}/{t} ({overall_stats['review']/t*100:.1f}%)")
    print(f"  Unknown:     {overall_stats['unknown']}/{t} ({overall_stats['unknown']/t*100:.1f}%)")
    
    mean_c = statistics.mean(overall_stats['confidences'])
    std_c = statistics.stdev(overall_stats['confidences']) if len(overall_stats['confidences']) > 1 else 0
    mean_m = statistics.mean(overall_stats['margins'])
    std_m = statistics.stdev(overall_stats['margins']) if len(overall_stats['margins']) > 1 else 0
    
    print(f"  Confidence:  μ={mean_c:.4f}  σ={std_c:.4f}")
    print(f"  Margin:      μ={mean_m:.4f}  σ={std_m:.4f}")
    print(f"  Signals:     {dict(overall_stats['signals'])}")
    
    # Margin distribution
    margins = overall_stats['margins']
    narrow = sum(1 for m in margins if m < 0.05)
    medium = sum(1 for m in margins if 0.05 <= m < 0.20)
    wide = sum(1 for m in margins if m >= 0.20)
    print(f"\n  Margin distribution:")
    print(f"    Narrow (<0.05):  {narrow} ({narrow/t*100:.1f}%)")
    print(f"    Medium (0.05-0.20): {medium} ({medium/t*100:.1f}%)")
    print(f"    Wide   (>=0.20): {wide} ({wide/t*100:.1f}%)")
    
    # Confidence distribution (with continuous scores)
    confs = overall_stats['confidences']
    bins = [(0, 0.50), (0.50, 0.75), (0.75, 0.85), (0.85, 0.93), (0.93, 0.95), (0.95, 1.01)]
    print(f"\n  Confidence distribution (continuous scores):")
    for lo, hi in bins:
        count = sum(1 for c in confs if lo <= c < hi)
        label = f"[{lo:.2f}-{hi:.2f})"
        print(f"    {label}: {count} ({count/t*100:.1f}%)")
    
    exact_1 = sum(1 for c in confs if c >= 1.0)
    print(f"    [1.00]:      {exact_1} ({exact_1/t*100:.1f}%)")
    
    print(f"\n{'=' * 80}")
    print("BASELINE COMPARISON (Pre-Phase-A → Post-Phase-A)")
    print(f"{'=' * 80}")
    print(f"  Pre:  97.1% accuracy, 77.7% perfect match (step-function confidence)")
    print(f"  Post: {overall_stats['correct']/t*100:.1f}% accuracy, continuous scores, margin instrumented")
    print(f"\n  New signals: margin (μ={mean_m:.4f}), semantic_match={overall_stats['signals'].get('semantic_match', 0)} items")
    print(f"  Config loaded from: config/learning_config.yaml")
    print(f"  Thresholds: auto_accept=0.93, review=0.75, disagreement_cap=0.84")


if __name__ == "__main__":
    run_baseline()
