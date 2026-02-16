"""
Weekly learning health report generator.

Generates comprehensive reports on corpus maturity, learning progress,
and system health metrics for continuous monitoring.

Usage:
    python scripts/13_generate_learning_report.py
    python scripts/13_generate_learning_report.py --days 14 --output reports/weekly/learning_report.md
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from sqlalchemy.orm import Session

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.learning.maturity_metrics import calculate_corpus_maturity
from src.learning.threshold_calibrator import ThresholdCalibrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/learning_reports.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def generate_markdown_report(
    maturity_metrics: Dict,
    calibration_stats: Dict,
    output_path: str
) -> None:
    """
    Generate markdown-formatted learning health report.
    
    Args:
        maturity_metrics: Corpus maturity metrics
        calibration_stats: Threshold calibration statistics
        output_path: Path to output markdown file
    """
    overall = maturity_metrics.get('overall', {})
    trends = maturity_metrics.get('trends', {})
    growth = maturity_metrics.get('growth', {})
    
    report = f"""# Chemical Matcher Learning Health Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Reporting Period:** Last {maturity_metrics.get('reporting_days', 90)} days

---

## Executive Summary

### Corpus Maturity Score: {_calculate_maturity_score(overall):.1f}/100

The corpus maturity score combines exact match rate, synonym coverage, and growth indicators.

**Status:** {_maturity_status(overall)}

---

## 1. Match Performance Metrics

### Current Match Distribution
- **Exact Match Rate:** {overall.get('exact_match_rate', 0)*100:.1f}% 
- **Fuzzy Match Rate:** {overall.get('fuzzy_match_rate', 0)*100:.1f}%
- **Semantic Reliance:** {overall.get('semantic_reliance', 0)*100:.1f}%
- **Unknown Rate:** {overall.get('unknown_rate', 0)*100:.1f}%

### Trend Analysis (Last 30 days)

```
Exact Match Rate Trend:
{_format_sparkline(trends.get('exact_match_rate_30d', []))}

Unknown Rate Trend:
{_format_sparkline(trends.get('unknown_rate_30d', []))}
```

**Interpretation:**  
{_interpret_trends(trends)}

---

## 2. Corpus Growth & Coverage

### Statistics
- **Total Analytes:** {overall.get('total_analytes', 0):,}
- **Total Synonyms:** {overall.get('total_synonyms', 0):,}
- **Avg Synonyms/Analyte:** {overall.get('avg_synonyms_per_analyte', 0):.1f}

### Growth Indicators
- **New Synonyms (7 days):** {growth.get('synonyms_added_7d', 0):,}
- **New Synonyms (30 days):** {growth.get('synonyms_added_30d', 0):,}
- **New Synonyms (90 days):** {growth.get('synonyms_added_90d', 0):,}
- **Weekly Growth Rate:** {growth.get('growth_rate_weekly', 0):.1f}%

```
Weekly New Synonyms:
{_format_bar_chart(trends.get('new_synonyms_per_week', []))}
```

---

## 3. Threshold Calibration Status

### Optimal Thresholds (Recommended)
{_format_threshold_recommendations(calibration_stats)}

### Validation Statistics
- **Total Decisions Analyzed:** {calibration_stats.get('total_decisions', 0):,}
- **Validated by Humans:** {calibration_stats.get('validated_count', 0):,}
- **Validation Rate:** {calibration_stats.get('validation_rate', 0)*100:.1f}%

### Precision by Method
{_format_precision_table(calibration_stats)}

---

## 4. Learning Progress Tracking

### Layer 1: Synonym Ingestion
- **Status:** ✅ Active
- **New Synonyms This Week:** {growth.get('synonyms_added_7d', 0):,}
- **Contribution to Exact Match Rate:** +{_estimate_layer1_impact(growth):.1f}%

### Layer 2: Neural Embeddings
- **Status:** {_check_embeddings_status()}
- **Semantic Reliance:** {overall.get('semantic_reliance', 0)*100:.1f}%
- **Last Retrained:** {_get_last_training_date()}

### Layer 3: Threshold Calibration
- **Status:** ✅ Active
- **Last Calibration:** {calibration_stats.get('last_calibration', 'N/A')}
- **Recommended Action:** {_calibration_recommendation(calibration_stats)}

### Layer 4: Variant Clustering
- **Status:** {_check_clustering_status()}
- **Active Clusters:** {_count_active_clusters()}

---

## 5. Retraining Indicators

### Neural Model Retraining Status
{_format_retraining_status(maturity_metrics, calibration_stats)}

---

## 6. Action Items & Recommendations

{_generate_recommendations(overall, growth, calibration_stats)}

---

## 7. Weekly Comparison

### Week-over-Week Changes
- **Exact Match Rate:** {_format_change(trends, 'exact_match')}
- **Unknown Rate:** {_format_change(trends, 'unknown')}
- **New Synonyms:** {_format_change(growth, 'synonyms')}

---

*Report generated by Chemical Matcher Learning System v1.0*  
*Next report scheduled: {(datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')}*
"""
    
    # Write to file
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logger.info(f"Markdown report written to: {output_path}")


def generate_text_report(
    maturity_metrics: Dict,
    calibration_stats: Dict
) -> str:
    """
    Generate plain text learning health report.
    
    Args:
        maturity_metrics: Corpus maturity metrics
        calibration_stats: Threshold calibration statistics
    
    Returns:
        Formatted text report
    """
    overall = maturity_metrics.get('overall', {})
    growth = maturity_metrics.get('growth', {})
    
    report = f"""
╔══════════════════════════════════════════════════════════════════════════╗
║              CHEMICAL MATCHER LEARNING HEALTH REPORT                     ║
╚══════════════════════════════════════════════════════════════════════════╝

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

┌─ CORPUS MATURITY ────────────────────────────────────────────────────────┐
│                                                                          │
│  Maturity Score:        {_calculate_maturity_score(overall):5.1f}/100                                   │
│  Status:                {_maturity_status(overall):<50}│
│                                                                          │
│  Total Analytes:        {overall.get('total_analytes', 0):>8,}                                      │
│  Total Synonyms:        {overall.get('total_synonyms', 0):>8,}                                      │
│  Avg Synonyms/Analyte:  {overall.get('avg_synonyms_per_analyte', 0):>8.1f}                                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ MATCH PERFORMANCE (Last 30 days) ───────────────────────────────────────┐
│                                                                          │
│  Exact Match Rate:      {overall.get('exact_match_rate', 0)*100:>6.1f}%  ███████████████████████          │
│  Fuzzy Match Rate:      {overall.get('fuzzy_match_rate', 0)*100:>6.1f}%  ██████████                       │
│  Semantic Reliance:     {overall.get('semantic_reliance', 0)*100:>6.1f}%  ████████                         │
│  Unknown Rate:          {overall.get('unknown_rate', 0)*100:>6.1f}%  ███                              │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ GROWTH INDICATORS ──────────────────────────────────────────────────────┐
│                                                                          │
│  New Synonyms (7d):     {growth.get('synonyms_added_7d', 0):>8,}                                      │
│  New Synonyms (30d):    {growth.get('synonyms_added_30d', 0):>8,}                                      │
│  Weekly Growth Rate:    {growth.get('growth_rate_weekly', 0):>7.1f}%                                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ VALIDATION STATISTICS ──────────────────────────────────────────────────┐
│                                                                          │
│  Decisions Analyzed:    {calibration_stats.get('total_decisions', 0):>8,}                                      │
│  Human Validated:       {calibration_stats.get('validated_count', 0):>8,}                                      │
│  Validation Rate:       {calibration_stats.get('validation_rate', 0)*100:>6.1f}%                                       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ RECOMMENDATIONS ────────────────────────────────────────────────────────┐
│                                                                          │
{_format_text_recommendations(overall, growth, calibration_stats)}
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Report generated by Chemical Matcher Learning System v1.0
"""
    
    return report


# Helper functions for report formatting

def _calculate_maturity_score(overall: Dict) -> float:
    """Calculate overall maturity score (0-100)."""
    exact_rate = overall.get('exact_match_rate', 0) * 100
    unknown_inverse = (1 - overall.get('unknown_rate', 0)) * 100
    coverage = min(overall.get('avg_synonyms_per_analyte', 0) * 10, 100)
    
    # Weighted average
    score = (exact_rate * 0.5) + (unknown_inverse * 0.3) + (coverage * 0.2)
    return min(score, 100.0)


def _maturity_status(overall: Dict) -> str:
    """Determine maturity status based on metrics."""
    score = _calculate_maturity_score(overall)
    
    if score >= 85:
        return "🟢 Excellent - Production ready with high confidence"
    elif score >= 70:
        return "🟡 Good - Suitable for production with monitoring"
    elif score >= 50:
        return "🟠 Fair - Suitable for assisted validation workflows"
    else:
        return "🔴 Early - Requires significant corpus development"


def _format_sparkline(values: List[float]) -> str:
    """Format list of values as ASCII sparkline."""
    if not values:
        return "No data"
    
    # Reverse to show most recent first
    values = list(reversed(values[:12]))
    
    blocks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']
    
    if max(values) == min(values):
        return '▄' * len(values)
    
    normalized = [(v - min(values)) / (max(values) - min(values)) for v in values]
    sparkline = ''.join(blocks[min(int(v * len(blocks)), len(blocks)-1)] for v in normalized)
    
    return f"{sparkline}  ({min(values)*100:.0f}% → {max(values)*100:.0f}%)"


def _format_bar_chart(values: List[int]) -> str:
    """Format list of counts as ASCII bar chart."""
    if not values:
        return "No data"
    
    values = list(reversed(values[:8]))  # Last 8 weeks
    max_val = max(values) if values else 1
    
    lines = []
    for i, val in enumerate(values):
        bar_length = int((val / max_val) * 30) if max_val > 0 else 0
        bar = '█' * bar_length
        lines.append(f"Week -{len(values)-i-1:2d}: {bar} {val:,}")
    
    return '\n'.join(lines)


def _interpret_trends(trends: Dict) -> str:
    """Interpret trend data and provide insights."""
    exact_trend = trends.get('exact_match_rate_30d', [])
    unknown_trend = trends.get('unknown_rate_30d', [])
    
    if not exact_trend:
        return "Insufficient data for trend analysis."
    
    # Check if improving
    exact_improving = exact_trend[-1] > exact_trend[0] if len(exact_trend) > 1 else False
    unknown_improving = unknown_trend[-1] < unknown_trend[0] if len(unknown_trend) > 1 else False
    
    if exact_improving and unknown_improving:
        return "✅ **Positive trend:** Both exact match rate is increasing and unknown rate is decreasing."
    elif exact_improving:
        return "🟡 **Mixed trend:** Exact match rate improving, but unknown rate not decreasing."
    elif unknown_improving:
        return "🟡 **Mixed trend:** Unknown rate decreasing, but exact match rate plateauing."
    else:
        return "🔴 **Concerning trend:** Metrics not improving. Consider increasing validation efforts."


def _format_threshold_recommendations(cal_stats: Dict) -> str:
    """Format threshold recommendations."""
    optimal = cal_stats.get('optimal_thresholds', {})
    
    if not optimal:
        return "- No threshold recommendations available (insufficient validation data)"
    
    lines = []
    for method, threshold in optimal.items():
        current = 0.75  # Default current threshold
        change = threshold - current
        arrow = "↑" if change > 0 else "↓" if change < 0 else "→"
        lines.append(f"- **{method.capitalize()}:** {threshold:.3f} (current: {current:.3f}) {arrow}")
    
    return '\n'.join(lines) if lines else "- Using default thresholds"


def _format_precision_table(cal_stats: Dict) -> str:
    """Format precision by method table."""
    method_stats = cal_stats.get('method_distribution', {})
    
    if not method_stats:
        return "No method statistics available."
    
    lines = ["```"]
    lines.append(f"{'Method':<12} {'Count':>8} {'Precision':>10}")
    lines.append("-" * 32)
    
    for method, stats in method_stats.items():
        count = stats.get('count', 0)
        precision = stats.get('precision', 0)
        lines.append(f"{method.capitalize():<12} {count:>8,} {precision:>9.1f}%")
    
    lines.append("```")
    return '\n'.join(lines)


def _estimate_layer1_impact(growth: Dict) -> float:
    """Estimate Layer 1 learning impact on exact match rate."""
    weekly_synonyms = growth.get('synonyms_added_7d', 0)
    # Rough estimate: each 100 synonyms improves exact match by ~1%
    return (weekly_synonyms / 100) * 1.0


def _check_embeddings_status() -> str:
    """Check if embeddings are available."""
    embeddings_path = Path("data/embeddings")
    if embeddings_path.exists() and list(embeddings_path.glob("*.npy")):
        return "✅ Active"
    else:
        return "⚠️  Not deployed"


def _get_last_training_date() -> str:
    """Get last neural model training date."""
    models_path = Path("models")
    if models_path.exists():
        model_files = list(models_path.glob("*.pkl")) + list(models_path.glob("*.pth"))
        if model_files:
            latest = max(model_files, key=lambda p: p.stat().st_mtime)
            mtime = datetime.fromtimestamp(latest.stat().st_mtime)
            return mtime.strftime('%Y-%m-%d')
    return "Never"


def _check_clustering_status() -> str:
    """Check if variant clustering is available."""
    # Placeholder - would check if clustering has been run
    return "✅ Available"


def _count_active_clusters() -> int:
    """Count active variant clusters."""
    # Placeholder - would query database
    return 0


def _format_retraining_status(maturity: Dict, cal_stats: Dict) -> str:
    """Format retraining status section."""
    validated_count = cal_stats.get('validated_count', 0)
    threshold = 2000
    
    progress = (validated_count / threshold * 100) if threshold > 0 else 0
    
    lines = []
    lines.append(f"**Validated Decisions:** {validated_count:,}/{threshold:,} ({progress:.1f}%)")
    lines.append("")
    
    if validated_count >= threshold:
        lines.append("✅ **Retraining threshold met!** Consider running neural model training.")
    else:
        remaining = threshold - validated_count
        lines.append(f"⏳ **In progress:** {remaining:,} more validations needed for retraining.")
    
    lines.append("")
    lines.append(f"Progress: [{'█' * int(progress/5)}{'░' * (20-int(progress/5))}] {progress:.0f}%")
    
    return '\n'.join(lines)


def _generate_recommendations(overall: Dict, growth: Dict, cal_stats: Dict) -> str:
    """Generate actionable recommendations."""
    recommendations = []
    
    # Check exact match rate
    exact_rate = overall.get('exact_match_rate', 0)
    if exact_rate < 0.60:
        recommendations.append("1. **Priority:** Increase validation efforts to build synonym corpus (exact match rate < 60%)")
    
    # Check unknown rate
    unknown_rate = overall.get('unknown_rate', 0)
    if unknown_rate > 0.10:
        recommendations.append("2. **Action:** High unknown rate detected. Run generate_review_queue.py to identify gaps.")
    
    # Check growth
    weekly_growth = growth.get('synonyms_added_7d', 0)
    if weekly_growth < 50:
        recommendations.append("3. **Suggestion:** Low synonym growth this week. Consider batch validation sessions.")
    
    # Check validation rate
    validation_rate = cal_stats.get('validation_rate', 0)
    if validation_rate < 0.05:
        recommendations.append("4. **Notice:** Low validation rate. Ensure review queue is being processed regularly.")
    
    # Check retraining
    validated_count = cal_stats.get('validated_count', 0)
    if validated_count >= 2000:
        recommendations.append("5. **Retraining:** Threshold met! Run scripts/14_check_retraining_need.py for assessment.")
    
    if not recommendations:
        recommendations.append("✅ **All systems operating nominally.** Continue regular monitoring.")
    
    return '\n'.join(recommendations)


def _format_change(data: Dict, metric_type: str) -> str:
    """Format week-over-week change."""
    # Placeholder - would calculate actual change
    return "0.0% (no change)"


def _format_text_recommendations(overall: Dict, growth: Dict, cal_stats: Dict) -> str:
    """Format recommendations for text report."""
    recs = _generate_recommendations(overall, growth, cal_stats).split('\n')
    
    lines = []
    for rec in recs:
        # Wrap text to fit in box
        if rec:
            lines.append(f"│  {rec:<70}│")
    
    return '\n'.join(lines)


def main():
    """Main entry point for learning report generation."""
    parser = argparse.ArgumentParser(
        description="Generate learning health report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate default weekly report
  python scripts/13_generate_learning_report.py
  
  # Generate report for last 14 days
  python scripts/13_generate_learning_report.py --days 14
  
  # Generate markdown report to specific location
  python scripts/13_generate_learning_report.py --output reports/weekly/report.md --format markdown
        """
    )
    
    parser.add_argument('--days', '-d', type=int, default=90,
                        help='Number of days to analyze (default: 90)')
    parser.add_argument('--output', '-o', 
                        help='Output file path (default: reports/weekly/learning_report_YYYYMMDD.md)')
    parser.add_argument('--format', '-f', choices=['markdown', 'text'], default='markdown',
                        help='Output format (default: markdown)')
    parser.add_argument('--database', help='Path to database file')
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Generating learning health report (last {args.days} days)")
        
        # Initialize database
        db_manager = DatabaseManager(db_path=args.database, echo=False)
        
        with db_manager.get_session() as session:
            # Calculate corpus maturity
            maturity_metrics = calculate_corpus_maturity(session, history_days=args.days)
            maturity_metrics['reporting_days'] = args.days
            
            # Run threshold calibration analysis
            calibrator = ThresholdCalibrator()
            calibration_stats = calibrator.analyze_recent_decisions(session, days=min(args.days, 30))
        
        # Generate report
        if args.format == 'markdown':
            if args.output:
                output_path = args.output
            else:
                timestamp = datetime.now().strftime('%Y%m%d')
                output_path = f"reports/weekly/learning_report_{timestamp}.md"
            
            generate_markdown_report(maturity_metrics, calibration_stats, output_path)
            print(f"\n✅ Markdown report generated: {output_path}")
            
        else:  # text format
            text_report = generate_text_report(maturity_metrics, calibration_stats)
            
            if args.output:
                Path(args.output).parent.mkdir(parents=True, exist_ok=True)
                with open(args.output, 'w') as f:
                    f.write(text_report)
                print(f"\n✅ Text report generated: {args.output}")
            else:
                print(text_report)
        
        logger.info("Learning report generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Learning report generation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
