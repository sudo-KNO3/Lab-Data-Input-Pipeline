"""
Match-rate telemetry for tracking system maturity over time.

Records periodic snapshots of match method distribution so the
40% → 82% exact-match trajectory described in the README can be
monitored and verified.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..database.models import MatchDecision

logger = logging.getLogger(__name__)

DEFAULT_TELEMETRY_PATH = Path(__file__).parent.parent.parent / 'logs' / 'telemetry.jsonl'


def compute_match_rate_snapshot(
    session: Session,
    days_back: int = 30
) -> Dict:
    """
    Compute match method distribution for the given period.

    Returns a snapshot dict with keys:
      period_days, total_decisions, exact_rate, fuzzy_rate,
      semantic_rate, pubchem_rate, vendor_cache_rate, unmatched_rate,
      overall_match_rate, timestamp
    """
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    decisions = session.execute(
        select(MatchDecision).where(MatchDecision.decision_timestamp >= cutoff)
    ).scalars().all()

    total = len(decisions)
    if total == 0:
        logger.warning(f"No decisions found in last {days_back} days")
        return {
            'period_days': days_back,
            'total_decisions': 0,
            'exact_rate': 0.0,
            'fuzzy_rate': 0.0,
            'semantic_rate': 0.0,
            'pubchem_rate': 0.0,
            'vendor_cache_rate': 0.0,
            'unmatched_rate': 0.0,
            'overall_match_rate': 0.0,
            'timestamp': datetime.utcnow().isoformat(),
        }

    counts = {
        'exact': 0,
        'cas_extracted': 0,
        'fuzzy': 0,
        'semantic': 0,
        'pubchem': 0,
        'vendor_cache': 0,
        'vendor_cache_stale': 0,
        'unmatched': 0,
        'other': 0,
    }

    for d in decisions:
        method = d.match_method or 'unmatched'
        if method in counts:
            counts[method] += 1
        elif d.matched_analyte_id is None:
            counts['unmatched'] += 1
        else:
            counts['other'] += 1

    def rate(n: int) -> float:
        return n / total if total > 0 else 0.0

    exact_rate = rate(counts['exact'] + counts['cas_extracted'])
    fuzzy_rate = rate(counts['fuzzy'])
    semantic_rate = rate(counts['semantic'])
    pubchem_rate = rate(counts['pubchem'])
    vendor_cache_rate = rate(counts['vendor_cache'] + counts['vendor_cache_stale'])
    unmatched_rate = rate(counts['unmatched'])
    overall_match_rate = 1.0 - unmatched_rate

    snapshot = {
        'period_days': days_back,
        'total_decisions': total,
        'exact_rate': round(exact_rate, 4),
        'fuzzy_rate': round(fuzzy_rate, 4),
        'semantic_rate': round(semantic_rate, 4),
        'pubchem_rate': round(pubchem_rate, 4),
        'vendor_cache_rate': round(vendor_cache_rate, 4),
        'unmatched_rate': round(unmatched_rate, 4),
        'overall_match_rate': round(overall_match_rate, 4),
        'timestamp': datetime.utcnow().isoformat(),
    }

    logger.info(
        f"Telemetry snapshot: match_rate={overall_match_rate:.1%}, "
        f"exact={exact_rate:.1%}, fuzzy={fuzzy_rate:.1%}, "
        f"semantic={semantic_rate:.1%}, unmatched={unmatched_rate:.1%}"
    )
    return snapshot


def save_telemetry_snapshot(snapshot: Dict, path: Optional[Path] = None) -> None:
    """Append a snapshot to the rolling JSONL telemetry log."""
    log_path = path or DEFAULT_TELEMETRY_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(snapshot) + '\n')
    logger.debug(f"Telemetry snapshot saved to {log_path}")


def load_telemetry_history(path: Optional[Path] = None) -> List[Dict]:
    """Load all historical telemetry snapshots from the JSONL log."""
    log_path = path or DEFAULT_TELEMETRY_PATH
    if not log_path.exists():
        return []
    snapshots = []
    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    snapshots.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed telemetry line: {line[:60]}")
    return snapshots


def compute_maturity_trajectory(history: List[Dict]) -> Dict:
    """
    Compute the maturity trajectory from historical snapshots.

    Returns a summary showing how overall_match_rate has evolved,
    allowing comparison against the README's 40% → 82% target.
    """
    if not history:
        return {'snapshots': 0, 'current_match_rate': None, 'trend': []}

    sorted_history = sorted(history, key=lambda s: s.get('timestamp', ''))
    trend = [
        {
            'timestamp': s['timestamp'],
            'overall_match_rate': s.get('overall_match_rate'),
            'exact_rate': s.get('exact_rate'),
            'unmatched_rate': s.get('unmatched_rate'),
            'total_decisions': s.get('total_decisions'),
        }
        for s in sorted_history
    ]

    current = sorted_history[-1].get('overall_match_rate')
    first = sorted_history[0].get('overall_match_rate')

    return {
        'snapshots': len(history),
        'current_match_rate': current,
        'first_match_rate': first,
        'improvement': round(current - first, 4) if (current is not None and first is not None) else None,
        'target_match_rate': 0.82,
        'gap_to_target': round(0.82 - current, 4) if current is not None else None,
        'trend': trend,
    }
