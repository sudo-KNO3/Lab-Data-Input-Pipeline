"""
Script 11: Match Rate Telemetry

Captures a snapshot of the current match method distribution and appends
it to the rolling telemetry log. Run monthly (or after large batches) to
track the system's 40% → 82% exact-match maturity trajectory.

Usage:
    python scripts/11_match_rate_telemetry.py
    python scripts/11_match_rate_telemetry.py --days 30
    python scripts/11_match_rate_telemetry.py --history          # Show trend only
    python scripts/11_match_rate_telemetry.py --no-save          # Print without saving
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.learning.telemetry import (
    compute_match_rate_snapshot,
    save_telemetry_snapshot,
    load_telemetry_history,
    compute_maturity_trajectory,
    DEFAULT_TELEMETRY_PATH,
)


def print_snapshot(snapshot: dict) -> None:
    print("\n" + "=" * 60)
    print("MATCH RATE TELEMETRY SNAPSHOT")
    print("=" * 60)
    print(f"  Period:           last {snapshot['period_days']} days")
    print(f"  Total decisions:  {snapshot['total_decisions']:,}")
    print()
    print(f"  Overall match rate:  {snapshot['overall_match_rate']:.1%}")
    print(f"  ├─ Exact / CAS:      {snapshot['exact_rate']:.1%}")
    print(f"  ├─ Fuzzy:            {snapshot['fuzzy_rate']:.1%}")
    print(f"  ├─ Semantic:         {snapshot['semantic_rate']:.1%}")
    print(f"  ├─ PubChem:          {snapshot['pubchem_rate']:.1%}")
    print(f"  ├─ Vendor cache:     {snapshot['vendor_cache_rate']:.1%}")
    print(f"  └─ Unmatched:        {snapshot['unmatched_rate']:.1%}")
    print()
    print(f"  Timestamp: {snapshot['timestamp']}")
    print("=" * 60)


def print_trajectory(trajectory: dict) -> None:
    print("\n" + "=" * 60)
    print("MATURITY TRAJECTORY")
    print("=" * 60)
    print(f"  Snapshots recorded:  {trajectory['snapshots']}")
    if trajectory['current_match_rate'] is not None:
        print(f"  Current match rate:  {trajectory['current_match_rate']:.1%}")
        print(f"  Target match rate:   {trajectory['target_match_rate']:.1%}")
        if trajectory['gap_to_target'] is not None:
            gap = trajectory['gap_to_target']
            if gap <= 0:
                print(f"  Target achieved! ({abs(gap):.1%} above target)")
            else:
                print(f"  Gap to target:       {gap:.1%}")
        if trajectory['improvement'] is not None:
            print(f"  Total improvement:   {trajectory['improvement']:+.1%}")
    print()
    if trajectory['trend']:
        print("  Recent trend (last 5 snapshots):")
        for entry in trajectory['trend'][-5:]:
            rate = entry['overall_match_rate']
            ts = entry['timestamp'][:10] if entry['timestamp'] else '?'
            bar = '█' * int((rate or 0) * 20)
            print(f"    {ts}  {bar:<20} {rate:.1%}" if rate is not None else f"    {ts}  (no data)")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Record and view match rate telemetry')
    parser.add_argument('--days', type=int, default=30,
                        help='Lookback period for snapshot (default: 30)')
    parser.add_argument('--history', action='store_true',
                        help='Show maturity trajectory without capturing new snapshot')
    parser.add_argument('--no-save', action='store_true',
                        help='Print snapshot without saving to log')
    parser.add_argument('--log', type=str, default=None,
                        help=f'Path to telemetry log (default: {DEFAULT_TELEMETRY_PATH})')
    args = parser.parse_args()

    log_path = Path(args.log) if args.log else None

    if args.history:
        history = load_telemetry_history(log_path)
        if not history:
            print("No telemetry history found.")
            print(f"Run without --history to capture the first snapshot.")
            sys.exit(0)
        trajectory = compute_maturity_trajectory(history)
        print_trajectory(trajectory)
        sys.exit(0)

    # Capture new snapshot
    db = DatabaseManager()
    with db.get_session() as session:
        snapshot = compute_match_rate_snapshot(session, days_back=args.days)

    print_snapshot(snapshot)

    if not args.no_save:
        save_telemetry_snapshot(snapshot, log_path)
        print(f"\n  Snapshot saved to: {log_path or DEFAULT_TELEMETRY_PATH}")

    # Show trajectory if history exists
    history = load_telemetry_history(log_path)
    if len(history) >= 2:
        trajectory = compute_maturity_trajectory(history)
        print_trajectory(trajectory)


if __name__ == '__main__':
    main()
