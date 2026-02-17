"""
Script 30: Pipeline Orchestrator

Single-command entry point for the Chemical Matcher pipeline.
Replaces the daily_learning_loop.bat with a cross-platform Python
implementation and adds batch-ingest + embedding refresh steps.

Modes:
    ingest   — Batch-ingest new lab files, match chemicals, archive
    learn    — Ingest validations, generate learning report
    calibrate — Monthly threshold recalibration
    embed    — Rebuild FAISS index from current synonyms
    full     — Run all steps in sequence
    status   — Print system health summary

Usage:
    python scripts/30_orchestrate.py full
    python scripts/30_orchestrate.py ingest --folder "Excel Lab examples"
    python scripts/30_orchestrate.py learn --days 7
    python scripts/30_orchestrate.py status
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

LOG_DIR = project_root / "logs" / "orchestrator"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _setup_logging(verbose: bool = False) -> logging.Logger:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"orchestrate_{ts}.log"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("orchestrator")
    logger.info(f"Log file: {log_file}")
    return logger


def _run_script(script: str, extra_args: list | None = None, logger=None) -> bool:
    """Run a Python script as a subprocess, streaming output."""
    cmd = [sys.executable, str(project_root / script)]
    if extra_args:
        cmd.extend(extra_args)

    label = Path(script).stem
    if logger:
        logger.info(f">>> Running {label} ...")

    t0 = time.perf_counter()
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - t0

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            if logger:
                logger.info(f"  [{label}] {line}")
    if result.returncode != 0:
        if logger:
            logger.error(f"  [{label}] FAILED (exit {result.returncode}, {elapsed:.1f}s)")
            if result.stderr:
                for line in result.stderr.strip().splitlines()[-10:]:
                    logger.error(f"  [{label}] {line}")
        return False

    if logger:
        logger.info(f"  [{label}] OK ({elapsed:.1f}s)")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  Pipeline steps
# ═══════════════════════════════════════════════════════════════════════════════

def step_ingest(args, logger) -> bool:
    """Batch-ingest new lab files."""
    extra = []
    if hasattr(args, "folder") and args.folder:
        extra.extend(["--folder", args.folder])
    if hasattr(args, "dry_run") and args.dry_run:
        extra.append("--dry-run")
    return _run_script("scripts/22_batch_ingest.py", extra, logger)


def step_learn(args, logger) -> bool:
    """Ingest validated decisions and generate learning report."""
    days = str(getattr(args, "days", 7))

    ok1 = _run_script(
        "scripts/12_validate_and_learn.py",
        ["--auto-ingest"],
        logger,
    )

    ts = datetime.now().strftime("%Y%m%d")
    report_dir = project_root / "reports" / "daily"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / f"learning_report_{ts}.md"

    ok2 = _run_script(
        "scripts/13_generate_learning_report.py",
        ["--output", str(report_file), "--days", days],
        logger,
    )
    return ok1 and ok2


def step_calibrate(args, logger) -> bool:
    """Monthly threshold calibration."""
    return _run_script("scripts/10_monthly_calibration.py", logger=logger)


def step_embed(args, logger) -> bool:
    """Rebuild FAISS index from current synonyms."""
    return _run_script("scripts/09_generate_embeddings.py", logger=logger)


def step_status(args, logger) -> bool:
    """Print system health summary."""
    import sqlite3

    logger.info("=" * 70)
    logger.info("SYSTEM STATUS")
    logger.info("=" * 70)

    # Matcher DB
    matcher_db = project_root / "data" / "reg153_matcher.db"
    if matcher_db.exists():
        conn = sqlite3.connect(str(matcher_db))
        analytes = conn.execute("SELECT COUNT(*) FROM analytes").fetchone()[0]
        synonyms = conn.execute("SELECT COUNT(*) FROM synonyms").fetchone()[0]
        embeddings = conn.execute("SELECT COUNT(*) FROM embeddings_metadata").fetchone()[0]
        conn.close()
        logger.info(f"Matcher DB:  {analytes} analytes, {synonyms} synonyms, {embeddings} embeddings")
    else:
        logger.warning("Matcher DB not found!")

    # Lab results DB
    lab_db = project_root / "data" / "lab_results.db"
    if lab_db.exists():
        conn = sqlite3.connect(str(lab_db))
        submissions = conn.execute("SELECT COUNT(*) FROM lab_submissions").fetchone()[0]
        results = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
        conn.close()
        logger.info(f"Lab DB:      {submissions} submissions, {results} results")
    else:
        logger.info("Lab DB:      not created yet")

    # FAISS index
    faiss_path = project_root / "data" / "embeddings" / "faiss_index.bin"
    if faiss_path.exists():
        size_mb = faiss_path.stat().st_size / (1024 ** 2)
        logger.info(f"FAISS index: {size_mb:.1f} MB")
    else:
        logger.warning("FAISS index: NOT BUILT")

    # Reports
    daily = list((project_root / "reports" / "daily").glob("*.md"))
    monthly = list((project_root / "reports" / "monthly").glob("*"))
    logger.info(f"Reports:     {len(daily)} daily, {len(monthly)} monthly")

    logger.info("=" * 70)
    return True


def step_export(args, logger) -> bool:
    """Export matched results to formatted Excel."""
    extra = ["--all"]
    ts = datetime.now().strftime("%Y%m%d")
    out = project_root / "reports" / "exports" / f"all_results_{ts}.xlsx"
    extra.extend(["--output", str(out)])
    return _run_script("scripts/40_export_results.py", extra, logger)


def step_watch(args, logger) -> bool:
    """Launch the inbox file watcher (blocks until Ctrl-C)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "watch_inbox",
        str(project_root / "scripts" / "50_watch_inbox.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    inbox = Path(getattr(args, "inbox", str(project_root / "data" / "inbox")))
    inbox.mkdir(parents=True, exist_ok=True)

    if getattr(args, "once", False):
        stats = mod.scan_inbox(inbox, logger)
        return stats["failed"] == 0
    else:
        mod.watch_inbox(inbox, logger)
        return True


def step_full(args, logger) -> bool:
    """Run all pipeline steps in sequence."""
    steps = [
        ("INGEST", step_ingest),
        ("LEARN", step_learn),
        ("EMBED", step_embed),
        ("EXPORT", step_export),
        ("STATUS", step_status),
    ]

    # Add calibration if it's the 1st of the month
    if datetime.now().day == 1:
        steps.insert(2, ("CALIBRATE", step_calibrate))

    results = {}
    for name, fn in steps:
        logger.info(f"\n{'─' * 70}")
        logger.info(f"  STEP: {name}")
        logger.info(f"{'─' * 70}")
        try:
            results[name] = fn(args, logger)
        except Exception as e:
            logger.error(f"  {name} raised: {e}")
            results[name] = False

    # Summary
    logger.info(f"\n{'═' * 70}")
    logger.info("PIPELINE SUMMARY")
    logger.info(f"{'═' * 70}")
    for name, ok in results.items():
        status = "PASS" if ok else "FAIL"
        logger.info(f"  {name:15s} {status}")
    logger.info(f"{'═' * 70}")

    return all(results.values())


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Chemical Matcher Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    p_ingest = sub.add_parser("ingest", help="Batch-ingest new lab files")
    p_ingest.add_argument("--folder", default="Excel Lab examples")
    p_ingest.add_argument("--dry-run", action="store_true")

    # learn
    p_learn = sub.add_parser("learn", help="Ingest validations + report")
    p_learn.add_argument("--days", type=int, default=7)

    # calibrate
    sub.add_parser("calibrate", help="Monthly threshold recalibration")

    # embed
    sub.add_parser("embed", help="Rebuild FAISS semantic index")

    # export
    p_export = sub.add_parser("export", help="Export results to formatted Excel")
    p_export.add_argument("--submission-id", type=int, help="Export single submission")

    # watch
    p_watch = sub.add_parser("watch", help="Watch inbox folder for new lab files")
    p_watch.add_argument("--inbox", default=str(project_root / "data" / "inbox"),
                         help="Folder to watch (default: data/inbox/)")
    p_watch.add_argument("--once", action="store_true",
                         help="Process current files and exit (no continuous watch)")

    # status
    sub.add_parser("status", help="Print system health summary")

    # full
    p_full = sub.add_parser("full", help="Run all steps end-to-end")
    p_full.add_argument("--folder", default="Excel Lab examples")
    p_full.add_argument("--days", type=int, default=7)

    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    logger = _setup_logging(args.verbose)

    dispatch = {
        "ingest": step_ingest,
        "learn": step_learn,
        "calibrate": step_calibrate,
        "embed": step_embed,
        "export": step_export,
        "watch": step_watch,
        "status": step_status,
        "full": step_full,
    }

    ok = dispatch[args.command](args, logger)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
