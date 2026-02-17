"""
Script 50: Lab File Watcher — Drop-Folder Automation

Monitors a drop folder for new Excel lab files (.xls, .xlsx) and
automatically runs the full ingest-match-export pipeline when a
file arrives.

Production workflow:
    1. User drops an Excel file into the watched folder (default: data/inbox/)
    2. Watcher detects the new file arrival
    3. Pipeline auto-runs:
       a. OCR vendor detection (SGS / Caduceon / Eurofins)
       b. Chemical extraction & matching
       c. Results stored in lab_results.db
       d. Formatted Excel export generated in reports/exports/
    4. Processed file is moved to data/inbox/processed/
    5. Failed files are moved to data/inbox/failed/

Folder structure:
    data/
      inbox/              ← DROP FILES HERE
        processed/        ← auto-moved after success
        failed/           ← auto-moved on error

Usage:
    python scripts/50_watch_inbox.py                      # Watch default inbox
    python scripts/50_watch_inbox.py --inbox "C:/LabData"  # Custom folder
    python scripts/50_watch_inbox.py --once                # Process current files and exit
    python scripts/50_watch_inbox.py --no-export           # Ingest only, skip export

Service mode (stays running, watches for new files):
    python scripts/50_watch_inbox.py

One-shot mode (process what's there now, then exit):
    python scripts/50_watch_inbox.py --once
"""

import argparse
import hashlib
import logging
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ═══════════════════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_INBOX = project_root / "data" / "inbox"
PROCESSED_DIR = "processed"
FAILED_DIR = "failed"
LAB_DB_PATH = str(project_root / "data" / "lab_results.db")
MATCHER_DB_PATH = str(project_root / "data" / "reg153_matcher.db")
EXPORT_DIR = project_root / "reports" / "exports"
LOG_DIR = project_root / "logs" / "watcher"

SUPPORTED_EXTENSIONS = {".xls", ".xlsx"}

# Minimum file size (bytes) — skip empty/temp files
MIN_FILE_SIZE = 1024

# Seconds to wait after first detecting a file, to ensure it's finished copying
SETTLE_DELAY = 2.0


# ═══════════════════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_logging(verbose: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"watcher_{ts}.log"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("watcher")
    logger.info(f"Log: {log_file}")
    return logger


# ═══════════════════════════════════════════════════════════════════════════════
#  File processing
# ═══════════════════════════════════════════════════════════════════════════════

def _file_hash(filepath: Path) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_duplicate(filepath: Path) -> bool:
    """Check if this file has already been ingested (by hash)."""
    fhash = _file_hash(filepath)
    try:
        conn = sqlite3.connect(LAB_DB_PATH)
        row = conn.execute(
            "SELECT submission_id FROM lab_submissions WHERE file_hash = ?",
            (fhash,),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def _wait_for_stable(filepath: Path, timeout: float = 30.0) -> bool:
    """
    Wait until the file size stops changing (copy is complete).
    Returns True if stable, False if timed out.
    """
    prev_size = -1
    waited = 0.0
    interval = 0.5
    while waited < timeout:
        try:
            size = filepath.stat().st_size
        except FileNotFoundError:
            return False
        if size == prev_size and size > 0:
            return True
        prev_size = size
        time.sleep(interval)
        waited += interval
    return False


def process_file(
    filepath: Path,
    logger: logging.Logger,
    export: bool = True,
) -> bool:
    """
    Process a single lab file through the full pipeline.

    1. Detect vendor (OCR)
    2. Extract chemicals
    3. Match to canonical analytes
    4. Store in lab_results.db
    5. Export to Excel

    Returns True on success.
    """
    import importlib.util
    import pandas as pd

    from src.database.connection import DatabaseManager
    from src.normalization.text_normalizer import TextNormalizer
    from src.extraction import detect_format, detect_vendor

    logger.info(f"Processing: {filepath.name}")

    # ── Read file ──────────────────────────────────────────────────────────
    try:
        df = pd.read_excel(filepath, sheet_name=0, header=None)
    except Exception as e:
        logger.error(f"  Cannot read Excel file: {e}")
        return False

    # ── Detect format & vendor ─────────────────────────────────────────────
    fmt = detect_format(df, filepath.name)
    vendor = detect_vendor(filepath, df=df)
    logger.info(f"  Format: {fmt}  |  Vendor: {vendor}")

    if fmt == "unknown":
        logger.error(f"  Unrecognised file layout — skipping")
        return False

    # ── Ingest via batch_ingest.ingest_file() ──────────────────────────────
    spec = importlib.util.spec_from_file_location(
        "batch_ingest",
        str(project_root / "scripts" / "22_batch_ingest.py"),
    )
    batch_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(batch_mod)

    db_manager = DatabaseManager(MATCHER_DB_PATH)
    normalizer = TextNormalizer()

    sub_id, total, high_conf, accuracy = batch_mod.ingest_file(
        file_path=filepath,
        lab_db_path=LAB_DB_PATH,
        db_manager=db_manager,
        normalizer=normalizer,
    )

    if total == -1:
        # Already processed (duplicate)
        logger.info(f"  Already ingested (submission #{sub_id}) — skipping")
        return True

    if sub_id < 0:
        logger.error(f"  Ingest failed (no submission created)")
        return False

    # Check auto-accept stats
    import sqlite3 as _sqlite3
    _conn = _sqlite3.connect(LAB_DB_PATH)
    auto_accepted = _conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE submission_id = ? AND validation_status = 'accepted'",
        (sub_id,),
    ).fetchone()[0]
    pending = _conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE submission_id = ? AND validation_status = 'pending'",
        (sub_id,),
    ).fetchone()[0]
    _conn.close()

    logger.info(
        f"  Ingested → submission #{sub_id}: "
        f"{total} chemicals, {high_conf} high-confidence, "
        f"accuracy ≈ {accuracy:.1f}%"
    )
    if auto_accepted > 0:
        logger.info(
            f"  Auto-accepted: {auto_accepted}/{total} (≥98% confidence)"
        )
    if pending > 0:
        logger.info(
            f"  Needs review: {pending} result(s) below threshold"
        )

    # ── Export to Excel ────────────────────────────────────────────────────
    if export:
        try:
            EXPORT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = filepath.stem.replace(" ", "_")
            export_path = EXPORT_DIR / f"{safe_name}_{ts}.xlsx"

            spec = importlib.util.spec_from_file_location(
                "export_results",
                str(project_root / "scripts" / "40_export_results.py"),
            )
            export_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(export_mod)

            export_mod.export_submission(
                submission_id=sub_id,
                output_path=str(export_path),
                lab_db_path=LAB_DB_PATH,
                matcher_db_path=MATCHER_DB_PATH,
            )
            logger.info(f"  Exported → {export_path.name}")
        except Exception as e:
            logger.warning(f"  Export failed (data still saved in DB): {e}")

    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  Inbox scanning (one-shot mode)
# ═══════════════════════════════════════════════════════════════════════════════

def scan_inbox(
    inbox: Path,
    logger: logging.Logger,
    export: bool = True,
) -> dict:
    """
    Scan the inbox for Excel files and process each one.

    Returns dict with counts: {processed, skipped, failed, duplicate}.
    """
    processed_dir = inbox / PROCESSED_DIR
    failed_dir = inbox / FAILED_DIR
    processed_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(
        f
        for f in inbox.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    if not files:
        logger.info("Inbox is empty — nothing to process")
        return {"processed": 0, "skipped": 0, "failed": 0, "duplicate": 0}

    logger.info(f"Found {len(files)} file(s) in inbox")

    stats = {"processed": 0, "skipped": 0, "failed": 0, "duplicate": 0}

    for filepath in files:
        # Skip very small files (temp/lock files)
        if filepath.stat().st_size < MIN_FILE_SIZE:
            logger.debug(f"  Skipping tiny file: {filepath.name}")
            stats["skipped"] += 1
            continue

        # Check duplicate before processing
        if _is_duplicate(filepath):
            logger.info(f"  Duplicate — already ingested: {filepath.name}")
            shutil.move(str(filepath), str(processed_dir / filepath.name))
            stats["duplicate"] += 1
            continue

        # Process
        try:
            ok = process_file(filepath, logger, export=export)
        except Exception as e:
            logger.error(f"  UNCAUGHT ERROR processing {filepath.name}: {e}")
            ok = False

        if ok:
            dest = processed_dir / filepath.name
            if dest.exists():
                # Add timestamp to avoid overwriting
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest = processed_dir / f"{filepath.stem}_{ts}{filepath.suffix}"
            shutil.move(str(filepath), str(dest))
            stats["processed"] += 1
        else:
            dest = failed_dir / filepath.name
            if dest.exists():
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest = failed_dir / f"{filepath.stem}_{ts}{filepath.suffix}"
            shutil.move(str(filepath), str(dest))
            stats["failed"] += 1

    logger.info(
        f"Inbox scan complete: "
        f"{stats['processed']} processed, "
        f"{stats['duplicate']} duplicate, "
        f"{stats['failed']} failed, "
        f"{stats['skipped']} skipped"
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
#  File watcher (service mode)
# ═══════════════════════════════════════════════════════════════════════════════

class LabFileHandler:
    """Watchdog event handler that processes new Excel files."""

    def __init__(self, inbox: Path, logger: logging.Logger, export: bool = True):
        self.inbox = inbox
        self.logger = logger
        self.export = export
        self.processed_dir = inbox / PROCESSED_DIR
        self.failed_dir = inbox / FAILED_DIR
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self._pending: set = set()

    def dispatch(self, event):
        """Called by watchdog on any filesystem event."""
        # Only care about file creation / moved-in events
        if event.is_directory:
            return
        if hasattr(event, "event_type") and event.event_type not in (
            "created",
            "moved",
        ):
            return

        filepath = Path(
            event.dest_path if hasattr(event, "dest_path") and event.event_type == "moved" else event.src_path
        )

        # Only Excel files in the inbox root (not subfolders)
        if filepath.parent != self.inbox:
            return
        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return
        if filepath.name.startswith("~$"):  # Skip Office temp files
            return

        # Avoid re-processing
        if filepath in self._pending:
            return
        self._pending.add(filepath)

        # Let the file finish copying
        self.logger.info(f"New file detected: {filepath.name}")
        if not _wait_for_stable(filepath):
            self.logger.warning(f"  File not stable (still copying?)")
            self._pending.discard(filepath)
            return

        time.sleep(SETTLE_DELAY)

        # Process
        try:
            if _is_duplicate(filepath):
                self.logger.info(f"  Duplicate — moving to processed/")
                shutil.move(str(filepath), str(self.processed_dir / filepath.name))
            else:
                ok = process_file(filepath, self.logger, export=self.export)
                if ok:
                    dest = self.processed_dir / filepath.name
                    if dest.exists():
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        dest = self.processed_dir / f"{filepath.stem}_{ts}{filepath.suffix}"
                    shutil.move(str(filepath), str(dest))
                else:
                    dest = self.failed_dir / filepath.name
                    if dest.exists():
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        dest = self.failed_dir / f"{filepath.stem}_{ts}{filepath.suffix}"
                    shutil.move(str(filepath), str(dest))
        except Exception as e:
            self.logger.error(f"  Error handling {filepath.name}: {e}")
        finally:
            self._pending.discard(filepath)


def watch_inbox(
    inbox: Path,
    logger: logging.Logger,
    export: bool = True,
):
    """Start the file watcher (blocks forever until Ctrl-C)."""
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class _Handler(FileSystemEventHandler):
        def __init__(self):
            super().__init__()
            self.lab_handler = LabFileHandler(inbox, logger, export)

        def on_created(self, event):
            self.lab_handler.dispatch(event)

        def on_moved(self, event):
            self.lab_handler.dispatch(event)

    handler = _Handler()
    observer = Observer()
    observer.schedule(handler, str(inbox), recursive=False)
    observer.start()

    logger.info(f"Watching: {inbox}")
    logger.info(f"Drop .xls/.xlsx files here — they'll be auto-processed")
    logger.info(f"Press Ctrl+C to stop")
    logger.info("")

    # Also process any files already sitting in the inbox
    scan_inbox(inbox, logger, export=export)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down watcher...")
        observer.stop()
    observer.join()
    logger.info("Watcher stopped")


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Lab File Watcher — Drop-Folder Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/50_watch_inbox.py                  # Watch default inbox folder
  python scripts/50_watch_inbox.py --once            # Process current files, then exit
  python scripts/50_watch_inbox.py --inbox "C:/Labs"  # Watch custom folder
        """,
    )
    parser.add_argument(
        "--inbox",
        type=str,
        default=str(DEFAULT_INBOX),
        help=f"Folder to watch (default: {DEFAULT_INBOX})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process current inbox files and exit (no watching)",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip Excel export (data still goes to database)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()
    logger = _setup_logging(args.verbose)

    inbox = Path(args.inbox)
    inbox.mkdir(parents=True, exist_ok=True)

    do_export = not args.no_export

    logger.info("=" * 60)
    logger.info("  LAB FILE WATCHER")
    logger.info(f"  Inbox:  {inbox}")
    logger.info(f"  Export: {'ON' if do_export else 'OFF'}")
    logger.info(f"  Mode:   {'one-shot' if args.once else 'continuous watch'}")
    logger.info("=" * 60)

    if args.once:
        stats = scan_inbox(inbox, logger, export=do_export)
        sys.exit(0 if stats["failed"] == 0 else 1)
    else:
        watch_inbox(inbox, logger, export=do_export)


if __name__ == "__main__":
    main()
