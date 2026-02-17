"""
OCR-based lab vendor detection.

Extracts embedded logo images from Excel files and uses Tesseract OCR
to identify the lab vendor name. This is critical for accurate tracking
because some labs (e.g., SGS) only place their name as an image logo
with no text-based identifier anywhere in the file.

Supports:
  - .xls  (BIFF8/OLE2): Extracts PNG/JPEG from raw binary stream
  - .xlsx (OOXML):       Extracts images via openpyxl

Known lab signatures (OCR output → canonical name):
  - "SGS"                           → "SGS"
  - "CADUCEON" / "CADUCE*"          → "Caduceon"
  - "EUROFINS" / "Eurofins"        → "Eurofins"
  - "BUREAU VERITAS"                → "Bureau Veritas"
  - "ALS"                           → "ALS"

Requires:
  - pytesseract (pip install pytesseract)
  - Tesseract OCR binary (winget install UB-Mannheim.TesseractOCR)
  - Pillow (pip install Pillow)
  - openpyxl (pip install openpyxl)  — for .xlsx only
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tesseract configuration
# ---------------------------------------------------------------------------

# Common install locations on Windows
_TESSERACT_PATHS = [
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
]


def _configure_tesseract() -> bool:
    """Find and configure the Tesseract binary path. Returns True if found."""
    try:
        import pytesseract
    except ImportError:
        logger.warning("pytesseract not installed — OCR vendor detection disabled")
        return False

    # Already configured or on PATH?
    try:
        pytesseract.get_tesseract_version()
        return True
    except Exception:
        pass

    # Check environment variable
    env_path = os.environ.get("TESSERACT_CMD")
    if env_path and Path(env_path).exists():
        pytesseract.pytesseract.tesseract_cmd = env_path
        return True

    # Scan common locations
    for p in _TESSERACT_PATHS:
        if Path(p).exists():
            pytesseract.pytesseract.tesseract_cmd = p
            return True

    logger.warning(
        "Tesseract OCR binary not found — install via: "
        "winget install UB-Mannheim.TesseractOCR"
    )
    return False


_tesseract_available: Optional[bool] = None


def _ensure_tesseract() -> bool:
    """Lazy one-time check for Tesseract availability."""
    global _tesseract_available
    if _tesseract_available is None:
        _tesseract_available = _configure_tesseract()
    return _tesseract_available


# ---------------------------------------------------------------------------
# Image extraction helpers
# ---------------------------------------------------------------------------

def _extract_images_from_xls(filepath: Path) -> List[Image.Image]:
    """
    Extract embedded PNG and JPEG images from a .xls (BIFF8) file.

    The BIFF8 format embeds images as MSODrawing records inside the
    Workbook stream. We scan the raw bytes for PNG (\\x89PNG) and
    JPEG (\\xff\\xd8\\xff) signatures and extract them.
    """
    images: List[Image.Image] = []
    try:
        data = filepath.read_bytes()
    except Exception as e:
        logger.debug("Could not read %s: %s", filepath.name, e)
        return images

    offset = 0
    while offset < len(data):
        png_pos = data.find(b"\x89PNG", offset)
        jpg_pos = data.find(b"\xff\xd8\xff", offset)

        if png_pos < 0 and jpg_pos < 0:
            break

        # Process whichever comes first
        if png_pos >= 0 and (jpg_pos < 0 or png_pos < jpg_pos):
            end = data.find(b"IEND", png_pos)
            if end >= 0:
                # IEND chunk = 4-byte length + "IEND" + 4-byte CRC
                img_bytes = data[png_pos : end + 12]
                try:
                    img = Image.open(io.BytesIO(img_bytes))
                    img.load()  # Force decode to verify validity
                    images.append(img)
                except Exception:
                    pass
            offset = png_pos + 4
        else:
            end = data.find(b"\xff\xd9", jpg_pos + 2)
            if end >= 0:
                img_bytes = data[jpg_pos : end + 2]
                try:
                    img = Image.open(io.BytesIO(img_bytes))
                    img.load()
                    images.append(img)
                except Exception:
                    pass
            offset = jpg_pos + 3

    return images


def _extract_images_from_xlsx(filepath: Path) -> List[Image.Image]:
    """Extract embedded images from a .xlsx file via openpyxl."""
    images: List[Image.Image] = []
    try:
        import openpyxl

        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.worksheets[0]
        for img in ws._images:
            try:
                pil_img = Image.open(io.BytesIO(img._data()))
                pil_img.load()
                images.append(pil_img)
            except Exception:
                pass
        wb.close()
    except Exception as e:
        logger.debug("Could not read xlsx images from %s: %s", filepath.name, e)

    return images


def extract_images(filepath: Path) -> List[Image.Image]:
    """Extract all embedded images from an Excel file (.xls or .xlsx)."""
    suffix = filepath.suffix.lower()
    if suffix == ".xlsx":
        return _extract_images_from_xlsx(filepath)
    elif suffix in (".xls",):
        return _extract_images_from_xls(filepath)
    else:
        return []


# ---------------------------------------------------------------------------
# Lab name matching
# ---------------------------------------------------------------------------

# Map from OCR text fragments (lowercased) to canonical vendor names.
# Order matters — first match wins, so put more specific patterns first.
_LAB_SIGNATURES: List[Tuple[str, str]] = [
    ("sgs", "SGS"),
    ("caduce", "Caduceon"),           # OCR sometimes garbles → "CADUCESZPWN"
    ("eurofins", "Eurofins"),
    ("bureau veritas", "Bureau Veritas"),
    ("als environmental", "ALS"),
    ("als lab", "ALS"),
    ("maxxam", "Maxxam"),
    ("testmark", "Testmark"),
]


def _match_lab_name(ocr_text: str) -> Optional[str]:
    """Match OCR text against known lab name signatures."""
    text_lower = ocr_text.lower()
    for fragment, canonical in _LAB_SIGNATURES:
        if fragment in text_lower:
            return canonical
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_vendor_ocr(filepath: Path) -> Optional[str]:
    """
    Detect lab vendor by OCR-ing embedded logo images in an Excel file.

    Args:
        filepath: Path to the Excel file (.xls or .xlsx).

    Returns:
        Canonical vendor name (e.g. "SGS", "Caduceon", "Eurofins"),
        or None if no vendor could be determined from images.
    """
    if not _ensure_tesseract():
        return None

    import pytesseract

    images = extract_images(filepath)
    if not images:
        logger.debug("No images found in %s", filepath.name)
        return None

    for img in images:
        try:
            text = pytesseract.image_to_string(img).strip()
            if not text:
                continue

            vendor = _match_lab_name(text)
            if vendor:
                logger.debug(
                    "OCR vendor detection: %s → %s (raw: %s)",
                    filepath.name,
                    vendor,
                    text[:60],
                )
                return vendor

        except Exception as e:
            logger.debug("OCR failed on image from %s: %s", filepath.name, e)

    logger.debug("OCR found images but no lab match in %s", filepath.name)
    return None


def detect_vendor(
    filepath: Path,
    df: Optional["pd.DataFrame"] = None,
) -> str:
    """
    Detect lab vendor using all available signals (OCR + cell text).

    Priority order:
      1. OCR logo image (most reliable, works for all labs)
      2. Cell text inspection (fallback for files without images)
      3. "Unknown" default

    Args:
        filepath: Path to the Excel file.
        df: Optional pre-loaded DataFrame (header=None) to inspect cell text.

    Returns:
        Canonical vendor name string (never None).
    """
    # --- 1. Try OCR on embedded images ---
    ocr_result = detect_vendor_ocr(filepath)
    if ocr_result:
        return ocr_result

    # --- 2. Fall back to cell text inspection ---
    if df is not None:
        import pandas as pd

        # Eurofins: row 0 col 5 contains "Eurofins Environment Testing..."
        if df.shape[1] > 5:
            cell_05 = str(df.iloc[0, 5]) if pd.notna(df.iloc[0, 5]) else ""
            if "eurofins" in cell_05.lower():
                return "Eurofins"

        # Caduceon: row 7 col 5 contains "CADUCEON Environmental Laboratories"
        if df.shape[0] > 7 and df.shape[1] > 5:
            cell_75 = str(df.iloc[7, 5]) if pd.notna(df.iloc[7, 5]) else ""
            if "caduceon" in cell_75.lower():
                return "Caduceon"

    # --- 3. Default ---
    return "Unknown"
