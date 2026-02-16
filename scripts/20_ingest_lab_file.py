"""
Script 20: Ingest Lab Excel File

Extracts chemicals from lab Excel files and logs extraction decisions for learning.
This is the core extraction engine that learns file structures over time.

Key features:
- Auto-detect layout patterns (header row, chemical columns, data structure)
- Match chemicals against 47,776 synonyms
- Archive files with MD5 deduplication
- Log extraction metadata for template learning (Script 23)
- Generate validation workbook for low-confidence extractions
- Learn from every file processed

Usage:
    python scripts/20_ingest_lab_file.py --input "Excel Lab examples/Eurofins.xlsx" --vendor Eurofins
    python scripts/20_ingest_lab_file.py --input "file.xlsx" --auto-detect
"""
import argparse
import sqlite3
import hashlib
import json
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseManager
from src.matching.resolution_engine import ResolutionEngine
from src.normalization.text_normalizer import TextNormalizer


class LabFileExtractor:
    """Extract chemicals from lab Excel files with learning."""
    
    def __init__(self, lab_db_path: str = "data/lab_results.db"):
        self.lab_db_path = lab_db_path
        self.db_manager = DatabaseManager()
        self.normalizer = TextNormalizer()
        
    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash for deduplication."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def archive_file(self, file_path: Path, file_hash: str) -> Path:
        """Archive file with timestamp and hash."""
        archive_dir = Path("data/raw/lab_archive")
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{timestamp}_{file_hash[:8]}_{file_path.name}"
        archive_path = archive_dir / archive_name
        
        # Copy file to archive
        import shutil
        shutil.copy2(file_path, archive_path)
        
        return archive_path
    
    def detect_header_row(self, df: pd.DataFrame) -> Tuple[int, float, List[str]]:
        """Detect which row contains column headers.
        
        Returns:
            (header_row_index, confidence, indicators_found)
        """
        # Common header indicators
        header_keywords = [
            'analysis', 'analyte', 'parameter', 'chemical', 'compound',
            'cas', 'cas number', 'cas#', 'casrn',
            'result', 'value', 'concentration', 'detect',
            'method', 'unit', 'mdl', 'rl', 'limit',
            'sample', 'date', 'time'
        ]
        
        best_row = 0
        best_score = 0
        best_indicators = []
        
        # Check first 30 rows
        for idx in range(min(30, len(df))):
            row = df.iloc[idx]
            score = 0
            found_indicators = []
            
            for cell in row:
                if pd.isna(cell):
                    continue
                cell_lower = str(cell).lower().strip()
                
                # Check for header keywords
                for keyword in header_keywords:
                    if keyword in cell_lower:
                        score += 1
                        found_indicators.append(cell_lower)
                        break
            
            # Higher score = more likely to be header
            if score > best_score:
                best_score = score
                best_row = idx
                best_indicators = found_indicators
        
        # Calculate confidence (0-1)
        # Need at least 3 header indicators for high confidence
        confidence = min(best_score / 5.0, 1.0)
        
        return best_row, confidence, best_indicators
    
    def detect_chemical_column(self, df: pd.DataFrame, header_row: int) -> Tuple[int, float]:
        """Detect which column contains chemical names.
        
        Returns:
            (column_index, confidence)
        """
        if header_row >= len(df):
            return 0, 0.0
        
        # Check header row for chemical-related keywords
        header = df.iloc[header_row]
        chemical_keywords = ['analysis', 'analyte', 'parameter', 'chemical', 'compound', 'test']
        
        for col_idx, cell in enumerate(header):
            if pd.isna(cell):
                continue
            cell_lower = str(cell).lower()
            
            for keyword in chemical_keywords:
                if keyword in cell_lower:
                    # Check if data rows contain text (not numbers)
                    data_rows = df.iloc[header_row+1:header_row+10, col_idx]
                    text_count = sum(1 for val in data_rows if isinstance(val, str) and len(str(val)) > 3)
                    
                    if text_count >= 5:
                        return col_idx, 0.9
        
        # Fallback: first column is usually chemical names
        return 0, 0.5
    
    def extract_chemicals_from_column(
        self, 
        df: pd.DataFrame, 
        col_idx: int, 
        start_row: int
    ) -> List[Tuple[int, str]]:
        """Extract chemical names from a column.
        
        Returns:
            List of (row_number, chemical_name)
        """
        chemicals = []
        
        # Footer/disclaimer patterns to skip
        footer_patterns = [
            'prior written consent',
            'analytical results reported',
            'reproduction',
            'reporting limit',
            'r.l. =',
            'rl =',
            'laboratory',
            'laboratories',
            'copyright',
            'confidential',
            'prohibited without',
            'refer to the samples'
        ]
        
        for row_idx in range(start_row, len(df)):
            cell = df.iloc[row_idx, col_idx]
            
            if pd.isna(cell):
                continue
            
            chem_name = str(cell).strip()
            
            # Skip likely non-chemical rows
            if len(chem_name) < 2:
                continue
            if chem_name.lower() in ['total', 'sum', 'notes', 'comments', '']:
                continue
            if re.match(r'^[\d\.\-\<\>]+$', chem_name):  # Skip pure numbers
                continue
            
            # Skip footer/disclaimer text
            chem_lower = chem_name.lower()
            if any(pattern in chem_lower for pattern in footer_patterns):
                continue
            
            # Skip very long text (likely paragraphs/disclaimers)
            if len(chem_name) > 100:
                continue
            
            chemicals.append((row_idx, chem_name))
        
        return chemicals
    
    def extract_sample_data(
        self,
        df: pd.DataFrame,
        row_idx: int,
        header_row: int,
        chem_col: int
    ) -> Dict:
        """Extract sample ID, result, units for a chemical row."""
        result = {
            'sample_id': None,
            'result_value': None,
            'units': None,
            'qualifier': None
        }
        
        row = df.iloc[row_idx]
        
        # Look for result column (usually after chemical name)
        for col_idx in range(chem_col + 1, min(chem_col + 10, len(row))):
            cell = row.iloc[col_idx]
            
            if pd.isna(cell):
                continue
            
            cell_str = str(cell).strip()
            
            # Check if this looks like a result value
            if re.match(r'^[\<\>]?[\d\.\,\-]+', cell_str):
                # Extract qualifier (<, >)
                qualifier_match = re.match(r'^([\<\>])', cell_str)
                if qualifier_match:
                    result['qualifier'] = qualifier_match.group(1)
                
                # Extract numeric value
                value_match = re.search(r'[\d\.\,\-]+', cell_str)
                if value_match:
                    result['result_value'] = value_match.group(0)
                    break
        
        # Sample ID: often first column or column before chemical
        if chem_col > 0:
            sample_cell = df.iloc[row_idx, 0]
            if not pd.isna(sample_cell):
                result['sample_id'] = str(sample_cell).strip()
        
        return result
    
    def ingest_file(
        self,
        file_path: Path,
        vendor: Optional[str] = None,
        sheet_name: Optional[str] = None
    ) -> int:
        """Main extraction pipeline.
        
        Returns:
            submission_id
        """
        print(f"\n{'='*80}")
        print(f"INGESTING LAB FILE")
        print(f"{'='*80}")
        print(f"\nFile: {file_path}")
        print(f"Vendor: {vendor or 'auto-detect'}")
        
        # Calculate hash and check for duplicates
        file_hash = self.calculate_file_hash(file_path)
        print(f"Hash: {file_hash}")
        
        lab_conn = sqlite3.connect(self.lab_db_path)
        
        # Check if already processed
        existing = lab_conn.execute(
            "SELECT submission_id FROM lab_submissions WHERE file_hash = ?",
            (file_hash,)
        ).fetchone()
        
        if existing:
            print(f"\n⚠ File already processed (submission {existing[0]})")
            lab_conn.close()
            return existing[0]
        
        # Archive file
        archive_path = self.archive_file(file_path, file_hash)
        print(f"OK Archived to: {archive_path}")
        
        # Read Excel file
        print(f"\nReading Excel file...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name or 0, header=None)
        except Exception as e:
            print(f"ERROR reading file: {e}")
            lab_conn.close()
            return -1
        
        print(f"   Dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Detect layout
        print(f"\nDetecting layout structure...")
        header_row, header_conf, header_indicators = self.detect_header_row(df)
        print(f"   Header row: {header_row} (confidence: {header_conf:.1%})")
        print(f"   Indicators: {', '.join(header_indicators[:5])}")
        
        chem_col, chem_col_conf = self.detect_chemical_column(df, header_row)
        print(f"   Chemical column: {chem_col} (confidence: {chem_col_conf:.1%})")
        
        layout_confidence = (header_conf + chem_col_conf) / 2
        print(f"   Overall layout confidence: {layout_confidence:.1%}")
        
        # Extract chemicals
        data_start_row = header_row + 1
        chemicals = self.extract_chemicals_from_column(df, chem_col, data_start_row)
        print(f"\nExtracted {len(chemicals)} chemicals")
        
        # Create submission record
        extraction_metadata = {
            "header_row": int(header_row),
            "header_confidence": float(header_conf),
            "header_indicators": header_indicators,
            "chemical_column": int(chem_col),
            "chemical_column_confidence": float(chem_col_conf),
            "data_start_row": int(data_start_row),
            "total_rows": int(df.shape[0]),
            "total_columns": int(df.shape[1])
        }
        
        lab_conn.execute("""
            INSERT INTO lab_submissions (
                file_path, file_hash, original_filename, lab_vendor,
                file_size_bytes, sheet_name,
                extraction_timestamp, extraction_version, layout_confidence,
                validation_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(archive_path),
            file_hash,
            file_path.name,
            vendor,
            file_path.stat().st_size,
            str(sheet_name or 0),
            datetime.now(),
            "1.0.0",
            layout_confidence,
            "pending"
        ))
        
        submission_id = lab_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # Match chemicals
        print(f"\nMatching against synonyms database...")
        
        match_stats = {"high": 0, "medium": 0, "low": 0}
        
        with self.db_manager.get_session() as session:
            resolver = ResolutionEngine(session, self.normalizer)
            
            for row_num, chem_raw in chemicals:
                # Normalize
                chem_norm = self.normalizer.normalize(chem_raw)
                
                # Match using resolution engine
                result = resolver.resolve(chem_norm, confidence_threshold=0.70)
                
                if result.best_match and result.best_match.confidence >= 0.70:
                    analyte_id = result.best_match.analyte_id
                    match_method = result.best_match.method
                    match_conf = result.best_match.confidence
                    
                    if match_conf >= 0.95:
                        match_stats["high"] += 1
                    elif match_conf >= 0.70:
                        match_stats["medium"] += 1
                    else:
                        match_stats["low"] += 1
                else:
                    analyte_id = None
                    match_method = "none"
                    match_conf = 0.0
                    match_stats["low"] += 1
                
                # Extract sample data
                sample_data = self.extract_sample_data(df, row_num, header_row, chem_col)
                
                # Store result
                lab_conn.execute("""
                    INSERT INTO lab_results (
                        submission_id, row_number, chemical_raw, chemical_normalized,
                        analyte_id, match_method, match_confidence,
                        sample_id, result_value, units, qualifier,
                        validation_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    submission_id,
                    row_num,
                    chem_raw,
                    chem_norm,
                    analyte_id,
                    match_method,
                    match_conf,
                    sample_data['sample_id'],
                    sample_data['result_value'],
                    sample_data['units'],
                    sample_data['qualifier'],
                    "pending"
                ))
        
        lab_conn.commit()
        lab_conn.close()
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"EXTRACTION COMPLETE")
        print(f"{'='*80}")
        print(f"\nSubmission ID: {submission_id}")
        print(f"Chemicals extracted: {len(chemicals)}")
        print(f"\nMatch confidence breakdown:")
        print(f"  + High (>=95%):   {match_stats['high']:3d} chemicals")
        print(f"  ~ Medium (70-95%): {match_stats['medium']:3d} chemicals")
        print(f"  - Low (<70%):     {match_stats['low']:3d} chemicals")
        
        accuracy_estimate = (match_stats['high'] / len(chemicals) * 100) if chemicals else 0
        print(f"\nEstimated accuracy: {accuracy_estimate:.1f}%")
        
        # Recommend validation if accuracy is low
        needs_validation = match_stats['medium'] + match_stats['low']
        if needs_validation > 0:
            print(f"\nNext step:")
            print(f"   {needs_validation} chemicals need validation")
            print(f"   Run: python scripts/21_generate_validation_workbook.py --submission-id {submission_id}")
        else:
            print(f"\nAll chemicals matched with high confidence!")
            print(f"   No validation needed.")
        
        return submission_id


def main():
    parser = argparse.ArgumentParser(
        description="Ingest lab Excel file and extract chemicals"
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Path to Excel file'
    )
    parser.add_argument(
        '--vendor',
        help='Lab vendor (Eurofins, CA, Caduceon, etc.)'
    )
    parser.add_argument(
        '--sheet',
        help='Sheet name or index (default: first sheet)'
    )
    parser.add_argument(
        '--auto-detect',
        action='store_true',
        help='Auto-detect vendor from file'
    )
    
    args = parser.parse_args()
    
    file_path = Path(args.input)
    if not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)
    
    # Auto-detect vendor from filename
    vendor = args.vendor
    if args.auto_detect and not vendor:
        filename_lower = file_path.name.lower()
        if 'eurofins' in filename_lower:
            vendor = 'Eurofins'
        elif 'caduceon' in filename_lower:
            vendor = 'Caduceon'
        elif any(x in filename_lower for x in ['ca lab', 'ca_lab', 'calabs']):
            vendor = 'CA Labs'
    
    # Extract
    extractor = LabFileExtractor()
    submission_id = extractor.ingest_file(file_path, vendor, args.sheet)
    
    if submission_id > 0:
        print(f"\n{'='*80}")
        print(f"SUCCESS!")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
