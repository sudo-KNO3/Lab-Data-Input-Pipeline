#!/usr/bin/env python3
"""Search Excel files for a specific string."""

import pandas as pd
import sys
from pathlib import Path

search_term = sys.argv[1] if len(sys.argv) > 1 else "1,1,1-TCA"
folder = sys.argv[2] if len(sys.argv) > 2 else "Excel Lab examples"

excel_files = list(Path(folder).glob("*.xlsx"))

print(f"Searching for '{search_term}' in {len(excel_files)} files...\n")

found_files = []

for file in excel_files:
    try:
        xl = pd.ExcelFile(file)
        found_in_file = False
        
        for sheet in xl.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet)
            # Convert all columns to string and search
            if df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any().any():
                found_in_file = True
                break
        
        if found_in_file:
            found_files.append(file.name)
            print(f"  FOUND: {file.name}")
    except Exception as e:
        pass

if not found_files:
    print(f"  Not found in any files")
else:
    print(f"\nTotal: {len(found_files)} file(s) contain '{search_term}'")
