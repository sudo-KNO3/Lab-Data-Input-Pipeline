# TERMINAL-BASED VALIDATION WORKFLOW

## Quick Start

```powershell
python scripts/21_validate_interactive.py --submission-id 2 --auto-accept-confident
```

## What You'll See

```
================================================================================
INTERACTIVE VALIDATION
================================================================================

File: 20240125 Eldon SW CKL Caduceon 24-002573.xlsx
Vendor: Caduceon
Layout Confidence: 65.0%

Total chemicals: 71
  Auto-accepting 54 high-confidence matches

================================================================================
Chemical 1/17 (Row 28)
================================================================================

  Original: Antimony
  Normalized: antimony
  Context: Sample=SW-01, Result=<1.0 µg/L

  Status: ✗ LOW CONFIDENCE (0.0% confidence)
  Current Match: None

  Top Suggestions:
     1. Antimony                                  (ELEMENT_051) - 99.9%
     2. Antimony trioxide                         (ELEMENT_051) - 75.2%
     3. Antimony pentachloride                    (ELEMENT_051) - 68.1%

  Actions:
    1-5    = Select suggestion
    ENTER  = Accept current match
    s      = Skip (don't validate)
    ID     = Type analyte ID (e.g., REG153_VOCS_005)
    q      = Quit and save

  Your choice: █
```

## Example Session

```
Choice: 1                    → Select "Antimony (ELEMENT_051)"
✓ Updated to: Antimony (ELEMENT_051)
   → Added synonym to knowledge base

Choice: 2                    → Select second suggestion
✓ Updated to: Chemical (ID)

Choice: ENTER                → Accept current match
✓ Accepted

Choice: REG153_VOCS_005      → Manual entry
✓ Updated to: Benzene (REG153_VOCS_005)

Choice: s                    → Skip this chemical
⊘ Skipped

Choice: q                    → Save and quit
Saving progress...
```

## Final Summary

```
================================================================================
VALIDATION COMPLETE
================================================================================

Submission ID: 2
File: 20240125 Eldon SW CKL Caduceon 24-002573.xlsx

Results:
  Total chemicals:      71
  Auto-accepted:        54
  Reviewed:              8
  Corrected:             9
  Skipped:               0
  Synonyms added:        9

Extraction Accuracy: 98.6%

💡 RETRAINING RECOMMENDED
   10 validated files ready for template learning
   Run: python scripts/23_retrain_from_validated.py
```

## Features

### ✅ Keyboard-Driven
- Type `1-5` to select suggestion
- Press `ENTER` to accept
- Type `s` to skip
- Type `q` to quit anytime
- Type full ID for manual entry

### ✅ Auto-Accept High Confidence
- `--auto-accept-confident` flag
- Skips 95%+ matches
- Focus only on errors

### ✅ Context Display
- Shows sample ID and result values
- Helps make informed decisions
- Color-coded status (✓⚠✗)

### ✅ Real-Time Learning
- Synonyms added immediately
- Updates database on each correction
- Progress saved continuously

## Advanced Usage

### Review Only Low Confidence
```powershell
# Auto-accept everything ≥95%, only review <95%
python scripts/21_validate_interactive.py --submission-id 2 --auto-accept-confident
```

### Custom Threshold
```powershell
# Auto-accept everything ≥90%
python scripts/21_validate_interactive.py --submission-id 2 --auto-accept-confident --confidence-threshold 0.90
```

### Batch Mode
```powershell
# Validate multiple files
foreach ($id in 2..10) {
    python scripts/21_validate_interactive.py --submission-id $id --auto-accept-confident
}
```

## Tips for Fast Validation

1. **Use auto-accept** - Skip 95%+ matches automatically
2. **Trust top suggestion** - Usually correct, just type `1`
3. **Use context** - Sample IDs help identify correct match
4. **Quit anytime** - Progress saved, resume later
5. **Batch corrections** - Similar chemicals appear together

## Time Estimates

| Extraction Accuracy | Chemicals to Review | Time per File |
|---------------------|---------------------|---------------|
| 76% (first file)    | 17 chemicals        | 5-10 min      |
| 90% (10th file)     | 7 chemicals         | 2-3 min       |
| 98% (50th file)     | 1 chemical          | 30 sec        |
| 99% (100th file)    | 0 chemicals         | 0 sec         |

## Comparison: Terminal vs Excel

| Feature              | Terminal | Excel   |
|----------------------|----------|---------|
| Speed                | ⚡ Fast  | 🐌 Slow |
| No app switching     | ✅       | ❌      |
| Keyboard shortcuts   | ✅       | ❌      |
| Top 5 suggestions    | ✅       | ❌      |
| Context display      | ✅       | ❌      |
| Real-time learning   | ✅       | ❌      |
| Progress save        | ✅ Auto  | Manual  |
| Batch processing     | ✅ Easy  | Hard    |

## Next Steps

After validation:
```powershell
# Check system accuracy
python scripts/24_calculate_system_accuracy.py

# Retrain templates (after 10+ files)
python scripts/23_retrain_from_validated.py

# Process next file
python scripts/20_ingest_lab_file.py --input "next_file.xlsx" --auto-detect
```
