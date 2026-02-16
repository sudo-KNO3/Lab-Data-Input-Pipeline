# Batch Processing & Production Workflow

## Overview

This document describes the production batch processing scripts for the Chemical Matcher system. These scripts enable automated matching of lab EDD files, human validation workflows, and continuous learning.

## Scripts Overview

### 1. `11_match_batch.py` - Batch Matching CLI

**Purpose:** Process lab EDD files and generate matched results with confidence scores.

**Usage:**
```bash
# Basic usage with auto-detection
python scripts/11_match_batch.py --input data/raw/lab_edds/sample.xlsx --output results.xlsx

# Specify column and threshold
python scripts/11_match_batch.py --input data.csv --column "Parameter" --output matched.xlsx --confidence-threshold 0.80

# Generate separate review queue
python scripts/11_match_batch.py --input data.xlsx --output matched.xlsx --review-queue review.xlsx
```

**Features:**
- Auto-detects analyte column in Excel/CSV files
- Processes thousands of analytes with progress tracking
- Generates two outputs:
  - `matched_results.xlsx`: All results with confidence scores
  - `review_queue.xlsx`: Only flagged/unknown cases (optional)
- Logs all decisions to database for learning
- Provides summary statistics

**Output Columns:**
- `Original_Name`: Input text as provided
- `Matched_Analyte_ID`: REG153_XXX identifier
- `Matched_Preferred_Name`: Canonical name
- `CAS_Number`: CAS Registry Number (if available)
- `Confidence`: Confidence score (0.0-1.0)
- `Match_Method`: exact, fuzzy, semantic, cas_extracted, or unknown
- `Review_Flag`: TRUE if human review recommended
- `Top_3_Candidates`: JSON array of alternative matches
- `Resolution_Time_ms`: Processing time

---

### 2. `12_validate_and_learn.py` - Validation Ingestion

**Purpose:** Ingest human-validated synonyms into the database for Layer 1 learning.

**Usage:**
```bash
# Ingest specific validation file
python scripts/12_validate_and_learn.py --review-queue reports/review_validated.xlsx

# Auto-ingest mode (daily automation)
python scripts/12_validate_and_learn.py --auto-ingest
```

**Features:**
- Supports multiple validation file formats
- Deduplicates existing synonyms
- Marks match decisions as validated
- Tracks retraining trigger threshold (2000+ validations)
- Provides summary statistics

**Validation File Format:**

**Option A: Review Queue Format (from generate_review_queue.py)**
- `raw_variant`: Original text
- `chosen_match`: Selected preferred name (dropdown)
- `validation_confidence`: HIGH, MEDIUM, or LOW

**Option B: Simple Format**
- Column 1: Variant text
- Column 2: Validated match (preferred name)

---

### 3. `13_generate_learning_report.py` - Learning Health Report

**Purpose:** Generate comprehensive learning health reports for monitoring system maturity.

**Usage:**
```bash
# Generate default weekly report
python scripts/13_generate_learning_report.py

# Generate report for last 14 days
python scripts/13_generate_learning_report.py --days 14

# Generate markdown report to specific location
python scripts/13_generate_learning_report.py --output reports/weekly/report.md --format markdown
```

**Features:**
- Corpus maturity scoring (0-100)
- Match method distribution trends
- Synonym growth tracking
- Threshold calibration status
- Retraining progress indicators
- Actionable recommendations

**Report Sections:**
1. Executive Summary with maturity score
2. Match Performance Metrics
3. Corpus Growth & Coverage
4. Threshold Calibration Status
5. Learning Progress Tracking (4 layers)
6. Retraining Indicators
7. Action Items & Recommendations

---

### 4. `14_check_retraining_need.py` - Retraining Assessment

**Purpose:** Evaluate whether neural model retraining is warranted based on multiple trigger conditions.

**Usage:**
```bash
# Run assessment and display results
python scripts/14_check_retraining_need.py

# Save assessment to file
python scripts/14_check_retraining_need.py --output reports/retraining_assessment.txt
```

**Trigger Conditions:**
1. **Validation Volume:** >= 2000 validated decisions since last training
2. **Unknown Rate Plateau:** Unknown rate not decreasing over time
3. **Semantic Reliance High:** > 30% of matches use semantic matching
4. **Low Confidence Prevalence:** > 20% of matches have confidence 0.75-0.85

**Decision Logic:**
- **RECOMMENDED** (≥2 triggers): Retraining strongly recommended
- **CONSIDER** (1 trigger): Consider retraining if manual load high
- **NOT NEEDED** (0 triggers): Continue current operations

**Exit Codes:**
- `0`: Not needed
- `1`: Consider (warning)
- `2`: Recommended
- `3`: Error

---

### 5. `generate_review_queue.py` - Smart Review Queue Generator

**Purpose:** Generate Excel review queues with intelligent clustering of similar unknown variants.

**Usage:**
```bash
# Generate review queue from last 30 days
python scripts/generate_review_queue.py --output reports/review_queue.xlsx

# Last 7 days with minimum frequency of 2
python scripts/generate_review_queue.py --days 7 --min-frequency 2 --output queue.xlsx

# Include low-confidence matches below 0.80
python scripts/generate_review_queue.py --confidence-threshold 0.80
```

**Features:**
- Collects unknowns and low-confidence matches
- Clusters similar variants using Levenshtein distance
- Provides top 3 match suggestions per variant
- Generates Excel file with validation dropdowns
- Tracks frequency and lab vendor context

**Excel Output Columns:**
- `raw_variant`: Variant text to validate
- `frequency`: Number of times observed
- `lab_vendor`: Lab that reported it (if known)
- `matrix`: Sample matrix (soil, groundwater, etc.)
- `cluster_id`: Cluster identifier for grouping
- `suggested_match_1`, `confidence_1`: Top suggestion
- `suggested_match_2`, `confidence_2`: Second suggestion
- `suggested_match_3`, `confidence_3`: Third suggestion
- `chosen_match`: **[Human fills this]** Dropdown of all analytes
- `validation_confidence`: **[Human fills this]** HIGH/MEDIUM/LOW
- `notes`: Optional validation notes

**Workflow:**
1. Generate review queue
2. Analyst opens Excel file
3. Analyst selects `chosen_match` from dropdown for each variant
4. Analyst selects `validation_confidence` (HIGH/MEDIUM/LOW)
5. Save file with "_validated" suffix
6. Run `12_validate_and_learn.py` to ingest

---

### 6. `daily_learning_loop.bat` - Automated Daily Workflow

**Purpose:** Windows batch script for daily automation of learning tasks.

**Schedule:** Run daily at 2 AM using Windows Task Scheduler

**Tasks Performed:**

**Daily (Every Day):**
- Step 1: Auto-ingest validated review queues (Layer 1 learning)
- Step 2: Generate learning health report

**Monthly (1st of month):**
- Step 3: Run threshold calibration (`10_monthly_calibration.py`)

**Quarterly (1st of Jan/Apr/Jul/Oct):**
- Step 4: Run retraining need assessment

**Weekly (Mondays):**
- Step 5: Generate new review queue for the week

**Setup Instructions:**

1. **Test the script manually:**
   ```cmd
   cd "N:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher"
   scripts\daily_learning_loop.bat
   ```

2. **Create Windows Task Scheduler entry:**
   - Open Task Scheduler
   - Create Basic Task
   - Name: "Chemical Matcher Daily Learning"
   - Trigger: Daily at 2:00 AM
   - Action: Start a program
   - Program: `C:\Windows\System32\cmd.exe`
   - Arguments: `/c "N:\Central\Staff\KG\Kiefer's Coding Corner\Reg 153 chemical matcher\scripts\daily_learning_loop.bat"`
   - Run whether user is logged on or not
   - Run with highest privileges

3. **Verify logs:**
   - Check `logs/daily_loop/` for execution logs
   - Review `reports/daily/` for generated reports

---

## Complete Production Workflow

### Weekly Workflow

**Monday Morning:**
1. Review queue generated automatically (via daily_learning_loop.bat)
2. Analyst receives email notification (optional, configure separately)
3. Analyst opens `reports/daily/validations/review_queue_YYYYMMDD.xlsx`

**Throughout Week:**
4. Analyst validates variants in Excel:
   - Select `chosen_match` from dropdown
   - Set `validation_confidence` (HIGH/MEDIUM/LOW)
   - Add notes if needed
5. Save completed file as `review_queue_YYYYMMDD_validated.xlsx`
6. Place in `reports/daily/validations/` folder

**Daily (Automated):**
7. Daily loop auto-ingests completed validation files (2 AM)
8. New synonyms added to database (Layer 1 learning)
9. Learning report generated

**Monthly (1st of month):**
10. Threshold calibration runs automatically
11. Optimal confidence thresholds recalculated

**Quarterly (Jan/Apr/Jul/Oct):**
12. Retraining assessment runs automatically
13. If recommended, coordinate neural model retraining

---

## Processing Large Lab EDD Files

### Example: Processing 5000-analyte lab report

```bash
# Step 1: Run batch matching
python scripts/11_match_batch.py \
    --input "data/raw/lab_edds/ALS_Report_2026_Q1.xlsx" \
    --output "reports/ALS_2026_Q1_matched.xlsx" \
    --review-queue "reports/ALS_2026_Q1_review.xlsx" \
    --confidence-threshold 0.75

# Output:
# - All 5000 analytes processed in ~2 minutes
# - 4200 auto-matched (84%)
# - 350 flagged for review (7%)  
# - 450 unknown (9%)

# Step 2: Validate review queue
# Open reports/ALS_2026_Q1_review.xlsx
# Validate the 800 flagged + unknown cases
# Save as reports/ALS_2026_Q1_review_validated.xlsx

# Step 3: Ingest validated synonyms
python scripts/12_validate_and_learn.py \
    --review-queue "reports/ALS_2026_Q1_review_validated.xlsx"

# Output:
# - 650 new synonyms added
# - 150 duplicates skipped
# - Retraining progress: 1850/2000 (92.5%)

# Step 4: Re-run batch matching (next time)
# With 650 new synonyms, exact match rate should improve by ~5-10%
```

---

## Monitoring & Maintenance

### Daily Checks
- Review daily learning loop logs in `logs/daily_loop/`
- Check for failed steps or warnings
- Monitor exact match rate trends

### Weekly Checks
- Review weekly learning report in `reports/weekly/`
- Validate review queue (15-30 minutes of analyst time)
- Track unknown rate and synonym growth

### Monthly Checks
- Review threshold calibration results
- Assess match method distribution
- Evaluate corpus maturity score

### Quarterly Checks
- Review retraining assessment
- If recommended, plan neural model retraining
- Archive old logs and reports

---

## Troubleshooting

### Issue: Auto-detection fails to find analyte column
**Solution:** Explicitly specify column name with `--column` parameter
```bash
python scripts/11_match_batch.py --input file.xlsx --column "Test Parameter" --output results.xlsx
```

### Issue: Validation ingestion reports many duplicates
**Cause:** Synonyms already exist in database (expected behavior)
**Action:** No action needed - system prevents duplicates

### Issue: Unknown rate not decreasing
**Cause:** May need more diverse validation coverage or semantic matching
**Action:**
1. Generate targeted review queue
2. Focus validation on high-frequency unknowns
3. Consider enabling/improving semantic matching

### Issue: Daily loop fails to run
**Check:**
1. Windows Task Scheduler is configured correctly
2. Python is accessible from system PATH
3. Project directory path is correct
4. Database permissions are correct
5. Check logs in `logs/daily_loop/`

### Issue: Excel dropdown shows blank or limited options
**Cause:** Large analyte list (>255 chars) triggers reference sheet mode
**Solution:** Analytes are in separate sheet "Analyte Names" - dropdown still works

---

## Performance Considerations

### Batch Matching Performance
- **Throughput:** ~500-1000 analytes/second (mostly exact matches)
- **Bottleneck:** Fuzzy matching (if many fuzzy matches needed)
- **Optimization:** Use higher confidence threshold to reduce fuzzy lookups

### Database Considerations
- **Growth Rate:** ~500-2000 new synonyms/month (depending on validation rate)
- **Index Maintenance:** Automatic (SQLite handles this)
- **Backup:** Recommended weekly backup of `data/reg153_matcher.db`

### Review Queue Size Management
- **Target:** 50-200 variants per weekly queue
- **If too large:** Increase `--min-frequency` or reduce `--days` lookback
- **If too small:** Decrease `--confidence-threshold` to catch more borderline cases

---

## Advanced Usage

### Custom Confidence Thresholds

Adjust based on use case:

**High Precision (minimize false positives):**
```bash
python scripts/11_match_batch.py --input file.xlsx --output results.xlsx --confidence-threshold 0.90
# More cases sent to review, but higher confidence in auto-matches
```

**High Recall (minimize unknowns):**
```bash
python scripts/11_match_batch.py --input file.xlsx --output results.xlsx --confidence-threshold 0.70
# More auto-matches, but some may need manual verification
```

### Batch Processing Multiple Files

```bash
# Windows batch script to process multiple files
for %%f in (data\raw\lab_edds\*.xlsx) do (
    python scripts\11_match_batch.py --input "%%f" --output "reports\%%~nf_matched.xlsx"
)
```

### Integration with Other Systems

**Example: Export matched results to CSV for import:**
```python
import pandas as pd

# Read matched results
df = pd.read_excel('reports/matched_results.xlsx')

# Filter auto-accepted only
auto_accepted = df[df['Review_Flag'] == False]

# Export for downstream system
auto_accepted.to_csv('exports/auto_matched_for_import.csv', index=False)
```

---

## Future Enhancements

Planned improvements:
- Email notifications for daily loop completion
- Web dashboard for learning metrics visualization
- Automated backup and archival
- Integration with lab vendor APIs for direct EDD retrieval
- A/B testing framework for threshold optimization
- Semantic matching integration (Layer 2 learning)

---

## Support & Documentation

- **System Architecture:** See `LEARNING_SYSTEM_IMPLEMENTATION.md`
- **Matching Engine:** See `MATCHING_ENGINE_DELIVERY.md`
- **Database Schema:** See `src/database/models.py`
- **API Documentation:** See inline docstrings

For questions or issues, contact the Chemical Matcher development team.

---

**Last Updated:** February 12, 2026  
**Version:** 1.0
