# Medical Claims ETL Pipeline - User Guide

## Overview

The comprehensive medical claims pipeline processes healthcare claims data through a complete lifecycle:

1. **Source Archival** - Immutable timestamped copies
2. **Processing** - Clean records for RAG/LLM indexing
3. **Quarantine** - Anomalies flagged for manual review
4. **Analytics** - Aggregated Parquet files for BI tools

## Quick Start

### Running the Pipeline

There are **two ways** to run the pipeline:

#### Option 1: Process a New File (Recommended)

Use this when you have a new CSV file to process:

```bash
# Activate virtual environment
source venv/bin/activate

# Run with --input flag
python test_comprehensive_pipeline.py --input /path/to/your/medical-claims.csv
```

**What happens:**
1. Your file is copied to `../data/etl/source/medical-claims/YYYY-MM-DD_HH-MM-SS/`
2. A `metadata.json` file is created with archival information
3. The pipeline processes the archived copy (your original remains untouched)
4. Outputs are generated in the ETL directory structure

#### Option 2: Reprocess Archived File

Use this to reprocess the most recently archived file:

```bash
# Activate virtual environment
source venv/bin/activate

# Run without --input flag
python test_comprehensive_pipeline.py
```

**Requirements:**
- At least one timestamped directory must exist in `../data/etl/source/medical-claims/`
- The directory must contain a CSV file
- Example: `../data/etl/source/medical-claims/2025-10-12_02-19-48/medical-claims.csv`

**What happens:**
1. Script automatically finds the latest timestamped directory
2. Processes the CSV file from that directory
3. Overwrites previous outputs with new results

## Directory Structure

The pipeline maintains the following structure under `../data/etl/`:

```
data/etl/
├── source/
│   └── medical-claims/
│       ├── 2025-10-12_02-19-48/          # Timestamped archive
│       │   ├── medical-claims.csv        # Immutable archived file
│       │   └── metadata.json             # Archival metadata
│       └── 2025-10-12_14-30-00/          # Another run (example)
│           ├── medical-claims.csv
│           └── metadata.json
│
├── processed/
│   └── medical-claims/
│       └── clean.csv                     # Clean records (for RAG indexing)
│
├── quarantine/
│   └── medical-claims/
│       └── anomalies.csv                 # Anomalies (for manual review)
│
└── analytics/
    └── dashboards/
        └── medical-claims/
            ├── claims_summary.parquet    # Overall metrics
            ├── claims_by_provider.parquet
            ├── claims_by_diagnosis.parquet
            └── claims_by_date.parquet    # Time-series data
```

## Output Files

### 1. Clean Records (`processed/medical-claims/clean.csv`)

**Purpose**: Production-ready data for RAG/LLM indexing

**Contents**:
- All records that passed quality checks
- Anomalies removed
- **Pure source data only** - all metadata columns removed

**Use Cases**:
- Feed into vector database for RAG
- LLM training/fine-tuning
- Production analytics
- Exact replication of source structure

**Column Structure**:
- 90 columns (original source structure)
- **All `_meta_*` columns removed** using generic prefix matching
- No synthetic columns added by pipeline

**Why remove metadata?**
- Consistent approach: clean = original structure
- Generic and reusable for any data source
- Metadata available in anomalies.csv and Parquet aggregations when needed

### 2. Anomalies (`quarantine/medical-claims/anomalies.csv`)

**Purpose**: Records flagged for manual review

**Contents**:
- Statistical outliers detected by AnomalyDetector
- Same schema as clean records
- Includes all metadata columns

**Use Cases**:
- Manual review for data quality issues
- Investigate potential fraud
- Identify data collection problems

**Common Anomalies**:
- Extreme billing amounts (Z-score > 3.0)
- Unusual claim patterns
- Statistical outliers

### 3. Dashboard Parquet Files (`analytics/dashboards/medical-claims/`)

**Purpose**: Pre-aggregated data optimized for BI tools (Tableau, PowerBI, Looker)

#### a. `claims_summary.parquet`

Overall summary metrics:
- `run_date` - Pipeline execution date
- `total_claims` - Total number of claims
- `total_billed` - Sum of all billed amounts
- `avg_billed` - Average billed amount
- `min_billed` - Minimum billed amount
- `max_billed` - Maximum billed amount
- `avg_quality_score` - Average data quality
- `anomaly_count` - Number of anomalies detected
- `anomaly_rate` - Percentage of anomalies

#### b. `claims_by_provider.parquet`

Provider-level aggregations:
- Provider name/ID column
- `total_claims` - Claims per provider
- `total_billed` - Total billed per provider
- `avg_billed` - Average per claim
- `min_billed` - Minimum claim
- `max_billed` - Maximum claim
- `avg_quality_score` - Average quality (if available)

**Sorted by**: `total_billed` (descending)

#### c. `claims_by_diagnosis.parquet`

Diagnosis-level aggregations:
- Diagnosis code column
- `total_claims` - Claims per diagnosis
- `total_billed` - Total billed per diagnosis
- `avg_billed` - Average per claim

**Sorted by**: `total_claims` (descending)

#### d. `claims_by_date.parquet`

Time-series aggregations:
- `date` - Service date
- `total_claims` - Claims per day
- `total_billed` - Total billed per day
- `avg_billed` - Average per claim

**Sorted by**: `date` (ascending)

## Pipeline Stages

### 1. Schema Inference
- Automatically detects data types
- Identifies patterns (dates, emails, phone numbers, etc.)
- No configuration needed

### 2. Null Removal
- Removes empty fields (columns with all nulls)
- Strategy: `remove_fields`
- Rows are preserved

### 3. Deduplication
- Removes exact duplicate records
- Mode: `exact`
- Keeps first occurrence

### 4. Anomaly Detection
- Multi-method detection:
  - Statistical (Z-score)
  - IQR (Interquartile Range)
  - Isolation Forest (ML-based)
- Method: `combined`
- Threshold: 3.0 standard deviations

### 5. Quality Scoring
- Scores each record on:
  - Completeness (% of filled fields)
  - Validity (data type correctness)
  - Consistency (cross-field validation)
- Range: 0.0 to 1.0

### 6. Metadata Export
- Adds metadata as CSV columns
- Prefix: `_meta_`
- All downstream files include metadata

### 7. Anomaly Splitting
- Separates clean records from anomalies
- Clean → `processed/`
- Anomalies → `quarantine/`

### 8. Dashboard Aggregation
- Generates 4 Parquet files
- Auto-detects column names (zero-config!)
- Optimized for BI tools

### 9. Column Removal (Clean Data Only)
- **Removes ALL metadata columns** from clean records using prefix matching
- Generic approach: any column starting with `_meta_` is removed
- Ensures clean.csv = pure source data structure
- **Reusable**: Works for any pipeline, any metadata columns
- **Future-proof**: New metadata columns automatically removed
- **Note**: Anomaly file keeps ALL metadata for investigation

## Zero-Configuration Design

The pipeline automatically detects column names without requiring configuration:

### Auto-Detection Examples

**Provider Column**: Searches for keywords
- `prov`, `provider`, `doctor`, `physician`, `practitioner`
- Example: Detects `Prov Name` automatically

**Diagnosis Column**: Searches for keywords
- `dx`, `diagnosis`, `condition`, `icd`, `disease`
- Example: Detects `Dx Code 1` automatically

**Amount Column**: Searches for keywords
- `billed`, `amount`, `charged`, `cost`, `price`, `payment`
- Example: Detects `Clm Line Billed Amt` automatically

**Date Column**: Searches for keywords
- `service`, `date`, `admission`, `claim`, `received`
- Example: Detects `Service Start Date` automatically

**Claim ID Column**: Searches for keywords
- `claim`, `id`, `number`
- Example: Detects `Claim ID` automatically

### Graceful Handling

If a required column isn't found:
- That specific aggregation is skipped
- Other aggregations continue normally
- Warning message logged
- Pipeline doesn't fail

## Common Usage Patterns

### First Time Setup

```bash
# 1. Navigate to project
cd /Users/pankajsharma/Documents/Development/ads/ai-etl-framework

# 2. Activate environment
source venv/bin/activate

# 3. Process your first file
python test_comprehensive_pipeline.py --input ~/Downloads/medical-claims.csv
```

### Daily Processing

```bash
# Process new day's file
python test_comprehensive_pipeline.py --input /data/claims-2025-10-13.csv
```

### Reprocessing After Changes

```bash
# Reprocess latest archived file (no --input needed)
python test_comprehensive_pipeline.py
```

### Custom Output Directory

```bash
# Use different base directory
python test_comprehensive_pipeline.py \
  --input ~/claims.csv \
  --base-dir /custom/etl/path
```

## Reading Parquet Files

### Using Python (Pandas)

```python
import pandas as pd

# Read summary
summary = pd.read_parquet('../data/etl/analytics/dashboards/medical-claims/claims_summary.parquet')
print(summary)

# Read provider aggregations
providers = pd.read_parquet('../data/etl/analytics/dashboards/medical-claims/claims_by_provider.parquet')
top_10 = providers.head(10)
print(top_10)

# Read time-series
by_date = pd.read_parquet('../data/etl/analytics/dashboards/medical-claims/claims_by_date.parquet')
print(by_date)
```

### Using Command Line (parquet-tools)

```bash
# Install parquet-tools
pip install parquet-tools

# View schema
parquet-tools schema claims_summary.parquet

# View first 10 rows
parquet-tools head -n 10 claims_by_provider.parquet

# Convert to JSON
parquet-tools cat --json claims_by_diagnosis.parquet
```

### Using BI Tools

#### Tableau
1. Connect to Data → More → Parquet
2. Browse to `../data/etl/analytics/dashboards/medical-claims/`
3. Select Parquet file(s)
4. Create visualizations

#### PowerBI
1. Get Data → More → Parquet
2. Navigate to Parquet files
3. Load into PowerBI
4. Build reports

#### Looker
1. Create database connection with Parquet support
2. Point to Parquet directory
3. Define explores
4. Build dashboards

## Pipeline Output Example

```
================================================================================
  Pipeline Results
================================================================================

--- Overall Statistics ---
Records Extracted:   1,000
Records Transformed: 899
Records Loaded:      899
Records Failed:      0

--- Stage Timings ---
Extract:   0.06 seconds
Transform: 0.92 seconds
Load:      0.01 seconds
Total:     1.00 seconds

--- Transformation Details ---

📋 Schema Inference:
   Fields detected: 90
   Sample size:     1000
   Patterns found:  7

🧹 Null Removal:
   Records processed: 1000
   Records modified:  1000
   Records filtered:  0

🔄 Deduplication:
   Records processed: 1000
   Duplicates found:  0

⚠️  Anomaly Detection:
   Records processed: 1000
   Records modified:  101 (flagged as anomalies)

⭐ Quality Scoring:
   Records processed: 1000
   Records modified:  1000 (scored)

🔀 Anomaly Splitting:
   Clean records:    899
   Anomalies:        101

📈 Dashboard Aggregations:
   Records aggregated: 899
   Files generated:
      - claims_summary.parquet
      - claims_by_provider.parquet
      - claims_by_diagnosis.parquet
      - claims_by_date.parquet

--- Output Files ---
✅ Clean records:  ../data/etl/processed/medical-claims/clean.csv
✅ Anomalies:      ../data/etl/quarantine/medical-claims/anomalies.csv
✅ Dashboards:     ../data/etl/analytics/dashboards/medical-claims/
```

## Troubleshooting

### Error: "No files in source/medical-claims"

**Problem**: No timestamped directories found

**Solution**:
```bash
# Provide input file
python test_comprehensive_pipeline.py --input /path/to/file.csv
```

### Error: "ModuleNotFoundError: pandas"

**Problem**: Virtual environment not activated

**Solution**:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Error: "Encoding issues"

**Problem**: CSV has non-UTF-8 encoding

**Solution**: Edit `test_comprehensive_pipeline.py` line 50:
```python
CSV_ENCODING = "latin-1"  # or "cp1252", "iso-8859-1"
```

### Warning: "No provider column found"

**Problem**: Auto-detection couldn't find provider column

**Impact**: `claims_by_provider.parquet` won't be created

**Solution**: This is normal if your CSV doesn't have provider data. Other aggregations will still work.

### Performance: Pipeline too slow

**Problem**: Large file processing

**Solution**: Adjust batch size in `test_comprehensive_pipeline.py`:
```python
# Around line 290
.extract(CSVSource(
    file_path=source_file,
    encoding=CSV_ENCODING,
    delimiter=CSV_DELIMITER,
    chunk_size=5000  # Increase for larger files
))
```

## Best Practices

1. **Always use --input for new files** - This creates proper archival with timestamps
2. **Never modify archived files** - They're immutable source copies
3. **Review quarantine regularly** - Anomalies may need investigation
4. **Use Parquet for dashboards** - Much faster than CSV for BI tools
5. **Keep processed/ for RAG** - Use `clean.csv` for vector database indexing
6. **Monitor quality scores** - Low scores indicate data quality issues

## Next Steps

After running the pipeline:

1. **View Clean Data**:
   ```bash
   head ../data/etl/processed/medical-claims/clean.csv
   ```

2. **Review Anomalies**:
   ```bash
   head ../data/etl/quarantine/medical-claims/anomalies.csv
   ```

3. **RAG Indexing**: Use `clean.csv` as input to your vector database

4. **Build Dashboards**: Connect Tableau/PowerBI to Parquet files

5. **Analyze Results**: Use Python/pandas for ad-hoc analysis

## Configuration Options

Edit `test_comprehensive_pipeline.py` to customize:

```python
# Lines 45-57
CSV_ENCODING = "utf-8"           # File encoding
CSV_DELIMITER = ","              # CSV delimiter
NULL_STRATEGY = "remove_fields"  # Null handling
DEDUP_MODE = "exact"             # Deduplication mode
ANOMALY_METHOD = "combined"      # Anomaly detection method
QUALITY_MIN_SCORE = 0.0          # Minimum quality score
```

## Support

For issues or questions:
- Check this guide first
- Review error messages in console output
- Check logs in the console
- Review the main [README.md](../README.md) for framework documentation

---

**Last Updated**: 2025-10-12
**Pipeline Version**: 1.0
**Framework Version**: 0.2.5
