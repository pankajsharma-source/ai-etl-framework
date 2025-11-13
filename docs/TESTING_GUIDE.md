# AI-ETL Framework - Testing Guide

**Purpose**: Step-by-step instructions for testing the framework
**When to Use**: First session after implementation, or anytime you need to validate

---

## 📋 Pre-Test Checklist

Before starting, ensure you have:
- [ ] Python 3.11+ installed (`python3 --version`)
- [ ] Terminal access to project directory
- [ ] 30-60 minutes for complete testing

---

## 🚀 Step 1: Environment Setup (10 minutes)

### Option A: Automated Setup (Recommended)

```bash
# Navigate to project
cd /Users/pankajsharma/Documents/Development/ads/ai-etl-framework

# Run setup script
./scripts/setup.sh
```

**What it does:**
- Creates virtual environment (`venv/`)
- Upgrades pip
- Installs all dependencies
- Creates necessary directories
- Copies .env.example to .env

**Expected Output:**
```
==================================================
AI-ETL Framework - Setup Script
==================================================

Checking Python version...
Found: Python 3.11.x

Creating virtual environment...
✓ Virtual environment created

Activating virtual environment...

Upgrading pip...
✓ pip upgraded

Installing dependencies...
✓ Dependencies installed

Install development dependencies? (y/n)
```

Press `y` if you want dev tools (pytest, black, mypy), or `n` for minimal install.

### Option B: Manual Setup

```bash
cd /Users/pankajsharma/Documents/Development/ads/ai-etl-framework

# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create directories
mkdir -p data output logs .state .errors

# Copy environment file
cp .env.example .env
```

### Verification

```bash
# Activate venv (if not already)
source venv/bin/activate

# Check Python packages are installed
pip list | grep pandas
pip list | grep pyyaml
pip list | grep sqlalchemy

# You should see:
# pandas        2.x.x
# pyyaml        6.x.x
# sqlalchemy    2.x.x
```

✅ **Success**: Virtual environment created, dependencies installed

---

## 🧪 Step 2: Test Simple CSV Pipeline (15 minutes)

### What This Tests
- CSV source adapter
- NullRemover transformer
- SQLite destination adapter
- Pipeline orchestration
- Basic E-T-L flow

### Run the Test

```bash
# Ensure venv is activated
source venv/bin/activate

# Run simple CSV pipeline
python examples/simple_csv_pipeline.py
```

### Expected Output

```
============================================================
Starting Simple CSV to SQLite Pipeline
============================================================
Input: /Users/pankajsharma/Documents/Development/ads/ai-etl-framework/data/sample.csv
Output: /Users/pankajsharma/Documents/Development/ads/ai-etl-framework/output/sample.db

2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Starting pipeline execution: pipeline_1704700800
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Stage 1: Extract - Starting
2025-01-08 10:00:00 - ai_etl.CSVSource - INFO - Connected to CSV file: ...
2025-01-08 10:00:00 - ai_etl.CSVSource - INFO - Inferred schema with 6 fields
2025-01-08 10:00:00 - ai_etl.CSVSource - INFO - Read 10 records from CSV
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Extracted 10 records
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Stage 2: Transform - 1 transformer(s)
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Applying transformer: NullRemover
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - After transform: 7 records remain
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Stage 3: Load - Starting
2025-01-08 10:00:00 - ai_etl.SQLiteLoader - INFO - Connected to SQLite database: ...
2025-01-08 10:00:00 - ai_etl.SQLiteLoader - INFO - Created table 'clean_data' with 6 columns
2025-01-08 10:00:00 - ai_etl.SQLiteLoader - INFO - Wrote 7 records to table 'clean_data'
2025-01-08 10:00:00 - ai_etl.Pipeline - INFO - Pipeline completed successfully in 0.15s

============================================================
Pipeline Results
============================================================
Status: SUCCESS
Records Extracted: 10
Records Transformed: 7
Records Loaded: 7
Records Filtered: 3
Total Duration: 0.15s
  - Extract: 0.05s
  - Transform: 0.02s
  - Load: 0.08s

Transformer Stats:
  - Processed: 7
  - Filtered: 3
  - Errors: 0
============================================================

Data saved to: /Users/pankajsharma/Documents/Development/ads/ai-etl-framework/output/sample.db

To view the data, run:
  sqlite3 output/sample.db 'SELECT * FROM clean_data;'
```

### Verify Results

```bash
# Check that database was created
ls -lh output/sample.db

# View the data
sqlite3 output/sample.db "SELECT * FROM clean_data;"
```

**Expected Data**:
```
1|John Doe|john@example.com|30|New York|75000
2|Jane Smith|jane@example.com|25|San Francisco|85000
4|Alice Williams|alice@example.com|35|Boston|95000
7|Eve Davis|eve@example.com|29|Austin|80000
8|Frank Miller|frank@example.com|32|Denver|72000
9|Grace Lee|grace@example.com|27|Portland|78000
10|Henry Wilson|henry@example.com|31|Miami|82000
```

Note: Rows 3, 5, and 6 filtered out because they contain null values.

### ✅ Success Criteria
- [ ] Script runs without errors
- [ ] Logs show 10 extracted, 7 loaded (3 filtered)
- [ ] output/sample.db exists
- [ ] clean_data table contains 7 rows
- [ ] No rows with null values in the output

### ❌ If It Fails

**Error: "ModuleNotFoundError: No module named 'pandas'"**
```bash
# Ensure venv is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

**Error: "CSV file not found"**
```bash
# Check data file exists
ls -lh data/sample.csv

# If missing, recreate it (see SESSION_STATE.md for content)
```

**Error: "Permission denied: output/sample.db"**
```bash
# Create output directory
mkdir -p output

# Remove old database if exists
rm output/sample.db
```

---

## 🧪 Step 3: Test Advanced JSON Pipeline (15 minutes)

### What This Tests
- JSON source adapter
- Multiple transformers chained
- Quality scoring functionality
- Complex pipeline orchestration

### Run the Test

```bash
python examples/advanced_json_pipeline.py
```

### Expected Output

```
============================================================
Advanced JSON to SQLite Pipeline with Quality Scoring
============================================================
Input: .../data/sample.json
Output: .../output/quality_scored.db

2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Starting pipeline execution: pipeline_1704701100
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Stage 1: Extract - Starting
2025-01-08 10:05:00 - ai_etl.JSONSource - INFO - Connected to JSON file: ...
2025-01-08 10:05:00 - ai_etl.JSONSource - INFO - Auto-detected mode: array
2025-01-08 10:05:00 - ai_etl.JSONSource - INFO - Inferred schema with 7 fields
2025-01-08 10:05:00 - ai_etl.JSONSource - INFO - Read 5 records from JSON
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Extracted 5 records
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Stage 2: Transform - 2 transformer(s)
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Applying transformer: NullRemover
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - After transform: 5 records remain
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Applying transformer: QualityScorer
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - After transform: 5 records remain
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Stage 3: Load - Starting
2025-01-08 10:05:00 - ai_etl.SQLiteLoader - INFO - Created table 'quality_data' with 7 columns
2025-01-08 10:05:00 - ai_etl.SQLiteLoader - INFO - Wrote 5 records to table 'quality_data'
2025-01-08 10:05:00 - ai_etl.Pipeline - INFO - Pipeline completed successfully in 0.18s

============================================================
Pipeline Results
============================================================
Status: SUCCESS
Records Extracted: 5
Records Transformed: 5
Records Loaded: 5
Total Duration: 0.18s

Transformer Stats:
  NullRemover:
    - Processed: 5
    - Modified: 2
    - Filtered: 0
  QualityScorer:
    - Processed: 5
    - Modified: 5
```

### Verify Results

```bash
# View the data
sqlite3 output/quality_scored.db "SELECT * FROM quality_data;"
```

**Expected**: 5 records with null fields removed (e.g., record 3 missing email, record 5 missing age)

### Quality Score Validation

The QualityScorer should have added quality scores to metadata. While SQLite stores the data, the scores are in the Record metadata during processing.

To verify scoring logic worked:
- Look at logs - should show "Modified: 5" for QualityScorer
- No errors in transformer stats
- All 5 records made it through (no filtering with filter_low_quality=False)

### ✅ Success Criteria
- [ ] Script runs without errors
- [ ] Logs show 5 extracted, 5 loaded
- [ ] output/quality_scored.db exists
- [ ] quality_data table contains 5 rows
- [ ] Records have null fields removed
- [ ] QualityScorer processed all 5 records

### ❌ If It Fails

**Error: "JSON file not found"**
```bash
ls -lh data/sample.json
# Recreate if missing
```

**Error: "Invalid JSON"**
```bash
# Validate JSON syntax
python3 -m json.tool data/sample.json
```

---

## 🧪 Step 4: Test JSONL Format (5 minutes)

### Test JSONL Auto-Detection

```bash
# Create a quick test
python3 << 'EOF'
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.orchestration.pipeline import Pipeline

pipeline = (Pipeline()
    .extract(JSONSource("data/sample.jsonl"))
    .load(SQLiteLoader("output/jsonl_test.db", table="jsonl_data"))
    .run())

print(f"Loaded {pipeline.result.records_loaded} records from JSONL")
EOF
```

**Expected Output**: "Loaded 5 records from JSONL"

### Verify

```bash
sqlite3 output/jsonl_test.db "SELECT COUNT(*) FROM jsonl_data;"
# Should show: 5
```

---

## 🧪 Step 5: Manual Component Tests (Optional, 15 minutes)

### Test CSV Source Independently

```python
python3 << 'EOF'
from src.adapters.sources.csv_source import CSVSource

with CSVSource("data/sample.csv") as source:
    schema = source.get_schema()
    print(f"Schema: {len(schema.fields)} fields")

    count = 0
    for record in source.read(batch_size=5):
        count += 1
        if count == 1:
            print(f"First record: {record.data}")

    print(f"Total records: {count}")
EOF
```

**Expected**: Schema with 6 fields, 10 total records

### Test NullRemover Independently

```python
python3 << 'EOF'
from src.transformers.cleaners.null_remover import NullRemover
from src.common.models import Record, RecordMetadata

# Create test record with nulls
record = Record(
    data={"id": 1, "name": "Test", "email": None},
    metadata=RecordMetadata(source_type="test", source_id="test")
)

# Test drop strategy
remover = NullRemover(strategy="drop")
result = remover.transform(record)
print(f"Drop strategy: {result}")  # Should be None

# Test remove_fields strategy
remover = NullRemover(strategy="remove_fields")
result = remover.transform(record)
print(f"Remove fields: {result.data}")  # Should be {"id": 1, "name": "Test"}
EOF
```

### Test QualityScorer Independently

```python
python3 << 'EOF'
from src.transformers.validators.quality_scorer import QualityScorer
from src.common.models import Record, RecordMetadata

# Test with good quality record
good_record = Record(
    data={
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "salary": 75000
    },
    metadata=RecordMetadata(source_type="test", source_id="test")
)

scorer = QualityScorer()
result = scorer.transform(good_record)
print(f"Quality score: {result.metadata.quality_score:.2f}")
print(f"Breakdown: {result.metadata.custom['quality_breakdown']}")

# Test with poor quality record
poor_record = Record(
    data={
        "id": 1,
        "name": "",
        "email": "invalid",
        "age": -5,
        "salary": None
    },
    metadata=RecordMetadata(source_type="test", source_id="test")
)

result = scorer.transform(poor_record)
print(f"Poor quality score: {result.metadata.quality_score:.2f}")
EOF
```

---

## 📊 Complete Test Checklist

### Core Functionality
- [ ] Virtual environment created successfully
- [ ] All dependencies installed without errors
- [ ] simple_csv_pipeline.py runs successfully
- [ ] advanced_json_pipeline.py runs successfully
- [ ] JSONL format detected and processed correctly

### Data Validation
- [ ] CSV: 10 records extracted, 7 loaded (3 filtered)
- [ ] JSON: 5 records extracted and loaded
- [ ] Output databases created correctly
- [ ] Data in tables matches expectations
- [ ] No data corruption or loss

### Component Tests
- [ ] CSVSource reads files correctly
- [ ] JSONSource handles both formats
- [ ] NullRemover filters/cleans as expected
- [ ] QualityScorer calculates scores
- [ ] SQLiteLoader creates tables and inserts data
- [ ] Pipeline orchestrates correctly

### Error Handling
- [ ] Graceful error messages (if any errors occur)
- [ ] Logs are clear and informative
- [ ] No cryptic stack traces

---

## 🐛 Troubleshooting Guide

### Common Issues

#### Issue: "command not found: python"
**Solution**: Use `python3` instead
```bash
python3 examples/simple_csv_pipeline.py
```

#### Issue: "Permission denied" on setup.sh
**Solution**: Make it executable
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

#### Issue: Slow performance during testing
**Possible Cause**: Running without batch processing
**Solution**: This is expected for small datasets. Performance improves with larger datasets.

#### Issue: "Table already exists" error
**Solution**: Remove old database or use drop_if_exists option
```bash
rm output/sample.db
# Then rerun
```

#### Issue: Quality scores seem incorrect
**Check**: Review the quality_scorer.py logic for your data
**Note**: Scoring is heuristic-based and may need tuning for your use case

---

## 📝 Recording Test Results

After testing, update `docs/SESSION_STATE.md`:

```markdown
## Test Results (YYYY-MM-DD)

### Environment
- Python version: 3.11.x
- OS: macOS / Linux / Windows
- Installation method: Automated / Manual

### Test Outcomes
- ✅/❌ Simple CSV Pipeline
- ✅/❌ Advanced JSON Pipeline
- ✅/❌ JSONL Format
- ✅/❌ Component Tests

### Bugs Found
1. [Description]
   - Fix: [What you did]
2. ...

### Performance Notes
- CSV pipeline: X.XX seconds
- JSON pipeline: X.XX seconds
```

---

## ✅ Success!

If all tests pass:
1. ✅ Update SESSION_STATE.md with test results
2. ✅ Mark Phase 1 as "Validated"
3. ✅ Ready to proceed with Phase 2 implementation
4. ✅ Consider adding unit tests (see IMPLEMENTATION_ROADMAP.md)

---

## 🚀 What's Next?

See `docs/IMPLEMENTATION_ROADMAP.md` for:
- Phase 2 features to implement
- Phase 3 and beyond
- Detailed implementation guides

See `docs/CLAUDE_CONTEXT.md` for:
- Quick project overview
- How to get a new Claude session up to speed

---

**Happy Testing! 🎉**
