# AI-ETL Framework

A generic, AI-powered ETL (Extract, Transform, Load) framework with pluggable adapters, intelligent automation, and scalability from laptop to cloud.

## Features

- **Pluggable Adapters**: Easily add/remove data source and destination adapters
- **AI-First Design**: ML/AI automation at every stage (schema inference, quality scoring, anomaly detection)
- **Multi-Format Support**: CSV, JSON, text files, PostgreSQL, SQLite (more coming)
- **Intelligent Transformations**: Auto-cleaning, validation, enrichment, deduplication
- **Incremental Processing**: State tracking to process only new/changed data
- **Scalable Architecture**: Run locally or deploy to cloud with same codebase
- **Batch & Streaming Ready**: Designed for batch with streaming extensibility
- **Configuration-Driven**: Define pipelines in YAML or Python

## Quick Start

### Installation

```bash
# Clone the repository
cd /path/to/ai-etl-framework

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development dependencies (optional)
pip install -r requirements-dev.txt
```

### Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings
nano .env
```

### Run Your First Pipeline

```python
# examples/simple_csv_pipeline.py
from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover

# Create and run pipeline
pipeline = (Pipeline()
    .extract(CSVSource("data/sample.csv"))
    .transform(NullRemover())
    .load(SQLiteLoader("output.db", table="clean_data"))
    .run())

print(f"Pipeline completed! Processed {pipeline.result.records_loaded} records")
```

```bash
# Run the example
python examples/simple_csv_pipeline.py
```

## Project Structure

```
ai-etl-framework/
├── docs/
│   ├── ARCHITECTURE.md          # Architecture documentation
│   ├── TECHNICAL_SPEC.md        # Technical specifications
│   └── IMPLEMENTATION_GUIDE.md  # Development guide
├── src/
│   ├── adapters/                # Pluggable source/destination adapters
│   │   ├── sources/            # Extract adapters (CSV, JSON, DB, etc.)
│   │   └── destinations/       # Load adapters (DB, files, etc.)
│   ├── transformers/           # Data transformation components
│   │   ├── cleaners/          # Data cleaning
│   │   ├── validators/        # Data validation
│   │   └── enrichers/         # Data enrichment
│   ├── orchestration/          # Pipeline orchestration
│   ├── ml/                     # AI/ML services
│   └── common/                 # Shared utilities
├── config/                     # Configuration files
├── tests/                      # Unit and integration tests
├── examples/                   # Example pipelines
└── scripts/                    # Utility scripts
```

## Usage Examples

### Example 1: CSV to SQLite

```python
from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover

pipeline = (Pipeline()
    .extract(CSVSource("customers.csv"))
    .transform(NullRemover(strategy="drop"))
    .load(SQLiteLoader("customers.db", table="customers"))
    .run())
```

### Example 2: Multiple Transformations

```python
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.validators.quality_scorer import QualityScorer
from src.transformers.enrichers.deduplicator import Deduplicator

pipeline = (Pipeline()
    .extract(CSVSource("data.csv"))
    .transform(NullRemover(strategy="remove_fields"))
    .transform(Deduplicator(match_mode="exact"))
    .transform(QualityScorer(min_score=0.7))
    .load(SQLiteLoader("output.db", table="clean_data"))
    .run())
```

### Example 3: Format Conversion

```python
from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.json_loader import JSONLoader

# CSV → JSON
Pipeline()
    .extract(CSVSource("data.csv"))
    .load(JSONLoader("output.json", mode="array", pretty=True))
    .run()

# JSON → CSV
Pipeline()
    .extract(JSONSource("data.json"))
    .load(CSVLoader("output.csv"))
    .run()

# CSV → JSONL (line-delimited)
Pipeline()
    .extract(CSVSource("data.csv"))
    .load(JSONLoader("output.jsonl", mode="lines"))
    .run()
```

### Example 4: YAML-Based Pipeline (Future)

```yaml
# pipelines/csv_to_postgres.yaml
pipeline:
  name: "csv-to-postgres"

  source:
    type: "csv"
    config:
      path: "data/input.csv"

  transforms:
    - type: "null_remover"
      config:
        strategy: "drop"
    - type: "quality_scorer"
      config:
        min_score: 0.5

  destination:
    type: "postgres"
    config:
      host: "localhost"
      database: "mydb"
      table: "clean_data"
```

```bash
# Run from YAML
python -m src.cli run pipelines/csv_to_postgres.yaml
```

## Available Adapters

### Source Adapters (Extract)

| Adapter | Description | Status |
|---------|-------------|--------|
| **CSVSource** | Read CSV files | ✅ Complete |
| **JSONSource** | Read JSON/JSONL files | ✅ Complete |
| **PostgreSQLSource** | Read from PostgreSQL | ✅ Complete |
| **S3Source** | Read from AWS S3 | 📋 Phase 3 |
| **TextSource** | Read plain text files | 📋 Phase 3 |
| **SQLiteSource** | Read from SQLite | 📋 Future |
| **MongoDBSource** | Read from MongoDB | 🔮 Future |
| **APISource** | Read from REST APIs | 🔮 Future |

### Destination Adapters (Load)

| Adapter | Description | Status |
|---------|-------------|--------|
| **SQLiteLoader** | Write to SQLite | ✅ Complete |
| **PostgreSQLLoader** | Write to PostgreSQL | ✅ Complete |
| **CSVLoader** | Write to CSV files | ✅ Complete |
| **JSONLoader** | Write to JSON/JSONL files | ✅ Complete |
| **ParquetLoader** | Write to Parquet files | 📋 Phase 3 |
| **S3Loader** | Write to AWS S3 | 📋 Phase 3 |
| **SnowflakeLoader** | Write to Snowflake | 🔮 Future |

## Available Transformers

### Cleaners

- **NullRemover**: Remove or impute missing values (✅ Complete)
- **Deduplicator**: Exact and fuzzy matching deduplication using sentence-transformers (✅ Complete)

### Validators

- **QualityScorer**: ML-based quality assessment with completeness, validity, consistency (✅ Complete)

### Enrichers

- **Aggregator**: SQL-style GROUP BY with 10+ aggregation functions (✅ Complete)

### Analyzers

- **SchemaInferrer**: Pattern detection (email, URL, phone, UUID, etc.) with confidence scores (✅ Complete)
- **AnomalyDetector**: Multi-method outlier detection (Z-score, IQR, Isolation Forest) (✅ Complete)

### Planned

- **Categorizer**: ML-based categorization (📋 Future)
- **EntityResolver**: Entity matching and merging (📋 Future)

## AI/ML Features

### Schema Inference
Automatically detect data types, patterns, and constraints:
```python
from src.transformers.analyzers.schema_inferrer import SchemaInferrer

# Detects types, patterns (email, URL, phone), constraints
SchemaInferrer(detect_patterns=True, infer_constraints=True)
```

### Quality Scoring
ML-based data quality assessment:
```python
from src.transformers.validators.quality_scorer import QualityScorer

# Scores on completeness, validity, consistency
QualityScorer(min_score=0.7, filter_low_quality=True)
```

### Anomaly Detection
Multi-method outlier detection:
```python
from src.transformers.analyzers.anomaly_detector import AnomalyDetector

# Uses statistical, IQR, or Isolation Forest methods
AnomalyDetector(method='combined', threshold=3.0)
```

### Deduplication
Smart duplicate removal with fuzzy matching:
```python
from src.transformers.enrichers.deduplicator import Deduplicator

# Exact or AI-powered fuzzy matching
Deduplicator(match_mode='fuzzy', similarity_threshold=0.95)
```

### Aggregation
SQL-style GROUP BY operations:
```python
from src.transformers.enrichers.aggregator import Aggregator

# Group and aggregate with sum, avg, count, etc.
Aggregator(group_by=['customer'], aggregations={...})
```

### Performance Auto-Tuning
ML-based optimization:
```python
from src.ml.auto_tuner import AutoTuner

# Learns optimal batch sizes and settings
tuner = AutoTuner(optimization_target='throughput')
```

## Incremental Processing

Track processed data to avoid reprocessing:

```python
pipeline = (Pipeline()
    .extract(CSVSource("data.csv", incremental=True))
    .transform(NullRemover())
    .load(SQLiteLoader("output.db"))
    .run())

# On subsequent runs, only new/changed data is processed
```

State is stored in `.state/` directory:
```json
{
  "last_run": "2025-01-08T10:00:00Z",
  "records_processed": 10000,
  "file_hash": "sha256:abc123..."
}
```

## Configuration

### Environment Variables (.env)

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_dev
DB_USER=etl_user
DB_PASSWORD=secret

# AWS (for S3 adapters)
AWS_REGION=us-east-1
S3_BUCKET=my-data-bucket

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Performance
DEFAULT_BATCH_SIZE=1000
MAX_WORKERS=4
```

### Pipeline Configuration (config/local.yaml)

```yaml
execution:
  batch_size: 1000
  max_workers: 4
  timeout_seconds: 3600

retry_policy:
  max_retries: 3
  backoff_multiplier: 2

monitoring:
  log_level: "INFO"
  metrics_enabled: true
```

## Testing with Your Own Data

### Step-by-Step Guide

#### Step 1: Prepare Your Environment

```bash
# Navigate to the project directory
cd /Users/pankajsharma/Documents/Development/thedatastudio/ai-etl-framework

# Activate virtual environment
source venv/bin/activate

# Verify installation
python -c "from src.orchestration.pipeline import Pipeline; print('✅ Framework ready!')"
```

#### Step 2: Place Your Data File

```bash
# Copy your file to the data directory (recommended)
cp /path/to/your/file.csv data/my_data.csv

# Or note the absolute path to use directly
# Example: /Users/yourname/Downloads/data.csv
```

#### Step 3: Create a Test Pipeline Script

Create a new file `test_my_pipeline.py` in the project root:

**For CSV files:**
```python
from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.validators.quality_scorer import QualityScorer

# Configure your pipeline
pipeline = (Pipeline()
    .extract(CSVSource(
        file_path="data/my_data.csv",  # Your file path
        encoding="utf-8",               # Adjust if needed
        delimiter=","                   # Use ';' for semicolon-separated
    ))
    .transform(NullRemover(strategy="drop"))  # Remove rows with nulls
    .transform(QualityScorer(min_score=0.5))  # Score data quality
    .load(SQLiteLoader(
        db_path="output/my_results.db",
        table="processed_data"
    ))
    .run())

# Print results
print(f"✅ Pipeline completed!")
print(f"📊 Records extracted: {pipeline.result.records_extracted}")
print(f"🔄 Records transformed: {pipeline.result.records_transformed}")
print(f"💾 Records loaded: {pipeline.result.records_loaded}")
print(f"⏱️  Duration: {pipeline.result.duration_seconds:.2f} seconds")
```

**For JSON files:**
```python
from src.adapters.sources.json_source import JSONSource

pipeline = (Pipeline()
    .extract(JSONSource(
        file_path="data/my_data.json",
        json_path=None  # Or specify nested path like "data.records"
    ))
    .transform(NullRemover(strategy="fill", fill_value="N/A"))
    .load(SQLiteLoader("output/my_results.db", table="json_data"))
    .run())
```

**For JSONL (line-delimited JSON) files:**
```python
from src.adapters.sources.json_source import JSONSource

pipeline = (Pipeline()
    .extract(JSONSource(
        file_path="data/my_data.jsonl"
        # JSONSource auto-detects JSONL format
    ))
    .transform(NullRemover())
    .load(SQLiteLoader("output/my_results.db", table="jsonl_data"))
    .run())
```

#### Step 4: Run Your Pipeline

```bash
# Run your test script
python test_my_pipeline.py

# Expected output:
# ✅ Pipeline completed!
# 📊 Records extracted: 1000
# 🔄 Records transformed: 950
# 💾 Records loaded: 950
# ⏱️  Duration: 2.34 seconds
```

#### Step 5: Verify the Results

**For SQLite database output:**
```bash
# View your data
sqlite3 output/my_results.db

# In SQLite shell:
sqlite> .tables                    # List tables
sqlite> .schema processed_data     # View table structure
sqlite> SELECT * FROM processed_data LIMIT 10;  # View first 10 rows
sqlite> SELECT COUNT(*) FROM processed_data;    # Count total rows
sqlite> .exit
```

**For CSV output:**
```python
# If you used CSVLoader instead
from src.adapters.destinations.csv_loader import CSVLoader

pipeline = (Pipeline()
    .extract(CSVSource("data/my_data.csv"))
    .transform(NullRemover())
    .load(CSVLoader(
        file_path="output/cleaned_data.csv",
        mode="overwrite",
        compression="gzip"  # Optional: creates .csv.gz file
    ))
    .run())
```

```bash
# View CSV output
head -20 output/cleaned_data.csv
# Or with compression:
zcat output/cleaned_data.csv.gz | head -20
```

**For JSON output:**
```python
from src.adapters.destinations.json_loader import JSONLoader

pipeline = (Pipeline()
    .extract(CSVSource("data/my_data.csv"))
    .load(JSONLoader(
        file_path="output/data.json",
        mode="array",        # or "lines" for JSONL
        pretty=True,         # Pretty-print JSON
        export_schema=True   # Creates data.schema.json
    ))
    .run())
```

```bash
# View JSON output
cat output/data.json | jq '.[:3]'  # View first 3 records (requires jq)
# Or
python -m json.tool output/data.json | head -50
```

#### Step 6: Common Pipeline Patterns

**Pattern 1: Data Cleaning Pipeline**
```python
from src.transformers.enrichers.deduplicator import Deduplicator

pipeline = (Pipeline()
    .extract(CSVSource("data/messy_data.csv"))
    .transform(NullRemover(strategy="remove_fields"))  # Remove null columns
    .transform(Deduplicator(match_mode="exact"))       # Remove duplicates
    .transform(QualityScorer(min_score=0.6, filter_low_quality=True))
    .load(SQLiteLoader("output/clean_data.db", table="cleaned"))
    .run())
```

**Pattern 2: Format Conversion**
```python
# CSV to JSON with cleaning
pipeline = (Pipeline()
    .extract(CSVSource("data/input.csv"))
    .transform(NullRemover())
    .load(JSONLoader("output/output.json", mode="array", pretty=True))
    .run())
```

**Pattern 3: Data Analysis Pipeline**
```python
from src.transformers.analyzers.schema_inferrer import SchemaInferrer
from src.transformers.analyzers.anomaly_detector import AnomalyDetector

pipeline = (Pipeline()
    .extract(CSVSource("data/sales_data.csv"))
    .transform(SchemaInferrer(detect_patterns=True))    # Infer schema
    .transform(AnomalyDetector(method="isolation_forest"))  # Detect outliers
    .load(SQLiteLoader("output/analyzed_data.db", table="sales"))
    .run())
```

**Pattern 4: Aggregation Pipeline**
```python
from src.transformers.enrichers.aggregator import Aggregator

pipeline = (Pipeline()
    .extract(CSVSource("data/transactions.csv"))
    .transform(Aggregator(
        group_by=["customer_id"],
        aggregations={
            "total_sales": ("amount", "sum"),
            "avg_amount": ("amount", "avg"),
            "transaction_count": ("id", "count")
        }
    ))
    .load(CSVLoader("output/customer_summary.csv"))
    .run())
```

### Troubleshooting

**Issue: ModuleNotFoundError**
```bash
# Solution: Ensure venv is activated and dependencies installed
source venv/bin/activate
pip install -r requirements.txt
```

**Issue: File not found**
```python
# Solution: Use absolute path
CSVSource("/Users/yourname/full/path/to/file.csv")
# Or verify relative path from project root
```

**Issue: Encoding errors**
```python
# Solution: Specify correct encoding
CSVSource("data.csv", encoding="latin-1")  # or "cp1252", "iso-8859-1"
```

**Issue: Wrong delimiter**
```python
# Solution: Check your CSV delimiter
CSVSource("data.csv", delimiter=";")  # For semicolon-separated
CSVSource("data.csv", delimiter="\t")  # For tab-separated
```

**Issue: Database locked (SQLite)**
```bash
# Solution: Close any open database connections
# Or use a different output file name
```

### Next Steps

1. **Explore AI/ML Features**: Try SchemaInferrer, AnomalyDetector, Deduplicator
2. **Add More Transformations**: Chain multiple transformers for complex workflows
3. **Performance Tuning**: Use AutoTuner to optimize batch sizes
4. **Create Custom Pipelines**: Combine different sources, transformers, and destinations

---

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/unit/test_csv_source.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/
```

### Adding a New Adapter

1. Create adapter class implementing `SourceAdapter` or `DestinationAdapter`
2. Add to `src/adapters/sources/` or `src/adapters/destinations/`
3. Write unit tests
4. Update documentation

Example:
```python
# src/adapters/sources/my_source.py
from src.adapters.base import SourceAdapter

class MySourceAdapter(SourceAdapter):
    def connect(self): ...
    def read(self): ...
    def get_schema(self): ...
    # Implement other required methods
```

## Deployment

### Local Development

```bash
# Run directly with Python
python examples/simple_csv_pipeline.py
```

### Docker Compose Deployment

The ETL framework is designed for easy deployment using Docker Compose alongside other services (RAG API, databases, etc.).

#### Quick Start

```bash
# Start all services (production mode)
docker-compose up -d

# Start with development mode (hot reload enabled)
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# View logs
docker-compose logs -f etl-api

# Stop all services
docker-compose down
```

#### Service Endpoints

Once running, the following services are available:

- **ETL API**: http://localhost:8002
- **API Documentation**: http://localhost:8002/docs
- **PostgreSQL**: localhost:5432

#### Environment Configuration

Create a `.env` file in the project root:

```bash
# PostgreSQL
DATABASE_URL=postgresql://etl_user:etl_dev_pass@postgres:5432/etl
POSTGRES_PASSWORD=etl_dev_pass

# AWS (for S3 source/destination - optional)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Performance
DEFAULT_BATCH_SIZE=1000
MAX_WORKERS=4
```

#### Health Checks

Verify all services are running:

```bash
# Check service status
docker-compose ps

# Test ETL API health
curl http://localhost:8002/health

# Test PostgreSQL connection
docker-compose exec postgres psql -U etl_user -d etl -c "\dt"
```

#### API-Based Pipeline Execution

The API provides endpoints for running ETL pipelines:

```bash
# Run a pipeline via API
curl -X POST "http://localhost:8002/pipelines/run" \
  -H "Content-Type: application/json" \
  -d '{
    "source": {"type": "csv", "path": "/data/input.csv"},
    "transforms": [{"type": "null_remover", "strategy": "drop"}],
    "destination": {"type": "postgres", "table": "processed_data"}
  }'

# Check pipeline status
curl http://localhost:8002/pipelines/status

# List available adapters
curl http://localhost:8002/adapters
```

#### Volume Mounts

The Docker Compose setup includes:

- `./ai-etl-framework/src:/app/src` - Source code (development mode only)
- `./data:/data` - Mount your local data directory
- `postgres-data:/var/lib/postgresql/data` - Persistent PostgreSQL data

#### Standalone Docker (Alternative)

For simple use cases without the full compose stack:

```bash
# Build image
docker build -t ai-etl-framework .

# Run container
docker run -v $(pwd)/data:/data ai-etl-framework python examples/simple_csv_pipeline.py
```

### Cloud Deployment

The Docker Compose setup can be deployed on any cloud platform:

**AWS EC2:**
```bash
# On EC2 instance
docker-compose up -d
```

**GCP Compute Engine:**
```bash
# On GCP VM
docker-compose up -d
```

**Azure VMs:**
```bash
# On Azure VM
docker-compose up -d
```

**Kubernetes (Advanced):**
```bash
# Convert docker-compose to k8s
kompose convert

# Deploy to k8s cluster
kubectl apply -f .
```

## Roadmap

### Phase 1: Foundation ✅ COMPLETE
- ✅ Project structure and documentation
- ✅ Base adapter interfaces
- ✅ 3 source adapters (CSV, JSON, PostgreSQL)
- ✅ 2 database destination adapters (SQLite, PostgreSQL)
- ✅ 2 basic transformers (NullRemover, QualityScorer)
- ✅ Pipeline orchestrator

### Phase 2: AI/ML Enhancement ✅ COMPLETE
- ✅ Schema inference (SchemaInferrer with pattern detection)
- ✅ Quality scoring (QualityScorer)
- ✅ Anomaly detection (AnomalyDetector with 4 methods)
- ✅ Deduplication (exact and fuzzy matching)
- ✅ Aggregation (GroupBy with 10+ functions)
- ✅ Auto-tuning (AutoTuner for performance optimization)
- ✅ 8 working examples demonstrating AI/ML features

### Phase 2.5: File-Based Destinations ✅ COMPLETE
- ✅ CSVLoader - Write to CSV files with compression
- ✅ JSONLoader - Write to JSON/JSONL files
- ✅ Format conversion capabilities (CSV ↔ JSON ↔ JSONL)
- ✅ 3 additional examples for file-to-file pipelines
- ✅ **Total: 7 adapters, 11 examples**

### Phase 3: Advanced Adapters 📋 PLANNED
- [ ] S3 source/destination
- [ ] Parquet loader
- [ ] Text source
- [ ] MySQL adapters

### Phase 4: Cloud Deployment (Months 4-5)
- [ ] Docker containerization
- [ ] Terraform for AWS
- [ ] Lambda/ECS deployment
- [ ] CI/CD pipeline

### Phase 5: Streaming Support (Months 6-9)
- [ ] Kafka integration
- [ ] Real-time processing
- [ ] Exactly-once semantics

## Contributing

We welcome contributions! Please see our contributing guidelines.

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Documentation

- [Architecture Documentation](docs/ARCHITECTURE.md)
- [Technical Specification](docs/TECHNICAL_SPEC.md)
- [Medical Claims Pipeline Guide](docs/MEDICAL_CLAIMS_PIPELINE.md) - Complete guide for processing medical claims data
- [Implementation Guide](docs/IMPLEMENTATION_GUIDE.md) (Coming soon)

## Support

- GitHub Issues: Report bugs or request features
- Discussions: Ask questions and share ideas

## Authors

AI-ETL Framework Team

---

**Status**: Phase 2 & 2.5 Complete | 7 Adapters, 11 Examples | Ready for Phase 3
**Version**: 0.2.5
**Last Updated**: 2025-10-11
